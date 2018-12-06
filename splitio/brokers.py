"""A module for Split.io SDK Brokers"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import abc
import logging
import os
import os.path
import random
import re
import threading
import time
from future.utils import raise_from


from splitio.api import SdkApi
from splitio.exceptions import TimeoutException
from splitio.metrics import Metrics, AsyncMetrics, ApiMetrics, \
    CacheBasedMetrics
from splitio.impressions import TreatmentLog, AsyncTreatmentLog, \
    SelfUpdatingTreatmentLog, CacheBasedTreatmentLog
from splitio.redis_support import RedisSplitCache, RedisImpressionsCache, \
    RedisMetricsCache, get_redis, RedisEventsCache
from splitio.splits import SelfRefreshingSplitFetcher, SplitParser, \
    ApiSplitChangeFetcher, JSONFileSplitFetcher, InMemorySplitFetcher, \
    AllKeysSplit, CacheBasedSplitFetcher
from splitio.segments import ApiSegmentChangeFetcher, \
    SelfRefreshingSegmentFetcher, JSONFileSegmentFetcher
from splitio.config import DEFAULT_CONFIG, MAX_INTERVAL, parse_config_file
from splitio.uwsgi import UWSGISplitCache, UWSGIImpressionsCache, \
    UWSGIMetricsCache, UWSGIEventsCache, get_uwsgi
from splitio.tasks import EventsSyncTask
from splitio.events import InMemoryEventStorage


def randomize_interval(value):
    """
    Generates a function that return a random integer in the [value/2,value)
    interval. The minimum generated value is 5.
    :param value: The maximum value for the random interval
    :type value: int
    :return: A function that returns a random integer in the interval.
    :rtype: function
    """
    def random_interval():
        return max(5, random.randint(value // 2, value))

    return random_interval


class BaseBroker(object):
    """
    Abstract class defining the interface that concrete brokers must implement,
    and including methods that use that interface to retrieve splits, log
    impressions and submit metrics.
    """

    __metaclass__ = abc.ABCMeta

    def __init__(self, config=None):
        """
        Class constructor, only sets up the logger
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._destroyed = False
        self._config = config

    def fetch_feature(self, name):
        """
        Fetch a feature
        :return: The split associated with that feature
        :rtype: Split
        """
        return self.get_split_fetcher().fetch(name)

    def get_change_number(self):
        """
        Returns the latest change number
        """
        return self.get_split_fetcher().change_number

    def log_impression(self, impression):
        """
        Logs an impression after a get_treatment call
        :return: The treatment log implementation.
        :rtype: TreatmentLog
        """
        return self.get_impression_log().log(impression)

    def log_operation_time(self, operation, elapsed):
        """Get the metrics implementation.
        :return: The metrics implementation.
        :rtype: Metrics
        """
        return self.get_metrics_handler().time(operation, elapsed)

    def log_event(self, event):
        """
        Logs an event after a .track() call
        """
        return self.get_events_log().log(event)

    @abc.abstractmethod
    def get_split_fetcher(self):
        pass

    @abc.abstractmethod
    def get_metrics_handler(self):
        pass

    @abc.abstractmethod
    def get_impression_log(self):
        pass

    @abc.abstractmethod
    def get_events_log(self):
        pass

    @abc.abstractmethod
    def destroy(self):
        pass

class JSONFileBroker(BaseBroker):
    def __init__(self, config, segment_changes_file_name, split_changes_file_name):
        """
        A Broker implementation that uses responses from the segmentChanges and
        splitChanges resources to provide access to splits. It is intended to be
        used on integration tests only.

        :param segment_changes_file_name: The name of the file with the
            segmentChanges response
        :type segment_changes_file_name: str
        :param split_changes_file_name: The name of the file with the
            splitChanges response
        :type split_changes_file_name: str
        """
        super(JSONFileBroker, self).__init__(config)
        self._segment_changes_file_name = segment_changes_file_name
        self._split_changes_file_name = split_changes_file_name
        self._split_fetcher = self._build_split_fetcher()
        self._treatment_log = TreatmentLog() # Does nothing on ._log()
        self._metrics = Metrics() # Does nothing on .count(), .time(), .gauge()

    def _build_split_fetcher(self):
        """
        Build the json backed split fetcher
        :return: The json backed split fetcher
        :rtype: JSONFileSplitFetcher
        """
        segment_fetcher = JSONFileSegmentFetcher(self._segment_changes_file_name)
        split_parser = SplitParser(segment_fetcher)
        split_fetcher = JSONFileSplitFetcher(
            self._split_changes_file_name,
            split_parser
        )

        return split_fetcher

    def get_split_fetcher(self):
        """
        Get the split fetcher implementation for the broker.
        :return: The split fetcher
        :rtype: SplitFetcher
        """
        return self._split_fetcher

    def get_metrics_handler(self):
        """
        """
        return self._metrics

    def get_impression_log(self):
        """
        """
        return self._treatment_log

    def destroy(self):
        self._destroyed = True
        self._split_fetcher.destroy()
        self._treatment_log.destroy()
        self._metrics.destroy()

    def get_events_log(self):
        return None

class SelfRefreshingBroker(BaseBroker):
    def __init__(self, api_key, config=None, sdk_api_base_url=None,
                 events_api_base_url=None, impression_listener=None):
        """
        A Broker implementation that refreshes itself at regular intervals.
        The config parameter is a dictionary that allows you to control the
        behaviour of the broker.
        The following configuration values are supported:
        * connectionTimeout: The TCP connection timeout (Default: 1500ms)
        * readTimeout: The HTTP read timeout (Default: 1500ms)
        * featuresRefreshRate: The refresh rate for features (Default: 30s)
        * segmentsRefreshRate: The refresh rate for segments (Default: 60s)
        * metricsRefreshRate: The refresh rate for metrics (Default: 60s)
        * impressionsRefreshRate: The refresh rate for impressions
            (Default: 60s)
        * randomizeIntervals: Whether to randomize the refres intervals
            (Default: False)
        * ready: How long to wait (in seconds) for the broker to be initialized.
            0 to return immediately without waiting. (Default: 0s)

        :param api_key: The API key provided by Split.io
        :type api_key: str
        :param config: The configuration dictionary
        :type config: dict
        :param sdk_api_base_url: An override for the default API base URL.
        :type sdk_api_base_url: str
        :param events_api_base_url: An override for the default events base URL.
        :type events_api_base_url: str
        """
        super(SelfRefreshingBroker, self).__init__()
        self._api_key = api_key
        self._sdk_api_base_url = sdk_api_base_url
        self._events_api_base_url = events_api_base_url
        self._impression_listener = impression_listener

        self._init_config(config)
        self._sdk_api = self._build_sdk_api()
        self._split_fetcher = self._build_split_fetcher()
        self._treatment_log = self._build_treatment_log()
        self._metrics = self._build_metrics()
        self._start()

        self._events_storage = InMemoryEventStorage(self._config['eventsQueueSize'])
        self._events_task = EventsSyncTask(
            self._sdk_api,
            self._events_storage,
            self._config['eventsPushRate'],
            self._config['eventsQueueSize'],
        )
        self._events_storage.set_queue_full_hook(lambda: self._events_task.flush())
        self._events_task.start()

    def _init_config(self, config=None):
        self._config = dict(DEFAULT_CONFIG)
        if config is not None:
            self._config.update(config)

        segment_fetcher_interval = min(
            MAX_INTERVAL,
            self._config['segmentsRefreshRate']
        )
        split_fetcher_interval = min(
            MAX_INTERVAL,
            self._config['featuresRefreshRate']
        )
        impressions_interval = min(
            MAX_INTERVAL,
            self._config['impressionsRefreshRate']
        )

        if self._config['randomizeIntervals']:
            self._segment_fetcher_interval = randomize_interval(segment_fetcher_interval)
            self._split_fetcher_interval = randomize_interval(split_fetcher_interval)
            self._impressions_interval = randomize_interval(impressions_interval)
        else:
            self._segment_fetcher_interval = segment_fetcher_interval
            self._split_fetcher_interval = split_fetcher_interval
            self._impressions_interval = impressions_interval

        self._metrics_max_time_between_calls = min(
            MAX_INTERVAL,
            self._config['metricsRefreshRate']
        )
        self._metrics_max_call_count = self._config['maxMetricsCallsBeforeFlush']

        self._connection_timeout = self._config['connectionTimeout']
        self._read_timeout = self._config['readTimeout']
        self._max_impressions_log_size = self._config['maxImpressionsLogSize']
        self._ready = self._config['ready']

    def _build_sdk_api(self):
        return SdkApi(
            self._api_key,
            sdk_api_base_url=self._sdk_api_base_url,
            events_api_base_url=self._events_api_base_url,
            connect_timeout=self._connection_timeout,
            read_timeout=self._read_timeout
        )

    def _build_split_fetcher(self):
        """
        Build the self refreshing split fetcher
        :return: The self refreshing split fetcher
        :rtype: SelfRefreshingSplitFetcher
        """
        segment_change_fetcher = ApiSegmentChangeFetcher(self._sdk_api)
        segment_fetcher = SelfRefreshingSegmentFetcher(
            segment_change_fetcher,
            interval=self._segment_fetcher_interval
        )
        split_change_fetcher = ApiSplitChangeFetcher(self._sdk_api)
        split_parser = SplitParser(segment_fetcher)
        split_fetcher = SelfRefreshingSplitFetcher(
            split_change_fetcher,
            split_parser,
            interval=self._split_fetcher_interval
        )
        return split_fetcher

    def _build_treatment_log(self):
        """Build the treatment log implementation.
        :return: The treatment log implementation.
        :rtype: TreatmentLog
        """
        self_updating_treatment_log = SelfUpdatingTreatmentLog(
            self._sdk_api,
            max_count=self._max_impressions_log_size,
            interval=self._impressions_interval,
        )
        return AsyncTreatmentLog(self_updating_treatment_log)

    def _build_metrics(self):
        """Build the metrics implementation.
        :return: The metrics implementation.
        :rtype: Metrics
        """
        api_metrics = ApiMetrics(
            self._sdk_api,
            max_call_count=self._metrics_max_call_count,
            max_time_between_calls=self._metrics_max_time_between_calls
        )
        return AsyncMetrics(api_metrics)

    def _start(self):
        self._treatment_log.delegate.start()

        if self._ready > 0:
            event = threading.Event()

            thread = threading.Thread(target=self._fetch_splits, args=(event,))
            thread.daemon = True
            thread.start()

            flag_set = event.wait(self._ready / 1000)
            if not flag_set:
                self._logger.info(
                    'Timeout reached. Returning broker in partial state.'
                )
                raise TimeoutException()
        else:
            self._split_fetcher.start()

    def _fetch_splits(self, event):
        """
        Fetches the split and segment information blocking until it is done.
        """
        self._split_fetcher.refresh_splits(block_until_ready=True)
        self._split_fetcher.start(delayed_update=True)
        event.set()

    def get_split_fetcher(self):
        """
        Get the split fetcher implementation for the broker.
        :return: The split fetcher
        :rtype: SplitFetcher
        """
        return self._split_fetcher

    def get_metrics_handler(self):
        return self._metrics

    def get_impression_log(self):
        return self._treatment_log

    def get_events_log(self):
        return self._events_storage

    def destroy(self):
        self._destroyed = True
        self._split_fetcher.destroy()
        self._treatment_log.destroy()
        self._metrics.destroy()
        self._events_task.stop()


class LocalhostBroker(BaseBroker):
    _COMMENT_LINE_RE = re.compile('^#.*$')
    _DEFINITION_LINE_RE = re.compile(
        '^(?<![^#])(?P<feature>[\w_-]+)\s+(?P<treatment>[\w_-]+)$'
    )

    class LocalhostEventStorage(object):
        def log(self, event):
            pass

    def __init__(self, config, split_definition_file_name=None, auto_refresh_period=2):
        """
        A broker implementation that builds its configuration from a split
        definition file. By default the definition is taken from $HOME/.split
        but the file name can be supplied as argument as well.
        :param split_definition_file_name: Name of definition file (Optional)
        :type split_definition_file_name: str
        :param auto_refresh_period: Number of seconds between split refresh calls
        :type auto_refresh_period: int
        """
        super(LocalhostBroker, self).__init__(config)

        if split_definition_file_name is None:
            self._split_definition_file_name = os.path.join(
                os.path.expanduser('~'), '.split'
            )
        else:
            self._split_definition_file_name = split_definition_file_name

        self._split_refresh_period = auto_refresh_period

        self._split_fetcher = self._build_split_fetcher()
        self._refresh_thread = threading.Thread(target=self.refresh_splits)
        self._refresh_thread.daemon = True
        self._refresh_thread.start()

        self._treatment_log = TreatmentLog()
        self._metrics = Metrics()
        self._event_storage = LocalhostBroker.LocalhostEventStorage()

    def _build_split_fetcher(self):
        """
        Build the in memory split fetcher using the local environment split
        definition file
        :return: The in memory split fetcher
        :rtype: InMemorySplitFetcher
        """
        splits = self._parse_split_file(self._split_definition_file_name)
        split_fetcher = InMemorySplitFetcher(splits=splits)

        return split_fetcher

    def _parse_split_file(self, file_name):
        splits = dict()

        try:
            with open(file_name) as f:
                for line in f:
                    if line.strip() == '':
                        continue

                    comment_match = LocalhostBroker._COMMENT_LINE_RE.match(line)
                    if comment_match:
                        continue

                    definition_match = LocalhostBroker._DEFINITION_LINE_RE.match(line)
                    if definition_match:
                        splits[definition_match.group('feature')] = AllKeysSplit(
                            definition_match.group('feature'),
                            definition_match.group('treatment')
                        )
                        continue

                    self._logger.warning(
                        'Invalid line on localhost environment split '
                        'definition. Line = %s',
                        line
                    )
            return splits
        except IOError as e:
            raise_from(ValueError(
                'There was a problem with '
                'the splits definition file "{}"'.format(file_name)),
                e
            )

    def get_split_fetcher(self):
        """
        Get the split fetcher implementation for the broker.
        :return: The split fetcher
        :rtype: SplitFetcher
        """
        return self._split_fetcher

    def get_metrics_handler(self):
        """
        """
        return self._metrics

    def get_impression_log(self):
        """
        """
        return self._treatment_log

    def get_events_log(self):
        return self._event_storage

    def refresh_splits(self):
        while not self._destroyed:
            time.sleep(self._split_refresh_period)
            if not self._destroyed: # DO NOT REMOVE
                                    # This check is used in case the client was
                                    # destroyed while the thread was sleeping
                                    # and the file was closed, in order to
                                    # prevent an exception.
                self._split_fetcher = self._build_split_fetcher()

    def destroy(self):
        self._destroyed = True
        self._split_fetcher.destroy()
        self._treatment_log.destroy()
        self._metrics.destroy()


class RedisBroker(BaseBroker):
    def __init__(self, redis, config):
        """A Broker implementation that uses Redis as its backend.
        :param redis: A redis broker
        :type redis: StrctRedis"""
        super(RedisBroker, self).__init__(config)

        split_cache = RedisSplitCache(redis)
        split_fetcher = CacheBasedSplitFetcher(split_cache)

        impressions_cache = RedisImpressionsCache(redis)
        treatment_log = CacheBasedTreatmentLog(impressions_cache)

        metrics_cache = RedisMetricsCache(redis)
        delegate_metrics = CacheBasedMetrics(metrics_cache)
        metrics = AsyncMetrics(delegate_metrics)

        self._split_fetcher = split_fetcher
        self._treatment_log = treatment_log
        self._metrics = metrics

        self._event_storage = RedisEventsCache(redis)

    def get_split_fetcher(self):
        """
        Get the split fetcher implementation for the broker.
        :return: The split fetcher
        :rtype: SplitFetcher
        """
        return self._split_fetcher

    def get_metrics_handler(self):
        """
        """
        return self._metrics

    def get_impression_log(self):
        """
        """
        return self._treatment_log

    def get_events_log(self):
        return self._event_storage

    def get_metrics(self):
        """
        Get the metrics implementation for the broker.
        :return: The metrics
        :rtype: Metrics
        """
        return self._metrics

    def destroy(self):
        self._destroyed = True
        self._split_fetcher.destroy()
        self._treatment_log.destroy()
        self._metrics.destroy()


class UWSGIBroker(BaseBroker):
    def __init__(self, uwsgi, config=None):
        """
        A Broker implementation that consumes data from uwsgi cache framework.
        The config parameter is a dictionary that allows you to control the
        behaviour of the broker.

        :param config: The configuration dictionary
        :type config: dict
        """
        super(UWSGIBroker, self).__init__(config)

        split_cache = UWSGISplitCache(uwsgi)
        split_fetcher = CacheBasedSplitFetcher(split_cache)

        impressions_cache = UWSGIImpressionsCache(uwsgi)
        delegate_treatment_log = CacheBasedTreatmentLog(impressions_cache)
        treatment_log = AsyncTreatmentLog(delegate_treatment_log)

        metrics_cache = UWSGIMetricsCache(uwsgi)
        delegate_metrics = CacheBasedMetrics(metrics_cache)
        metrics = AsyncMetrics(delegate_metrics)

        self._event_log = UWSGIEventsCache(uwsgi, events_queue_size=config['eventsQueueSize'])

        self._split_fetcher = split_fetcher
        self._treatment_log = treatment_log
        self._metrics = metrics


    def get_split_fetcher(self):
        """
        Get the split fetcher implementation for the broker.
        :return: The split fetcher
        :rtype: SplitFetcher
        """
        return self._split_fetcher

    def get_metrics_handler(self):
        """
        """
        return self._metrics

    def get_impression_log(self):
        """
        """
        return self._treatment_log

    def get_events_log(self):
        return self._event_log

    def destroy(self):
        self._destroyed = True
        self._split_fetcher.destroy()
        self._treatment_log.destroy()
        self._metrics.destroy()


def _init_config(api_key, **kwargs):
    config = dict(DEFAULT_CONFIG)
    user_cfg = kwargs.pop('config', dict())
    config.update(user_cfg)
    sdk_api_base_url = kwargs.pop('sdk_api_base_url', None)
    events_api_base_url = kwargs.pop('events_api_base_url', None)

    if 'config_file' in kwargs:
        file_config = parse_config_file(kwargs['config_file'])

        file_api_key = file_config.pop('apiKey', None)
        file_sdk_api_base_url = file_config.pop('sdkApiBaseUrl', None)
        file_events_api_base_url = file_config.pop('eventsApiBaseUrl', None)

        api_key = api_key or file_api_key
        sdk_api_base_url = sdk_api_base_url or file_sdk_api_base_url
        events_api_base_url = events_api_base_url or file_events_api_base_url

        file_config.update(config)
        config = file_config

    return api_key, config, sdk_api_base_url, events_api_base_url


def get_self_refreshing_broker(api_key, **kwargs):
    """
    Builds a Split Broker that refreshes itself at regular intervals.

    The config_file parameter is the name of a file that contains the broker
    configuration. Here's an example of a config file:

    {
      "apiKey": "some-api-key",
      "sdkApiBaseUrl": "https://sdk.split.io/api",
      "eventsApiBaseUrl": "https://events.split.io/api",
      "connectionTimeout": 1500,
      "readTimeout": 1500,
      "featuresRefreshRate": 30,
      "segmentsRefreshRate": 60,
      "metricsRefreshRate": 60,
      "impressionsRefreshRate": 60,
      "randomizeIntervals": False,
      "maxImpressionsLogSize": -1,
      "maxMetricsCallsBeforeFlush": 1000,
      "ready": 0
    }

    The config parameter is a dictionary that allows you to control the
    behaviour of the broker. The following configuration values are supported:

    * connectionTimeout: The TCP connection timeout (Default: 1500ms)
    * readTimeout: The HTTP read timeout (Default: 1500ms)
    * featuresRefreshRate: The refresh rate for features (Default: 30s)
    * segmentsRefreshRate: The refresh rate for segments (Default: 60s)
    * metricsRefreshRate: The refresh rate for metrics (Default: 60s)
    * impressionsRefreshRate: The refresh rate for impressions (Default: 60s)
    * randomizeIntervals: Whether to randomize the refres intervals
        (Default: False)
    * ready: How long to wait (in seconds) for the broker to be initialized.
        0 to return immediately without waiting. (Default: 0s)

    If the api_key argument is 'localhost' a localhost environment broker is
    built based on the contents of a .split file in the user's home directory.
    The definition file has the following syntax:

        file: (comment | split_line)+
        comment : '#' string*\n
        split_line : feature_name ' ' treatment\n
        feature_name : string
        treatment : string

    It is possible to change the location of the split file by using the
    split_definition_file_name argument.

    :param api_key: The API key provided by Split.io
    :type api_key: str
    :param config_file: Filename of the config file
    :type config_file: str
    :param config: The configuration dictionary
    :type config: dict
    :param sdk_api_base_url: An override for the default API base URL.
    :type sdk_api_base_url: str
    :param events_api_base_url: An override for the default events base URL.
    :type events_api_base_url: str
    :param split_definition_file_name: Name of the definition file (Optional)
    :type split_definition_file_name: str
    """
    api_key, config, sdk_api_base_url, events_api_base_url = _init_config(
        api_key,
        **kwargs
    )

    if api_key == 'localhost':
        return LocalhostBroker(config, **kwargs)

    return SelfRefreshingBroker(
        api_key,
        config=config,
        sdk_api_base_url=sdk_api_base_url,
        events_api_base_url=events_api_base_url,
        impression_listener=kwargs.get('impression_listener')
    )


def get_redis_broker(api_key, **kwargs):
    """
    Builds a Split Broker that that gets its information from a Redis instance.
    It also writes impressions and metrics to the same instance.

    In order for this work properly, you need to periodically call the
    update_splits and update_segments scripts.
    You also need to run the send_impressions and send_metrics scripts in order
    to push the impressions and metrics onto the Split.io backend.

    The config_file parameter is the name of a file that contains the broker
    configuration. Here's an example of a config file:

    {
      "apiKey": "some-api-key",
      "sdkApiBaseUrl": "https://sdk.split.io/api",
      "eventsApiBaseUrl": "https://events.split.io/api",
      "redisFactory": 'some.redis.factory',
      "redisHost": "localhost",
      "redisPort": 6879,
      "redisDb": 0,
    }

    If the redisFactory entry is present, it is used to build the redis broker
    instance, otherwise the values of redisHost, redisPort and redisDb are used.

    If the api_key argument is 'localhost' a localhost environment broker is
    built based on the contents of a .split file in the user's home directory.
    The definition file has the following syntax:

        file: (comment | split_line)+
        comment : '#' string*\n
        split_line : feature_name ' ' treatment\n
        feature_name : string
        treatment : string

    It is possible to change the location of the split file by using the
    split_definition_file_name argument.

    :param api_key: The API key provided by Split.io
    :type api_key: str
    :param config_file: Filename of the config file
    :type config_file: str
    :param sdk_api_base_url: An override for the default API base URL.
    :type sdk_api_base_url: str
    :param events_api_base_url: An override for the default events base URL.
    :type events_api_base_url: str
    :param split_definition_file_name: Name of the definition file (Optional)
    :type split_definition_file_name: str
    """
    api_key, config, _, _ = _init_config(api_key, **kwargs)

    if api_key == 'localhost':
        return LocalhostBroker(config, **kwargs)

    redis = get_redis(config)

    redis_broker = RedisBroker(redis, config)

    return redis_broker


def get_uwsgi_broker(api_key, **kwargs):
    """
    Builds a Split Broker that that gets its information from a uWSGI cache
    instance. It also writes impressions and metrics to the same instance.

    In order for this work properly, you need to periodically call the spooler
    uwsgi_update_splits and uwsgi_update_segments scripts.
    You also need to run the uwsgi_report_impressions and uwsgi_report_metrics
    scripts in order to push the impressions and metrics onto the Split.io
    backend.

    The config_file parameter is the name of a file that contains the broker
    configuration. Here's an example of a config file:

    {
      "apiKey": "some-api-key",
      "sdkApiBaseUrl": "https://sdk.split.io/api",
      "eventsApiBaseUrl": "https://events.split.io/api",
      "featuresRefreshRate": 30,
      "segmentsRefreshRate": 60,
      "metricsRefreshRate": 60,
      "impressionsRefreshRate": 60
    }

    If the api_key argument is 'localhost' a localhost environment broker is
    built based on the contents of a .split file in the user's home directory.
    The definition file has the following syntax:

        file: (comment | split_line)+
        comment : '#' string*\n
        split_line : feature_name ' ' treatment\n
        feature_name : string
        treatment : string

    It is possible to change the location of the split file by using the
    split_definition_file_name argument.

    :param api_key: The API key provided by Split.io
    :type api_key: str
    :param config_file: Filename of the config file
    :type config_file: str
    :param sdk_api_base_url: An override for the default API base URL.
    :type sdk_api_base_url: str
    :param events_api_base_url: An override for the default events base URL.
    :type events_api_base_url: str
    :param split_definition_file_name: Name of the definition file (Optional)
    :type split_definition_file_name: str
    """
    api_key, config, _, _ = _init_config(api_key, **kwargs)

    if api_key == 'localhost':
        return LocalhostBroker(config, **kwargs)

    uwsgi = get_uwsgi()
    uwsgi_broker = UWSGIBroker(uwsgi, config)

    return uwsgi_broker
