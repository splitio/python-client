"""A module for Split.io SDK API clients"""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import time

from os.path import expanduser, join
from random import randint
from re import compile
from threading import Event, Thread

from future.utils import raise_from

from splitio.api import SdkApi
from splitio.exceptions import TimeoutException
from splitio.metrics import (Metrics, AsyncMetrics, ApiMetrics, CacheBasedMetrics,
                             SDK_GET_TREATMENT)
from splitio.impressions import (TreatmentLog, AsyncTreatmentLog, SelfUpdatingTreatmentLog,
                                 CacheBasedTreatmentLog, Impression, Label)
from splitio.redis_support import (RedisSplitCache, RedisImpressionsCache, RedisMetricsCache,
                                       get_redis)
from splitio.splitters import Splitter
from splitio.splits import (SelfRefreshingSplitFetcher, SplitParser, ApiSplitChangeFetcher,
                            JSONFileSplitFetcher, InMemorySplitFetcher, AllKeysSplit,
                            CacheBasedSplitFetcher, ConditionType)
from splitio.segments import (ApiSegmentChangeFetcher, SelfRefreshingSegmentFetcher,
                              JSONFileSegmentFetcher)
from splitio.config import DEFAULT_CONFIG, MAX_INTERVAL, parse_config_file
from splitio.treatments import CONTROL

from splitio.uwsgi import (UWSGISplitCache, UWSGIImpressionsCache, UWSGIMetricsCache, get_uwsgi)


class Key(object):
    def __init__(self, matching_key, bucketing_key):
        """Bucketing Key implementation"""
        self.matching_key = matching_key
        self.bucketing_key = bucketing_key

class Client(object):
    def __init__(self, labels_enabled=True):
        """Basic interface of a Client. Specific implementations need to override the
        get_split_fetcher method (and optionally the get_splitter method).
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._splitter = Splitter()
        self._labels_enabled = labels_enabled

    def get_split_fetcher(self):  # pragma: no cover
        """Get the split fetcher implementation. Subclasses need to override this method.
        :return: The split fetcher implementation.
        :rtype: SplitFetcher
        """
        raise NotImplementedError()

    def get_splitter(self):
        """Get the splitter implementation.
        :return: The splitter implementation.
        :rtype: Splitter
        """
        return self._splitter

    def get_treatment_log(self):  # pragma: no cover
        """Get the treatment log implementation.
        :return: The treatment log implementation.
        :rtype: TreatmentLog
        """
        raise NotImplementedError()

    def get_metrics(self):  # pragma: no cover
        """Get the metrics implementation.
        :return: The metrics implementation.
        :rtype: Metrics
        """
        raise NotImplementedError()

    def get_treatment(self, key, feature, attributes=None):
        """
        Get the treatment for a feature and key, with an optional dictionary of attributes. This
        method never raises an exception. If there's a problem, the appropriate log message will
        be generated and the method will return the CONTROL treatment.
        :param key: The key for which to get the treatment
        :type key: str
        :param feature: The name of the feature for which to get the treatment
        :type feature: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: The treatment for the key and feature
        :rtype: str
        """
        if key is None or feature is None:
            return CONTROL

        start = int(round(time.time() * 1000))

        matching_key = None
        bucketing_key = None
        if isinstance(key, Key):
            matching_key = key.matching_key
            bucketing_key = key.bucketing_key
        else:
            matching_key = str(key)
            bucketing_key = None

        try:
            label = ''
            _treatment = CONTROL
            _change_number = -1

            #Fetching Split definition
            split = self.get_split_fetcher().fetch(feature)

            if split is None:
                self._logger.warning('Unknown or invalid feature: %s', feature)
                label = Label.SPLIT_NOT_FOUND
                _treatment = CONTROL
            else:
                _change_number = split.change_number
                if split.killed:
                    label = Label.KILLED
                    _treatment = split.default_treatment
                else:
                    treatment, label = self._get_treatment_for_split(split, matching_key, bucketing_key, attributes)
                    if treatment is None:
                        label = Label.NO_CONDITION_MATCHED
                        _treatment = split.default_treatment
                    else:
                        _treatment = treatment

            impression = self._build_impression(matching_key, feature, _treatment, label,
                                                _change_number, bucketing_key, start)
            self._record_stats(impression, start, SDK_GET_TREATMENT)
            return _treatment
        except:
            self._logger.exception('Exception caught getting treatment for feature')

            try:
                impression = self._build_impression(matching_key, feature, CONTROL, Label.EXCEPTION,
                                                    self.get_split_fetcher().change_number, bucketing_key, start)
                self._record_stats(impression, start, SDK_GET_TREATMENT)
            except:
                self._logger.exception('Exception reporting impression into get_treatment exception block')

            return CONTROL

    def _build_impression(self, matching_key, feature_name, treatment, label, change_number, bucketing_key, time):

        if not self._labels_enabled:
            label = None

        return Impression(matching_key=matching_key, feature_name=feature_name, treatment=treatment, label=label,
                                    change_number=change_number, bucketing_key=bucketing_key, time=time)

    def _record_stats(self, impression, start, operation):
        try:
            end = int(round(time.time() * 1000))
            self.get_treatment_log().log(impression)
            self.get_metrics().time(operation, end - start)
        except:
            self._logger.exception('Exception caught recording impressions and metrics')

    def _get_treatment_for_split(self, split, matching_key, bucketing_key, attributes=None):
        """
        Internal method to get the treatment for a given Split and optional attributes. This
        method might raise exceptions and should never be used directly.
        :param split: The split for which to get the treatment
        :type split: Split
        :param key: The key for which to get the treatment
        :type key: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: The treatment for the key and split
        :rtype: str
        """
        if bucketing_key is None:
            bucketing_key = matching_key

        roll_out = False
        for condition in split.conditions:
            if (not roll_out and
                    condition.condition_type == ConditionType.ROLLOUT):
                if split.traffic_allocation < 100:
                    bucket = self.get_splitter().get_bucket(
                        bucketing_key,
                        split.traffic_allocation_seed,
                        split.algo
                    )
                    if bucket >= split.traffic_allocation:
                        return split.default_treatment, Label.NOT_IN_SPLIT
                roll_out = True

            if condition.matcher.match(matching_key, attributes=attributes):
                return self.get_splitter().get_treatment(
                    bucketing_key,
                    split.seed,
                    condition.partitions,
                    split.algo
                ), condition.label

        # No condition matches
        return None, None


def randomize_interval(value):
    """
    Generates a function that return a random integer in the [value/2,value) interval. The minimum
    generated value is 5.
    :param value: The maximum value for the random interval
    :type value: int
    :return: A function that returns a random integer in the interval.
    :rtype: function
    """
    def random_interval():
        return max(5, randint(value // 2, value))

    return random_interval

class JSONFileClient(Client):
    def __init__(self, segment_changes_file_name, split_changes_file_name):
        """
        A Client implementation that uses responses from the segmentChanges and splitChanges
        resources to provide access to splits. It is intended to be used on integration
        tests only.

        :param segment_changes_file_name: The name of the file with the segmentChanges response
        :type segment_changes_file_name: str
        :param split_changes_file_name: The name of the file with the splitChanges response
        :type split_changes_file_name: str
        """
        super(JSONFileClient, self).__init__()
        self._segment_changes_file_name = segment_changes_file_name
        self._split_changes_file_name = split_changes_file_name
        self._split_fetcher = self._build_split_fetcher()
        self._treatment_log = TreatmentLog()
        self._metrics = Metrics()

    def _build_split_fetcher(self):
        """
        Build the json backed split fetcher
        :return: The json backed split fetcher
        :rtype: SelfRefreshingSplitFetcher
        """
        segment_fetcher = JSONFileSegmentFetcher(self._segment_changes_file_name)
        split_parser = SplitParser(segment_fetcher)
        split_fetcher = JSONFileSplitFetcher(self._split_changes_file_name, split_parser)

        return split_fetcher

    def get_split_fetcher(self):
        """
        Get the split fetcher implementation for the client.
        :return: The split fetcher
        :rtype: SplitFetcher
        """
        return self._split_fetcher

    def get_treatment_log(self):
        """Get the treatment log implementation.
        :return: The treatment log implementation.
        :rtype: TreatmentLog
        """
        return self._treatment_log

    def get_metrics(self):
        """Get the metrics implementation.
        :return: The metrics implementation.
        :rtype: Metrics
        """
        return self._metrics


class SelfRefreshingClient(Client):
    def __init__(self, api_key, config=None, sdk_api_base_url=None, events_api_base_url=None):
        """
        A Client implementation that refreshes itself at regular intervals. The config parameter
        is a dictionary that allows you to control the behaviour of the client. The following
        configuration values are supported:

        * connectionTimeout: The TCP connection timeout (Default: 1500ms)
        * readTimeout: The HTTP read timeout (Default: 1500ms)
        * featuresRefreshRate: The refresh rate for features (Default: 30s)
        * segmentsRefreshRate: The refresh rate for segments (Default: 60s)
        * metricsRefreshRate: The refresh rate for metrics (Default: 60s)
        * impressionsRefreshRate: The refresh rate for impressions (Default: 60s)
        * randomizeIntervals: Whether to randomize the refres intervals (Default: False)
        * ready: How long to wait (in seconds) for the client to be initialized. 0 to return
          immediately without waiting. (Default: 0s)

        :param api_key: The API key provided by Split.io
        :type api_key: str
        :param config: The configuration dictionary
        :type config: dict
        :param sdk_api_base_url: An override for the default API base URL.
        :type sdk_api_base_url: str
        :param events_api_base_url: An override for the default events base URL.
        :type events_api_base_url: str
        """
        labels_enabled = True
        if config is not None and 'labelsEnabled' in config:
            labels_enabled = config['labelsEnabled']

        super(SelfRefreshingClient, self).__init__(labels_enabled)

        self._api_key = api_key
        self._sdk_api_base_url = sdk_api_base_url
        self._events_api_base_url = events_api_base_url

        self._init_config(config)
        self._sdk_api = self._build_sdk_api()
        self._split_fetcher = self._build_split_fetcher()
        self._treatment_log = self._build_treatment_log()
        self._metrics = self._build_metrics()
        self._start()

    def _init_config(self, config=None):
        self._config = dict(DEFAULT_CONFIG)
        if config is not None:
            self._config.update(config)

        segment_fetcher_interval = min(MAX_INTERVAL, self._config['segmentsRefreshRate'])
        split_fetcher_interval = min(MAX_INTERVAL, self._config['featuresRefreshRate'])
        impressions_interval = min(MAX_INTERVAL, self._config['impressionsRefreshRate'])

        if self._config['randomizeIntervals']:
            self._segment_fetcher_interval = randomize_interval(segment_fetcher_interval)
            self._split_fetcher_interval = randomize_interval(split_fetcher_interval)
            self._impressions_interval = randomize_interval(impressions_interval)
        else:
            self._segment_fetcher_interval = segment_fetcher_interval
            self._split_fetcher_interval = split_fetcher_interval
            self._impressions_interval = impressions_interval

        self._metrics_max_time_between_calls = min(MAX_INTERVAL, self._config['metricsRefreshRate'])
        self._metrics_max_call_count = self._config['maxMetricsCallsBeforeFlush']

        self._connection_timeout = self._config['connectionTimeout']
        self._read_timeout = self._config['readTimeout']
        self._max_impressions_log_size = self._config['maxImpressionsLogSize']
        self._ready = self._config['ready']

    def _build_sdk_api(self):
        return SdkApi(self._api_key, sdk_api_base_url=self._sdk_api_base_url,
                      events_api_base_url=self._events_api_base_url,
                      connect_timeout=self._connection_timeout, read_timeout=self._read_timeout)

    def _build_split_fetcher(self):
        """
        Build the self refreshing split fetcher
        :return: The self refreshing split fetcher
        :rtype: SelfRefreshingSplitFetcher
        """
        segment_change_fetcher = ApiSegmentChangeFetcher(self._sdk_api)
        segment_fetcher = SelfRefreshingSegmentFetcher(segment_change_fetcher,
                                                       interval=self._segment_fetcher_interval)
        split_change_fetcher = ApiSplitChangeFetcher(self._sdk_api)
        split_parser = SplitParser(segment_fetcher)
        split_fetcher = SelfRefreshingSplitFetcher(split_change_fetcher, split_parser,
                                                   interval=self._split_fetcher_interval)
        return split_fetcher

    def _build_treatment_log(self):
        """Build the treatment log implementation.
        :return: The treatment log implementation.
        :rtype: TreatmentLog
        """
        self_updating_treatment_log = SelfUpdatingTreatmentLog(
            self._sdk_api, max_count=self._max_impressions_log_size,
            interval=self._impressions_interval)
        return AsyncTreatmentLog(self_updating_treatment_log)

    def _build_metrics(self):
        """Build the metrics implementation.
        :return: The metrics implementation.
        :rtype: Metrics
        """
        api_metrics = ApiMetrics(self._sdk_api, max_call_count=self._metrics_max_call_count,
                                 max_time_between_calls=self._metrics_max_time_between_calls)
        return AsyncMetrics(api_metrics)

    def _start(self):
        self._treatment_log.delegate.start()

        if self._ready > 0:
            event = Event()

            thread = Thread(target=self._fetch_splits, args=(event,))
            thread.daemon = True
            thread.start()

            flag_set = event.wait(self._ready / 1000)
            if not flag_set:
                self._logger.info('Timeout reached. Returning client in partial state.')
                raise TimeoutException()
        else:
            self._split_fetcher.start()

    def _fetch_splits(self, event):
        """Fetches the split and segment information blocking until it is done."""
        self._split_fetcher.refresh_splits(block_until_ready=True)
        self._split_fetcher.start(delayed_update=True)
        event.set()

    def get_split_fetcher(self):
        """
        Get the split fetcher implementation for the client.
        :return: The split fetcher
        :rtype: SplitFetcher
        """
        return self._split_fetcher

    def get_treatment_log(self):
        """Get the treatment log implementation.
        :return: The treatment log implementation.
        :rtype: TreatmentLog
        """
        return self._treatment_log

    def get_metrics(self):
        """Get the metrics implementation.
        :return: The metrics implementation.
        :rtype: Metrics
        """
        return self._metrics


class LocalhostEnvironmentClient(Client):
    _COMMENT_LINE_RE = compile('^#.*$')
    _DEFINITION_LINE_RE = compile('^(?<![^#])(?P<feature>[\w_]+)\s+(?P<treatment>[\w_]+)$')

    def __init__(self, split_definition_file_name=None):
        """
        A client implementation that builds its configuration from a split definition file. By
        default the definition is taken from $HOME/.split but the file name can be supplied as
        argument as well.

        The definition file has the following syntax:

        file: (comment | split_line)+
        comment : '#' string*\n
        split_line : feature_name ' ' treatment\n
        feature_name : string
        treatment : string

        :param split_definition_file_name: Name of the definition file (Optional)
        :type split_definition_file_name: str
        """
        super(LocalhostEnvironmentClient, self).__init__()

        if split_definition_file_name is None:
            self._split_definition_file_name = join(expanduser('~'), '.split')
        else:
            self._split_definition_file_name = split_definition_file_name
        self._split_fetcher = self._build_split_fetcher()
        self._treatment_log = TreatmentLog()
        self._metrics = Metrics()

    def get_split_fetcher(self):
        """
        Get the split fetcher implementation for the client.
        :return: The split fetcher
        :rtype: SplitFetcher
        """
        return self._split_fetcher

    def _build_split_fetcher(self):
        """
        Build the in memory split fetcher using the local environment split definition file
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

                    comment_match = LocalhostEnvironmentClient._COMMENT_LINE_RE.match(line)
                    if comment_match:
                        continue

                    definition_match = LocalhostEnvironmentClient._DEFINITION_LINE_RE.match(line)
                    if definition_match:
                        splits[definition_match.group('feature')] = AllKeysSplit(
                            definition_match.group('feature'), definition_match.group('treatment'))
                        continue

                    self._logger.warning('Invalid line on localhost environment split definition. '
                                         'line = %s', line)
            return splits
        except IOError as e:
            raise_from(ValueError('There was a problem with '
                                  'the splits definition file "{}"'.format(file_name)), e)

    def get_treatment_log(self):
        """Get the treatment log implementation.
        :return: The treatment log implementation.
        :rtype: TreatmentLog
        """
        return self._treatment_log

    def get_metrics(self):
        """Get the metrics implementation.
        :return: The metrics implementation.
        :rtype: Metrics
        """
        return self._metrics


class RedisClient(Client):
    def __init__(self, redis, labels_enabled=True):
        """A Client implementation that uses Redis as its backend.
        :param redis: A redis client
        :type redis: StrctRedis"""
        super(RedisClient, self).__init__(labels_enabled)

        split_cache = RedisSplitCache(redis)
        split_fetcher = CacheBasedSplitFetcher(split_cache)

        impressions_cache = RedisImpressionsCache(redis)
        delegate_treatment_log = CacheBasedTreatmentLog(impressions_cache)
        treatment_log = AsyncTreatmentLog(delegate_treatment_log)

        metrics_cache = RedisMetricsCache(redis)
        delegate_metrics = CacheBasedMetrics(metrics_cache)
        metrics = AsyncMetrics(delegate_metrics)

        self._split_fetcher = split_fetcher
        self._treatment_log = treatment_log
        self._metrics = metrics

    def get_split_fetcher(self):
        """
        Get the split fetcher implementation for the client.
        :return: The split fetcher
        :rtype: SplitFetcher
        """
        return self._split_fetcher

    def get_treatment_log(self):
        """
        Get the treatment log implementation for the client.
        :return: The treatment log
        :rtype: TreatmentLog
        """
        return self._treatment_log

    def get_metrics(self):
        """
        Get the metrics implementation for the client.
        :return: The metrics
        :rtype: Metrics
        """
        return self._metrics

class UWSGIClient(Client):
    def __init__(self, uwsgi, config=None):
        """
        A Client implementation that consumes data from uwsgi cache framework. The config parameter
        is a dictionary that allows you to control the behaviour of the client.

        :param config: The configuration dictionary
        :type config: dict
        """
        labels_enabled = True
        if config is not None and 'labelsEnabled' in config:
            labels_enabled = config['labelsEnabled']

        super(UWSGIClient, self).__init__(labels_enabled)

        split_cache = UWSGISplitCache(uwsgi)
        split_fetcher = CacheBasedSplitFetcher(split_cache)

        impressions_cache = UWSGIImpressionsCache(uwsgi)
        delegate_treatment_log = CacheBasedTreatmentLog(impressions_cache)
        treatment_log = AsyncTreatmentLog(delegate_treatment_log)

        metrics_cache = UWSGIMetricsCache(uwsgi)
        delegate_metrics = CacheBasedMetrics(metrics_cache)
        metrics = AsyncMetrics(delegate_metrics)

        self._split_fetcher = split_fetcher
        self._treatment_log = treatment_log
        self._metrics = metrics


    def get_split_fetcher(self):
        """
        Get the split fetcher implementation for the client.
        :return: The split fetcher
        :rtype: SplitFetcher
        """
        return self._split_fetcher

    def get_treatment_log(self):
        """
        Get the treatment log implementation for the client.
        :return: The treatment log
        :rtype: TreatmentLog
        """
        return self._treatment_log

    def get_metrics(self):
        """
        Get the metrics implementation for the client.
        :return: The metrics
        :rtype: Metrics
        """
        return self._metrics



def _init_config(api_key, **kwargs):
    config = kwargs.pop('config', dict())
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


def get_client(api_key, **kwargs):
    """
    Builds a Split Client that refreshes itself at regular intervals.

    The config_file parameter is the name of a file that contains the client configuration. Here's
    an example of a config file:

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

    The config parameter is a dictionary that allows you to control the behaviour of the client.
    The following configuration values are supported:

    * connectionTimeout: The TCP connection timeout (Default: 1500ms)
    * readTimeout: The HTTP read timeout (Default: 1500ms)
    * featuresRefreshRate: The refresh rate for features (Default: 30s)
    * segmentsRefreshRate: The refresh rate for segments (Default: 60s)
    * metricsRefreshRate: The refresh rate for metrics (Default: 60s)
    * impressionsRefreshRate: The refresh rate for impressions (Default: 60s)
    * randomizeIntervals: Whether to randomize the refres intervals (Default: False)
    * ready: How long to wait (in seconds) for the client to be initialized. 0 to return
      immediately without waiting. (Default: 0s)

    If the api_key argument is 'localhost' a localhost environment client is built based on the
    contents of a .split file in the user's home directory. The definition file has the following
    syntax:

        file: (comment | split_line)+
        comment : '#' string*\n
        split_line : feature_name ' ' treatment\n
        feature_name : string
        treatment : string

    It is possible to change the location of the split file by using the split_definition_file_name
    argument.

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
    api_key, config, sdk_api_base_url, events_api_base_url = _init_config(api_key, **kwargs)

    if api_key == 'localhost':
        return LocalhostEnvironmentClient(**kwargs)

    return SelfRefreshingClient(api_key, config=config, sdk_api_base_url=sdk_api_base_url,
                                events_api_base_url=events_api_base_url)


def get_redis_client(api_key, **kwargs):
    """
    Builds a Split Client that that gets its information from a Redis instance. It also writes
    impressions and metrics to the same instance.

    In order for this work properly, you need to periodically call the update_splits and
    update_segments scripts. You also need to run the send_impressions and send_metrics scripts in
    order to push the impressions and metrics onto the Split.io backend-

    The config_file parameter is the name of a file that contains the client configuration. Here's
    an example of a config file:

    {
      "apiKey": "some-api-key",
      "sdkApiBaseUrl": "https://sdk.split.io/api",
      "eventsApiBaseUrl": "https://events.split.io/api",
      "redisFactory": 'some.redis.factory',
      "redisHost": "localhost",
      "redisPort": 6879,
      "redisDb": 0,
    }

    If the redisFactory entry is present, it is used to build the redis client instance, otherwise
    the values of redisHost, redisPort and redisDb are used.

    If the api_key argument is 'localhost' a localhost environment client is built based on the
    contents of a .split file in the user's home directory. The definition file has the following
    syntax:

        file: (comment | split_line)+
        comment : '#' string*\n
        split_line : feature_name ' ' treatment\n
        feature_name : string
        treatment : string

    It is possible to change the location of the split file by using the split_definition_file_name
    argument.

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
        return LocalhostEnvironmentClient(**kwargs)

    redis = get_redis(config)

    if 'labelsEnabled' in config:
        redis_client = RedisClient(redis, config['labelsEnabled'])
    else:
        redis_client = RedisClient(redis)

    return redis_client

def get_uwsgi_client(api_key, **kwargs):
    """
    Builds a Split Client that that gets its information from a uWSGI cache instance. It also writes
    impressions and metrics to the same instance.

    In order for this work properly, you need to periodically call the spooler uwsgi_update_splits and
    uwsgi_update_segments scripts. You also need to run the uwsgi_report_impressions and uwsgi_report_metrics scripts in
    order to push the impressions and metrics onto the Split.io backend-

    The config_file parameter is the name of a file that contains the client configuration. Here's
    an example of a config file:

    {
      "apiKey": "some-api-key",
      "sdkApiBaseUrl": "https://sdk.split.io/api",
      "eventsApiBaseUrl": "https://events.split.io/api",
      "featuresRefreshRate": 30,
      "segmentsRefreshRate": 60,
      "metricsRefreshRate": 60,
      "impressionsRefreshRate": 60
    }

    If the api_key argument is 'localhost' a localhost environment client is built based on the
    contents of a .split file in the user's home directory. The definition file has the following
    syntax:

        file: (comment | split_line)+
        comment : '#' string*\n
        split_line : feature_name ' ' treatment\n
        feature_name : string
        treatment : string

    It is possible to change the location of the split file by using the split_definition_file_name
    argument.

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
        return LocalhostEnvironmentClient(**kwargs)

    uwsgi = get_uwsgi()
    uwsgi_client = UWSGIClient(uwsgi, config)

    return uwsgi_client
