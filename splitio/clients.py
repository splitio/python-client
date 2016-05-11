"""A module for Split.io SDK API clients"""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging

import arrow

from os.path import expanduser, join
from random import randint
from re import compile

from future.utils import raise_from

from splitio.api import SdkApi
from splitio.metrics import (Metrics, AsyncMetrics, ApiMetrics, SDK_GET_TREATMENT)
from splitio.impressions import (TreatmentLog, AsyncTreatmentLog, SelfUpdatingTreatmentLog)
from splitio.splitters import Splitter
from splitio.splits import (SelfRefreshingSplitFetcher, SplitParser, ApiSplitChangeFetcher,
                            JSONFileSplitFetcher, InMemorySplitFetcher, AllKeysSplit)
from splitio.segments import (ApiSegmentChangeFetcher, SelfRefreshingSegmentFetcher,
                              JSONFileSegmentFetcher)
from splitio.settings import DEFAULT_CONFIG, SDK_API_BASE_URL, MAX_INTERVAL
from splitio.treatments import CONTROL


class Client(object):
    def __init__(self):
        """Basic interface of a Client. Specific implementations need to override the
        get_split_fetcher method (and optionally the get_splitter method).
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._splitter = None
        self._treatment_log = None
        self._metrics = None

    def get_split_fetcher(self):
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
        if self._splitter is None:
            self._splitter = Splitter()

        return self._splitter

    def get_treatment_log(self):
        """Get the treatment log implementation.
        :return: The treatment log implementation.
        :rtype: TreatmentLog
        """
        if self._treatment_log is None:
            self._treatment_log = TreatmentLog()

        return self._treatment_log

    def get_metrics(self):
        """Get the metrics implementation.
        :return: The metrics implementation.
        :rtype: Metrics
        """
        if self._metrics is None:
            self._metrics = Metrics()

        return self._metrics

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

        try:
            start = arrow.utcnow().timestamp * 1000

            split = self.get_split_fetcher().fetch(feature)
            treatment = self._get_treatment_for_split(split, key, attributes)

            self._record_stats(key, feature, treatment, start, SDK_GET_TREATMENT)

            return treatment
        except:
            self._logger.exception('Exception caught getting treatment for feature')
            return CONTROL

    def _record_stats(self, key, feature, treatment, start, operation):
        try:
            end = arrow.utcnow().timestamp * 1000
            self.get_treatment_log().log(key, feature, treatment, end)
            self.get_metrics().time(operation, end - start)
        except:
            self._logger.exception('Exception caught recording impressions and metrics')

    def _get_treatment_for_split(self, split, key, attributes=None):
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
        if split is None:
            return CONTROL

        if split.killed:
            return split.default_treatment

        for condition in split.conditions:
            if condition.matcher.match(key, attributes=attributes):
                return self.get_splitter().get_treatment(key, split.seed, condition.partitions)

        return split.default_treatment


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

        :param api_key: The API key provided by Split.io
        :type api_key: str
        :param config: The configuration dictionary
        :type config: dict
        :param sdk_api_base_url: An override for the default API base URL.
        :type sdk_api_base_url: str
        :param events_api_base_url: An override for the default events base URL.
        :type events_api_base_url: str
        """
        super(SelfRefreshingClient, self).__init__()
        self._api_key = api_key

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

        self._sdk_api = SdkApi(self._api_key, sdk_api_base_url=sdk_api_base_url,
                               events_api_base_url=events_api_base_url,
                               connect_timeout=self._connection_timeout,
                               read_timeout=self._read_timeout)
        self._split_fetcher = None

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
        split_fetcher.start()

        return split_fetcher

    def get_split_fetcher(self):
        """
        Get the split fetcher implementation for the client.
        :return: The split fetcher
        :rtype: SplitFetcher
        """
        if self._split_fetcher is None:
            self._split_fetcher = self._build_split_fetcher()

        return self._split_fetcher

    def get_treatment_log(self):
        """Get the treatment log implementation.
        :return: The treatment log implementation.
        :rtype: TreatmentLog
        """
        if self._treatment_log is None:
            self_updating_treatment_log = SelfUpdatingTreatmentLog(
                self._sdk_api, max_count=self._max_impressions_log_size,
                interval=self._impressions_interval)
            self._treatment_log = AsyncTreatmentLog(self_updating_treatment_log)
            self_updating_treatment_log.start()

        return self._treatment_log

    def get_metrics(self):
        """Get the metrics implementation.
        :return: The metrics implementation.
        :rtype: Metrics
        """
        if self._metrics is None:
            api_metrics = ApiMetrics(self._sdk_api, max_call_count=self._metrics_max_call_count,
                                     max_time_between_calls=self._metrics_max_time_between_calls)
            self._metrics = AsyncMetrics(api_metrics)

        return self._metrics


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
        self._split_fetcher = None

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
        if self._split_fetcher is None:
            self._split_fetcher = self._build_split_fetcher()

        return self._split_fetcher

    def get_treatment_log(self):
        """Get the treatment log implementation.
        :return: The treatment log implementation.
        :rtype: TreatmentLog
        """
        if self._treatment_log is None:
            self._treatment_log = AsyncTreatmentLog(TreatmentLog())

        return self._treatment_log

    def get_metrics(self):
        """Get the metrics implementation.
        :return: The metrics implementation.
        :rtype: Metrics
        """
        if self._metrics is None:
            self._metrics = AsyncMetrics(Metrics())

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

    def get_split_fetcher(self):
        """
        Get the split fetcher implementation for the client.
        :return: The split fetcher
        :rtype: SplitFetcher
        """
        if self._split_fetcher is None:
            self._split_fetcher = self._build_split_fetcher()

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
