"""This module contains everything related to metrics"""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging

from collections import namedtuple, defaultdict
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from threading import RLock, Thread, Timer

from six import iteritems

Impression = namedtuple('Impression', ['key', 'feature_name', 'treatment', 'time'])


def build_impressions_data(impressions):
    """Builds a list of dictionaries that can be used with the test_impressions API endpoint from
    a dictionary of lists of impressions grouped by feature name.
    :param impressions: List of impression tuples
    :type impressions: list
    :return: List of dictionaries with impressions data for each feature
    :rtype: list
    """
    return [
        {
            'testName': feature_name,
            'keyImpressions': [
                {
                    'keyName': impression.key,
                    'treatment': impression.treatment,
                    'time': impression.time
                }
                for impression in feature_impressions
            ]
        }
        for feature_name, feature_impressions in iteritems(impressions)
        if len(feature_impressions) > 0
    ]


class TreatmentLog(object):
    def __init__(self, ignore_impressions=False):
        """A log for impressions. Specific implementations need to override the log and
        fetch_all_and_clear methods.
        :param ignore_impressions: Whether to ignore log requests
        :type ignore_impressions: bool
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._ignore_impressions = ignore_impressions

    @property
    def ignore_impressions(self):
        """
        :return: Whether to ignore log requests
        :rtype: bool
        """
        return self._ignore_impressions

    @ignore_impressions.setter
    def ignore_impressions(self, ignore_impressions):
        """Set ignore_impressions property
        :param ignore_impressions: Whether to ignore log requests
        :type ignore_impressions: bool
        """
        self._ignore_impressions = ignore_impressions

    def fetch_all_and_clear(self):
        """Fetch all logged impressions and clear the log.
        :return: The logged impressions
        :rtype: list
        """
        return dict()

    def _log(self, key, feature_name, treatment, time):
        """Log an impression. Implementing classes need to override this method.
        :param key: The key of the impression
        :type key: str
        :param feature_name: The feature of the impression
        :type feature_name: str
        :param treatment: The treatment of the impression
        :type treatment: str
        :param time: The time of the impression in milliseconds since the epoch
        :type time: int
        """
        pass  # Do nothing

    def log(self, key, feature_name, treatment, time):
        """Log an impression.
        :param key: The key of the impression
        :type key: str
        :param feature_name: The feature of the impression
        :type feature_name: str
        :param treatment: The treatment of the impression
        :type treatment: str
        :param time: The time of the impression in milliseconds since the epoch
        :type time: int
        """
        if not self._ignore_impressions:
            self._log(key, feature_name, treatment, time)


class LoggerBasedTreatmentLog(TreatmentLog):
    def __init__(self, ignore_impressions=False):
        """A log for impressions that writes impressions as log messages.
        :param ignore_impressions: Whether to ignore log requests
        :type ignore_impressions: bool
        """
        super(LoggerBasedTreatmentLog, self).__init__(ignore_impressions=ignore_impressions)

    def _log(self, key, feature_name, treatment, time):
        """Log an impression as a log message.
        :param key: The key of the impression
        :type key: str
        :param feature_name: The feature of the impression
        :type feature_name: str
        :param treatment: The treatment of the impression
        :type treatment: str
        :param time: The time of the impression in milliseconds since the epoch
        :type time: int
        """
        self._logger.info('feature_name = %s, key = %s, treatment = %s, time = %s', feature_name,
                          key, treatment, time)


class InMemoryTreatmentLog(TreatmentLog):
    def __init__(self, ignore_impressions=False):
        """A thread safe impressions log implementation that stores the impressions in memory.
        Access to the impressions storage is synchronized with a re-entrant lock.
        :param ignore_impressions: Whether to ignore log requests
        :type ignore_impressions: bool
        """
        super(InMemoryTreatmentLog, self).__init__(ignore_impressions=ignore_impressions)
        self._impressions = defaultdict(list)
        self._rlock = RLock()

    def fetch_all_and_clear(self):
        """Fetch all logged impressions and clear the log.
        :return: The logged impressions
        :rtype: list
        """
        with self._rlock:
            existing_impressions = deepcopy(self._impressions)
            self._impressions = defaultdict(list)

        return existing_impressions

    def _log(self, key, feature_name, treatment, time):
        """Logs an impression.
        :param key: The key of the impression
        :type key: str
        :param feature_name: The name of the feature of the impression
        :type feature_name: str
        :param treatment: The treatment of the impression
        :type treatment: str
        :param time: Timestamp as milliseconds from epoch of the impression
        :return: int
        """
        with self._rlock:
            self._impressions[feature_name].append(
                Impression(key=key, feature_name=feature_name, treatment=treatment, time=time))


class CacheBasedTreatmentLog(TreatmentLog):
    def __init__(self, impressions_cache, ignore_impressions=False):
        """A cache based impressions log implementation.
        :param impressions_cache: An impressions cache
        :type impressions_cache: ImpressionsCache
        :param ignore_impressions: Whether to ignore log requests
        :type ignore_impressions: bool
        """
        super(CacheBasedTreatmentLog, self).__init__(ignore_impressions=ignore_impressions)
        self._impressions_cache = impressions_cache

    def fetch_all_and_clear(self):
        """Fetch all logged impressions and clear the log.
        :return: The logged impressions
        :rtype: list
        """
        return self._impressions_cache.fetch_all_and_clear()

    def _log(self, key, feature_name, treatment, time):
        """Logs an impression.
        :param key: The key of the impression
        :type key: str
        :param feature_name: The name of the feature of the impression
        :type feature_name: str
        :param treatment: The treatment of the impression
        :type treatment: str
        :param time: Timestamp as milliseconds from epoch of the impression
        :return: int
        """
        self._impressions_cache.add_impression(
            Impression(key=key, feature_name=feature_name, treatment=treatment, time=time))


class SelfUpdatingTreatmentLog(InMemoryTreatmentLog):
    def __init__(self, api, interval=180, ignore_impressions=False):
        """An impressions implementation that sends the in impressions stored periodically to the
        Split.io back-end.
        :param api: The SDK api client
        :type api: SdkApi
        :param interval: Optional update interval (Default: 180s)
        :type interval: int
        """
        super(SelfUpdatingTreatmentLog, self).__init__(ignore_impressions=ignore_impressions)
        self._api = api
        self._interval = interval
        self._stopped = True

    @property
    def stopped(self):
        """
        :return: Whether the update process has been stopped
        :rtype: bool
        """
        return self._stopped

    @stopped.setter
    def stopped(self, stopped):
        """
        :param stopped: Whether to stop the update process
        :type stopped: bool
        """
        self._stopped = stopped

    def start(self):
        """Starts the update process"""
        if not self._stopped:
            return

        self._stopped = False
        SelfUpdatingTreatmentLog._timer_refresh(self)

    @staticmethod
    def _update_impressions(treatment_log):
        """Sends the impressions stored back to the Split.io back-end
        :param treatment_log: A self updating impressions object
        :type treatment_log: SelfUpdatingImpressions
        """
        try:
            impressions_by_feature = treatment_log.fetch_all_and_clear()
            test_impressions_data = build_impressions_data(impressions_by_feature)
            for feature_test_impressions_data in test_impressions_data:
                treatment_log._api.test_impressions(feature_test_impressions_data)
        except:
            treatment_log._logger.exception('Exception caught updating impressions')
            treatment_log._stopped = True

    @staticmethod
    def _timer_refresh(treatment_log):
        """Responsible for setting the periodic calls to _update_impressions using a Timer thread.
        :param treatment_log: A self updating traeatment log object
        :type treatment_log: SelfUpdatingTreatmentLog
        """
        if treatment_log._stopped:
            return

        try:
            thread = Thread(target=SelfUpdatingTreatmentLog._update_impressions,
                            args=(treatment_log,))
            thread.daemon = True
            thread.start()
        except:
            treatment_log._logger.exception('Exception caught starting impressions update thread')

        try:
            if hasattr(treatment_log._interval, '__call__'):
                interval = treatment_log._interval()
            else:
                interval = treatment_log._interval

            timer = Timer(interval, SelfUpdatingTreatmentLog._timer_refresh,
                          (treatment_log,))
            timer.daemon = True
            timer.start()
        except:
            treatment_log._logger.exception('Exception caught refreshing timer')
            treatment_log._stopped = True


class AsyncTreatmentLog(TreatmentLog):
    def __init__(self, impressions_log, max_workers=5):
        """A treatment log implementation that uses threads to execute the actual logging onto a
        delegate log to avoid blocking the caller.
        :param impressions_log: The delegate impression log
        :type impressions_log: ImpressionLog
        :param max_workers: How many workers to use for logging
        """
        super(AsyncTreatmentLog, self).__init__()
        self._impressions_log = impressions_log
        self._thread_pool_executor = ThreadPoolExecutor(max_workers=max_workers)

    @staticmethod
    def _log_fn(impressions_log, key, feature_name, treatment, time):
        """Execute the blocking log call"""
        impressions_log.log(key, feature_name, treatment, time)

    def log(self, key, feature_name, treatment, time):
        """Logs an impression asynchronously.
        :param key: The key of the impression
        :type key: str
        :param feature_name: The name of the feature of the impression
        :type feature_name: str
        :param treatment: The treatment of the impression
        :type treatment: str
        :param time: Timestamp as milliseconds from epoch of the impression
        :return: int
        """
        try:
            self._thread_pool_executor.submit(AsyncTreatmentLog._log_fn, self._impressions_log, key,
                                              feature_name, treatment, time)
        except:
            self._logger.exception('Exception caught logging impression asynchronously')
