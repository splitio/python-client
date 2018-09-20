"""This module contains everything related to metrics"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import logging
import six
import abc
from threading import Thread

from collections import namedtuple, defaultdict
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy
from threading import RLock, Timer
from splitio.config import SDK_VERSION, DEFAULT_CONFIG


Impression = namedtuple(
    'Impression',
    [
        'matching_key',
        'feature_name',
        'treatment',
        'label',
        'change_number',
        'bucketing_key',
        'time'
    ]
)


def build_impressions_data(impressions):
    """
    Builds a list of dictionaries that can be used with the test_impressions
    API endpoint from a dictionary of lists of impressions grouped by feature
    name.
    :param impressions: Dict of impression tuples
    :type impressions: dict
    :return: List of dictionaries with impressions data for each feature
    :rtype: list
    """
    return [
        {
            'testName': feature_name,
            'keyImpressions': [
                {
                    'keyName': impression.matching_key,
                    'treatment': impression.treatment,
                    'time': impression.time,
                    'changeNumber': impression.change_number,
                    'label': impression.label,
                    'bucketingKey': impression.bucketing_key
                }
                for impression in feature_impressions
            ]
        }
        for feature_name, feature_impressions in six.iteritems(impressions)
        if len(feature_impressions) > 0
    ]


class Label(object):
    # Condition: Split Was Killed
    # Treatment: Default treatment
    # Label: killed
    KILLED = 'killed'

    # Condition: No condition matched
    # Treatment: Default Treatment
    # Label: no condition matched
    NO_CONDITION_MATCHED = 'default rule'

    #Condition: Split definition was not found
    #Treatment: control
    #Label: split not found
    SPLIT_NOT_FOUND = 'definition not found'

    # Condition: Traffic allocation failed
    # Treatment: Default Treatment
    # Label: not in split
    NOT_IN_SPLIT = 'not in split'

    # Condition: There was an exception
    # Treatment: control
    # Label: exception
    EXCEPTION = 'exception'


class TreatmentLog(object):
    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)
        self._destroyed = False

    def _log(self, impression):
        """Log an impression. Implementing classes need to override this method.
        :param impression: The impression class representation
        :type impression: Impression
        """
        pass  # Do nothing

    def log(self, impression):
        """Log an impression.
        :param impression: The impression
        :type impression: Impression
        """
        if isinstance(impression, Impression):
            if impression.feature_name is not None \
                and impression.matching_key is not None \
                and impression.treatment is not None \
                and impression.time > 0:
                    self._log(impression)
        return

    def destroy(self):
        """
        Prevent future thread scheduling.
        """
        self._destroyed = True


class LoggerBasedTreatmentLog(TreatmentLog):
    def _log(self, impression):
        """Log an impression.
        :param impression: The impression class representation
        :type impression: Impression
        """
        if isinstance(impression, Impression):
            self._logger.info(
                'feature_name = %s, matching_key = %s, treatment = %s, '
                'time = %s, label = %s, change_number = %s, bucketing_key = %s',
                impression.feature_name,
                impression.matching_key,
                impression.treatment,
                impression.time,
                impression.label,
                impression.change_number,
                impression.bucketing_key
            )


class InMemoryTreatmentLog(TreatmentLog):
    def __init__(self, max_count=-1, ignore_impressions=False):
        """
        A thread safe impressions log implementation that stores the impressions
        in memory. Access to the impressions storage is synchronized with a
        re-entrant lock.
        :param max_count: Max number of impressions per feature before eviction
        :type max_count: int
        :param ignore_impressions: Whether to ignore log requests
        :type ignore_impressions: bool
        """
        super(InMemoryTreatmentLog, self).__init__()
        self._max_count = max_count
        self._ignore_impressions = ignore_impressions
        self._impressions = defaultdict(list)
        self._rlock = RLock()

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

    @property
    def max_count(self):
        """
        :return: Max number of stored impressions allowed
        :rtype: int
        """
        return self._max_count

    @max_count.setter
    def max_count(self, max_count):
        """Sets the max number of stored impressions allowed
        :param max_count: Max number of stored impressions allowed
        :type max_count: int
        """
        self._max_count = max_count

    def fetch_all_and_clear(self):
        """Fetch all logged impressions and clear the log.
        :return: The logged impressions
        :rtype: dict
        """
        with self._rlock:
            existing_impressions = deepcopy(self._impressions)
            self._impressions = defaultdict(list)

        return existing_impressions

    def _notify_eviction(self, feature_name, feature_impressions):
        """
        Notifies that the max count was reached for a feature.
        This gives the opportunity to
        subclasses to do something about the eviction
        :param feature_name: The name of the feature
        :type feature_name: str
        :param feature_impressions: The evicted impressions
        :type feature_impressions: list
        """
        pass  # Do nothing

    def _log(self, impression):
        """Log an impression.
        :param impression: The impression class representation
        :type impression: Impression
        """
        if isinstance(impression, Impression):
            with self._rlock:
                feature_impressions = self._impressions[impression.feature_name]

                if self._max_count < 0 or len(feature_impressions) < self._max_count:
                    feature_impressions.append(impression)
                else:
                    self._logger.warning(
                        'Count limit for feature treatment log. '
                        'Clearing impressions for feature.'
                    )
                    self._impressions[impression.feature_name] = [impression]
                    self._notify_eviction(
                        impression.feature_name,
                        feature_impressions
                    )


class CacheBasedTreatmentLog(TreatmentLog):
    def __init__(self, impressions_cache):
        """A cache based impressions log implementation.
        :param impressions_cache: An impressions cache
        :type impressions_cache: ImpressionsCache
        """
        super(CacheBasedTreatmentLog, self).__init__()
        self._impressions_cache = impressions_cache

    def _log(self, impression):
        """Log an impression.
        :param impression: The impression class representation
        :type impression: Impression
        """
        if isinstance(impression, Impression):
            self._impressions_cache.add_impression(impression)


class SelfUpdatingTreatmentLog(InMemoryTreatmentLog):
    def __init__(self, api, interval=180, max_workers=5, max_count=-1,
                 ignore_impressions=False):
        """
        An impressions implementation that sends the in impressions stored
        periodically to the Split.io back-end.
        :param api: The SDK api client
        :type api: SdkApi
        :param interval: Optional update interval (Default: 180s)
        :type interval: int
        :param max_workers: The max number of workers used to update impressions
        :type max_workers: int
        :param max_count: Max number of impressions per feature before eviction
        :type max_count: int
        :param ignore_impressions: Whether to ignore log requests
        :type ignore_impressions: bool
        """
        super(SelfUpdatingTreatmentLog, self).__init__(
            max_count=max_count,
            ignore_impressions=ignore_impressions
        )
        self._api = api
        self._interval = interval
        self._stopped = True
        self._thread_pool_executor = ThreadPoolExecutor(max_workers=max_workers)

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
        self._timer_refresh()

    def _update_evictions(self, feature_name, feature_impressions):
        """
        Sends evicted impressions to the Split.io back-end.
        :param feature_name: The name of the feature
        :type feature_name: str
        :param feature_impressions: The evicted impressions
        :type feature_impressions: list
        """
        try:
            test_impressions_data = build_impressions_data(
                {feature_name: feature_impressions}
            )

            if len(test_impressions_data) > 0:
                self._api.test_impressions(test_impressions_data)
        except:
            self._logger.exception(
                'Exception caught updating evicted impressions'
            )
            self._stopped = True


    def _update_impressions(self):
        """
        Sends the impressions stored back to the Split.io back-end
        """
        try:
            impressions_by_feature = self.fetch_all_and_clear()
            test_impressions_data = build_impressions_data(
                impressions_by_feature
            )

            if len(test_impressions_data) > 0:
                self._api.test_impressions(test_impressions_data)
        except:
            self._logger.exception('Exception caught updating impressions')
            self._stopped = True



    def _notify_eviction(self, feature_name, feature_impressions):
        """
        Notifies that the max count was reached for a feature. The evicted
        impressions are going to be sent to the back-end.
        :param feature_name: The name of the feature
        :type feature_name: str
        :param feature_impressions: The evicted impressions
        :type feature_impressions: list
        """
        if self._destroyed \
                or feature_name is None \
                or feature_impressions is None or len(feature_impressions) == 0:
            return

        try:
            self._thread_pool_executor.submit(
                self._update_evictions, feature_name, feature_impressions
            )
        except:
            self._logger.exception(
                'Exception caught starting evicted impressions update thread'
            )

    def _timer_refresh(self):
        """
        Responsible for setting the periodic calls to _update_impressions using
        a Timer thread.
        """
        if self._destroyed:
            return

        try:
            self._thread_pool_executor.submit(self._update_impressions)
        except:
            self._logger.exception(
                'Exception caught starting impressions update thread'
            )

        try:
            if hasattr(self._interval, '__call__'):
                interval = self._interval()
            else:
                interval = self._interval

            timer = Timer(interval, self._timer_refresh)
            timer.daemon = True
            timer.start()
        except:
            self._logger.exception('Exception caught refreshing timer')
            self._stopped = True


class AsyncTreatmentLog(TreatmentLog):
    def __init__(self, delegate, max_workers=5):
        """
        A treatment log implementation that uses threads to execute the
        actual logging onto a delegate log to avoid blocking the caller.
        :param delegate: The delegate impression log
        :type delegate: ImpressionLog
        :param max_workers: How many workers to use for logging
        """
        super(AsyncTreatmentLog, self).__init__()
        self._delegate = delegate
        self._thread_pool_executor = ThreadPoolExecutor(max_workers=max_workers)
        self._destroyed = False

    @property
    def delegate(self):
        return self._delegate

    def destroy(self):
        """
        Prevent future "log" calls from scheduling threads.
        If delegate has a custom destroy() method, call it.
        """
        self._destroyed = True
        delegate_destroy = getattr(self._delegate, 'destroy', None)
        if six.callable(delegate_destroy):
            self._delegate.destroy()

    def log(self, impression):
        """Logs an impression asynchronously.
        :param impression: The impression
        :type impression: Impression
        :return: int
        """
        if self._destroyed:
            return

        if isinstance(impression, Impression):
            try:
                self._thread_pool_executor.submit(self._delegate.log, impression)
            except:
                self._logger.exception(
                    'Exception caught logging impression asynchronously'
                )


class ImpressionListenerException(Exception):
    '''
    Custom Exception for Impression Listener
    '''
    pass


class ImpressionListenerWrapper(object):
    """
    Wrapper in charge of building all the data that client would require in case
    of adding some logic with the treatment and impression results.
    """

    impression_listener = None

    def __init__(self, impression_listener):
        self.impression_listener = impression_listener

    def log_impression(self, impression, attributes=None):
        data = {}
        data['impression'] = impression
        data['attributes'] = attributes
        data['instance-id'] = DEFAULT_CONFIG['splitSdkMachineIp']
        data['sdk-language-version'] = SDK_VERSION
        try:
            self.impression_listener.log_impression(data)
        except:
            raise ImpressionListenerException('Exception caught in log_impression user\'s'
                                              'method is throwing exceptions')


class ImpressionListener(object):
    """
    Abstract class defining the interface that concrete client must implement,
    and including methods that use that interface to add client's logic for each
    impression.
    """
    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def log_impression(self, data):
        pass
