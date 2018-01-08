"""This module contains everything related to metrics"""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging

import arrow

from bisect import bisect_left
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from threading import RLock

from six import iteritems


SDK_GET_TREATMENT = 'sdk.getTreatment'

BUCKETS = (
    1000, 1500, 2250, 3375, 5063,
    7594, 11391, 17086, 25629, 38443,
    57665, 86498, 129746, 194620, 291929,
    437894, 656841, 985261, 1477892, 2216838,
    3325257, 4987885, 7481828
)
MAX_LATENCY = 7481828


def get_latency_bucket_index(micros):
    """Finds the bucket index for a measure latency
    :param micros: Measured latency in microseconds
    :type micros: int
    :return: Bucket index for the given latency
    :rtype: int
    """
    if micros > MAX_LATENCY:
        return len(BUCKETS) - 1

    return bisect_left(BUCKETS, micros)


class LatencyTracker(object):
    def __init__(self, latencies=None):
        """An object to count latencies that fall within certain buckets.
        :param latencies: Existing latency counts
        :type latencies: list"""
        self._latencies = latencies if latencies is not None else [0] * len(BUCKETS)

    def add_latency_millis(self, millis):
        """Increments the count bucket for milliseconds latency.
        :param millis: The measured latency in milliseconds
        :type millis: int
        """
        self._latencies[get_latency_bucket_index(millis * 1000)] += 1

    def add_latency_micros(self, micros):
        """Increments the count bucket for microsecond latency.
        :param micros: The measured latency in microseconds
        :type micros: int
        """
        self._latencies[get_latency_bucket_index(micros)] += 1

    def get_latencies(self):
        """
        :return: The current measured latencies
        :rtype: list
        """
        return list(self._latencies)

    def get_latency(self, index):
        """
        :param index: The bucket index
        :param index: int
        :return: The current measured latency for a given bucket index
        :rtype: int
        """
        return self._latencies[index]

    def clear(self):
        """Clears the latency counts"""
        self._latencies = [0] * len(BUCKETS)

    def get_bucket_for_latency_millis(self, latency):
        """:param latency: The measured latency in milliseconds
        :type latency: int
        :return: The bucket count for the measured latency
        :rtype: int
        """
        return self._latencies[get_latency_bucket_index(latency * 1000)]

    def get_bucket_for_latency_micros(self, latency):
        """:param latency: The measured latency in microseconds
        :type latency: int
        :return: The bucket count for the measured latency
        :rtype: int
        """
        return self._latencies[get_latency_bucket_index(latency)]


class Metrics(object):  # pragma: no cover
    def __init__(self):
        self._logger = logging.getLogger(self.__class__.__name__)

    def count(self, counter, delta):
        """
        Adjusts the specified counter by a given delta. This method is is non-blocking and is
        guaranteed not to throw an exception
        :param counter: The name of the counter to adjust
        :type counter: str
        :param delta: The amount ot adjust the counter by
        :type delta: int"""
        pass  # Do nothing

    def time(self, operation, time_in_ms):
        """
        Records an execution time in milliseconds for the specified named operation. This method
        is non-blocking and is guaranteed not to throw an exception.
        :param operation: The name of the timed operation
        :type operation: str
        :param time_in_ms: The time in milliseconds
        :type: int
        """
        pass  # Do nothing

    def gauge(self, gauge, value):
        """
        Records the latest fixed value for the specified named gauge. This method is
        non-blocking and is guaranteed not to throw an exception.
        :param gauge: The name of the gauge
        :type gauge: str
        :param value: The new reading of the gauge
        :type: float
        """
        pass

    def destroy(self):
        """
        Dummy method for dummy implementation.
        """
        pass


class InMemoryMetrics(Metrics):
    def __init__(self, count_metrics=None, time_metrics=None, gauge_metrics=None, max_call_count=-1,
                 max_time_between_calls=-1):
        """
        A metrics implementation that stores them in memory and keeps track of calls. When too many
        calls have been made consecutively or when too much time has passed between calls, the
        appropriate update callback is called. Sub-classes implement these callbacks to react
        accordingly.
        :param count_metrics: Optional existing count metrics
        :type count_metrics: defaultdict
        :param time_metrics: Optional existing time metrics
        :type time_metrics: defaultdict
        :param gauge_metrics: Optional existing gauge metrics
        :type gauge_metrics: defaultdict
        :param max_call_count: How many calls before triggering an update
        :type max_call_count: int
        :param max_time_between_calls: How much time to wait between calls to trigger an update
        :type max_time_between_calls: int
        """
        super(InMemoryMetrics, self).__init__()
        self._count_metrics = count_metrics if count_metrics is not None else defaultdict(int)
        self._time_metrics = time_metrics if time_metrics is not None \
            else defaultdict(LatencyTracker)
        self._gauge_metrics = gauge_metrics if gauge_metrics is not None else defaultdict(float)
        self._max_call_count = max_call_count
        self._max_time_between_calls = max_time_between_calls

        utcnow_timestamp = arrow.utcnow().timestamp

        self._count_call_count = 0
        self._count_last_call_time = utcnow_timestamp
        self._time_call_count = 0
        self._time_last_call_time = utcnow_timestamp
        self._gauge_call_count = 0
        self._gauge_last_call_time = utcnow_timestamp
        self._count_rlock = RLock()
        self._time_rlock = RLock()
        self._gauge_rlock = RLock()
        self._ignore_metrics = False

    @property
    def ignore_metrics(self):
        return self._ignore_metrics

    @ignore_metrics.setter
    def ignore_metrics(self, ignore_metrics):
        self._ignore_metrics = ignore_metrics

    def _fetch_count_metrics_and_clear(self):
        """Returns the existing count metrics and clears the information.
        :return: Existing count metrics
        :rtype: dict
        """
        with self._count_rlock:
            count_metrics = self._count_metrics
            self._count_metrics = defaultdict(int)

        return count_metrics

    def _fetch_time_metrics_and_clear(self):
        """Returns the existing time metrics and clears the information.
        :return: Existing time metrics
        :rtype: dict
        """
        with self._time_rlock:
            time_metrics = self._time_metrics
            self._time_metrics = defaultdict(LatencyTracker)

        return time_metrics

    def _fetch_gauge_metrics_and_clear(self):
        """Returns the existing gauge metrics and clears the information.
        :return: Existing gauge metrics
        :rtype: dict
        """
        with self._gauge_rlock:
            gauge_metrics = self._gauge_metrics
            self._gauge_metrics = defaultdict(int)

        return gauge_metrics

    def count(self, counter, delta):
        """Adjusts the specified counter by a given delta. This method is is non-blocking and is
        guaranteed not to throw an exception
        :param counter: The name of the counter to adjust
        :type counter: str
        :param delta: The amount ot adjust the counter by
        :type delta: int"""
        if self.ignore_metrics:
            return

        with self._count_rlock:
            self._count_metrics[counter] += delta
            self._count_call_count += 1

            old_call_time = self._count_last_call_time
            self._count_last_call_time = arrow.utcnow().timestamp
            if (self._count_call_count == self._max_call_count > 0) or \
                    self._count_last_call_time - old_call_time > self._max_time_between_calls > 0:
                self._count_call_count = 0
                self.update_count()

    def update_count(self):
        """Signals that an update on count metrics should be sent to the Split.io back-end"""
        pass  # Do nothing

    def time(self, operation, time_in_ms):
        """Records an execution time in milliseconds for the specified named operation. This method
        is non-blocking and is guaranteed not to throw an exception.
        :param operation: The name of the timed operation
        :type operation: str
        :param time_in_ms: The time in milliseconds
        :type: int
        """
        if self.ignore_metrics:
            return

        with self._time_rlock:
            self._time_metrics[operation].add_latency_millis(time_in_ms)
            self._time_call_count += 1

            old_call_time = self._time_last_call_time
            self._time_last_call_time = arrow.utcnow().timestamp
            if (self._time_call_count == self._max_call_count > 0) or \
                    self._time_last_call_time - old_call_time > self._max_time_between_calls > 0:
                self._time_call_count = 0
                self.update_time()

    def update_time(self):
        """Signals that an update on time metrics should be sent to the Split.io back-end"""
        pass  # Do nothing

    def gauge(self, gauge, value):
        """Records the latest fixed value for the specified named gauge. This method is
        non-blocking and is guaranteed not to throw an exception.
        :param gauge: The name of the gauge
        :type gauge: str
        :param value: The new reading of the gauge
        :type: float
        """
        if self.ignore_metrics:
            return

        with self._gauge_rlock:
            self._gauge_metrics[gauge] = value
            self._gauge_call_count += 1

            old_call_time = self._gauge_last_call_time
            self._gauge_last_call_time = arrow.utcnow().timestamp
            if (self._gauge_call_count == self._max_call_count > 0) or \
                    self._gauge_last_call_time - old_call_time > self._max_time_between_calls > 0:
                self._gauge_call_count = 0
                self.update_gauge()

    def update_gauge(self):
        """Signals that an update on time metrics should be sent to the Split.io back-end"""
        pass  # Do nothing


def build_metrics_counter_data(count_metrics):
    """Convert count metrics information to the format used by the API.
    :param count_metrics: A dictionary with the count metrics data
    :type count_metrics: dict
    :return: List of count metrics in the API format
    :rtype: list
    """
    return [{'name': name, 'delta': delta} for name, delta in iteritems(count_metrics)]


def build_metrics_times_data(time_metrics):
    """Convert times metrics information to the format used by the API.
    :param time_metrics: A dictionary with the times metrics data
    :type time_metrics: dict
    :return: List of times metrics in the API format
    :rtype: list
    """
    return [{'name': name, 'latencies': latencies.get_latencies()}
            for name, latencies in iteritems(time_metrics)]


def build_metrics_gauge_data(gauge_metrics):
    """Convert gauge metrics information to the format used by the API.
    :param gauge_metrics: A dictionary with the gauge metrics data
    :type gauge_metrics: dict
    :return: List of gauge metrics in the API format
    :rtype: list
    """
    return [{'name': name, 'value': value} for name, value in iteritems(gauge_metrics)]


class ApiMetrics(InMemoryMetrics):
    def __init__(self, api, max_workers=5, count_metrics=None, time_metrics=None,
                 gauge_metrics=None, max_call_count=-1, max_time_between_calls=-1):
        """
        A metrics implementation that stores them in memory and sends them back to the Split.io
        back-end when an update is triggered.
        :param api: The SDK API client
        :type api: ApiSdk
        :param max_workers: How many workers to use in the update thread pool executor
        :type max_workers: int
        :param count_metrics: Optional existing count metrics
        :type count_metrics: defaultdict
        :param time_metrics: Optional existing time metrics
        :type time_metrics: defaultdict
        :param gauge_metrics: Optional existing gauge metrics
        :type gauge_metrics: defaultdict
        :param max_call_count: How many calls before triggering an update
        :type max_call_count: int
        :param max_time_between_calls: How much time to wait between calls to trigger an update
        :type max_time_between_calls: int
        """
        super(ApiMetrics, self).__init__(count_metrics=count_metrics, time_metrics=time_metrics,
                                         gauge_metrics=gauge_metrics, max_call_count=max_call_count,
                                         max_time_between_calls=max_time_between_calls)
        self._api = api
        self._thread_pool_executor = ThreadPoolExecutor(max_workers=max_workers)

    def _update_count_fn(self):
        count_metrics = self._fetch_count_metrics_and_clear()

        try:
            self._api.metrics_counters(build_metrics_counter_data(count_metrics))
        except:
            self._logger.exception('Exception caught sending count metrics to the back-end. '
                                   'Ignoring metrics.')
            self._ignore_metrics = True

    def update_count(self):
        """Signals that an update on count metrics should be sent to the Split.io back-end"""
        try:
            self._thread_pool_executor.submit(self._update_count_fn)
        except:
            self._logger.exception('Exception caught submitting count metrics update task.')

    def _update_time_fn(self):
        time_metrics = self._fetch_time_metrics_and_clear()

        try:
            self._api.metrics_times(build_metrics_times_data(time_metrics))
        except:
            self._logger.exception('Exception caught sending time metrics to the back-end. '
                                   'Ignoring metrics.')
            self._ignore_metrics = True

    def update_time(self):
        """Signals that an update on time metrics should be sent to the Split.io back-end"""
        try:
            self._thread_pool_executor.submit(self._update_time_fn)
        except:
            self._logger.exception('Exception caught submitting time metrics update task.')

    def _update_gauge_fn(self):
        gauge_metrics = self._fetch_gauge_metrics_and_clear()

        try:
            self._api.metrics_gauge(build_metrics_gauge_data(gauge_metrics))
        except:
            self._logger.exception('Exception caught sending gauge metrics to the back-end. '
                                   'Ignoring metrics.')
            self._ignore_metrics = True

    def update_gauge(self):
        """Signals that an update on time metrics should be sent to the Split.io back-end"""
        try:
            self._thread_pool_executor.submit(self._update_gauge_fn)
        except:
            self._logger.exception('Exception caught submitting gauge metrics update task.')


class LoggerMetrics(InMemoryMetrics):
    def __init__(self, max_call_count=-1, max_time_between_calls=-1):
        """
        A metrics implementation that stores them in memory and logs update attempts.
        :param max_call_count: How many calls before triggering an update
        :type max_call_count: int
        :param max_time_between_calls: How much time to wait between calls to trigger an update
        :type max_time_between_calls: int
        """
        super(LoggerMetrics, self).__init__(max_call_count=max_call_count,
                                            max_time_between_calls=max_time_between_calls)

    def update_count(self):
        """Logs a count update request"""
        count_metrics = self._fetch_count_metrics_and_clear()
        self._logger.info('update_count. count_metrics = %s',
                          build_metrics_counter_data(count_metrics))

    def update_time(self):
        """Logs a time update request"""
        time_metrics = self._fetch_time_metrics_and_clear()
        self._logger.info('update_time. time_metrics = %s', build_metrics_times_data(time_metrics))

    def update_gauge(self):
        """Logs a gauge update request"""
        gauge_metrics = self._fetch_gauge_metrics_and_clear()
        self._logger.info('update_gauge. gauge_metrics = %s',
                          build_metrics_gauge_data(gauge_metrics))


class AsyncMetrics(Metrics):
    def __init__(self, delegate, max_workers=5):
        """A non-blocking Metrics implementation that offloads calls to a delegate Metrics object
        through a ThreadPoolExecutor
        :param delegate: The delegate Metrics object
        :type delegate: Metrics
        :param max_workers: The max number of workers to use in the thread pool
        :type max_workers: int
        """
        super(AsyncMetrics, self).__init__()
        self._delegate = delegate
        self._thread_pool_executor = ThreadPoolExecutor(max_workers=max_workers)
        self._destroyed = False

    def destroy(self):
        self._destroyed = True

    def count(self, counter, delta):
        """Adjusts the specified counter by a given delta. This method is is non-blocking and is
        guaranteed not to throw an exception
        :param counter: The name of the counter to adjust
        :type counter: str
        :param delta: The amount ot adjust the counter by
        :type delta: int"""
        if self._destroyed:
            return

        try:
            self._thread_pool_executor.submit(self._delegate.count, counter, delta)
        except:
            self._logger.exception('Exception caught submitting count metric')

    def time(self, operation, time_in_ms):
        """Records an execution time in milliseconds for the specified named operation. This method
        is non-blocking and is guaranteed not to throw an exception.
        :param operation: The name of the timed operation
        :type operation: str
        :param time_in_ms: The time in milliseconds
        :type: int
        """
        if self._destroyed:
            return

        try:
            self._thread_pool_executor.submit(self._delegate.time, operation, time_in_ms)
        except:
            self._logger.exception('Exception caught submitting time metric')

    def gauge(self, gauge, value):
        """Records the latest fixed value for the specified named gauge. This method is
        non-blocking and is guaranteed not to throw an exception.
        :param gauge: The name of the gauge
        :type gauge: str
        :param value: The new reading of the gauge
        :type: float
        """
        if self._destroyed:
            return

        try:
            self._thread_pool_executor.submit(self._delegate.gauge, gauge, value)
        except:
            self._logger.exception('Exception caught submitting gauge metric')


class CacheBasedMetrics(Metrics):
    def __init__(self, metrics_cache):
        """A Metrics implementation that uses a MetricsCache to keep track of the metrics
        information.
        :param metrics_cache: The metrics cache
        :type metrics_cache: MetricsCache
        """
        self._metrics_cache = metrics_cache

    def gauge(self, gauge, value):
        self._metrics_cache.set_gague(gauge, value)

    def time(self, operation, time_in_ms):
        self._metrics_cache.increment_latency_bucket_counter(operation,
                                                             get_latency_bucket_index(time_in_ms))

    def count(self, counter, delta):
        self._metrics_cache.increment_count(counter, delta)
