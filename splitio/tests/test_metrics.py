"""Unit tests for the metrics module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase

from splitio.metrics import (LatencyTracker, InMemoryMetrics, build_metrics_counter_data,
                             build_metrics_times_data, build_metrics_gauge_data, ApiMetrics,
                             AsyncMetrics, get_latency_bucket_index)
from splitio.tests.utils import MockUtilsMixin


class LatencyTrackerFindIndexTests(TestCase):
    def setUp(self):
        self.latency_tracker = LatencyTracker()

    def test_works_with_0(self):
        """Test that _find_index works with 0"""
        self.assertEqual(0, get_latency_bucket_index(0))

    def test_works_with_max(self):
        """Test that _find_index works with max_latency"""
        self.assertEqual(22, get_latency_bucket_index(7481828))

    def test_works_with_values_over_max(self):
        """Test that _find_index works with values_over_max_latency"""
        self.assertEqual(22, get_latency_bucket_index(7481829))
        self.assertEqual(22, get_latency_bucket_index(8481829))

    def test_works_with_values_between_0_and_max(self):
        """Test that _find_index works with values between 0 and max"""
        self.assertEqual(0, get_latency_bucket_index(500))
        self.assertEqual(0, get_latency_bucket_index(1000))
        self.assertEqual(1, get_latency_bucket_index(1250))
        self.assertEqual(1, get_latency_bucket_index(1500))
        self.assertEqual(2, get_latency_bucket_index(2000))
        self.assertEqual(2, get_latency_bucket_index(2250))
        self.assertEqual(3, get_latency_bucket_index(3000))
        self.assertEqual(3, get_latency_bucket_index(3375))
        self.assertEqual(4, get_latency_bucket_index(4000))
        self.assertEqual(4, get_latency_bucket_index(5063))
        self.assertEqual(5, get_latency_bucket_index(6000))
        self.assertEqual(5, get_latency_bucket_index(7594))
        self.assertEqual(6, get_latency_bucket_index(10000))
        self.assertEqual(6, get_latency_bucket_index(11391))
        self.assertEqual(7, get_latency_bucket_index(15000))
        self.assertEqual(7, get_latency_bucket_index(17086))
        self.assertEqual(8, get_latency_bucket_index(20000))
        self.assertEqual(8, get_latency_bucket_index(25629))
        self.assertEqual(9, get_latency_bucket_index(30000))
        self.assertEqual(9, get_latency_bucket_index(38443))
        self.assertEqual(10, get_latency_bucket_index(50000))
        self.assertEqual(10, get_latency_bucket_index(57665))
        self.assertEqual(11, get_latency_bucket_index(80000))
        self.assertEqual(11, get_latency_bucket_index(86498))
        self.assertEqual(12, get_latency_bucket_index(100000))
        self.assertEqual(12, get_latency_bucket_index(129746))
        self.assertEqual(13, get_latency_bucket_index(150000))
        self.assertEqual(13, get_latency_bucket_index(194620))
        self.assertEqual(14, get_latency_bucket_index(200000))
        self.assertEqual(14, get_latency_bucket_index(291929))
        self.assertEqual(15, get_latency_bucket_index(300000))
        self.assertEqual(15, get_latency_bucket_index(437894))
        self.assertEqual(16, get_latency_bucket_index(500000))
        self.assertEqual(16, get_latency_bucket_index(656841))
        self.assertEqual(17, get_latency_bucket_index(800000))
        self.assertEqual(17, get_latency_bucket_index(985261))
        self.assertEqual(18, get_latency_bucket_index(1000000))
        self.assertEqual(18, get_latency_bucket_index(1477892))
        self.assertEqual(19, get_latency_bucket_index(2000000))
        self.assertEqual(19, get_latency_bucket_index(2216838))
        self.assertEqual(20, get_latency_bucket_index(2500000))
        self.assertEqual(20, get_latency_bucket_index(3325257))
        self.assertEqual(21, get_latency_bucket_index(4000000))
        self.assertEqual(21, get_latency_bucket_index(4987885))
        self.assertEqual(22, get_latency_bucket_index(6000000))


class LatencyTrackerTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_millis = 1000
        self.some_micros = 1000000
        self.some_latencies = list(range(23))
        self.latency_tracker = LatencyTracker()
        self.get_latency_bucket_index_mock = self.patch(
            'splitio.metrics.get_latency_bucket_index', return_value=5)

    def test_add_latency_millis_calls_get_latency_bucket_index(self):
        """Test that add_latency_millis calls _find_index"""
        self.latency_tracker.add_latency_millis(self.some_millis)
        self.get_latency_bucket_index_mock.assert_called_once_with(self.some_millis * 1000)

    def test_add_latency_millis_sets_right_element(self):
        """Test that add_latency_millis adds 1 to the right element"""
        self.latency_tracker.add_latency_millis(self.some_millis)
        self.assertListEqual([0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                             self.latency_tracker._latencies)

    def test_add_latency_micros_calls_get_latency_bucket_index(self):
        """Test that add_latency_micros calls _find_index"""
        self.latency_tracker.add_latency_micros(self.some_micros)
        self.get_latency_bucket_index_mock.assert_called_once_with(self.some_micros)

    def test_add_latency_micros_sets_right_element(self):
        """Test that add_latency_micros adds 1 to the right element"""
        self.latency_tracker.add_latency_micros(self.some_micros)
        self.assertListEqual([0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                             self.latency_tracker._latencies)

    def test_clear_resets_latencies(self):
        """Test that clear resets latencies"""
        self.latency_tracker._latencies = self.some_latencies
        self.latency_tracker.clear()
        self.assertListEqual([0] * 23, self.latency_tracker._latencies)


class LatencyTrackerGetBucketTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_latency_millis = 1000
        self.some_latency_micros = 1000000
        self.some_latencies = [mock.MagicMock() for _ in range(23)]
        self.latency_tracker = LatencyTracker(latencies=self.some_latencies)
        self.get_latency_bucket_index_mock = self.patch(
            'splitio.metrics.get_latency_bucket_index', return_value=5)

    def test_get_bucket_for_latency_millis_calls_get_latency_bucket_index(self):
        """Test that get_bucket_for_latency_millis calls _find_index"""
        self.latency_tracker.get_bucket_for_latency_millis(self.some_latency_millis)
        self.get_latency_bucket_index_mock.assert_called_once_with(self.some_latency_millis * 1000)

    def test_get_bucket_for_latency_millis_returns_right_element(self):
        """Test that get_bucket_for_latency_millis returns the right element"""
        self.assertEqual(self.some_latencies[self.get_latency_bucket_index_mock.return_value],
                         self.latency_tracker.get_bucket_for_latency_millis(
                             self.some_latency_millis))

    def test_get_bucket_for_latency_micros_calls_get_latency_bucket_index(self):
        """Test that get_bucket_for_latency_micros calls _find_index"""
        self.latency_tracker.get_bucket_for_latency_micros(self.some_latency_micros)
        self.get_latency_bucket_index_mock.assert_called_once_with(self.some_latency_micros)

    def test_get_bucket_for_latency_micros_returns_right_element(self):
        """Test that get_bucket_for_latency_micros returns the right element"""
        self.assertEqual(self.some_latencies[self.get_latency_bucket_index_mock.return_value],
                         self.latency_tracker.get_bucket_for_latency_micros(
                             self.some_latency_micros))


class InMemoryMetricsCountTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_counter = mock.MagicMock()
        self.some_delta = 5
        self.rlock_mock = self.patch('splitio.metrics.RLock')
        self.arrow_mock = self.patch('splitio.metrics.arrow')
        self.arrow_mock.utcnow.return_value.timestamp = 1234567
        self.defaultdict_side_effect = [mock.MagicMock(), mock.MagicMock(), mock.MagicMock()]
        self.defaultdict_side_effect[0].__getitem__.return_value = 3
        self.defaultdict_mock = self.patch('splitio.metrics.defaultdict',
                                           side_effect=self.defaultdict_side_effect)
        self.metrics = InMemoryMetrics()
        self.update_count_mock = self.patch_object(self.metrics, 'update_count')

    def test_counter_not_set_if_ignore_metrics_is_true(self):
        """Test that the counter is not set if ignore metrics is true"""
        self.metrics._ignore_metrics = True
        self.metrics.count(self.some_counter, self.some_delta)
        self.defaultdict_side_effect[0].__setitem__.assert_not_called()

    def test_call_count_not_increased_if_ignore_metrics_is_true(self):
        """Test that the call count is not increased if ignore metrics is true"""
        self.metrics._ignore_metrics = True
        self.metrics.count(self.some_counter, self.some_delta)
        self.assertEqual(0, self.metrics._count_call_count)

    def test_increases_counter_by_delta(self):
        """Test that the counter is increased by delta"""
        self.metrics.count(self.some_counter, self.some_delta)
        self.defaultdict_side_effect[0].__setitem__.assert_called_once_with(
            self.some_counter,
            self.defaultdict_side_effect[0].__getitem__.return_value + self.some_delta)

    def test_increases_call_count(self):
        """Test that the call counte is increased by one"""
        self.metrics.count(self.some_counter, self.some_delta)
        self.assertEqual(1, self.metrics._count_call_count)

    def test_that_if_no_update_conditions_are_met_update_count_not_called(self):
        """Test that if neither update conditions are met, update_count is not called"""
        self.metrics.count(self.some_counter, self.some_delta)
        self.update_count_mock.assert_not_called()

    def test_that_if_no_update_conditions_are_met_call_count_not_reset(self):
        """Test that if neither update conditions are met, the call count is not reset"""
        self.metrics.count(self.some_counter, self.some_delta)
        self.assertLess(0, self.metrics._count_call_count)

    def test_update_count_called_if_max_call_count_reached(self):
        """Test that update_count is called if max call count is reached"""
        self.metrics._max_call_count = 5
        self.metrics._count_call_count = 4
        self.metrics.count(self.some_counter, self.some_delta)
        self.update_count_mock.assert_called_once_with()

    def test_call_count_reset_if_max_call_count_reached(self):
        """Test that call count is reset if max call count is reached"""
        self.metrics._max_call_count = 5
        self.metrics._count_call_count = 4
        self.metrics.count(self.some_counter, self.some_delta)
        self.assertEqual(0, self.metrics._count_call_count)

    def test_update_count_not_called_if_max_call_count_reached_and_ignore_metrics_is_true(self):
        """Test that update_count is not called if max call count is reached but ignore_metrics
        is True"""
        self.metrics._max_call_count = 5
        self.metrics._count_call_count = 4
        self.metrics._ignore_metrics = True
        self.metrics.count(self.some_counter, self.some_delta)
        self.update_count_mock.assert_not_called()

    def test_call_count_not_reset_if_max_call_count_reached_and_ignore_metrics_is_true(self):
        """Test that call count is not reset if max call count is reached but ignore_metrics is
        True"""
        self.metrics._max_call_count = 5
        self.metrics._count_call_count = 4
        self.metrics._ignore_metrics = True
        self.metrics.count(self.some_counter, self.some_delta)
        self.assertEqual(4, self.metrics._count_call_count)

    def test_update_count_called_if_max_time_between_calls_reached(self):
        """Test that update_count is called if max time between calls reached"""
        self.metrics._max_time_between_calls = 10
        self.metrics._count_last_call_time = 100
        self.arrow_mock.utcnow.return_value.timestamp = 1000
        self.metrics.count(self.some_counter, self.some_delta)
        self.update_count_mock.assert_called_once_with()

    def test_call_count_reset_if_max_time_between_calls_reached(self):
        """Test that call count is reset if max time between calls reached"""
        self.metrics._max_time_between_calls = 10
        self.metrics._count_last_call_time = 100
        self.arrow_mock.utcnow.return_value.timestamp = 1000
        self.metrics.count(self.some_counter, self.some_delta)
        self.assertEqual(0, self.metrics._count_call_count)

    def test_update_count_not_called_max_time_beween_calls_reached_and_ignore_metrics_is_true(self):
        """Test that update_count is not called if max time between calls is reached but
        ignore_metrics is True"""
        self.metrics._max_time_between_calls = 10
        self.metrics._count_last_call_time = 100
        self.arrow_mock.utcnow.return_value.timestamp = 1000
        self.metrics._ignore_metrics = True
        self.metrics.count(self.some_counter, self.some_delta)
        self.update_count_mock.assert_not_called()

    def test_call_count_not_reset_max_time_between_calls_reached_and_ignore_metrics_is_true(self):
        """Test that call count is not reset if max time between calls is reached but
        ignore_metrics is True"""
        self.metrics._count_call_count = 4
        self.metrics._max_time_between_calls = 10
        self.metrics._count_last_call_time = 100
        self.arrow_mock.utcnow.return_value.timestamp = 1000
        self.metrics._ignore_metrics = True
        self.metrics.count(self.some_counter, self.some_delta)
        self.assertEqual(4, self.metrics._count_call_count)


class InMemoryMetricsTimeTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_operation = mock.MagicMock()
        self.some_time_in_ms = mock.MagicMock()
        self.rlock_mock = self.patch('splitio.metrics.RLock')
        self.arrow_mock = self.patch('splitio.metrics.arrow')
        self.arrow_mock.utcnow.return_value.timestamp = 1234567
        self.defaultdict_side_effect = [mock.MagicMock(), mock.MagicMock(), mock.MagicMock()]
        self.latency_tracker_mock = self.defaultdict_side_effect[1].__getitem__.return_value
        self.defaultdict_mock = self.patch('splitio.metrics.defaultdict',
                                           side_effect=self.defaultdict_side_effect)
        self.metrics = InMemoryMetrics()
        self.update_time_mock = self.patch_object(self.metrics, 'update_time')

    def test_add_latency_millis_not_called_if_ignore_metrics_is_true(self):
        """Test that the add_latency_millis is not calledif ignore metrics is true"""
        self.metrics._ignore_metrics = True
        self.metrics.time(self.some_operation, self.some_time_in_ms)
        self.defaultdict_side_effect[1].__getitem__.return_value.\
            add_latency_millis.assert_not_called()

    def test_call_count_not_increased_if_ignore_metrics_is_true(self):
        """Test that the call count is not increased if ignore metrics is true"""
        self.metrics._ignore_metrics = True
        self.metrics.time(self.some_operation, self.some_time_in_ms)
        self.assertEqual(0, self.metrics._time_call_count)

    def test_calls_add_latency_millis(self):
        """Test that the add_latency_millis is called"""
        self.metrics.time(self.some_operation, self.some_time_in_ms)
        self.defaultdict_side_effect[1].__getitem__.return_value.\
            add_latency_millis.assert_called_once_with(self.some_time_in_ms)

    def test_increases_call_count(self):
        """Test that the call count is increased by one"""
        self.metrics.time(self.some_operation, self.some_time_in_ms)
        self.assertEqual(1, self.metrics._time_call_count)

    def test_that_if_no_update_conditions_are_met_update_count_not_called(self):
        """Test that if neither update conditions are met, update_count is not called"""
        self.metrics.time(self.some_operation, self.some_time_in_ms)
        self.update_time_mock.assert_not_called()

    def test_that_if_no_update_conditions_are_met_call_count_not_reset(self):
        """Test that if neither update conditions are met, the call count is not reset"""
        self.metrics.time(self.some_operation, self.some_time_in_ms)
        self.assertLess(0, self.metrics._time_call_count)

    def test_update_count_called_if_max_call_count_reached(self):
        """Test that update_count is called if max call count is reached"""
        self.metrics._max_call_count = 5
        self.metrics._time_call_count = 4
        self.metrics.time(self.some_operation, self.some_time_in_ms)
        self.update_time_mock.assert_called_once_with()

    def test_call_count_reset_if_max_call_count_reached(self):
        """Test that call count is reset if max call count is reached"""
        self.metrics._max_call_count = 5
        self.metrics._time_call_count = 4
        self.metrics.time(self.some_operation, self.some_time_in_ms)
        self.assertEqual(0, self.metrics._time_call_count)

    def test_update_count_not_called_if_max_call_count_reached_and_ignore_metrics_is_true(self):
        """Test that update_count is not called if max call count is reached but ignore_metrics
        is True"""
        self.metrics._max_call_count = 5
        self.metrics._time_call_count = 4
        self.metrics._ignore_metrics = True
        self.metrics.time(self.some_operation, self.some_time_in_ms)
        self.update_time_mock.assert_not_called()

    def test_call_count_not_reset_if_max_call_count_reached_and_ignore_metrics_is_true(self):
        """Test that call count is not reset if max call count is reached but ignore_metrics is
        True"""
        self.metrics._max_call_count = 5
        self.metrics._time_call_count = 4
        self.metrics._ignore_metrics = True
        self.metrics.time(self.some_operation, self.some_time_in_ms)
        self.assertEqual(4, self.metrics._time_call_count)

    def test_update_count_called_if_max_time_between_calls_reached(self):
        """Test that update_count is called if max time between calls reached"""
        self.metrics._max_time_between_calls = 10
        self.metrics._time_last_call_time = 100
        self.arrow_mock.utcnow.return_value.timestamp = 1000
        self.metrics.time(self.some_operation, self.some_time_in_ms)
        self.update_time_mock.assert_called_once_with()

    def test_call_count_reset_if_max_time_between_calls_reached(self):
        """Test that call count is reset if max time between calls reached"""
        self.metrics._max_time_between_calls = 10
        self.metrics._time_last_call_time = 100
        self.arrow_mock.utcnow.return_value.timestamp = 1000
        self.metrics.time(self.some_operation, self.some_time_in_ms)
        self.assertEqual(0, self.metrics._time_call_count)

    def test_update_count_not_called_max_time_beween_calls_reached_and_ignore_metrics_is_true(self):
        """Test that update_count is not called if max time between calls is reached but
        ignore_metrics is True"""
        self.metrics._max_time_between_calls = 10
        self.metrics._time_last_call_time = 100
        self.arrow_mock.utcnow.return_value.timestamp = 1000
        self.metrics._ignore_metrics = True
        self.metrics.time(self.some_operation, self.some_time_in_ms)
        self.update_time_mock.assert_not_called()

    def test_call_count_not_reset_max_time_between_calls_reached_and_ignore_metrics_is_true(self):
        """Test that call count is not reset if max time between calls is reached but
        ignore_metrics is True"""
        self.metrics._time_call_count = 4
        self.metrics._max_time_between_calls = 10
        self.metrics._time_last_call_time = 100
        self.arrow_mock.utcnow.return_value.timestamp = 1000
        self.metrics._ignore_metrics = True
        self.metrics.time(self.some_operation, self.some_time_in_ms)
        self.assertEqual(4, self.metrics._time_call_count)


class InMemoryMetricsGaugeTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_gauge = mock.MagicMock()
        self.some_value = mock.MagicMock()
        self.rlock_mock = self.patch('splitio.metrics.RLock')
        self.arrow_mock = self.patch('splitio.metrics.arrow')
        self.arrow_mock.utcnow.return_value.timestamp = 1234567
        self.defaultdict_side_effect = [mock.MagicMock(), mock.MagicMock(), mock.MagicMock()]
        self.defaultdict_mock = self.patch('splitio.metrics.defaultdict',
                                           side_effect=self.defaultdict_side_effect)
        self.metrics = InMemoryMetrics()
        self.update_gauge_mock = self.patch_object(self.metrics, 'update_gauge')

    def test_value_not_set_if_ignore_metrics_is_true(self):
        """Test that the value is not set if ignore metrics is true"""
        self.metrics._ignore_metrics = True
        self.metrics.gauge(self.some_gauge, self.some_value)
        self.defaultdict_side_effect[2].__setitem__.assert_not_called()

    def test_call_count_not_increased_if_ignore_metrics_is_true(self):
        """Test that the call count is not increased if ignore metrics is true"""
        self.metrics._ignore_metrics = True
        self.metrics.gauge(self.some_gauge, self.some_value)
        self.assertEqual(0, self.metrics._count_call_count)

    def test_increases_counter_by_delta(self):
        """Test that the gauge is set to value"""
        self.metrics.gauge(self.some_gauge, self.some_value)
        self.defaultdict_side_effect[2].__setitem__.assert_called_once_with(
            self.some_gauge,
            self.some_value)

    def test_increases_call_count(self):
        """Test that the call count is increased by one"""
        self.metrics.gauge(self.some_gauge, self.some_value)
        self.assertEqual(1, self.metrics._gauge_call_count)

    def test_that_if_no_update_conditions_are_met_update_count_not_called(self):
        """Test that if neither update conditions are met, update_count is not called"""
        self.metrics.gauge(self.some_gauge, self.some_value)
        self.update_gauge_mock.assert_not_called()

    def test_that_if_no_update_conditions_are_met_call_count_not_reset(self):
        """Test that if neither update conditions are met, the call count is not reset"""
        self.metrics.gauge(self.some_gauge, self.some_value)
        self.assertLess(0, self.metrics._gauge_call_count)

    def test_update_count_called_if_max_call_count_reached(self):
        """Test that update_count is called if max call count is reached"""
        self.metrics._max_call_count = 5
        self.metrics._gauge_call_count = 4
        self.metrics.gauge(self.some_gauge, self.some_value)
        self.update_gauge_mock.assert_called_once_with()

    def test_call_count_reset_if_max_call_count_reached(self):
        """Test that call count is reset if max call count is reached"""
        self.metrics._max_call_count = 5
        self.metrics._gauge_call_count = 4
        self.metrics.gauge(self.some_gauge, self.some_value)
        self.assertEqual(0, self.metrics._gauge_call_count)

    def test_update_count_not_called_if_max_call_count_reached_and_ignore_metrics_is_true(self):
        """Test that update_count is not called if max call count is reached but ignore_metrics
        is True"""
        self.metrics._max_call_count = 5
        self.metrics._gauge_call_count = 4
        self.metrics._ignore_metrics = True
        self.metrics.gauge(self.some_gauge, self.some_value)
        self.update_gauge_mock.assert_not_called()

    def test_call_count_not_reset_if_max_call_count_reached_and_ignore_metrics_is_true(self):
        """Test that call count is not reset if max call count is reached but ignore_metrics is
        True"""
        self.metrics._max_call_count = 5
        self.metrics._gauge_call_count = 4
        self.metrics._ignore_metrics = True
        self.metrics.gauge(self.some_gauge, self.some_value)
        self.assertEqual(4, self.metrics._gauge_call_count)

    def test_update_count_called_if_max_time_between_calls_reached(self):
        """Test that update_count is called if max time between calls reached"""
        self.metrics._max_time_between_calls = 10
        self.metrics._gauge_last_call_time = 100
        self.arrow_mock.utcnow.return_value.timestamp = 1000
        self.metrics.gauge(self.some_gauge, self.some_value)
        self.update_gauge_mock.assert_called_once_with()

    def test_call_count_reset_if_max_time_between_calls_reached(self):
        """Test that call count is reset if max time between calls reached"""
        self.metrics._max_time_between_calls = 10
        self.metrics._gauge_last_call_time = 100
        self.arrow_mock.utcnow.return_value.timestamp = 1000
        self.metrics.gauge(self.some_gauge, self.some_value)
        self.assertEqual(0, self.metrics._gauge_call_count)

    def test_update_count_not_called_max_time_beween_calls_reached_and_ignore_metrics_is_true(self):
        """Test that update_count is not called if max time between calls is reached but
        ignore_metrics is True"""
        self.metrics._max_time_between_calls = 10
        self.metrics._gauge_last_call_time = 100
        self.arrow_mock.utcnow.return_value.timestamp = 1000
        self.metrics._ignore_metrics = True
        self.metrics.gauge(self.some_gauge, self.some_value)
        self.update_gauge_mock.assert_not_called()

    def test_call_count_not_reset_max_time_between_calls_reached_and_ignore_metrics_is_true(self):
        """Test that call count is not reset if max time between calls is reached but
        ignore_metrics is True"""
        self.metrics._gauge_call_count = 4
        self.metrics._max_time_between_calls = 10
        self.metrics._gauge_last_call_time = 100
        self.arrow_mock.utcnow.return_value.timestamp = 1000
        self.metrics._ignore_metrics = True
        self.metrics.gauge(self.some_gauge, self.some_value)
        self.assertEqual(4, self.metrics._gauge_call_count)


class BuildMetricsCounterDataTests(TestCase):
    def test_works_with_empty_data(self):
        """Test that build_metrics_counter_data works with empty data"""
        self.assertListEqual([], build_metrics_counter_data(dict()))

    def test_works_with_non_empty_data(self):
        """Tests that build_metrics_counter_data works with non-empty data"""
        count_metrics = {'some_name': mock.MagicMock(), 'some_other_name': mock.MagicMock()}
        self.assertListEqual(
            [{'name': 'some_name', 'delta': count_metrics['some_name']},
             {'name': 'some_other_name', 'delta': count_metrics['some_other_name']}],
            sorted(build_metrics_counter_data(count_metrics), key=lambda d: d['name'])
        )


class BuildMetricsTimeDataTests(TestCase):
    def test_works_with_empty_data(self):
        """Test that build_metrics_time_data works with empty data"""
        self.assertListEqual([], build_metrics_times_data(dict()))

    def test_works_with_non_empty_data(self):
        """Tests that build_metrics_counter_data works with non-empty data"""
        times_metrics = {'some_name': mock.MagicMock(), 'some_other_name': mock.MagicMock()}
        self.assertListEqual(
            [{'name': 'some_name',
              'latencies': times_metrics['some_name'].get_latencies.return_value},
             {'name': 'some_other_name',
              'latencies': times_metrics['some_other_name'].get_latencies.return_value}],
            sorted(build_metrics_times_data(times_metrics), key=lambda d: d['name'])
        )


class BuildMetricsGagueDataTests(TestCase):
    def test_works_with_empty_data(self):
        """Test that build_metrics_gauge_data works with empty data"""
        self.assertListEqual([], build_metrics_gauge_data(dict()))

    def test_works_with_non_empty_data(self):
        """Tests that build_metrics_gauge_data works with non-empty data"""
        gauge_metrics = {'some_name': mock.MagicMock(), 'some_other_name': mock.MagicMock()}
        self.assertListEqual(
            [{'name': 'some_name', 'value': gauge_metrics['some_name']},
             {'name': 'some_other_name', 'value': gauge_metrics['some_other_name']}],
            sorted(build_metrics_gauge_data(gauge_metrics), key=lambda d: d['name'])
        )


class ApiMetricsTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_count_metrics = mock.MagicMock()
        self.some_time_metrics = mock.MagicMock()
        self.some_gauge_metrics = mock.MagicMock()
        self.some_api = mock.MagicMock()
        self.rlock_mock = self.patch('splitio.metrics.RLock')
        self.thread_pool_executor_mock = self.patch('splitio.metrics.ThreadPoolExecutor')
        self.build_metrics_counter_data_mock = self.patch(
            'splitio.metrics.build_metrics_counter_data')
        self.build_metrics_times_data_mock = self.patch(
            'splitio.metrics.build_metrics_times_data')
        self.build_metrics_gauge_data_mock = self.patch(
            'splitio.metrics.build_metrics_gauge_data')
        self.metrics = ApiMetrics(self.some_api, count_metrics=self.some_count_metrics,
                                  time_metrics=self.some_time_metrics,
                                  gauge_metrics=self.some_gauge_metrics)

    def test_update_count_fn_resets_count_metrics(self):
        """Test that update_count_fn resets count metrics"""
        self.metrics._update_count_fn()
        self.assertDictEqual(dict(), self.metrics._count_metrics)

    def test_update_count_fn_calls_metrics_counters(self):
        """Test that update_count_fn calls metrics_counters"""
        self.metrics._update_count_fn()
        self.some_api.metrics_counters.assert_called_once_with(
            self.build_metrics_counter_data_mock.return_value)

    def test_update_count_fn_sets_ignore_if_metrics_counters_raises_exception(self):
        """Test that if metrics_counters raises an exception ignore_metrics is set to True"""
        self.some_api.metrics_counters.side_effect = Exception()
        self.metrics._update_count_fn()
        self.assertTrue(self.metrics._ignore_metrics)

    def test_update_time_fn_resets_time_metrics(self):
        """Test that update_count_fn resets time metrics"""
        self.metrics._update_time_fn()
        self.assertDictEqual(dict(), self.metrics._time_metrics)

    def test_update_time_fn_calls_metrics_counters(self):
        """Test that update_count_fn calls metrics_times"""
        self.metrics._update_time_fn()
        self.some_api.metrics_times.assert_called_once_with(
            self.build_metrics_times_data_mock.return_value)

    def test_update_time_fn_sets_ignore_if_metrics_counters_raises_exception(self):
        """Test that if metrics_times raises an exception ignore_metrics is set to True"""
        self.some_api.metrics_times.side_effect = Exception()
        self.metrics._update_time_fn()
        self.assertTrue(self.metrics._ignore_metrics)

    def test_update_gauge_fn_resets_time_metrics(self):
        """Test that _update_gauge_fn resets gauge metrics"""
        self.metrics._update_gauge_fn()
        self.assertDictEqual(dict(), self.metrics._gauge_metrics)

    def test_update_gauge_fn_calls_metrics_counters(self):
        """Test that _update_gauge_fn calls metrics_times"""
        self.metrics._update_gauge_fn()
        self.some_api.metrics_gauge.assert_called_once_with(
            self.build_metrics_gauge_data_mock.return_value)

    def test_update_gauge_fn_sets_ignore_if_metrics_counters_raises_exception(self):
        """Test that if metrics_gauge raises an exception ignore_metrics is set to True"""
        self.some_api.metrics_gauge.side_effect = Exception()
        self.metrics._update_gauge_fn()
        self.assertTrue(self.metrics._ignore_metrics)

    def test_update_count_calls_submit(self):
        """Test that update_count calls thread pool executor submit"""
        self.metrics.update_count()
        self.thread_pool_executor_mock.return_value.submit.assert_called_once_with(
            self.metrics._update_count_fn)

    def test_update_count_doesnt_raise_exceptions(self):
        """Test that update_count doesn't raise an exception when submit does"""
        self.thread_pool_executor_mock.return_value.submit.side_effect = Exception()

        try:
            self.metrics.update_count()
        except:
            self.fail('Unexpected exception raised')

    def test_update_time_calls_submit(self):
        """Test that update_time calls thread pool executor submit"""
        self.metrics.update_time()
        self.thread_pool_executor_mock.return_value.submit.assert_called_once_with(
            self.metrics._update_time_fn)

    def test_update_time_doesnt_raise_exceptions(self):
        """Test that update_time doesn't raise an exception when submit does"""
        self.thread_pool_executor_mock.return_value.submit.side_effect = Exception()

        try:
            self.metrics.update_time()
        except:
            self.fail('Unexpected exception raised')

    def test_update_gauge_calls_submit(self):
        """Test that update_gauge calls thread pool executor submit"""
        self.metrics.update_gauge()
        self.thread_pool_executor_mock.return_value.submit.assert_called_once_with(
            self.metrics._update_gauge_fn)

    def test_update_gauge_doesnt_raise_exceptions(self):
        """Test that update_gauge doesn't raise an exception when submit does"""
        self.thread_pool_executor_mock.return_value.submit.side_effect = Exception()

        try:
            self.metrics.update_gauge()
        except:
            self.fail('Unexpected exception raised')


class AsyncMetricsTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_counter = mock.MagicMock()
        self.some_delta = mock.MagicMock()
        self.some_operation = mock.MagicMock()
        self.some_time_in_ms = mock.MagicMock()
        self.some_gauge = mock.MagicMock()
        self.some_value = mock.MagicMock()
        self.some_delegate_metrics = mock.MagicMock()
        self.thread_pool_executor_mock = self.patch('splitio.metrics.ThreadPoolExecutor')
        self.metrics = AsyncMetrics(self.some_delegate_metrics)

    def test_count_calls_submit_with_delegate_count(self):
        """Test that count calls submit with the delegate count"""
        self.metrics.count(self.some_counter, self.some_delta)
        self.thread_pool_executor_mock.return_value.submit.assert_called_once_with(
            self.some_delegate_metrics.count, self.some_counter, self.some_delta)

    def test_count_doesnt_raise_exceptions_if_submit_does(self):
        """Test that count doesnt't raise an exception even if submit does"""
        self.thread_pool_executor_mock.return_value.submit.side_effect = Exception()
        try:
            self.metrics.count(self.some_counter, self.some_delta)
        except:
            self.fail('Unexpected exception raised')

    def test_time_calls_submit_with_delegate_time(self):
        """Test that time calls submit with the delegate time"""
        self.metrics.time(self.some_operation, self.some_time_in_ms)
        self.thread_pool_executor_mock.return_value.submit.assert_called_once_with(
            self.some_delegate_metrics.time, self.some_operation, self.some_time_in_ms)

    def test_time_doesnt_raise_exceptions_if_submit_does(self):
        """Test that time doesnt't raise an exception even if submit does"""
        self.thread_pool_executor_mock.return_value.submit.side_effect = Exception()
        try:
            self.metrics.time(self.some_operation, self.some_time_in_ms)
        except:
            self.fail('Unexpected exception raised')

    def test_guage_calls_submit_with_delegate_guage(self):
        """Test that gauge calls submit with the delegate gauge"""
        self.metrics.gauge(self.some_gauge, self.some_value)
        self.thread_pool_executor_mock.return_value.submit.assert_called_once_with(
            self.some_delegate_metrics.gauge, self.some_gauge, self.some_value)

    def test_gauge_doesnt_raise_exceptions_if_submit_does(self):
        """Test that gauge doesnt't raise an exception even if submit does"""
        self.thread_pool_executor_mock.return_value.submit.side_effect = Exception()
        try:
            self.metrics.gauge(self.some_gauge, self.some_value)
        except:
            self.fail('Unexpected exception raised')
