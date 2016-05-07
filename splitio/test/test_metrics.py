"""Unit tests for the metrics module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase

from splitio.metrics import LatencyTracker, InMemoryMetrics
from splitio.test.utils import MockUtilsMixin


class LatencyTrackerFindIndexTests(TestCase):
    def setUp(self):
        self.latency_tracker = LatencyTracker()

    def test_works_with_0(self):
        """Test that _find_index works with 0"""
        self.assertEqual(0, self.latency_tracker._find_index(0))

    def test_works_with_max(self):
        """Test that _find_index works with max_latency"""
        self.assertEqual(22, self.latency_tracker._find_index(7481828))

    def test_works_with_values_over_max(self):
        """Test that _find_index works with values_over_max_latency"""
        self.assertEqual(22, self.latency_tracker._find_index(7481829))
        self.assertEqual(22, self.latency_tracker._find_index(8481829))

    def test_works_with_values_between_0_and_max(self):
        """Test that _find_index works with values between 0 and max"""
        self.assertEqual(0, self.latency_tracker._find_index(500))
        self.assertEqual(0, self.latency_tracker._find_index(1000))
        self.assertEqual(1, self.latency_tracker._find_index(1250))
        self.assertEqual(1, self.latency_tracker._find_index(1500))
        self.assertEqual(2, self.latency_tracker._find_index(2000))
        self.assertEqual(2, self.latency_tracker._find_index(2250))
        self.assertEqual(3, self.latency_tracker._find_index(3000))
        self.assertEqual(3, self.latency_tracker._find_index(3375))
        self.assertEqual(4, self.latency_tracker._find_index(4000))
        self.assertEqual(4, self.latency_tracker._find_index(5063))
        self.assertEqual(5, self.latency_tracker._find_index(6000))
        self.assertEqual(5, self.latency_tracker._find_index(7594))
        self.assertEqual(6, self.latency_tracker._find_index(10000))
        self.assertEqual(6, self.latency_tracker._find_index(11391))
        self.assertEqual(7, self.latency_tracker._find_index(15000))
        self.assertEqual(7, self.latency_tracker._find_index(17086))
        self.assertEqual(8, self.latency_tracker._find_index(20000))
        self.assertEqual(8, self.latency_tracker._find_index(25629))
        self.assertEqual(9, self.latency_tracker._find_index(30000))
        self.assertEqual(9, self.latency_tracker._find_index(38443))
        self.assertEqual(10, self.latency_tracker._find_index(50000))
        self.assertEqual(10, self.latency_tracker._find_index(57665))
        self.assertEqual(11, self.latency_tracker._find_index(80000))
        self.assertEqual(11, self.latency_tracker._find_index(86498))
        self.assertEqual(12, self.latency_tracker._find_index(100000))
        self.assertEqual(12, self.latency_tracker._find_index(129746))
        self.assertEqual(13, self.latency_tracker._find_index(150000))
        self.assertEqual(13, self.latency_tracker._find_index(194620))
        self.assertEqual(14, self.latency_tracker._find_index(200000))
        self.assertEqual(14, self.latency_tracker._find_index(291929))
        self.assertEqual(15, self.latency_tracker._find_index(300000))
        self.assertEqual(15, self.latency_tracker._find_index(437894))
        self.assertEqual(16, self.latency_tracker._find_index(500000))
        self.assertEqual(16, self.latency_tracker._find_index(656841))
        self.assertEqual(17, self.latency_tracker._find_index(800000))
        self.assertEqual(17, self.latency_tracker._find_index(985261))
        self.assertEqual(18, self.latency_tracker._find_index(1000000))
        self.assertEqual(18, self.latency_tracker._find_index(1477892))
        self.assertEqual(19, self.latency_tracker._find_index(2000000))
        self.assertEqual(19, self.latency_tracker._find_index(2216838))
        self.assertEqual(20, self.latency_tracker._find_index(2500000))
        self.assertEqual(20, self.latency_tracker._find_index(3325257))
        self.assertEqual(21, self.latency_tracker._find_index(4000000))
        self.assertEqual(21, self.latency_tracker._find_index(4987885))
        self.assertEqual(22, self.latency_tracker._find_index(6000000))


class LatencyTrackerTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_millis = 1000
        self.some_micros = 1000000
        self.some_latencies = list(range(23))
        self.latency_tracker = LatencyTracker()
        self.find_index_mock = self.patch_object(self.latency_tracker, '_find_index',
                                                 return_value=5)

    def test_add_latency_millis_calls_find_index(self):
        """Test that add_latency_millis calls _find_index"""
        self.latency_tracker.add_latency_millis(self.some_millis)
        self.find_index_mock.assert_called_once_with(self.some_millis * 1000)

    def test_add_latency_millis_sets_right_element(self):
        """Test that add_latency_millis adds 1 to the right element"""
        self.latency_tracker.add_latency_millis(self.some_millis)
        self.assertListEqual([0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                             self.latency_tracker._latencies)

    def test_add_latency_micros_calls_find_index(self):
        """Test that add_latency_micros calls _find_index"""
        self.latency_tracker.add_latency_micros(self.some_micros)
        self.find_index_mock.assert_called_once_with(self.some_micros)

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
        self.find_index_mock = self.patch_object(self.latency_tracker, '_find_index',
                                                 return_value=5)

    def test_get_bucket_for_latency_millis_calls_find_index(self):
        """Test that get_bucket_for_latency_millis calls _find_index"""
        self.latency_tracker.get_bucket_for_latency_millis(self.some_latency_millis)
        self.find_index_mock.assert_called_once_with(self.some_latency_millis * 1000)

    def test_get_bucket_for_latency_millis_returns_right_element(self):
        """Test that get_bucket_for_latency_millis returns the right element"""
        self.assertEqual(self.some_latencies[self.find_index_mock.return_value],
                         self.latency_tracker.get_bucket_for_latency_millis(
                             self.some_latency_millis))

    def test_get_bucket_for_latency_micros_calls_find_index(self):
        """Test that get_bucket_for_latency_micros calls _find_index"""
        self.latency_tracker.get_bucket_for_latency_micros(self.some_latency_micros)
        self.find_index_mock.assert_called_once_with(self.some_latency_micros)

    def test_get_bucket_for_latency_micros_returns_right_element(self):
        """Test that get_bucket_for_latency_micros returns the right element"""
        self.assertEqual(self.some_latencies[self.find_index_mock.return_value],
                         self.latency_tracker.get_bucket_for_latency_micros(
                             self.some_latency_micros))


class InMemoryMetricsCountTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_counter = mock.MagicMock()
        self.some_delta = 5
        self.rlock_mock = self.patch('splitio.metrics.RLock')
        self.arrow_mock = self.patch('splitio.metrics.arrow')
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
        """Test that the counter is increased by delta"""
        self.metrics.gauge(self.some_gauge, self.some_value)
        self.defaultdict_side_effect[2].__setitem__.assert_called_once_with(
            self.some_gauge,
            self.some_value)

    def test_increases_call_count(self):
        """Test that the call counte is increased by one"""
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

