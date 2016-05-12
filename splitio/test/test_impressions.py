"""Unit tests for the impressions module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase

from splitio.impressions import (Impression, build_impressions_data, TreatmentLog,
                                 LoggerBasedTreatmentLog, InMemoryTreatmentLog,
                                 CacheBasedTreatmentLog, SelfUpdatingTreatmentLog,
                                 AsyncTreatmentLog)
from splitio.test.utils import MockUtilsMixin


class BuildImpressionsDataTests(TestCase):
    def setUp(self):
        self.some_feature = 'feature_0'
        self.some_other_feature = 'feature_1'
        self.some_impression_0 = Impression(key=mock.MagicMock(), feature_name=self.some_feature,
                                            treatment=mock.MagicMock(), time=mock.MagicMock())
        self.some_impression_1 = Impression(key=mock.MagicMock(),
                                            feature_name=self.some_other_feature,
                                            treatment=mock.MagicMock(), time=mock.MagicMock())
        self.some_impression_2 = Impression(key=mock.MagicMock(),
                                            feature_name=self.some_other_feature,
                                            treatment=mock.MagicMock(), time=mock.MagicMock())

    def test_build_impressions_data_works(self):
        """Tests that build_impressions_data works"""
        result = build_impressions_data(
            {self.some_feature: [self.some_impression_0],
             self.some_other_feature: [self.some_impression_1, self.some_impression_2]})

        self.assertIsInstance(result, list)
        self.assertEqual(2, len(result))

        result = sorted(result, key=lambda d: d['testName'])

        self.assertDictEqual({
            'testName': self.some_feature,
            'keyImpressions': [
                {
                    'keyName': self.some_impression_0.key,
                    'treatment': self.some_impression_0.treatment,
                    'time': self.some_impression_0.time
                }
            ]
        }, result[0])
        self.assertDictEqual({
            'testName': self.some_other_feature,
            'keyImpressions': [
                {
                    'keyName': self.some_impression_1.key,
                    'treatment': self.some_impression_1.treatment,
                    'time': self.some_impression_1.time
                },
                {
                    'keyName': self.some_impression_2.key,
                    'treatment': self.some_impression_2.treatment,
                    'time': self.some_impression_2.time
                }
            ]
        }, result[1])

    def test_build_impressions_data_skipts_features_with_no_impressions(self):
        """Tests that build_impressions_data skips features with no impressions"""
        result = build_impressions_data(
            {self.some_feature: [self.some_impression_0],
             self.some_other_feature: []})

        self.assertIsInstance(result, list)
        self.assertEqual(1, len(result))

        self.assertDictEqual({
            'testName': self.some_feature,
            'keyImpressions': [
                {
                    'keyName': self.some_impression_0.key,
                    'treatment': self.some_impression_0.treatment,
                    'time': self.some_impression_0.time
                }
            ]
        }, result[0])


class TreatmentLogTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_feature_name = mock.MagicMock()
        self.some_treatment = mock.MagicMock()
        self.some_time = 123456
        self.treatment_log = TreatmentLog()
        self.log_mock = self.patch_object(self.treatment_log, '_log')

    def test_ignore_impressions_false_by_default(self):
        """Tests that ignore impressions is False by default"""
        self.assertFalse(self.treatment_log.ignore_impressions)

    def test_fetch_all_and_clear_returns_empty_dict(self):
        """Tests that the default implementation of fetch_all_and_clear returns an empty dict"""
        self.assertEqual(dict(), self.treatment_log.fetch_all_and_clear())

    def test_log_calls_internal_log_if_ignore_impressions_is_false(self):
        """Tests that log calls _log if ignore impressions is False"""
        self.treatment_log.ignore_impressions = False
        self.treatment_log.log(self.some_key, self.some_feature_name, self.some_treatment,
                               self.some_time)
        self.log_mock.assert_called_once_with(self.some_key, self.some_feature_name,
                                              self.some_treatment, self.some_time)

    def test_log_doesnt_call_internal_log_if_ignore_impressions_is_true(self):
        """Tests that log doesn't call _log if ignore impressions is True"""
        self.treatment_log.ignore_impressions = True
        self.treatment_log.log(self.some_key, self.some_feature_name, self.some_treatment,
                               self.some_time)
        self.log_mock.assert_not_called()

    def test_log_doesnt_call_internal_log_if_key_is_none(self):
        """Tests that log doesn't call _log if key is None"""
        self.treatment_log.log(None, self.some_feature_name, self.some_treatment, self.some_time)
        self.log_mock.assert_not_called()

    def test_log_doesnt_call_internal_log_if_feature_name_is_none(self):
        """Tests that log doesn't call _log if feature name is None"""
        self.treatment_log.log(self.some_key, None, self.some_treatment, self.some_time)
        self.log_mock.assert_not_called()

    def test_log_doesnt_call_internal_log_if_treatment_is_none(self):
        """Tests that log doesn't call _log if treatment is None"""
        self.treatment_log.log(self.some_key, self.some_feature_name, None, self.some_time)
        self.log_mock.assert_not_called()

    def test_log_doesnt_call_internal_log_if_time_is_none(self):
        """Tests that log doesn't call _log if time is None"""
        self.treatment_log.log(self.some_key, self.some_feature_name, self.some_treatment, None)
        self.log_mock.assert_not_called()

    def test_log_doesnt_call_internal_log_if_time_is_lt_0(self):
        """Tests that log doesn't call _log if time is less than 0"""
        self.treatment_log.log(self.some_key, self.some_feature_name, self.some_treatment, -1)
        self.log_mock.assert_not_called()


class LoggerBasedTreatmentLogTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_feature_name = mock.MagicMock()
        self.some_treatment = mock.MagicMock()
        self.some_time = mock.MagicMock()
        self.logger_mock = self.patch('splitio.impressions.logging.getLogger').return_value
        self.treatment_log = LoggerBasedTreatmentLog()

    def test_log_calls_logger_info(self):
        """Tests that log calls logger info"""
        self.treatment_log._log(self.some_key, self.some_feature_name, self.some_treatment,
                                self.some_time)
        self.logger_mock.info.assert_called_once_with(mock.ANY, self.some_feature_name,
                                                      self.some_key, self.some_treatment,
                                                      self.some_time)


class InMemoryTreatmentLogTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_feature_name = mock.MagicMock()
        self.some_treatment = mock.MagicMock()
        self.some_time = mock.MagicMock()
        self.deepcopy_mock = self.patch('splitio.impressions.deepcopy')
        self.defaultdict_mock_side_effect = [
            mock.MagicMock(),  # during __init__
            mock.MagicMock()   # afterwards
        ]
        self.defaultdict_mock = self.patch('splitio.impressions.defaultdict',
                                           side_effect=self.defaultdict_mock_side_effect)
        self.rlock_mock = self.patch('splitio.impressions.RLock')
        self.treatment_log = InMemoryTreatmentLog()
        self.notify_eviction_mock = self.patch_object(self.treatment_log, '_notify_eviction')

    def test_impressions_is_defaultdict(self):
        """Tests that impressions is a defaultdict"""
        self.assertEqual(self.defaultdict_mock_side_effect[0], self.treatment_log._impressions)

    def test_fetch_all_and_clear_calls_deepcopy(self):
        """Tests that fetch all and clear calls deepcopy on impressions"""
        self.treatment_log.fetch_all_and_clear()
        self.deepcopy_mock.assert_called_once_with(self.defaultdict_mock_side_effect[0])

    def test_fetch_all_and_clear_returns_deepcopy_of_impressions(self):
        """Tests that fetch all and clear returns deepcopy of impressions"""
        self.assertEqual(self.deepcopy_mock.return_value, self.treatment_log.fetch_all_and_clear())

    def test_fetch_all_and_clear_clears_impressions(self):
        """Tests that fetch all and clear clears impressions"""
        self.treatment_log.fetch_all_and_clear()
        self.assertEqual(self.defaultdict_mock_side_effect[1], self.treatment_log._impressions)

    def test_log_calls_appends_impression_to_feature_entry(self):
        """Tests that _log appends an impression to the feature name entry in the impressions
        dictionary"""
        self.treatment_log._log(self.some_key, self.some_feature_name, self.some_treatment,
                                self.some_time)
        impressions = self.treatment_log._impressions
        impressions.__getitem__.assert_called_once_with(self.some_feature_name)
        impressions.__getitem__.return_value.append.assert_called_once_with(
            Impression(key=self.some_key, feature_name=self.some_feature_name,
                       treatment=self.some_treatment, time=self.some_time))

    def test_log_resets_impressions_if_max_count_reached(self):
        """Tests that _log resets impressions if max_count is reached"""
        self.treatment_log._max_count = 5
        impressions = self.treatment_log._impressions
        impressions.__getitem__.return_value.__len__.return_value = 10
        self.treatment_log._log(self.some_key, self.some_feature_name, self.some_treatment,
                                self.some_time)
        impressions.__setitem__.assert_called_once_with(
            self.some_feature_name, [Impression(key=self.some_key,
                                                feature_name=self.some_feature_name,
                                                treatment=self.some_treatment,
                                                time=self.some_time)])

    def test_log_calls__notify_eviction_if_max_count_reached(self):
        """Tests that _log calls _notify_eviction if max_count is reached"""
        self.treatment_log._max_count = 5
        impressions = self.treatment_log._impressions
        impressions.__getitem__.return_value.__len__.return_value = 10
        self.treatment_log._log(self.some_key, self.some_feature_name, self.some_treatment,
                                self.some_time)
        self.notify_eviction_mock.assert_called_once_with(self.some_feature_name,
                                                          impressions.__getitem__.return_value)


class CacheBasedTreatmentLogTests(TestCase):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_feature_name = mock.MagicMock()
        self.some_treatment = mock.MagicMock()
        self.some_time = mock.MagicMock()
        self.some_impressions_cache = mock.MagicMock()
        self.treatment_log = CacheBasedTreatmentLog(self.some_impressions_cache)

    def test_fetch_all_and_clear_calls_cache_fetch_all_and_clear(self):
        """Tests that fetch_all_and_clear calls the cache fetch_all_and_clear"""
        self.treatment_log.fetch_all_and_clear()
        self.some_impressions_cache.fetch_all_and_clear.assert_called_once_with()

    def test_fetch_all_and_clear_returns_result_cache_fetch_all_and_clear(self):
        """Tests that fetch_all_and_clear returns the result of calling the cache
        fetch_all_and_clear"""
        self.assertEqual(self.some_impressions_cache.fetch_all_and_clear.return_value,
                         self.treatment_log.fetch_all_and_clear())

    def test_log_calls_cache_add_impression(self):
        """Tests that _log calls add_impression on cache"""
        self.treatment_log._log(self.some_key, self.some_feature_name, self.some_treatment,
                                self.some_time)
        self.some_impressions_cache.add_impression(
            Impression(key=self.some_key, feature_name=self.some_feature_name,
                       treatment=self.some_treatment, time=self.some_time))


class SelfUpdatingTreatmentLogTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_api = mock.MagicMock()
        self.some_interval = mock.MagicMock()
        self.treatment_log = SelfUpdatingTreatmentLog(self.some_api, interval=self.some_interval)
        self.timer_refresh_mock = self.patch_object(self.treatment_log, '_timer_refresh')

    def test_start_calls_timer_refresh_if_stopped_true(self):
        """Test that start calls _timer_refresh if stopped is True"""
        self.treatment_log.stopped = True
        self.treatment_log.start()
        self.timer_refresh_mock.assert_called_once_with()

    def test_start_sets_stopped_to_false_if_stopped_true(self):
        """Test that start sets stopped to False if stopped is True before"""
        self.treatment_log.stopped = True
        self.treatment_log.start()
        self.assertFalse(self.treatment_log.stopped)

    def test_start_doesnt_call_timer_refresh_if_stopped_false(self):
        """Test that start doesn't call _timer_refresh if stopped is False"""
        self.treatment_log.stopped = False
        self.treatment_log.start()
        self.timer_refresh_mock.assert_not_called()


class SelfUpdatingTreatmentLogTimerRefreshTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_api = mock.MagicMock()
        self.some_interval = mock.MagicMock()
        self.timer_mock = self.patch('splitio.impressions.Timer')
        self.thread_pool_executor = self.patch('splitio.impressions.ThreadPoolExecutor')
        self.treatment_log = SelfUpdatingTreatmentLog(self.some_api, interval=self.some_interval)
        self.treatment_log.stopped = False

    def test_doesnt_call_submit_if_stopped(self):
        """Test that _timer_refresh doesn't call submit on the executor pool if it is stopped"""
        self.treatment_log._stopped = True
        self.treatment_log._timer_refresh()
        self.thread_pool_executor.return_value.submit.assert_not_called()

    def test_doesnt_create_timer_if_stopped(self):
        """Test that _timer_refresh doesn't refresh the timer it is stopped"""
        self.treatment_log._stopped = True
        self.treatment_log._timer_refresh()
        self.timer_mock.assert_not_called()

    def test_calls_submit(self):
        """Test that _timer_refresh calls submit on the executor pool if it is not stopped"""
        self.treatment_log._timer_refresh()
        self.thread_pool_executor.return_value.submit.assert_called_once_with(
            self.treatment_log._update_impressions)

    def test_creates_timer_with_fixed_interval(self):
        """Test that _timer_refresh creates a timer with fixed interval if it isn't callable if it
        is not stopped"""
        self.treatment_log._interval = mock.NonCallableMagicMock()
        self.treatment_log._timer_refresh()
        self.timer_mock.assert_called_once_with(self.treatment_log._interval,
                                                self.treatment_log._timer_refresh)

    def test_creates_timer_with_randomized_interval(self):
        """Test that _timer_refresh creates a timer with interval return value if it is callable
        and it is not stopped"""
        self.treatment_log._timer_refresh()
        self.timer_mock.assert_called_once_with(self.treatment_log._interval.return_value,
                                                self.treatment_log._timer_refresh)

    def test_creates_even_if_worker_thread_raises_exception(self):
        """Test that _timer_refresh creates a timer even if an exception is raised submiting to the
        executor pool"""
        self.thread_pool_executor.return_value.submit.side_effect = Exception()
        self.treatment_log._timer_refresh()
        self.timer_mock.assert_called_once_with(self.treatment_log._interval.return_value,
                                                self.treatment_log._timer_refresh)

    def test_starts_timer(self):
        """Test that _timer_refresh starts the timer if it is not stopped"""
        self.treatment_log._timer_refresh()
        self.timer_mock.return_value.start.assert_called_once_with()

    def test_stopped_if_timer_raises_exception(self):
        """Test that _timer_refresh stops the refresh if an exception is raise setting up the timer
        """
        self.timer_mock.side_effect = Exception
        self.treatment_log._timer_refresh()
        self.assertTrue(self.treatment_log.stopped)


class SelfUpdatingTreatmentLogNotifyEvictionTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_api = mock.MagicMock()
        self.some_feature_name = mock.MagicMock()
        self.some_feature_impressions = [mock.MagicMock()]
        self.thread_pool_executor_mock = self.patch('splitio.impressions.ThreadPoolExecutor')
        self.treatment_log = SelfUpdatingTreatmentLog(self.some_api)

    def test_doesnt_call_submit_if_feature_name_is_none(self):
        """Test that _notify_eviction doesn't call the executor submit if feature_name is None"""
        self.treatment_log._notify_eviction(None, self.some_feature_impressions)
        self.thread_pool_executor_mock.return_value.submit.assert_not_called()

    def test_doesnt_call_submit_if_feature_impressions_is_none(self):
        """Test that _notify_eviction doesn't call the executor submit if feature_impressions is
        None"""
        self.treatment_log._notify_eviction(self.some_feature_name, None)
        self.thread_pool_executor_mock.return_value.submit.assert_not_called()

    def test_doesnt_call_submit_if_feature_impressions_is_empty(self):
        """Test that _notify_eviction doesn't call the executor submit if feature_impressions is
        empty"""
        self.treatment_log._notify_eviction(self.some_feature_name, [])
        self.thread_pool_executor_mock.return_value.submit.assert_not_called()

    def test_calls_submit(self):
        """Test that _notify_eviction calls submit on the executor"""
        self.treatment_log._notify_eviction(self.some_feature_name, self.some_feature_impressions)
        self.thread_pool_executor_mock.return_value.submit.assert_called_once_with(
            self.treatment_log._update_evictions, self.some_feature_name,
            self.some_feature_impressions)


class SelfUpdatingTreatmentLogUpdateImpressionsTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_api = mock.MagicMock()
        self.some_interval = mock.MagicMock()
        self.build_impressions_data_mock = self.patch(
            'splitio.impressions.build_impressions_data',
            return_value=[mock.MagicMock(), mock.MagicMock()])
        self.treatment_log = SelfUpdatingTreatmentLog(self.some_api, interval=self.some_interval)
        self.fetch_all_and_clear_mock = self.patch_object(
            self.treatment_log, 'fetch_all_and_clear')

    def test_calls_fetch_all_and_clear(self):
        """Test that _update_impressions call fetch_all_and_clear"""
        self.treatment_log._update_impressions()
        self.fetch_all_and_clear_mock.assert_called_once_with()

    def test_calls_build_impressions_data(self):
        """Test that _update_impressions call build_impressions_data"""
        self.treatment_log._update_impressions()
        self.build_impressions_data_mock.assert_called_once_with(
            self.fetch_all_and_clear_mock.return_value)

    def test_calls_test_impressions(self):
        """Test that _update_impressions call test_impressions on the api"""
        self.treatment_log._update_impressions()
        self.some_api.test_impressions.assert_called_once_with(
            self.build_impressions_data_mock.return_value)

    def test_doesnt_call_test_impressions_with_empty_data(self):
        """Test that _update_impressions doesn't call test_impressions on the api"""
        self.build_impressions_data_mock.return_value = []
        self.treatment_log._update_impressions()
        self.some_api.test_impressions.assert_not_called()


class SelfUpdatingTreatmentLogUpdateEvictionsTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_api = mock.MagicMock()
        self.some_interval = mock.MagicMock()
        self.some_feature_name = mock.MagicMock()
        self.some_feature_impressions = [mock.MagicMock()]
        self.build_impressions_data_mock = self.patch(
            'splitio.impressions.build_impressions_data',
            return_value=[mock.MagicMock(), mock.MagicMock()])
        self.treatment_log = SelfUpdatingTreatmentLog(self.some_api, interval=self.some_interval)

    def test_calls_build_impressions_data(self):
        """Test that _update_evictions calls build_impressions_data_mock"""
        self.treatment_log._update_evictions(self.some_feature_name, self.some_feature_impressions)
        self.build_impressions_data_mock.assert_called_once_with(
            {self.some_feature_name: self.some_feature_impressions})

    def test_calls_test_impressions(self):
        """Test that _update_evictions calls test_impressions on the API client"""
        self.treatment_log._update_evictions(self.some_feature_name, self.some_feature_impressions)
        self.some_api.test_impressions.assert_called_once_with(
            self.build_impressions_data_mock.return_value)

    def test_doesnt_call_test_impressions_if_data_is_empty(self):
        """Test that _update_evictions calls test_impressions on the API client"""
        self.build_impressions_data_mock.return_value = []
        self.treatment_log._update_evictions(self.some_feature_name, self.some_feature_impressions)
        self.some_api.test_impressions.assert_not_called()

    def test_doesnt_raise_exceptions(self):
        """Test that _update_evictions doesn't raise exceptions when the API client does"""
        self.some_api.test_impressions.side_effect = Exception()
        try:
            self.treatment_log._update_evictions(self.some_feature_name,
                                                 self.some_feature_impressions)
        except:
            self.fail('Unexpected exception raised')


class AsyncTreatmentLogTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_feature_name = mock.MagicMock()
        self.some_treatment = mock.MagicMock()
        self.some_time = mock.MagicMock()
        self.some_max_workers = mock.MagicMock()
        self.some_delegate_treatment_log = mock.MagicMock()
        self.thread_pool_executor_mock = self.patch('splitio.impressions.ThreadPoolExecutor')
        self.treatment_log = AsyncTreatmentLog(self.some_delegate_treatment_log,
                                               max_workers=self.some_max_workers)

    def test_log_calls_thread_pool_executor_submit(self):
        """Tests that log calls submit on the thread pool executor"""
        self.treatment_log.log(self.some_key, self.some_feature_name, self.some_treatment,
                               self.some_time)
        self.thread_pool_executor_mock.return_value.submit.assert_called_once_with(
            self.some_delegate_treatment_log.log, self.some_key, self.some_feature_name,
            self.some_treatment, self.some_time)

    def test_log_doenst_raise_exceptions_if_submit_does(self):
        """Tests that log doesn't raise exceptions when submit does"""
        self.thread_pool_executor_mock.return_value.submit.side_effect = Exception()
        try:
            self.treatment_log.log(self.some_key, self.some_feature_name, self.some_treatment,
                                   self.some_time)
        except:
            self.fail('Unexpected exception raised')
