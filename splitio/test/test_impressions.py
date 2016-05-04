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
        self.some_time = mock.MagicMock()
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


class LoggerBasedTreatmentLogTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_feature_name = mock.MagicMock()
        self.some_treatment = mock.MagicMock()
        self.some_time = mock.MagicMock()
        self.treatment_log = LoggerBasedTreatmentLog()
        self.logger_mock = self.patch_object(self.treatment_log, '_logger')

    def test_log_calls_logger_info(self):
        """Tests that log calls logger info"""
        self.treatment_log.log(self.some_key, self.some_feature_name, self.some_treatment,
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

    def test_call_appends_impression_to_feature_entry(self):
        """Tests that _log appends an impression to the feature name entry in the impressions
        dictionary"""
        self.treatment_log._log(self.some_key, self.some_feature_name, self.some_treatment,
                                self.some_time)
        impressions = self.treatment_log._impressions
        impressions.__getitem__.assert_called_once_with(self.some_feature_name)
        impressions.__getitem__.return_value.append.assert_called_once_with(
            Impression(key=self.some_key, feature_name=self.some_feature_name,
                       treatment=self.some_treatment, time=self.some_time))


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
        self.timer_refresh_mock = self.patch(
            'splitio.impressions.SelfUpdatingTreatmentLog._timer_refresh')

    def test_start_calls_timer_refresh_if_stopped_true(self):
        """Test that start calls _timer_refresh if stopped is True"""
        self.treatment_log.stopped = True
        self.treatment_log.start()
        self.timer_refresh_mock.assert_called_once_with(self.treatment_log)

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
        self.timer_mock = self.patch('splitio.impressions.Timer')
        self.thread_mock = self.patch('splitio.impressions.Thread')
        self.some_treatment_log = mock.MagicMock()
        self.some_treatment_log._stopped = False

    def test_doesnt_create_thread_if_stopped(self):
        """Test that _timer_refresh doesn't create a worker thread if it is stopped"""
        self.some_treatment_log._stopped = True
        SelfUpdatingTreatmentLog._timer_refresh(self.some_treatment_log)
        self.thread_mock.assert_not_called()

    def test_doesnt_create_timer_if_stopped(self):
        """Test that _timer_refresh doesn't refresh the timer it is stopped"""
        self.some_treatment_log._stopped = True
        SelfUpdatingTreatmentLog._timer_refresh(self.some_treatment_log)
        self.timer_mock.assert_not_called()

    def test_creates_thread(self):
        """Test that _timer_refresh creates a worker thread if it is not stopped"""
        SelfUpdatingTreatmentLog._timer_refresh(self.some_treatment_log)
        self.thread_mock.assert_called_once_with(
            target=SelfUpdatingTreatmentLog._update_impressions, args=(self.some_treatment_log,))

    def test_starts_thread(self):
        """Test that _timer_refresh starts the worker thread if it is not stopped"""
        SelfUpdatingTreatmentLog._timer_refresh(self.some_treatment_log)
        self.thread_mock.return_value.start.assert_called_once_with()

    def test_creates_timer_with_fixed_interval(self):
        """Test that _timer_refresh creates a timer with fixed interval if it isn't callable if it
        is not stopped"""
        self.some_treatment_log._interval = mock.NonCallableMagicMock()
        SelfUpdatingTreatmentLog._timer_refresh(self.some_treatment_log)
        self.timer_mock.assert_called_once_with(self.some_treatment_log._interval,
                                                SelfUpdatingTreatmentLog._timer_refresh,
                                                (self.some_treatment_log,))

    def test_creates_timer_with_randomized_interval(self):
        """Test that _timer_refresh creates a timer with interval return value if it is callable
        and it is not stopped"""
        SelfUpdatingTreatmentLog._timer_refresh(self.some_treatment_log)
        self.timer_mock.assert_called_once_with(self.some_treatment_log._interval.return_value,
                                                SelfUpdatingTreatmentLog._timer_refresh,
                                                (self.some_treatment_log,))

    def test_creates_even_if_worker_thread_raises_exception(self):
        """Test that _timer_refresh creates a timer even if an exception is raised creating the
        worker thread"""
        self.thread_mock.side_effect = Exception()
        SelfUpdatingTreatmentLog._timer_refresh(self.some_treatment_log)
        self.timer_mock.assert_called_once_with(self.some_treatment_log._interval.return_value,
                                                SelfUpdatingTreatmentLog._timer_refresh,
                                                (self.some_treatment_log,))

    def test_starts_timer(self):
        """Test that _timer_refresh starts the timer if it is not stopped"""
        SelfUpdatingTreatmentLog._timer_refresh(self.some_treatment_log)
        self.timer_mock.return_value.start.assert_called_once_with()

    def test_stopped_if_timer_raises_exception(self):
        """Test that _timer_refresh stops the refresh if an exception is raise setting up the timer
        """
        self.timer_mock.side_effect = Exception
        SelfUpdatingTreatmentLog._timer_refresh(self.some_treatment_log)
        self.assertTrue(self.some_treatment_log.stopped)


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
            AsyncTreatmentLog._log_fn, self.some_delegate_treatment_log, self.some_key,
            self.some_feature_name, self.some_treatment, self.some_time)

    def test_log_doenst_raise_exceptions_if_submit_does(self):
        """Tests that log doesn't raise exceptions when submit does"""
        self.thread_pool_executor_mock.return_value.submit.side_effect = Exception()
        try:
            self.treatment_log.log(self.some_key, self.some_feature_name, self.some_treatment,
                                   self.some_time)
        except:
            self.fail('Unexpected exception raised')

    def test_log_fn_calls_log_on_delegate(self):
        """Tests that _log_fn calls log on the delegate treatment log"""
        AsyncTreatmentLog._log_fn(self.some_delegate_treatment_log, self.some_key,
                                  self.some_feature_name, self.some_treatment, self.some_time)
        self.some_delegate_treatment_log.log.assert_called_once_with(self.some_key,
                                                                     self.some_feature_name,
                                                                     self.some_treatment,
                                                                     self.some_time)
