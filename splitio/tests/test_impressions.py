"""Unit tests for the impressions module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase
from itertools import groupby

from splitio.impressions import (Impression, build_impressions_data, TreatmentLog,
                                 LoggerBasedTreatmentLog, InMemoryTreatmentLog,
                                 CacheBasedTreatmentLog, SelfUpdatingTreatmentLog,
                                 AsyncTreatmentLog)
from splitio.tests.utils import MockUtilsMixin

from splitio.tasks import report_impressions


class BuildImpressionsDataTests(TestCase):
    def setUp(self):
        self.some_feature = 'feature_0'
        self.some_other_feature = 'feature_1'
        self.some_impression_0 = Impression(matching_key=mock.MagicMock(), feature_name=self.some_feature,
                                          treatment=mock.MagicMock(), label=mock.MagicMock(),
                                          change_number=mock.MagicMock(), bucketing_key=mock.MagicMock(),
                                          time=mock.MagicMock())
        self.some_impression_1 = Impression(matching_key=mock.MagicMock(), feature_name=self.some_other_feature,
                                           treatment=mock.MagicMock(), label=mock.MagicMock(),
                                           change_number=mock.MagicMock(), bucketing_key=mock.MagicMock(),
                                           time=mock.MagicMock())
        self.some_impression_2 = Impression(matching_key=mock.MagicMock(), feature_name=self.some_other_feature,
                                           treatment=mock.MagicMock(), label=mock.MagicMock(),
                                           change_number=mock.MagicMock(), bucketing_key=mock.MagicMock(),
                                           time=mock.MagicMock())

    def test_build_impressions_data_works(self):
        """Tests that build_impressions_data works"""

        impressions = [self.some_impression_0, self.some_impression_1, self.some_impression_2]
        grouped_impressions = groupby(impressions, key=lambda impression: impression.feature_name)

        impression_dict = dict((feature_name, list(group)) for feature_name, group in grouped_impressions)

        result = build_impressions_data(impression_dict)

        self.assertIsInstance(result, list)
        self.assertEqual(2, len(result))

        result = sorted(result, key=lambda d: d['testName'])

        self.assertDictEqual({
            'testName': self.some_feature,
            'keyImpressions': [
                {
                    'keyName': self.some_impression_0.matching_key,
                    'treatment': self.some_impression_0.treatment,
                    'time': self.some_impression_0.time,
                    'changeNumber': self.some_impression_0.change_number,
                    'label': self.some_impression_0.label,
                    'bucketingKey': self.some_impression_0.bucketing_key
                }
            ]
        }, result[0])
        self.assertDictEqual({
            'testName': self.some_other_feature,
            'keyImpressions': [
                {
                    'keyName': self.some_impression_1.matching_key,
                    'treatment': self.some_impression_1.treatment,
                    'time': self.some_impression_1.time,
                    'changeNumber': self.some_impression_1.change_number,
                    'label': self.some_impression_1.label,
                    'bucketingKey': self.some_impression_1.bucketing_key
                },
                {
                    'keyName': self.some_impression_2.matching_key,
                    'treatment': self.some_impression_2.treatment,
                    'time': self.some_impression_2.time,
                    'changeNumber': self.some_impression_2.change_number,
                    'label': self.some_impression_2.label,
                    'bucketingKey': self.some_impression_2.bucketing_key
                }
            ]
        }, result[1])

    def test_build_impressions_data_skipts_features_with_no_impressions(self):
        """Tests that build_impressions_data skips features with no impressions"""

        grouped_impressions = groupby([self.some_impression_0],
                                      key=lambda impression: impression.feature_name)

        impression_dict = dict((feature_name, list(group)) for feature_name, group in grouped_impressions)

        result = build_impressions_data(impression_dict)

        self.assertIsInstance(result, list)
        self.assertEqual(1, len(result))

        self.assertDictEqual(
            {
                'testName': self.some_impression_0.feature_name,
                'keyImpressions': [
                        {
                            'keyName': self.some_impression_0.matching_key,
                            'treatment': self.some_impression_0.treatment,
                            'time': self.some_impression_0.time,
                            'changeNumber': self.some_impression_0.change_number,
                            'label': self.some_impression_0.label,
                            'bucketingKey': self.some_impression_0.bucketing_key
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

        self.some_label = mock.MagicMock()
        self.some_change_number = mock.MagicMock()
        self.some_impression = Impression(matching_key=self.some_key, feature_name=self.some_feature_name,
                                          treatment=self.some_treatment, label=self.some_label,
                                          change_number=self.some_change_number, bucketing_key=self.some_key,
                                          time=self.some_time)

    def test_log_doesnt_call_internal_log_if_key_is_none(self):
        """Tests that log doesn't call _log if key is None"""
        impression = Impression(matching_key=None, feature_name=self.some_feature_name,
                                treatment=self.some_treatment, label=self.some_label,
                                change_number=self.some_change_number, bucketing_key=self.some_key,
                                time=self.some_time)

        self.treatment_log.log(impression)
        self.log_mock.assert_not_called()

    def test_log_doesnt_call_internal_log_if_feature_name_is_none(self):
        """Tests that log doesn't call _log if feature name is None"""
        impression = Impression(matching_key=self.some_key, feature_name=None,
                              treatment=self.some_treatment, label=self.some_label,
                              change_number=self.some_change_number, bucketing_key=self.some_key,
                              time=self.some_time)
        self.treatment_log.log(impression)
        self.log_mock.assert_not_called()

    def test_log_doesnt_call_internal_log_if_treatment_is_none(self):
        """Tests that log doesn't call _log if treatment is None"""
        impression = Impression(matching_key=self.some_key, feature_name=self.some_feature_name,
                                treatment=None, label=self.some_label,
                                change_number=self.some_change_number, bucketing_key=self.some_key,
                                time=self.some_time)

        self.treatment_log.log(impression)
        self.log_mock.assert_not_called()

    def test_log_doesnt_call_internal_log_if_time_is_none(self):
        """Tests that log doesn't call _log if time is None"""
        impression = Impression(matching_key=self.some_key, feature_name=self.some_feature_name,
                                treatment=self.some_treatment, label=self.some_label,
                                change_number=self.some_change_number, bucketing_key=self.some_key,
                                time=None)

        self.treatment_log.log(impression)
        self.log_mock.assert_not_called()

    def test_log_doesnt_call_internal_log_if_time_is_lt_0(self):
        """Tests that log doesn't call _log if time is less than 0"""
        impression = Impression(matching_key=self.some_key, feature_name=self.some_feature_name,
                                treatment=self.some_treatment, label=self.some_label,
                                change_number=self.some_change_number, bucketing_key=self.some_key,
                                time=-1)

        self.treatment_log.log(impression)
        self.log_mock.assert_not_called()


class LoggerBasedTreatmentLogTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_feature_name = mock.MagicMock()
        self.some_treatment = mock.MagicMock()
        self.some_time = mock.MagicMock()
        self.logger_mock = self.patch('splitio.impressions.logging.getLogger').return_value
        self.treatment_log = LoggerBasedTreatmentLog()

        self.some_label = mock.MagicMock()
        self.some_change_number = mock.MagicMock()
        self.some_impression = Impression(matching_key=self.some_key, feature_name=self.some_feature_name,
                                          treatment=self.some_treatment, label=self.some_label,
                                          change_number=self.some_change_number, bucketing_key=self.some_key,
                                          time=self.some_time)

    def test_log_calls_logger_info(self):
        """Tests that log calls logger info"""
        self.treatment_log._log(self.some_impression)
        self.logger_mock.info.assert_called_once_with(mock.ANY, self.some_feature_name,
                                                      self.some_key, self.some_treatment,
                                                      self.some_time, self.some_label, self.some_change_number,
                                                      self.some_key)


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

        self.some_label = mock.MagicMock()
        self.some_change_number = mock.MagicMock()
        self.some_impression = Impression(matching_key=self.some_key, feature_name=self.some_feature_name,
                                          treatment=self.some_treatment, label=self.some_label,
                                          change_number=self.some_change_number, bucketing_key=self.some_key,
                                          time=self.some_time)

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
        self.treatment_log._log(self.some_impression)
        impressions = self.treatment_log._impressions
        impressions.__getitem__.assert_called_once_with(self.some_feature_name)
        impressions.__getitem__.return_value.append.assert_called_once_with(self.some_impression)

    def test_log_resets_impressions_if_max_count_reached(self):
        """Tests that _log resets impressions if max_count is reached"""
        self.treatment_log._max_count = 5
        impressions = self.treatment_log._impressions
        impressions.__getitem__.return_value.__len__.return_value = 10
        self.treatment_log._log(self.some_impression)
        impressions.__setitem__.assert_called_once_with(
            self.some_feature_name, [self.some_impression])

    def test_log_calls__notify_eviction_if_max_count_reached(self):
        """Tests that _log calls _notify_eviction if max_count is reached"""
        self.treatment_log._max_count = 5
        impressions = self.treatment_log._impressions
        impressions.__getitem__.return_value.__len__.return_value = 10
        self.treatment_log._log(self.some_impression)
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
        self.some_label = mock.MagicMock()
        self.some_change_number = mock.MagicMock()

        self.some_impression = Impression(matching_key=self.some_key, feature_name=self.some_feature_name,
                                          treatment=self.some_treatment, label=self.some_label,
                                          change_number=self.some_change_number, bucketing_key=self.some_key,
                                          time=self.some_time)

    def test_log_calls_cache_add_impression(self):
        """Tests that _log calls add_impression on cache"""
        self.treatment_log._log(self.some_impression)
        self.some_impressions_cache.add_impression(self.some_impression)


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

    def test_creates_timer_even_if_worker_thread_raises_exception(self):
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
        self.some_label = mock.MagicMock()
        self.some_change_number = mock.MagicMock()
        self.some_time = mock.MagicMock()
        self.some_max_workers = mock.MagicMock()
        self.some_delegate_treatment_log = mock.MagicMock()
        self.thread_pool_executor_mock = self.patch('splitio.impressions.ThreadPoolExecutor')
        self.treatment_log = AsyncTreatmentLog(self.some_delegate_treatment_log,
                                               max_workers=self.some_max_workers)

        self.some_impression = Impression(matching_key=self.some_key, feature_name=self.some_feature_name,
                                treatment=self.some_treatment, label=self.some_label,
                                change_number=self.some_change_number, bucketing_key=self.some_key,
                                time=self.some_time)

    def test_log_calls_thread_pool_executor_submit(self):
        """Tests that log calls submit on the thread pool executor"""
        self.treatment_log.log(self.some_impression)
        self.thread_pool_executor_mock.return_value.submit.assert_called_once_with(
            self.some_delegate_treatment_log.log, self.some_impression)

    def test_log_doesnt_raise_exceptions_if_submit_does(self):
        """Tests that log doesn't raise exceptions when submit does"""
        self.thread_pool_executor_mock.return_value.submit.side_effect = Exception()
        self.treatment_log.log(self.some_impression)
        #
        # try:
        # except:
        #     self.fail('Unexpected exception raised')


class TestImpressionListener(TestCase):
    """
    Tests for impression listener in "in-memory" and "uwsgi-cache" operation
    modes
    """

    def test_inmemory_impression_listener(self):
        some_api = mock.MagicMock()
        listener = mock.MagicMock()
        treatment_log = SelfUpdatingTreatmentLog(some_api, listener=listener)
        with mock.patch(
            'splitio.impressions.build_impressions_data',
            return_value=[1, 2, 3]
        ):
            treatment_log._update_evictions('some_feature', [])

        listener.assert_called_once_with([1, 2, 3])

    def test_uwsgi_impression_listener(self):
        impressions_cache = mock.MagicMock()
        impressions = {
            'testName': 'someTest',
            'keyImpressions': [1, 2, 3]
        }

        impressions_cache.fetch_all_and_clear.return_value = {
            'someTest': [1, 2, 3]
        }
        some_api = mock.MagicMock()
        listener = mock.MagicMock()
        with mock.patch(
            'splitio.tasks.build_impressions_data',
            return_value=impressions
        ):
            report_impressions(
                impressions_cache,
                some_api,
                listener=listener
            )

        listener.assert_called_with(impressions)
