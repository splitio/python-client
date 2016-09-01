"""Unit tests for the tasks module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase

from splitio.tests.utils import MockUtilsMixin

from splitio.tasks import (report_impressions, report_metrics, update_segments, update_segment,\
    update_splits)


class ReportImpressionsTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.build_impressions_data_mock = self.patch(
            'splitio.tasks.build_impressions_data')
        self.some_impressions_cache = mock.MagicMock()
        self.some_impressions_cache.is_enabled.return_value = True
        self.some_api_sdk = mock.MagicMock()

    def test_calls_is_enabled(self):
        """Test that report_impressions call is_enabled"""
        report_impressions(self.some_impressions_cache, self.some_api_sdk)
        self.some_impressions_cache.is_enabled.assert_called_once_with()

    def test_doesnt_call_fetch_all_and_clear_if_disabled(self):
        """Test that report_impressions doesn't call fetch_all_and_clear if the cache is disabled"""
        self.some_impressions_cache.is_enabled.return_value = False
        report_impressions(self.some_impressions_cache, self.some_api_sdk)
        self.some_impressions_cache.fetch_all_and_clear.assert_not_called()

    def test_doesnt_call_test_impressions_if_disabled(self):
        """Test that report_impressions doesn't call test_impressions if the cache is disabled"""
        self.some_impressions_cache.is_enabled.return_value = False
        report_impressions(self.some_impressions_cache, self.some_api_sdk)
        self.some_api_sdk.test_impressions.assert_not_called()

    def test_calls_fetch_all_and_clear(self):
        """Test that report_impressions calls fetch_all_and_clear"""
        report_impressions(self.some_impressions_cache, self.some_api_sdk)
        self.some_impressions_cache.fetch_all_and_clear.assert_called_once_with()

    def test_calls_build_impressions_data(self):
        """Test that report_impressions calls build_impressions_data_mock"""
        report_impressions(self.some_impressions_cache, self.some_api_sdk)
        self.build_impressions_data_mock.assert_called_once_with(
            self.some_impressions_cache.fetch_all_and_clear.return_value)

    def test_doesnt_call_test_impressions_if_data_is_empty(self):
        """Test that report_impressions doesn't call test_impressions if build_impressions_data
        returns an empty list"""
        self.build_impressions_data_mock.return_value = []
        report_impressions(self.some_impressions_cache, self.some_api_sdk)
        self.some_api_sdk.test_impressions.assert_not_called()

    def test_calls_test_impressions(self):
        """Test that report_impressions calls test_impression with the result of
        build_impressions_data"""
        self.build_impressions_data_mock.return_value = [mock.MagicMock()]
        report_impressions(self.some_impressions_cache, self.some_api_sdk)
        self.some_api_sdk.test_impressions.assert_called_once_with(
            self.build_impressions_data_mock.return_value)

    def test_cache_disabled_if_exception_is_raised(self):
        """Test that report_impressions disables the cache if an exception is raised"""
        self.build_impressions_data_mock.side_effect = Exception()
        report_impressions(self.some_impressions_cache, self.some_api_sdk)
        self.some_impressions_cache.disable.assert_called_once_with()


class ReportMetricsTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_metrics_cache = mock.MagicMock()
        self.some_metrics_cache.is_enabled.return_value = True
        self.some_api_sdk = mock.MagicMock()

    def test_doenst_call_fetch_all_and_clear_if_disabled(self):
        """Test that report_metrics doesn't call fetch_all_and_clear if the cache is disabled"""
        self.some_metrics_cache.is_enabled.return_value = False
        report_metrics(self.some_metrics_cache, self.some_api_sdk)
        self.some_metrics_cache.fetch_all_and_clear.assert_not_called()

    def test_doesnt_call_metrics_times_if_disabled(self):
        """Test that report_metrics doesn't call metrics_times if the cache is disabled"""
        self.some_metrics_cache.is_enabled.return_value = False
        report_metrics(self.some_metrics_cache, self.some_api_sdk)
        self.some_api_sdk.metrics_times.assert_not_called()

    def test_doesnt_call_metrics_counters_if_disabled(self):
        """Test that report_metrics doesn't call metrics_counters if the cache is disabled"""
        self.some_metrics_cache.is_enabled.return_value = False
        report_metrics(self.some_metrics_cache, self.some_api_sdk)
        self.some_api_sdk.metrics_counters.assert_not_called()

    def test_doesnt_call_metrics_gauge_if_disabled(self):
        """Test that report_metrics doesn't call metrics_gauge if the cache is disabled"""
        self.some_metrics_cache.is_enabled.return_value = False
        report_metrics(self.some_metrics_cache, self.some_api_sdk)
        self.some_api_sdk.metrics_gauge.assert_not_called()

    def test_doesnt_call_metrics_times_if_time_metrics_is_empty(self):
        """Test that report_metrics doesn't call metrics_times if time metrics are empty"""
        self.some_metrics_cache.fetch_all_and_clear.return_value = {'time': [],
                                                                    'count': mock.MagicMock(),
                                                                    'gauge': mock.MagicMock()}
        report_metrics(self.some_metrics_cache, self.some_api_sdk)
        self.some_api_sdk.metrics_times.assert_not_called()

    def test_calls_metrics_times(self):
        """Test that report_metrics calls metrics_times if time metrics are not empty"""
        self.some_metrics_cache.fetch_all_and_clear.return_value = {'time': [mock.MagicMock()],
                                                                    'count': mock.MagicMock(),
                                                                    'gauge': mock.MagicMock()}
        report_metrics(self.some_metrics_cache, self.some_api_sdk)
        self.some_api_sdk.metrics_times.assert_called_once_with(
            self.some_metrics_cache.fetch_all_and_clear.return_value['time'])

    def test_doesnt_call_metrics_counters_if_counter_metrics_is_empty(self):
        """Test that report_metrics doesn't call metrics_counters if counter metrics are empty"""
        self.some_metrics_cache.fetch_all_and_clear.return_value = {'time': mock.MagicMock(),
                                                                    'count': [],
                                                                    'gauge': mock.MagicMock()}
        report_metrics(self.some_metrics_cache, self.some_api_sdk)
        self.some_api_sdk.metrics_counters.assert_not_called()

    def test_calls_metrics_counters(self):
        """Test that report_metrics calls metrics_counters if counter metrics are not empty"""
        self.some_metrics_cache.fetch_all_and_clear.return_value = {'time': mock.MagicMock(),
                                                                    'count': [mock.MagicMock()],
                                                                    'gauge': mock.MagicMock()}
        report_metrics(self.some_metrics_cache, self.some_api_sdk)
        self.some_api_sdk.metrics_counters.assert_called_once_with(
            self.some_metrics_cache.fetch_all_and_clear.return_value['count'])

    def test_doesnt_call_metrics_gauge_if_gauge_metrics_is_empty(self):
        """Test that report_metrics doesn't call metrics_gauge if counter metrics are empty"""
        self.some_metrics_cache.fetch_all_and_clear.return_value = {'time': mock.MagicMock(),
                                                                    'count': mock.MagicMock(),
                                                                    'gauge': []}
        report_metrics(self.some_metrics_cache, self.some_api_sdk)
        self.some_api_sdk.metrics_gauge.assert_not_called()

    def test_calls_metrics_gauge(self):
        """Test that report_metrics calls metrics_gauge if gauge metrics are not empty"""
        self.some_metrics_cache.fetch_all_and_clear.return_value = {'time': mock.MagicMock(),
                                                                    'count': mock.MagicMock(),
                                                                    'gauge': [mock.MagicMock()]}
        report_metrics(self.some_metrics_cache, self.some_api_sdk)
        self.some_api_sdk.metrics_gauge.assert_called_once_with(
            self.some_metrics_cache.fetch_all_and_clear.return_value['gauge'])

    def test_disables_cache_if_exception_is_raised(self):
        """Test that report_metrics disables cache if exception is raised"""
        self.some_metrics_cache.fetch_all_and_clear.side_effect = Exception()
        report_metrics(self.some_metrics_cache, self.some_api_sdk)
        self.some_metrics_cache.disable.assert_called_once_with()


class UpdateSegmentsTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_segments_cache = mock.MagicMock()
        self.some_segments_cache.is_enabled.return_value = True
        self.some_segment_change_fetcher = mock.MagicMock()
        self.update_segment_mock = self.patch('splitio.tasks.update_segment')

    def test_doesnt_call_update_segment_if_cache_is_disabled(self):
        """Test that update_segments doesn't call update_segment if the segment cache is disabled"""
        self.some_segments_cache.is_enabled.return_value = False
        update_segments(self.some_segments_cache, self.some_segment_change_fetcher)
        self.update_segment_mock.assert_not_called()

    def test_doesnt_call_update_segment_if_there_are_no_registered_segments(self):
        """Test that update_segments doesn't call update_segment there are no registered segments"""
        self.some_segments_cache.get_registered_segments.return_value = []
        update_segments(self.some_segments_cache, self.some_segment_change_fetcher)
        self.update_segment_mock.assert_not_called()

    def test_calls_update_segment_for_each_registered_segment(self):
        """Test that update_segments calls update_segment on each registered segment"""
        some_segment_name = mock.MagicMock()
        some_other_segment_name = mock.MagicMock()
        self.some_segments_cache.get_registered_segments.return_value = [some_segment_name,
                                                                         some_other_segment_name]
        update_segments(self.some_segments_cache, self.some_segment_change_fetcher)
        self.assertListEqual([mock.call(self.some_segments_cache, some_segment_name,
                                        self.some_segment_change_fetcher),
                              mock.call(self.some_segments_cache, some_other_segment_name,
                                        self.some_segment_change_fetcher)],
                             self.update_segment_mock.call_args_list)

    def test_disables_cache_if_update_segment_raises_exception(self):
        """Test that update_segments disables segment_cache if update segment raises an exception"""
        self.update_segment_mock.side_effect = Exception()
        self.some_segments_cache.get_registered_segments.return_value = [mock.MagicMock()]
        update_segments(self.some_segments_cache, self.some_segment_change_fetcher)
        self.some_segments_cache.disable.assert_called_once_with()


class UpdateSegmentTests(TestCase):
    def setUp(self):
        self.some_segment_cache = mock.MagicMock()
        self.some_segment_cache.get_change_number.return_value = -1
        self.some_segment_name = mock.MagicMock()
        self.some_segment_change_fetcher = mock.MagicMock()
        self.some_segment_change_fetcher.fetch.side_effect = [  # Two updates
            {
                'name': 'some_segment_name',
                'added': ['user_id_6'],
                'removed': ['user_id_1', 'user_id_2'],
                'since': -1,
                'till': 1
            },
            {
                'name': 'some_segment_name',
                'added': ['user_id_7'],
                'removed': ['user_id_4'],
                'since': 1,
                'till': 2
            },
            {
                'name': 'some_segment_name',
                'added': [],
                'removed': [],
                'since': 2,
                'till': 2
            }
        ]

    def test_calls_get_change_number(self):
        """Test update_segment calls get_change_number on the segment cache"""
        update_segment(self.some_segment_cache, self.some_segment_name,
                       self.some_segment_change_fetcher)
        self.some_segment_cache.get_change_number.assert_called_once_with(self.some_segment_name)

    def test_calls_segment_change_fetcher_fetch(self):
        """Test that update_segment calls segment_change_fetcher's fetch until change numbers
        match"""
        update_segment(self.some_segment_cache, self.some_segment_name,
                       self.some_segment_change_fetcher)
        self.assertListEqual([mock.call(self.some_segment_name, -1),
                              mock.call(self.some_segment_name, 1),
                              mock.call(self.some_segment_name, 2)],
                             self.some_segment_change_fetcher.fetch.call_args_list)

    def test_calls_remove_keys_from_segment_for_all_removed_keys(self):
        """Test update_segment calls remove_keys_from_segment for keys removed on each update"""
        update_segment(self.some_segment_cache, self.some_segment_name,
                       self.some_segment_change_fetcher)
        self.assertListEqual([mock.call(self.some_segment_name, ['user_id_1', 'user_id_2']),
                              mock.call(self.some_segment_name, ['user_id_4'])],
                             self.some_segment_cache.remove_keys_from_segment.call_args_list)

    def test_calls_add_keys_to_segment_for_all_added_keys(self):
        """Test update_segment calls add_keys_to_segment for keys added on each update"""
        update_segment(self.some_segment_cache, self.some_segment_name,
                       self.some_segment_change_fetcher)
        self.assertListEqual([mock.call(self.some_segment_name, 1),
                              mock.call(self.some_segment_name, 2)],
                             self.some_segment_cache.set_change_number.call_args_list)

    def test_calls_set_change_number_for_updates(self):
        """Test update_segment calls set_change_number on each update"""
        update_segment(self.some_segment_cache, self.some_segment_name,
                       self.some_segment_change_fetcher)
        self.assertListEqual([mock.call(self.some_segment_name, ['user_id_6']),
                              mock.call(self.some_segment_name, ['user_id_7'])],
                             self.some_segment_cache.add_keys_to_segment.call_args_list)


class UpdateSplitsTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_split_cache = mock.MagicMock()
        self.some_split_cache.get_change_number.return_value = -1
        self.some_split_parser = mock.MagicMock()
        self.parse_side_effect = [mock.MagicMock(), mock.MagicMock(), mock.MagicMock()]
        self.some_split_parser.parse.side_effect = self.parse_side_effect
        self.some_split_change_fetcher = mock.MagicMock()
        self.some_split_change_fetcher.fetch.side_effect = [
            {
                'till': 1,
                'splits': [
                    {
                        'status': 'ACTIVE',
                        'name': 'some_split'
                    },
                    {
                        'status': 'ACTIVE',
                        'name': 'some_other_split'
                    }
                ]
            },
            {
                'till': 2,
                'splits': [
                    {
                        'status': 'ACTIVE',
                        'name': 'some_split'
                    },
                    {
                        'status': 'ARCHIVED',
                        'name': 'some_other_split'
                    }
                ]
            },
            {
                'till': 2,
                'splits': []
            }
        ]

    def test_calls_get_change_number(self):
        """Test that update_splits calls get_change_number on the split cache"""
        update_splits(self.some_split_cache, self.some_split_change_fetcher, self.some_split_parser)
        self.some_split_cache.get_change_number.assert_called_once_with()

    def test_calls_split_change_fetcher_fetch(self):
        """Test that update_splits calls split_change_fetcher's fetch method until change numbers
        match"""
        update_splits(self.some_split_cache, self.some_split_change_fetcher, self.some_split_parser)
        self.assertListEqual([mock.call(-1), mock.call(1), mock.call(2)],
                             self.some_split_change_fetcher.fetch.call_args_list)

    def test_calls_split_parser_parse(self):
        """Test that update_split calls split_parser's parse method on all active splits on each
        update"""
        update_splits(self.some_split_cache, self.some_split_change_fetcher, self.some_split_parser)
        self.assertListEqual([mock.call({'status': 'ACTIVE', 'name': 'some_split'}),
                              mock.call({'status': 'ACTIVE', 'name': 'some_other_split'}),
                              mock.call({'status': 'ACTIVE', 'name': 'some_split'})],
                             self.some_split_parser.parse.call_args_list)

    def test_calls_remove_split(self):
        """Test that update_split calls split_cache's remove_split method on archived splits"""
        update_splits(self.some_split_cache, self.some_split_change_fetcher, self.some_split_parser)
        self.some_split_cache.remove_split.assert_called_once_with('some_other_split')

    def test_calls_add_split(self):
        """Test that update_split calls split_cache's add_split method on active splits"""
        update_splits(self.some_split_cache, self.some_split_change_fetcher, self.some_split_parser)
        self.assertListEqual([mock.call('some_split', self.parse_side_effect[0]),
                              mock.call('some_other_split', self.parse_side_effect[1]),
                              mock.call('some_split', self.parse_side_effect[2])],
                             self.some_split_cache.add_split.call_args_list)

    def test_calls_set_change_number(self):
        """Test that update_split calls set_change_number on every update"""
        update_splits(self.some_split_cache, self.some_split_change_fetcher, self.some_split_parser)
        self.assertListEqual([mock.call(1), mock.call(2)],
                             self.some_split_cache.set_change_number.call_args_list)

    def test_disables_cache_on_exception(self):
        """Test that update_split calls disable on the split_cache when an exception is raised"""
        self.some_split_change_fetcher.fetch.side_effect = Exception()
        update_splits(self.some_split_cache, self.some_split_change_fetcher, self.some_split_parser)
        self.some_split_cache.disable.assert_called_once_with()
