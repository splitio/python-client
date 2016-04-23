"""Unit tests for the segments module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase

from splitio.segments import (Segment, SelfRefreshingSegmentFetcher, SelfRefreshingSegment,
                              SegmentChangeFetcher, ApiSegmentChangeFetcher)
from splitio.test.utils import MockUtilsMixin


class SegmentTests(TestCase):
    def setUp(self):
        self.some_name = 'some_name'
        self.some_key_set = ['user_id_1', 'user_id_2', 'user_id_3']
        self.key_set_mock = mock.MagicMock()
        self.some_key = 'some_key'

    def test_empty_segment_by_default(self):
        """Tests that the segments are empty by default"""
        segment = Segment(self.some_name)
        self.assertEqual(0, len(segment._key_set))

    def test_key_set_is_initialized(self):
        """Tests that the segments can be initialized to a specific key_set"""
        segment = Segment(self.some_name, self.some_key_set)
        self.assertSetEqual(set(self.some_key_set), segment._key_set)

    def test_contains_calls_in(self):
        """Tests that the segments can be initialized to a specific key_set"""
        segment = Segment(self.some_name)
        segment._key_set = self.key_set_mock

        segment.contains(self.some_key)

        self.key_set_mock.__contains__.assert_called_once_with(self.some_key)


class SelfRefreshingSegmentFetcherTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_name = mock.MagicMock()
        self.some_interval = mock.MagicMock()
        self.some_max_workers = mock.MagicMock()
        self.some_segment = mock.MagicMock()

        self.segment_change_fetcher_mock = mock.MagicMock()
        self.self_refreshing_segment_mock = self.patch('splitio.segments.SelfRefreshingSegment')

        self.segment_fetcher = SelfRefreshingSegmentFetcher(self.segment_change_fetcher_mock,
                                                            interval=self.some_interval,
                                                            max_workers=self.some_max_workers)
        self.segments_mock = mock.MagicMock()
        self.segment_fetcher._segments = self.segments_mock

    def test_cached_segment_are_returned(self):
        """Tests that if a segment is cached, it is returned"""
        self.segments_mock.__contains__.return_value = True
        self.segments_mock.__getitem__.return_value = self.some_segment

        segment = self.segment_fetcher.get_segment(self.some_name)

        self.assertEqual(self.some_segment, segment)

    def test_if_segment_is_cached_no_new_segments_are_created(self):
        """Tests that if a segment is cached no calls to the segment constructor are made"""
        self.segments_mock.__contains__.return_value = True
        self.segments_mock.__getitem__.return_value = self.some_segment

        self.segment_fetcher.get_segment(self.some_name)

        self.self_refreshing_segment_mock.assert_not_called()

    def test_if_segment_is_not_cached_constructor_is_called(self):
        """Tests that if a segment is not cached the SelfRefreshingSegment constructor is called"""
        self.segments_mock.__contains__.return_value = False

        self.segment_fetcher.get_segment(self.some_name)

        self.self_refreshing_segment_mock.assert_called_once_with(self.some_name,
                                                                  self.segment_change_fetcher_mock,
                                                                  self.segment_fetcher._executor,
                                                                  self.some_interval)

    def test_if_segment_is_not_cached_new_segment_inserted_on_cache(self):
        """Tests that if a segment is not cached the a new segment is inserted into the cache"""
        self.segments_mock.__contains__.return_value = False

        self.segment_fetcher.get_segment(self.some_name)

        self.segments_mock.__setitem__.assert_called_once_with(
            self.some_name, self.self_refreshing_segment_mock.return_value)

    def test_start_is_called_in_new_segment(self):
        """Tests that start() is called on the newly created segment"""
        self.segments_mock.__contains__.return_value = False

        self.segment_fetcher.get_segment(self.some_name)

        self.self_refreshing_segment_mock.return_value.start.assert_called_once_with()

    def test_new_segment_is_returned(self):
        """Tests that the newly created segment is returned"""
        self.segments_mock.__contains__.return_value = False

        segment = self.segment_fetcher.get_segment(self.some_name)

        self.assertEqual(self.self_refreshing_segment_mock.return_value, segment)


class SelfRefreshingSegmentTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_name = mock.MagicMock()
        self.some_segment_change_fetcher = mock.MagicMock()
        self.some_executor = mock.MagicMock()
        self.some_interval = mock.MagicMock()

        self.rlock_mock = self.patch('splitio.segments.RLock')

        self.refresh_segment_mock = self.patch(
            'splitio.segments.SelfRefreshingSegment._refresh_segment')
        self.timer_refresh_mock = self.patch(
            'splitio.segments.SelfRefreshingSegment._timer_refresh')

        self.segment = SelfRefreshingSegment(self.some_name, self.some_segment_change_fetcher,
                                             self.some_executor, self.some_interval)

    def test_greedy_by_default(self):
        """Tests that _greedy is set to True by default"""

        self.assertTrue(self.segment._greedy)

    def test_force_refresh_calls_refresh_segment(self):
        """Tests that force_refresh calls _refresh_segment"""

        self.segment.force_refresh()

        self.refresh_segment_mock.assert_called_once_with(self.segment)

    def test_start_calls_timer_refresh_if_not_already_started(self):
        """Tests that start calls _refresh_timer if it hasn't already been started"""
        self.segment._stopped = True

        self.segment.start()

        self.timer_refresh_mock.assert_called_once_with(self.segment)

    def test_start_sets_stopped_to_false(self):
        """Tests that start sets stopped to False if it hasn't been started"""
        self.segment._stopped = True

        self.segment.start()

        self.assertFalse(self.segment.stopped)

    def test_start_doesnt_call_timer_refresh_if_already_started(self):
        """Tests that start doesn't call _refresh_timer if it has already been started"""
        self.segment._stopped = False

        self.segment.start()

        self.timer_refresh_mock.assert_not_called()


class SelfRefreshingSegmentRefreshSegmentTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.segment = mock.MagicMock()
        self.segment._change_number = -1
        self.segment._greedy = True
        self.segment._key_set = frozenset(['user_id_1', 'user_id_2', 'user_id_3',
                                           'user_id_4', 'user_id_5'])
        self.segment._segment_change_fetcher.fetch.side_effect = [  # Two updates
            {
                'name': 'some_name',
                'added': ['user_id_6'],
                'removed': ['user_id_1', 'user_id_2'],
                'since': -1,
                'till': 1
            },
            {
                'name': 'some_name',
                'added': ['user_id_7'],
                'removed': ['user_id_4'],
                'since': 1,
                'till': 2
            },
            {
                'name': 'some_name',
                'added': [],
                'removed': [],
                'since': 2,
                'till': 2
            }
        ]

    def test_refreshes_key_set_with_all_changes_on_greedy(self):
        """
        Tests that _refresh_segment updates the key set properly consuming all changes if greedy is
        set
        """
        SelfRefreshingSegment._refresh_segment(self.segment)

        self.assertSetEqual(
            {'user_id_3', 'user_id_5', 'user_id_6', 'user_id_7'},
            self.segment._key_set
        )

    def test_refreshes_key_set_with_all_changes_on_non_greedy(self):
        """
        Tests that _refresh_segment updates the key set properly consuming all changes if greedy is
        not set
        """
        self.segment._greedy = False

        SelfRefreshingSegment._refresh_segment(self.segment)

        self.assertSetEqual(
            {'user_id_3', 'user_id_4', 'user_id_5', 'user_id_6'},
            self.segment._key_set
        )

    def test_key_set_is_not_updated_if_no_changes_were_received(self):
        """
        Tests that _refresh_segment doesn't update key set if no changes are received from the
        server
        """
        original_key_set = self.segment._key_set

        self.segment._change_number = 2
        self.segment._segment_change_fetcher.fetch.side_effect = [
            {
                'name': 'some_name',
                'added': [],
                'removed': [],
                'since': 2,
                'till': 2
            }
        ]

        SelfRefreshingSegment._refresh_segment(self.segment)

        self.assertEqual(
            original_key_set,
            self.segment._key_set
        )

    def test_updates_change_number(self):
        """
        Tests that _refresh_segment updates the change number with the last "till" value from the
        response of the segment change fetcher
        """
        SelfRefreshingSegment._refresh_segment(self.segment)

        self.assertEqual(
            2,
            self.segment._change_number
        )


class SelfRefreshingSegmentTimerRefreshTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.segment = mock.MagicMock()
        self.segment._stopped = False
        self.timer_mock = self.patch('splitio.segments.Timer')

    def test_calls_executor_submitif_not_stopped(self):
        """Tests that if the segment refresh is not stopped, a call to the executor submit method
        is made"""
        SelfRefreshingSegment._timer_refresh(self.segment)

        self.segment._executor.submit.assert_called_once_with(
            SelfRefreshingSegment._refresh_segment, self.segment)

    def test_new_timer_created_if_not_stopped(self):
        """Tests that if the segment refresh is not stopped, a new Timer is created and started"""
        SelfRefreshingSegment._timer_refresh(self.segment)

        self.timer_mock.assert_called_once_with(self.segment._interval,
                                                SelfRefreshingSegment._timer_refresh,
                                                (self.segment,))
        self.timer_mock.return_value.start.assert_called_once_with()

    def test_doesnt_call_executor_submitif_stopped(self):
        """Tests that if the segment refresh is stopped, no call to the executor submit method is
        made"""
        self.segment._stopped = True
        SelfRefreshingSegment._timer_refresh(self.segment)

        self.segment._executor.submit.assert_not_called()

    def test_new_timer_not_created_if_stopped(self):
        """Tests that if the segment refresh is stopped, no new Timer is created"""
        self.segment._stopped = True
        SelfRefreshingSegment._timer_refresh(self.segment)

        self.timer_mock.assert_not_called()


class SegmentChangeFetcherTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_name = mock.MagicMock()
        self.some_since = mock.MagicMock()

        self.segment_change_fetcher = SegmentChangeFetcher()

        self.fetch_from_backend_mock = self.patch_object(self.segment_change_fetcher,
                                                         'fetch_from_backend')

    def test_fetch_calls_fetch_from_backend(self):
        """Tests that fetch calls fetch_from_backend"""
        self.segment_change_fetcher.fetch(self.some_name, self.some_since)

        self.fetch_from_backend_mock.assert_called_once_with(self.some_name, self.some_since)

    def test_fetch_doesnt_raise_exceptions(self):
        """
        Tests that if fetch_from_backend raises an exception, no exception is raised from fetch
        """
        self.fetch_from_backend_mock.side_effect = Exception()

        try:
            self.segment_change_fetcher.fetch(self.some_name, self.some_since)
        except:
            self.fail('Unexpected exception raised')

    def test_fetch_calls_fetch_from_backend(self):
        """
        Tests that if fetch_from_backend raises an exception, an empty segment change is returned
        """
        self.fetch_from_backend_mock.side_effect = Exception()

        segment_change = self.segment_change_fetcher.fetch(self.some_name, self.some_since)

        self.assertDictEqual(
            {
                'name': self.some_name,
                'added': [],
                'removed': [],
                'since': self.some_since,
                'till': self.some_since
            },
            segment_change
        )


class ApiSegmentChangeFetcherTests(TestCase):
    def setUp(self):
        self.some_name = mock.MagicMock()
        self.some_since = mock.MagicMock()
        self.some_api = mock.MagicMock()

        self.segment_change_fetcher = ApiSegmentChangeFetcher(self.some_api)

    def test_fetch_from_backend_cals_api_segment_changes(self):
        """Tests that fetch_from_backend calls segment_changes on the api"""

        self.segment_change_fetcher.fetch_from_backend(self.some_name, self.some_since)

        self.some_api.segment_changes.assert_called_once_with(self.some_name, self.some_since)
