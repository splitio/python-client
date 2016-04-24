"""Unit tests for the transformers module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase

from splitio.splits import SplitFetcher, SelfRefreshingSplitFetcher
from splitio.test.utils import MockUtilsMixin


class SplitFetcherTests(TestCase):
    def setUp(self):
        self.some_feature = mock.MagicMock()
        self.some_splits = mock.MagicMock()
        self.some_splits.values.return_value.__iter__.return_value = [mock.MagicMock(),
                                                                      mock.MagicMock(),
                                                                      mock.MagicMock()]

        self.fetcher = SplitFetcher(self.some_splits)

    def test_fetch_calls_get_on_splits(self):
        """Test that fetch calls get on splits"""
        self.fetcher.fetch(self.some_feature)

        self.some_splits.get.assert_called_once_with(self.some_feature)

    def test_fetch_returns_result_of_get(self):
        """Test that fetch calls returns the result of calling fetch"""
        self.assertEqual(self.some_splits.get.return_value, self.fetcher.fetch(self.some_feature))

    def test_fetch_all_calls_values_on_splits(self):
        """Test that fetch_all calls values on splits"""
        self.fetcher.fetch_all()

        self.some_splits.values.assert_called_once_with()

    def test_fetch_all_returns_list_of_splits_values(self):
        """Test that fetch_all returns a list of values of splits"""
        self.assertListEqual(self.some_splits.values.return_value.__iter__.return_value,
                             self.fetcher.fetch_all())


class SelfRefreshingSplitFetcherTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.rlock_mock = self.patch('splitio.splits.RLock')
        self.refresh_splits_mock = self.patch(
            'splitio.splits.SelfRefreshingSplitFetcher._refresh_splits')
        self.timer_refresh_mock = self.patch(
            'splitio.splits.SelfRefreshingSplitFetcher._timer_refresh')

        self.some_split_change_fetcher = mock.MagicMock()
        self.some_split_parser = mock.MagicMock()

        self.fetcher = SelfRefreshingSplitFetcher(self.some_split_change_fetcher,
                                                  self.some_split_parser)

    def test_force_refresh_calls_refresh_splits(self):
        """Tests that force_refresh calls _refresh_splits"""
        self.fetcher.force_refresh()

        self.refresh_splits_mock.assert_called_once_with(self.fetcher)

    def test_start_calls_timer_refresh_if_stopped(self):
        """Tests that if stopped is True, start calls _timer_refresh"""
        self.fetcher.stopped = True
        self.fetcher.start()

        self.timer_refresh_mock.assert_called_once_with(self.fetcher)

    def test_start_sets_stopped_False_if_stopped(self):
        """Tests that if stopped is True, start sets it to True"""
        self.fetcher.stopped = True
        self.fetcher.start()

        self.assertFalse(self.fetcher.stopped)

    def test_start_doesnt_call_timer_refresh_if_not_stopped(self):
        """Tests that if stopped is false, start doesn't call _timer_refresh"""
        self.fetcher.stopped = False
        self.fetcher.start()

        self.timer_refresh_mock.assert_not_called()


class SelfRefreshingSplitFetcherUpdateSplitsFromChangeFetcherResponseTests(TestCase,
                                                                           MockUtilsMixin):
    def setUp(self):
        self.rlock_mock = self.patch('splitio.splits.RLock')

        self.some_feature = mock.MagicMock()
        self.some_splits = mock.MagicMock()
        self.some_split_change_fetcher = mock.MagicMock()
        self.some_split_parser = mock.MagicMock()

        self.fetcher = SelfRefreshingSplitFetcher(self.some_split_change_fetcher,
                                                  self.some_split_parser, splits=self.some_splits)

        self.split_to_add = {'status': 'ACTIVE', 'name': 'split_to_add'}
        self.split_to_remove = {'status': 'ARCHIVED', 'name': 'split_to_remove'}
        self.some_response = {
            'splits': [self.split_to_add, self.split_to_remove]
        }

    def test_pop_is_called_on_removed_split(self):
        """Tests that pop is called on splits for removed features"""
        self.fetcher._update_splits_from_change_fetcher_response(self.some_response)

        self.some_splits.pop.assert_called_once_with(self.split_to_remove['name'], None)

    def test_split_parser_parse_is_called_for_split_to_add(self):
        """Tests that parse is called on split_parser with the split to add"""
        self.fetcher._update_splits_from_change_fetcher_response(self.some_response)

        self.some_split_parser.parse.assert_called_once_with(self.split_to_add)

    def test_setitem_is_called_with_parsed_split_to_add(self):
        """Tests that pop is called on splits for removed features"""
        self.fetcher._update_splits_from_change_fetcher_response(self.some_response)

        self.some_splits.__setitem__.assert_called_once_with(
            self.split_to_add['name'], self.some_split_parser.parse.return_value)

    def test_pop_is_called_if_split_parse_fails(self):
        """Tests that pop is called on splits if parse returns None"""
        self.some_split_parser.parse.return_value = None

        self.fetcher._update_splits_from_change_fetcher_response(
            {'splits': [self.split_to_add]})

        self.some_splits.pop.assert_called_once_with(self.split_to_add['name'], None)


class SelfRefreshingSplitFetcherRefreshSplitsTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.rlock_mock = self.patch('splitio.splits.RLock')

        self.some_feature = mock.MagicMock()
        self.some_splits = mock.MagicMock()
        self.some_split_change_fetcher = mock.MagicMock()
        self.some_split_parser = mock.MagicMock()

        self.response_0 = {'till': 1, 'splits': [mock.MagicMock()]}
        self.response_1 = {'till': 2, 'splits': [mock.MagicMock()]}
        self.response_2 = {'till': 2, 'splits': []}

        self.some_split_change_fetcher.fetch.side_effect = [
            self.response_0,
            self.response_1,
            self.response_2
        ]

        self.fetcher = SelfRefreshingSplitFetcher(self.some_split_change_fetcher,
                                                  self.some_split_parser,
                                                  change_number=-1,
                                                  splits=self.some_splits)
        self.update_splits_from_change_fetcher_response_mock = self.patch_object(
            self.fetcher, '_update_splits_from_change_fetcher_response')

    def test_calls_split_change_fetcher_until_change_number_ge_till_if_greedy(self):
        """
        Tests that if greedy is True _refresh_splits calls fetch on split_change_fetcher until
        change_number >= till
        """
        SelfRefreshingSplitFetcher._refresh_splits(self.fetcher)

        self.assertListEqual(
            [mock.call(-1), mock.call(1), mock.call(2)],
            self.some_split_change_fetcher.fetch.call_args_list)

    def test_calls_split_change_fetcher_once_if_non_greedy(self):
        """
        Tests that if greedy is False _refresh_splits calls fetch on split_change_fetcher once
        """
        self.fetcher._greedy = False
        SelfRefreshingSplitFetcher._refresh_splits(self.fetcher)
        self.some_split_change_fetcher.fetch.assert_called_once_with(-1)

    def test_calls_update_splits_from_change_fetcher_response_on_each_response_if_greedy(self):
        """
        Tests that if greedy is True _refresh_splits calls
        _update_splits_from_change_fetcher_response on all responses from split change fetcher
        """
        SelfRefreshingSplitFetcher._refresh_splits(self.fetcher)
        self.assertListEqual(
            [mock.call(self.response_0), mock.call(self.response_1)],
            self.update_splits_from_change_fetcher_response_mock.call_args_list)

    def test_calls_update_splits_from_change_fetcher_response_once_if_non_greedy(self):
        """
        Tests that if greedy is False _refresh_splits calls
        _update_splits_from_change_fetcher_response once
        """
        self.fetcher._greedy = False
        SelfRefreshingSplitFetcher._refresh_splits(self.fetcher)
        self.update_splits_from_change_fetcher_response_mock.assert_called_once_with(
            self.response_0)

    def test_sets_change_number_to_latest_value_of_till_response(self):
        """
        Tests that _refresh_splits sets change_number to the largest value of "till" in the
        response of the split_change_fetcher fetch response.
        """
        SelfRefreshingSplitFetcher._refresh_splits(self.fetcher)
        self.assertEqual(2, self.fetcher.change_number)

    def test_stop_set_true_on_exception(self):
        """
        Tests that stopped is set to True if an exception is raised
        """
        self.some_split_change_fetcher.fetch.side_effect = [
            self.response_0,
            Exception()
        ]
        SelfRefreshingSplitFetcher._refresh_splits(self.fetcher)
        self.assertTrue(self.fetcher.stopped)

    def test_change_number_set_value_till_latest_successful_iteration(self):
        """
        Tests that change_number is set to the value of "till" in the latest successful iteration
        before an exception is raised
        """
        self.some_split_change_fetcher.fetch.side_effect = [
            self.response_0,
            Exception()
        ]
        SelfRefreshingSplitFetcher._refresh_splits(self.fetcher)
        self.assertEqual(1, self.fetcher.change_number)


class SelfRefreshingSplitFetcherTimerRefreshTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.rlock_mock = self.patch('splitio.splits.RLock')
        self.refresh_splits_thread = mock.MagicMock()
        self.timer_refresh_thread = mock.MagicMock()
        self.thread_mock = self.patch('splitio.splits.Thread')
        self.timer_mock = self.patch('splitio.splits.Timer')
        self.some_split_change_fetcher = mock.MagicMock()
        self.some_split_parser = mock.MagicMock()
        self.some_interval = mock.MagicMock()

        self.fetcher = SelfRefreshingSplitFetcher(self.some_split_change_fetcher,
                                                  self.some_split_parser,
                                                  interval=self.some_interval)
        self.fetcher.stopped = False

    def test_thread_created_and_started_with_refresh_splits(self):
        """Tests that _timer_refresh creates and starts a Thread with _refresh_splits target"""
        SelfRefreshingSplitFetcher._timer_refresh(self.fetcher)

        self.thread_mock.assert_called_once_with(target=SelfRefreshingSplitFetcher._refresh_splits)
        self.thread_mock.return_value.start.assert_called_once_with()

    def test_timer_created_and_started_with_timer_refresh(self):
        """Tests that _timer_refresh creates and starts a Timer with _timer_refresh target"""
        SelfRefreshingSplitFetcher._timer_refresh(self.fetcher)

        self.timer_mock.assert_called_once_with(self.some_interval,
                                                SelfRefreshingSplitFetcher._timer_refresh,
                                                (self.fetcher,))
        self.timer_mock.return_value.start.assert_called_once_with()

    def test_no_thread_created_if_stopped(self):
        """Tests that _timer_refresh doesn't create a Thread if it is stopped"""
        self.fetcher.stopped = True
        SelfRefreshingSplitFetcher._timer_refresh(self.fetcher)

        self.thread_mock.assert_not_called()

    def test_no_timer_created_if_stopped(self):
        """Tests that _timer_refresh doesn't create a Timerif it is stopped"""
        self.fetcher.stopped = True
        SelfRefreshingSplitFetcher._timer_refresh(self.fetcher)

        self.timer_mock.assert_not_called()

    def test_timer_created_if_thread_raises_exception(self):
        """
        Tests that _timer_refresh creates and starts a Timer even if the _refresh_splits thread
        setup raises an exception
        """
        self.thread_mock.return_value = None
        self.thread_mock.side_effect = Exception()
        SelfRefreshingSplitFetcher._timer_refresh(self.fetcher)

        self.timer_mock.assert_called_once_with(self.some_interval,
                                                SelfRefreshingSplitFetcher._timer_refresh,
                                                (self.fetcher,))

    def test_stops_if_timer_raises_exception(self):
        """Tests that _timer_refresh sets stop to True if the Timer setup raises an exception"""
        self.timer_mock.return_value = None
        self.timer_mock.side_effect = Exception()
        SelfRefreshingSplitFetcher._timer_refresh(self.fetcher)

        self.assertTrue(self.fetcher.stopped)

