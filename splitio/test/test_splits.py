"""Unit tests for the transformers module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase

from splitio.splits import (InMemorySplitFetcher, SelfRefreshingSplitFetcher, SplitChangeFetcher,
                            ApiSplitChangeFetcher, SplitParser)
from splitio.matchers import (AndCombiner, AllKeysMatcher, UserDefinedSegmentMatcher,
                              WhitelistMatcher, AttributeMatcher)
from splitio.test.utils import MockUtilsMixin


class InMemorySplitFetcherTests(TestCase):
    def setUp(self):
        self.some_feature = mock.MagicMock()
        self.some_splits = mock.MagicMock()
        self.some_splits.values.return_value.__iter__.return_value = [mock.MagicMock(),
                                                                      mock.MagicMock(),
                                                                      mock.MagicMock()]

        self.fetcher = InMemorySplitFetcher(self.some_splits)

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

        self.thread_mock.assert_called_once_with(target=SelfRefreshingSplitFetcher._refresh_splits,
                                                 args=(self.fetcher,))
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


class SplitChangeFetcherTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_since = mock.MagicMock()
        self.fetcher = SplitChangeFetcher()
        self.fetch_from_backend_mock = self.patch_object(self.fetcher, 'fetch_from_backend')

    def test_fetch_calls_fetch_from_backend(self):
        """Tests that fetch calls fetch_from_backend"""
        self.fetcher.fetch(self.some_since)
        self.fetch_from_backend_mock.assert_called_once_with(self.some_since)

    def test_fetch_returns_fetch_from_backend_result(self):
        """Tests that fetch returns fetch_from_backend call return"""
        self.assertEqual(self.fetch_from_backend_mock.return_value,
                         self.fetcher.fetch(self.some_since))

    def test_fetch_doesnt_raise_an_exception_fetch_from_backend_raises_one(self):
        """Tests that fetch doesn't raise an exception if fetch_from_backend does"""
        self.fetch_from_backend_mock.side_effect = Exception()

        try:
            self.fetcher.fetch(self.some_since)
        except:
            self.fail('Unexpected exception raised')

    def test_fetch_returns_empty_response_if_fetch_from_backend_raises_an_exception(self):
        """Tests that fetch returns fetch_from_backend call return"""
        self.fetch_from_backend_mock.side_effect = Exception()
        self.assertDictEqual({'since': self.some_since, 'till': self.some_since, 'splits': []},
                             self.fetcher.fetch(self.some_since))


class ApiSplitChangeFetcherTests(TestCase):
    def setUp(self):
        self.some_since = mock.MagicMock()
        self.some_api = mock.MagicMock()
        self.fetcher = ApiSplitChangeFetcher(self.some_api)

    def test_fetch_from_backend_calls_api_split_changes(self):
        """Tests that fetch_from_backend calls api split_changes"""
        self.fetcher.fetch_from_backend(self.some_since)
        self.some_api.split_changes.assert_called_once_with(self.some_since)

    def test_fetch_from_backend_returns_api_split_changes_result(self):
        """Tests that fetch_from_backend returns api split_changes call result"""
        self.assertEqual(self.some_api.split_changes.return_value,
                         self.fetcher.fetch_from_backend(self.some_since))

    def test_fetch_from_backend_raises_exception_if_api_split_changes_does(self):
        """Tests that fetch_from_backend raises an exception if split_changes does"""
        self.some_api.split_changes.side_effect = Exception()
        with self.assertRaises(Exception):
            self.fetcher.fetch_from_backend(self.some_since)


class SplitParserParseTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_split = mock.MagicMock()
        self.some_segment_fetcher = mock.MagicMock()

        self.parser = SplitParser(self.some_segment_fetcher)
        self.internal_parse_mock = self.patch_object(self.parser, '_parse')

    def test_parse_calls_internal_parse(self):
        """Tests that parse calls _parse"""
        self.parser.parse(self.some_split)
        self.internal_parse_mock.assert_called_once_with(self.some_split)

    def test_parse_returns_none_if_internal_parse_raises_an_exception(self):
        """Tests that parse returns None if _parse raises an exception"""
        self.internal_parse_mock.side_effect = Exception()
        self.assertIsNone(self.parser.parse(self.some_split))


class SplitParserInternalParseTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_split = mock.MagicMock()
        self.some_segment_fetcher = mock.MagicMock()

        self.partition_mock = self.patch('splitio.splits.Partition')
        self.partition_mock_side_effect = [mock.MagicMock() for _ in range(3)]
        self.partition_mock.side_effect = self.partition_mock_side_effect

        self.condition_mock = self.patch('splitio.splits.Condition')
        self.condition_mock_side_effect = [mock.MagicMock() for _ in range(2)]
        self.condition_mock.side_effect = self.condition_mock_side_effect

        self.split_mock = self.patch('splitio.splits.Split')
        self.parser = SplitParser(self.some_segment_fetcher)
        self.parse_matcher_group_mock = self.patch_object(self.parser, '_parse_matcher_group')
        self.parse_matcher_group_mock_side_effect = [mock.MagicMock() for _ in range(2)]
        self.parse_matcher_group_mock.side_effect = self.parse_matcher_group_mock_side_effect

        self.partition_0 = {'treatment': mock.MagicMock(), 'size': mock.MagicMock()}
        self.partition_1 = {'treatment': mock.MagicMock(), 'size': mock.MagicMock()}
        self.partition_2 = {'treatment': mock.MagicMock(), 'size': mock.MagicMock()}

        self.matcher_group_0 = mock.MagicMock()
        self.matcher_group_1 = mock.MagicMock()

        self.some_split = {
            'status': 'ACTIVE',
            'name': mock.MagicMock(),
            'seed': mock.MagicMock(),
            'killed': mock.MagicMock(),
            'defaultTreatment': mock.MagicMock(),
            'conditions': [
                {
                    'matcherGroup': self.matcher_group_0,
                    'partitions': [

                        self.partition_0
                    ]
                },
                {
                    'matcherGroup': self.matcher_group_1,
                    'partitions': [
                        self.partition_1,
                        self.partition_2
                    ]
                }
            ]
        }

    def test_returns_none_if_status_is_not_active(self):
        """Tests that _parse returns None if split is not ACTIVE"""
        self.assertIsNone(self.parser._parse({'status': 'ARCHIVED'}))

    def test_creates_partition_on_each_condition_partition(self):
        """Test that _parse calls Partition constructor on each partition"""
        self.parser._parse(self.some_split)

        self.assertListEqual(
            [mock.call(self.partition_0['treatment'], self.partition_0['size']),
             mock.call(self.partition_1['treatment'], self.partition_1['size']),
             mock.call(self.partition_2['treatment'], self.partition_2['size'])],
            self.partition_mock.call_args_list
        )

    def test_calls_parse_matcher_group_on_each_matcher_group(self):
        """Tests that _parse calls _parse_matcher_group on each matcher group"""
        self.parser._parse(self.some_split)

        self.assertListEqual(
            [mock.call(self.matcher_group_0),
             mock.call(self.matcher_group_1)],
            self.parse_matcher_group_mock.call_args_list
        )

    def test_creates_condition_on_each_condition(self):
        """Tests that _parse calls Condition constructor on each condition"""
        self.parser._parse(self.some_split)

        self.assertListEqual(
            [mock.call(self.parse_matcher_group_mock_side_effect[0],
                       [self.partition_mock_side_effect[0]]),
             mock.call(self.parse_matcher_group_mock_side_effect[1],
                       [self.partition_mock_side_effect[1], self.partition_mock_side_effect[2]])],
            self.condition_mock.call_args_list
        )

    def test_creates_split(self):
        """Tests that _parse calls Split constructor"""
        self.parser._parse(self.some_split)

        self.split_mock.assert_called_once_with(self.some_split['name'], self.some_split['seed'],
                                                self.some_split['killed'],
                                                self.some_split['defaultTreatment'],
                                                self.condition_mock_side_effect)


class SplitParserParseMatcherGroupTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_split = mock.MagicMock()
        self.some_segment_fetcher = mock.MagicMock()

        self.combining_matcher_mock = self.patch('splitio.splits.CombiningMatcher')
        self.parser = SplitParser(self.some_segment_fetcher)
        self.parse_matcher_mock = self.patch_object(self.parser, '_parse_matcher')
        self.parse_matcher_side_effect = [mock.MagicMock() for _ in range(2)]
        self.parse_matcher_mock.side_effect = self.parse_matcher_side_effect
        self.parse_combiner_mock = self.patch_object(self.parser, '_parse_combiner')

        self.some_matchers = [mock.MagicMock(), mock.MagicMock()]
        self.some_matcher_group = {
            'matchers': self.some_matchers,
            'combiner': mock.MagicMock()
        }

    def test_calls_parse_matcher_on_each_matcher(self):
        """Tests that _parse_matcher_group calls _parse_matcher on each matcher"""
        self.parser._parse_matcher_group(self.some_matcher_group)
        self.assertListEqual([mock.call(self.some_matchers[0]), mock.call(self.some_matchers[1])],
                             self.parse_matcher_mock.call_args_list)

    def test_calls_parse_combiner_on_combiner(self):
        """Tests that _parse_matcher_group calls _parse_combiner on combiner"""
        self.parser._parse_matcher_group(self.some_matcher_group)
        self.parse_combiner_mock.assert_called_once_with(self.some_matcher_group['combiner'])

    def test_creates_combining_matcher(self):
        """Tests that _parse_matcher_group calls CombiningMatcher constructor"""
        self.parser._parse_matcher_group(self.some_matcher_group)
        self.combining_matcher_mock.assert_called_once_with(self.parse_combiner_mock.return_value,
                                                            self.parse_matcher_side_effect)
    def test_returns_combining_matcher(self):
        """Tests that _parse_matcher_group returns a CombiningMatcher"""
        self.assertEqual(self.combining_matcher_mock.return_value,
                         self.parser._parse_matcher_group(self.some_matcher_group))


class SplitParserParseCombinerTests(TestCase):
    def setUp(self):
        self.some_segment_fetcher = mock.MagicMock()

        self.parser = SplitParser(self.some_segment_fetcher)

    def test_returns_and_combiner(self):
        """Tests that _parse_combiner returns an AndCombiner"""
        self.assertIsInstance(self.parser._parse_combiner('AND'), AndCombiner)

    def test_raises_exception_on_invalid_combiner(self):
        """Tests that _parse_combiner raises an exception on an invalid combiner"""
        with self.assertRaises(ValueError):
            self.parser._parse_combiner('foobar')


class SplitParserMatcherParseMethodsTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_segment_fetcher = mock.MagicMock()
        self.some_matcher = mock.MagicMock()

        self.parser = SplitParser(self.some_segment_fetcher)

        self.get_matcher_data_data_type_mock = self.patch_object(self.parser,
                                                                 '_get_matcher_data_data_type')
        self.equal_to_matcher_mock = self.patch('splitio.splits.EqualToMatcher')
        self.greater_than_or_equal_to_matcher_mock = self.patch(
            'splitio.splits.GreaterThanOrEqualToMatcher')
        self.less_than_or_equal_to_matcher_mock = self.patch(
            'splitio.splits.LessThanOrEqualToMatcher')
        self.between_matcher_mock = self.patch(
            'splitio.splits.BetweenMatcher')

        self.some_in_segment_matcher = {
            'matcherType': 'IN_SEGMENT',
            'userDefinedSegmentMatcherData': {
                'segmentName': mock.MagicMock()
            }
        }
        self.some_whitelist_matcher = {
            'matcherType': 'WHITELIST',
            'whitelistMatcherData': {
                'whitelist': mock.MagicMock()
            }
        }
        self.some_equal_to_matcher = self._get_unary_number_matcher('EQUAL_TO')
        self.some_greater_than_or_equal_to_matcher = self._get_unary_number_matcher(
            'GREATER_THAN_OR_EQUAL_TO')
        self.some_less_than_or_equal_to_matcher = self._get_unary_number_matcher(
            'LESS_THAN_OR_EQUAL_TO')
        self.some_between_matcher = {
            'matcherType': 'BETWEEN',
            'betweenMatcherData': {
                'start': mock.MagicMock(),
                'end': mock.MagicMock(),
            }
        }

    def _get_unary_number_matcher(self, matcher_type):
        return {
            'matcherType': matcher_type,
            'unaryNumericMatcherData': {
                'dataType': mock.MagicMock(),
                'value': mock.MagicMock()
            }
        }

    def test_parse_matcher_all_keys_returns_all_keys_matcher(self):
        """Tests that _parser_matcher_all_keys returns an AllKeysMatcher"""
        self.assertIsInstance(self.parser._parse_matcher_all_keys(self.some_matcher),
                              AllKeysMatcher)

    def test_parse_matcher_in_segment_calls_segment_fetcher_fetch(self):
        """Tests that _parse_matcher_in_segment calls segment_fetcher fetch method"""
        self.parser._parse_matcher_in_segment(self.some_in_segment_matcher)
        self.some_segment_fetcher.fetch.assert_called_once_with(
            self.some_in_segment_matcher['userDefinedSegmentMatcherData']['segmentName'])

    def test_parse_matcher_in_segment_returns_user_defined_segment_matcher(self):
        """Tests that _parse_matcher_in_segment calls segment_fetcher fetch method"""
        self.assertIsInstance(self.parser._parse_matcher_in_segment(self.some_in_segment_matcher),
                              UserDefinedSegmentMatcher)

    def test_parse_matcher_whitelist_returns_whitelist_matcher(self):
        """Tests that _parse_matcher_whitelist returns a WhitelistMatcher"""
        self.assertIsInstance(self.parser._parse_matcher_whitelist(self.some_whitelist_matcher),
                      WhitelistMatcher)

    def test_parse_matcher_equal_to_calls_equal_to_matcher_for_data_type(self):
        """Tests that _parse_matcher_equal_to calls EqualToMatcher.for_data_type"""
        self.parser._parse_matcher_equal_to(self.some_equal_to_matcher)
        self.equal_to_matcher_mock.for_data_type.assert_called_once_with(
            self.get_matcher_data_data_type_mock.return_value,
            self.some_equal_to_matcher['unaryNumericMatcherData']['value'])

    def test_parse_matcher_equal_to_returns_equal_to_matcher(self):
        """
        Tests that _parse_matcher_equal_to returns the result of calling
        EqualToMatcher.for_data_type
        """
        self.assertEqual(self.equal_to_matcher_mock.for_data_type.return_value,
                         self.parser._parse_matcher_equal_to(self.some_equal_to_matcher))

    def test_parse_matcher_greater_than_or_equal_to_calls_equal_to_matcher_for_data_type(self):
        """
        Tests that _parse_matcher_greater_than_or_equal_to calls
        GreaterThanOrEqualToMatcher.for_data_type
        """
        self.parser._parse_matcher_greater_than_or_equal_to(
            self.some_greater_than_or_equal_to_matcher)
        self.greater_than_or_equal_to_matcher_mock.for_data_type.assert_called_once_with(
            self.get_matcher_data_data_type_mock.return_value,
            self.some_greater_than_or_equal_to_matcher['unaryNumericMatcherData']['value'])

    def test_parse_matcher_greater_than_or_equal_to_returns_equal_to_matcher(self):
        """
        Tests that _parse_matcher_greater_than_or_equal_to returns the result of calling
        GreaterThanOrEqualToMatcher.for_data_type
        """
        self.assertEqual(
            self.greater_than_or_equal_to_matcher_mock.for_data_type.return_value,
            self.parser._parse_matcher_greater_than_or_equal_to(
                self.some_greater_than_or_equal_to_matcher))

    def test_parse_matcher_less_than_or_equal_to_calls_equal_to_matcher_for_data_type(self):
        """
        Tests that _parse_matcher_less_than_or_equal_to calls
        LessThanOrEqualToMatcher.for_data_type
        """
        self.parser._parse_matcher_less_than_or_equal_to(
            self.some_less_than_or_equal_to_matcher)
        self.less_than_or_equal_to_matcher_mock.for_data_type.assert_called_once_with(
            self.get_matcher_data_data_type_mock.return_value,
            self.some_less_than_or_equal_to_matcher['unaryNumericMatcherData']['value'])

    def test_parse_matcher_less_than_or_equal_to_returns_equal_to_matcher(self):
        """
        Tests that _parse_matcher_less_than_or_equal_to returns the result of calling
        LessThanOrEqualToMatcher.for_data_type
        """
        self.assertEqual(
            self.less_than_or_equal_to_matcher_mock.for_data_type.return_value,
            self.parser._parse_matcher_less_than_or_equal_to(
                self.some_less_than_or_equal_to_matcher))

    def test_parse_matcher_between_calls_between_matcher_for_data_type(self):
        """Tests that _parse_matcher_between calls BetweenMatcher.for_data_type"""
        self.parser._parse_matcher_between(self.some_between_matcher)
        self.between_matcher_mock.for_data_type.assert_called_once_with(
            self.get_matcher_data_data_type_mock.return_value,
            self.some_between_matcher['betweenMatcherData']['start'],
            self.some_between_matcher['betweenMatcherData']['end'])

    def test_parse_matcher_between_returns_between_matcher(self):
        """
        Tests that _parse_matcher_between returns the result of calling
        BetweenMatcher.for_data_type
        """
        self.assertEqual(self.between_matcher_mock.for_data_type.return_value,
                         self.parser._parse_matcher_between(self.some_between_matcher))


class SplitParserParseMatcherTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_segment_fetcher = mock.MagicMock()
        self.some_matcher = mock.MagicMock()

        self.parser = SplitParser(self.some_segment_fetcher)

        self.parse_matcher_all_keys_mock = self.patch_object(self.parser,
                                                             '_parse_matcher_all_keys')
        self.parse_matcher_in_segment_mock = self.patch_object(self.parser,
                                                               '_parse_matcher_in_segment')
        self.parse_matcher_whitelist_mock = self.patch_object(self.parser,
                                                              '_parse_matcher_whitelist')
        self.parse_matcher_equal_to_mock = self.patch_object(self.parser,
                                                             '_parse_matcher_equal_to')
        self.parse_matcher_greater_than_or_equal_to_mock = self.patch_object(
            self.parser, '_parse_matcher_greater_than_or_equal_to')
        self.parse_matcher_less_than_or_equal_to_mock = self.patch_object(
            self.parser, '_parse_matcher_less_than_or_equal_to')
        self.parse_matcher_between_mock = self.patch_object(self.parser, '_parse_matcher_between')

        self.parser._parse_matcher_fake = mock.MagicMock()

    def _get_matcher(self, matcher_type):
        return {
            'matcherType': matcher_type,
            'negate': mock.MagicMock(),
            'keySelector': {
                'attribute': mock.MagicMock()
            }
        }

    def test_calls_parse_matcher_all_keys(self):
        """Test that _parse_matcher calls _parse_matcher_all_keys on ALL_KEYS matcher"""
        matcher = self._get_matcher('ALL_KEYS')
        self.parser._parse_matcher(matcher)
        self.parse_matcher_all_keys_mock.assert_called_once_with(matcher)

    def test_calls_parse_matcher_in_segment(self):
        """Test that _parse_matcher calls _parse_matcher_in_segment on IN_SEGMENT matcher"""
        matcher = self._get_matcher('IN_SEGMENT')
        self.parser._parse_matcher(matcher)
        self.parse_matcher_in_segment_mock.assert_called_once_with(matcher)

    def test_calls_parse_matcher_whitelist(self):
        """Test that _parse_matcher calls _parse_matcher_in_segment on WHITELIST matcher"""
        matcher = self._get_matcher('WHITELIST')
        self.parser._parse_matcher(matcher)
        self.parse_matcher_whitelist_mock.assert_called_once_with(matcher)

    def test_calls_parse_matcher_equal_to(self):
        """Test that _parse_matcher calls _parse_matcher_equal_to on EQUAL_TO matcher"""
        matcher = self._get_matcher('EQUAL_TO')
        self.parser._parse_matcher(matcher)
        self.parse_matcher_equal_to_mock.assert_called_once_with(matcher)

    def test_calls_parse_matcher_greater_than_or_equal_to(self):
        """
        Test that _parse_matcher calls _parse_matcher_greater_than_or_equal_to on
        GREATER_THAN_OR_EQUAL_TO matcher
        """
        matcher = self._get_matcher('GREATER_THAN_OR_EQUAL_TO')
        self.parser._parse_matcher(matcher)
        self.parse_matcher_greater_than_or_equal_to_mock.assert_called_once_with(matcher)

    def test_calls_parse_matcher_less_than_or_equal_to(self):
        """
        Test that _parse_matcher calls _parse_matcher_less_than_or_equal_to on
        LESS_THAN_OR_EQUAL_TO matcher
        """
        matcher = self._get_matcher('LESS_THAN_OR_EQUAL_TO')
        self.parser._parse_matcher(matcher)
        self.parse_matcher_less_than_or_equal_to_mock.assert_called_once_with(matcher)

    def test_calls_parse_matcher_between(self):
        """Test that _parse_matcher calls _parse_between on BETWEEN matcher"""
        matcher = self._get_matcher('BETWEEN')
        self.parser._parse_matcher(matcher)
        self.parse_matcher_between_mock.assert_called_once_with(matcher)

    def test_raises_exception_if_parse_method_returns_none(self):
        """
        Tests that _parse_matcher raises an exception if the specific parse method returns None
        """
        self.parser._parse_matcher_fake.return_value = None
        with self.assertRaises(ValueError):
            self.parser._parse_matcher(self._get_matcher('FAKE'))

    def test_returns_attribute_matcher(self):
        """Tests that _parse_matcher returns an AttributeMatcher"""
        self.assertIsInstance(self.parser._parse_matcher(self._get_matcher('FAKE')),
                              AttributeMatcher)
