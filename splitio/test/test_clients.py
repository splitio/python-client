"""Unit tests for the matchers module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase
from os.path import dirname, join

import arrow

from splitio.clients import Client, SelfRefreshingClient, randomize_interval, JSONFileClient
from splitio.settings import DEFAULT_CONFIG, SDK_API_BASE_URL, MAX_INTERVAL
from splitio.treatments import CONTROL
from splitio.test.utils import MockUtilsMixin


class ClientTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_feature = mock.MagicMock()
        self.some_attributes = mock.MagicMock()

        self.some_conditions = [
            mock.MagicMock(),
            mock.MagicMock(),
            mock.MagicMock()
        ]

        self.some_split = mock.MagicMock()
        self.some_split.killed = False
        self.some_split.conditions.__iter__.return_value = self.some_conditions
        self.client = Client()
        self.get_split_fetcher_mock = self.patch_object(self.client, 'get_split_fetcher')
        self.record_stats_mock = self.patch_object(self.client, '_record_stats')
        self.splitter_mock = self.patch('splitio.clients.Splitter')
        self.treatment_log_mock = self.patch('splitio.clients.TreatmentLog')

    def test_get_splitter_returns_a_splitter(self):
        """Test that get_splitter returns a splitter"""
        self.assertEqual(self.splitter_mock.return_value, self.client.get_splitter())

    def test_get_treatment_log_returns_treatment_log(self):
        """Test that get_splitter returns a treatment_log"""
        self.assertEqual(self.treatment_log_mock.return_value, self.client.get_treatment_log())

    def test_get_treatment_returns_control_if_key_is_none(self):
        """Test that get_treatment returns CONTROL treatment if key is None"""
        self.assertEqual(CONTROL, self.client.get_treatment(None, self.some_feature,
                                                            self.some_attributes))

    def test_get_treatment_returns_control_if_feature_is_none(self):
        """Test that get_treatment returns CONTROL treatment if feature is None"""
        self.assertEqual(CONTROL, self.client.get_treatment(self.some_key, None,
                                                            self.some_attributes))

    def test_get_treatment_calls_get_split_fetcher(self):
        """Test that get_treatment calls get_split_fetcher"""
        self.client.get_treatment(self.some_key, self.some_feature, self.some_attributes)
        self.get_split_fetcher_mock.assert_called_once_with()

    def test_get_treatment_calls_split_fetcher_fetch(self):
        """Test that get_treatment calls split fetcher fetch"""
        self.client.get_treatment(self.some_key, self.some_feature, self.some_attributes)
        self.get_split_fetcher_mock.return_value.fetch.assert_called_once_with(self.some_feature)

    def test_get_treatment_calls_get_treatment_for_split(self):
        """Test that get_treatment calls get_treatment_for_split"""
        get_treatment_for_split_mock = self.patch_object(self.client, '_get_treatment_for_split')

        self.client.get_treatment(self.some_key, self.some_feature, self.some_attributes)
        get_treatment_for_split_mock.assert_called_once_with(
            self.get_split_fetcher_mock.return_value.fetch.return_value, self.some_key,
            self.some_attributes)

    def test_get_treatment_returns_get_treatment_for_split_result(self):
        """Test that get_treatment returns get_treatment_for_split result"""
        get_treatment_for_split_mock = self.patch_object(self.client, '_get_treatment_for_split')

        self.assertEqual(get_treatment_for_split_mock.return_value,
                         self.client.get_treatment(self.some_key, self.some_feature,
                                                   self.some_attributes))

    def test_get_treatment_returns_control_if_get_split_fetcher_raises_exception(self):
        """
        Test that get_treatment returns CONTROL treatment if get_split_fetcher raises an exception
        """
        self.get_split_fetcher_mock.side_effect = Exception()
        self.assertEqual(CONTROL, self.client.get_treatment(self.some_key, self.some_feature,
                                                            self.some_attributes))

    def test_get_treatment_returns_control_if_fetch_raises_exception(self):
        """
        Test that get_treatment returns CONTROL treatment if fetch raises an exception
        """
        self.get_split_fetcher_mock.return_value.fetch.side_effect = Exception()
        self.assertEqual(CONTROL, self.client.get_treatment(self.some_key, self.some_feature,
                                                            self.some_attributes))

    def test_get_treatment_returns_control_if_get_treatment_for_split_raises_exception(self):
        """
        Test that get_treatment returns CONTROL treatment _get_treatment_for_split raises an
        exception
        """
        self.patch_object(self.client, '_get_treatment_for_split', side_effect=Exception())
        self.assertEqual(CONTROL, self.client.get_treatment(self.some_key, self.some_feature,
                                                            self.some_attributes))

    def test_get_treatment_for_split_returns_control_if_split_is_none(self):
        """Test that _get_treatment_for_split returns CONTROL if split is None"""
        self.assertEqual(CONTROL, self.client._get_treatment_for_split(None, self.some_key,
                                                                       self.some_feature))

    def test_get_treatment_for_split_returns_default_treatment_if_feature_is_killed(self):
        """Test that _get_treatment_for_split returns CONTROL if split is None"""
        self.some_split.killed = True
        self.assertEqual(self.some_split.default_treatment,
                         self.client._get_treatment_for_split(self.some_split, self.some_key,
                                                              self.some_feature))

    def test_get_treatment_returns_default_treatment_if_no_conditions_match(self):
        """Test that _get_treatment_for_split returns CONTROL if no split conditions_match"""
        self.some_conditions[0].matcher.match.return_value = False
        self.some_conditions[1].matcher.match.return_value = False
        self.some_conditions[2].matcher.match.return_value = False
        self.assertEqual(self.some_split.default_treatment,
                         self.client._get_treatment_for_split(self.some_split, self.some_key,
                                                              self.some_feature))

    def test_get_treatment_calls_condition_matcher_match_with_short_circuit(self):
        """
        Test that _get_treatment_for_split calls the conditions matcher match method until a match
        is found
        """
        self.some_conditions[0].matcher.match.return_value = False
        self.some_conditions[1].matcher.match.return_value = True
        self.some_conditions[2].matcher.match.return_value = False
        self.client._get_treatment_for_split(self.some_split, self.some_key, self.some_attributes)
        self.some_conditions[0].matcher.match.assert_called_once_with(
            self.some_key, attributes=self.some_attributes)
        self.some_conditions[1].matcher.match.assert_called_once_with(
            self.some_key, attributes=self.some_attributes)
        self.some_conditions[2].matcher.match.assert_not_called()

    def test_get_treatment_calls_get_splitter_if_a_condition_match(self):
        """
        Test that _get_treatment_for_split calls get_treatment on splitter if a condition match
        """
        self.some_conditions[0].matcher.match.return_value = False
        self.some_conditions[1].matcher.match.return_value = True
        self.client._get_treatment_for_split(self.some_split, self.some_key, self.some_attributes)
        self.splitter_mock.return_value.get_treatment.assert_called_once_with(
            self.some_key, self.some_split.seed, self.some_conditions[1].partitions)

    def test_get_treatment_calls_record_stats(self):
        """Test that get_treatment calls get_split_fetcher"""
        get_treatment_for_split_mock = self.patch_object(self.client, '_get_treatment_for_split')
        self.client.get_treatment(self.some_key, self.some_feature, self.some_attributes)
        self.record_stats_mock.assert_called_once_with(self.some_key, self.some_feature,
                                                       get_treatment_for_split_mock.return_value,
                                                       mock.ANY, 'sdk.getTreatment')


class ClientRecordStatsTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_feature = mock.MagicMock()
        self.some_treatment = mock.MagicMock()
        self.some_start = 123456000
        self.some_operation = mock.MagicMock()

        self.client = Client()
        self.get_treatment_log_mock = self.patch_object(self.client, 'get_treatment_log')
        self.arrow_mock = self.patch('splitio.clients.arrow')
        self.arrow_mock.utcnow.return_value.timestamp = 123457

    def test_record_stats_calls_treatment_log_log(self):
        """Test that _record_stats calls log on the treatment log"""
        self.client._record_stats(self.some_key, self.some_feature, self.some_treatment,
                                  self.some_start, self.some_operation)
        self.get_treatment_log_mock.return_value.log.assert_called_once_with(
            self.some_key, self.some_feature, self.some_treatment, 123457000)

    def test_record_stats_doesnt_raise_an_exception_if_log_does(self):
        """Test that _record_stats doesn't raise an exception if log does"""
        self.get_treatment_log_mock.return_value.log.side_effect = Exception()
        try:
            self.client._record_stats(self.some_key, self.some_feature, self.some_treatment,
                                      self.some_start, self.some_operation)
        except:
            self.fail('Unexpected exception raised')


class RandomizeIntervalTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_value = mock.MagicMock()
        self.max_mock = self.patch_builtin('max')
        self.randint_mock = self.patch('splitio.clients.randint')

    def test_returns_callable(self):
        """
        Tests that randomize_interval returns a callable
        """

        self.assertTrue(hasattr(randomize_interval(self.some_value), '__call__'))

    def test_returned_function_calls_randint(self):
        """
        Tests that the function returned by randomize_interval calls randint with the proper
        parameters
        """
        randomize_interval(self.some_value)()
        self.some_value.__floordiv__.assert_called_once_with(2)
        self.randint_mock.assert_called_once_with(self.some_value.__floordiv__.return_value,
                                                  self.some_value)

    def test_returned_function_calls_max(self):
        """
        Tests that the function returned by randomize_interval calls max with the proper
        parameters
        """
        randomize_interval(self.some_value)()
        self.max_mock.assert_called_once_with(5, self.randint_mock.return_value)

    def test_returned_function_returns_max_result(self):
        """
        Tests that the function returned by randomize_interval returns the result of calling max
        """
        self.assertEqual(self.max_mock.return_value, randomize_interval(self.some_value)())


class SelfRefreshingClientInitTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.sdk_api_mock = self.patch('splitio.clients.SdkApi')
        self.randomize_interval_side_effect = [mock.MagicMock(), mock.MagicMock(), mock.MagicMock()]
        self.randomize_interval_mock = self.patch(
            'splitio.clients.randomize_interval', side_effect=self.randomize_interval_side_effect)
        self.some_api_key = mock.MagicMock()
        self.some_sdk_api_url_base = mock.MagicMock()
        self.some_config = {
            'connectionTimeout': mock.MagicMock(),
            'readTimeout': mock.MagicMock(),
            'featuresRefreshRate': 31,
            'segmentsRefreshRate': 32,
            'metricsRefreshRate': 33,
            'impressionsRefreshRate': 34,
            'randomizeIntervals': False,
            'maxImpressionsLogSize': -1,
            'maxMetricsCallsBeforeFlush': -1
        }

    def test_sets_api_key(self):
        """
        Tests that __init__ sets api key to the given value
        """
        client = SelfRefreshingClient(self.some_api_key)
        self.assertEqual(self.some_api_key, client._api_key)

    def test_sets_default_config_if_config_is_none(self):
        """
        Tests that __init__ sets the config values to their defaults if no custom config is given
        """
        client = SelfRefreshingClient(self.some_api_key)
        self.assertDictEqual(DEFAULT_CONFIG, client._config)
        self.assertEqual(DEFAULT_CONFIG['connectionTimeout'], client._connection_timeout)
        self.assertEqual(DEFAULT_CONFIG['readTimeout'], client._read_timeout)
        self.assertEqual(DEFAULT_CONFIG['featuresRefreshRate'], client._split_fetcher_interval)
        self.assertEqual(DEFAULT_CONFIG['segmentsRefreshRate'], client._segment_fetcher_interval)
        self.assertEqual(SDK_API_BASE_URL, client._sdk_api_url_base)

    def test_sets_custom_config_if_given(self):
        """
        Tests that __init__ sets the config values to the values given in the config parameter
        """
        client = SelfRefreshingClient(self.some_api_key, config=self.some_config)
        self.assertDictEqual(self.some_config, client._config)
        self.assertEqual(self.some_config['connectionTimeout'], client._connection_timeout)
        self.assertEqual(self.some_config['readTimeout'], client._read_timeout)
        self.assertEqual(self.some_config['featuresRefreshRate'], client._split_fetcher_interval)
        self.assertEqual(self.some_config['segmentsRefreshRate'], client._segment_fetcher_interval)

    def test_forces_interval_max_on_intervals(self):
        """
        Tests that __init__ forces default maximum on intervals
        """
        self.some_config.update({
            'featuresRefreshRate': MAX_INTERVAL + 10,
            'segmentsRefreshRate': MAX_INTERVAL + 20,
            'metricsRefreshRate': MAX_INTERVAL + 30,
            'impressionsRefreshRate': MAX_INTERVAL + 40
        })
        client = SelfRefreshingClient(self.some_api_key, config=self.some_config)
        self.assertDictEqual(self.some_config, client._config)
        self.assertEqual(MAX_INTERVAL, client._split_fetcher_interval)
        self.assertEqual(MAX_INTERVAL, client._segment_fetcher_interval)
        self.assertEqual(MAX_INTERVAL, client._impressions_interval)

    def test_randomizes_intervales_if_randomize_intervals_is_true(self):
        """
        Tests that __init__ calls randomize_interval on intervals if randomizeIntervals is True
        """
        self.some_config['randomizeIntervals'] = True
        client = SelfRefreshingClient(self.some_api_key, config=self.some_config)

        self.assertListEqual([mock.call(self.some_config['segmentsRefreshRate']),
                              mock.call(self.some_config['featuresRefreshRate']),
                              mock.call(self.some_config['impressionsRefreshRate'])],
                             self.randomize_interval_mock.call_args_list)
        self.assertEqual(self.randomize_interval_side_effect[0], client._segment_fetcher_interval)
        self.assertEqual(self.randomize_interval_side_effect[1], client._split_fetcher_interval)
        self.assertEqual(self.randomize_interval_side_effect[2], client._impressions_interval)

    def test_sets_custom_sdk_api_base_url(self):
        """
        Tests that __init__ sets custom sdk_api_base_url value, if given
        """
        client = SelfRefreshingClient(self.some_api_key,
                                      sdk_api_base_url=self.some_sdk_api_url_base)
        self.assertEqual(self.some_sdk_api_url_base, client._sdk_api_url_base)

    def test_builds_sdk_api(self):
        """Tests that __init__ calls the SdkApi constructor"""
        client = SelfRefreshingClient(self.some_api_key,
                                      sdk_api_base_url=self.some_sdk_api_url_base)
        self.sdk_api_mock.assert_called_once_with(self.some_api_key,
                                                  sdk_api_base_url=self.some_sdk_api_url_base,
                                                  connect_timeout=client._connection_timeout,
                                                  read_timeout=client._read_timeout)


class SelfRefreshingClientBuildSplitFetcherTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.sdk_api_mock = self.patch('splitio.clients.SdkApi')
        self.api_segment_change_fetcher_mock = self.patch('splitio.clients.ApiSegmentChangeFetcher')
        self.self_refreshing_segment_fetcher_mock = self.patch(
            'splitio.clients.SelfRefreshingSegmentFetcher')
        self.api_split_change_fetcher_mock = self.patch('splitio.clients.ApiSplitChangeFetcher')
        self.split_parser_mock = self.patch('splitio.clients.SplitParser')
        self.self_refreshing_split_fetcher_mock = self.patch(
            'splitio.clients.SelfRefreshingSplitFetcher')

        self.some_api_key = mock.MagicMock()
        self.client = SelfRefreshingClient(self.some_api_key)

        self.segment_fetcher_interval_mock = self.patch_object(self.client,
                                                               '_segment_fetcher_interval')
        self.split_fetcher_interval_mock = self.patch_object(self.client, '_split_fetcher_interval')
        self.connection_timeout_mock = self.patch_object(self.client, '_connection_timeout')
        self.read_timeout_mock = self.patch_object(self.client, '_read_timeout')

    def test_builds_segment_change_fetcher(self):
        """Tests that _build_split_fetcher calls the ApiSegmentChangeFetcher constructor"""
        self.client._build_split_fetcher()
        self.api_segment_change_fetcher_mock.assert_called_once_with(self.sdk_api_mock.return_value)

    def test_builds_segment_fetcher(self):
        """Tests that _build_split_fetcher calls the SelfRefreshingSegmentFetcher constructor"""
        self.client._build_split_fetcher()
        self.self_refreshing_segment_fetcher_mock.assert_called_once_with(
            self.api_segment_change_fetcher_mock.return_value,
            interval=self.segment_fetcher_interval_mock)

    def test_builds_split_change_fetcher(self):
        """Tests that _build_split_fetcher calls the ApiSplitChangeFetcher constructor"""
        self.client._build_split_fetcher()
        self.api_split_change_fetcher_mock.assert_called_once_with(self.sdk_api_mock.return_value)

    def test_builds_split_parser(self):
        """Tests that _build_split_fetcher calls the SplitParser constructor"""
        self.client._build_split_fetcher()
        self.split_parser_mock.assert_called_once_with(
            self.self_refreshing_segment_fetcher_mock.return_value)

    def test_builds_split_fetcher(self):
        """Tests that _build_split_fetcher calls the SplitParser constructor"""
        self.client._build_split_fetcher()
        self.self_refreshing_split_fetcher_mock.assert_called_once_with(
            self.api_split_change_fetcher_mock.return_value, self.split_parser_mock.return_value,
            interval=self.split_fetcher_interval_mock)

    def test_starts_split_fetcher(self):
        """Tests that _build_split_fetcher calls start on the SelfRefreshingSplitFetcher"""
        self.client._build_split_fetcher()
        self.self_refreshing_split_fetcher_mock.return_value.start.assert_called_once_with()

    def test_returns_split_fetcher(self):
        """Tests that _build_split_fetcher returns the result of calling the
        SelfRefreshingSplitFetcher constructor"""
        self.assertEqual(self.self_refreshing_split_fetcher_mock.return_value,
                         self.client._build_split_fetcher())


class SelfRefreshingGetSplitFetcherTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_api_key = mock.MagicMock()
        self.client = SelfRefreshingClient(self.some_api_key)

        self.build_split_fetcher_mock = self.patch_object(self.client, '_build_split_fetcher')

    def test_calls_build_split_fetcher_if_split_fetcher_is_none(self):
        """Tests that get_split_fetcher calls _build_split_fetcher if _split_fetcher is None"""
        self.client.get_split_fetcher()
        self.build_split_fetcher_mock.assert_called_once_with()

    def test_sets_split_fetcher_to_result_of_calling_build_split_fetcher(self):
        """Tests that get_split_fetcher sets _split_fetcher to the result of calling
        _build_split_fetcher if it is None"""
        self.client.get_split_fetcher()
        self.assertEqual(self.build_split_fetcher_mock.return_value, self.client._split_fetcher)

    def test_doesnt_call_build_split_fetcher_if_split_fetcher_is_not_none(self):
        """
        Tests that get_split_fetcher doesn't call _build_split_fetcher if _split_fetcher is not
        None
        """
        some_split_fetcher = mock.MagicMock()
        self.client._split_fetcher = some_split_fetcher
        self.client.get_split_fetcher()
        self.build_split_fetcher_mock.assert_not_called()

    def test_returns_the_value_of_split_fetcher(self):
        """
        Tests that get_split_fetcher returns the value of _split_fetcher
        """
        some_split_fetcher = mock.MagicMock()
        self.client._split_fetcher = some_split_fetcher
        self.assertEqual(some_split_fetcher, self.client.get_split_fetcher())


class SelfRefreshingClientGetTreatmentLogTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.sdk_api_mock = self.patch('splitio.clients.SdkApi')
        self.self_updating_treatment_log_mock = self.patch(
            'splitio.clients.SelfUpdatingTreatmentLog')
        self.aync_treatment_log_mock = self.patch(
            'splitio.clients.AsyncTreatmentLog')
        self.some_api_key = mock.MagicMock()
        self.client = SelfRefreshingClient(self.some_api_key)

    def test_doesnt_call_constructors_if_treatment_log_is_not_none(self):
        """Tests that get_treatment_log doesn't call any constructors if _treatment_log is not
        None"""
        self.client._treatment_log = mock.MagicMock()
        self.client.get_treatment_log()
        self.self_updating_treatment_log_mock.assert_not_called()
        self.aync_treatment_log_mock.assert_not_called()

    def test_returns_existing_treatment_log_if_treatment_log_is_not_none(self):
        """Tests that get_treatment_log returns the value of _treatment_log if _treatment_log is
        not None"""
        some_treatment_log = mock.MagicMock()
        self.client._treatment_log = some_treatment_log
        self.assertEqual(some_treatment_log, self.client.get_treatment_log())

    def test_calls_self_updating_treatment_log_constructor(self):
        """Tests that get_treatment_log calls SelfUpdatingTreatmentLog constructor"""
        self.client.get_treatment_log()
        self.self_updating_treatment_log_mock.assert_called_once_with(
            self.client._sdk_api, max_count=self.client._max_impressions_log_size,
            interval=self.client._impressions_interval)

    def test_calls_async_treatment_log_constructor(self):
        """Tests that get_treatment_log calls AsyncTreatmentLog constructor"""
        self.client.get_treatment_log()
        self.aync_treatment_log_mock.assert_called_once_with(
            self.self_updating_treatment_log_mock.return_value)

    def test_sets_treatment_log_to_async_treatment_log(self):
        """Tests that get_treatment_log sets _treatment_log to an AsyncTreatmentLog"""
        self.client.get_treatment_log()
        self.assertEqual(self.aync_treatment_log_mock.return_value,
                         self.client._treatment_log)

    def test_returns_async_treatment_log(self):
        """Tests that get_treatment_log returns an AsyncTreatmentLog"""
        self.assertEqual(self.aync_treatment_log_mock.return_value, self.client.get_treatment_log())


class JSONFileClientIntegrationTests(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.segment_changes_file_name = join(dirname(__file__), 'segmentChanges.json')
        cls.split_changes_file_name = join(dirname(__file__), 'splitChanges.json')
        cls.client = JSONFileClient(cls.segment_changes_file_name, cls.split_changes_file_name)
        cls.on_treatment = 'on'
        cls.off_treatment = 'off'
        cls.some_key = 'some_key'
        cls.fake_id_in_segment = 'fake_id_1'
        cls.fake_id_not_in_segment = 'foobar'
        cls.fake_id_on_key = 'fake_id_on'
        cls.fake_id_off_key = 'fake_id_off'
        cls.fake_id_some_treatment_key = 'fake_id_some_treatment'
        cls.attribute_name = 'some_attribute'
        cls.unknown_feature_name = 'foobar'
        cls.in_between_datetime = arrow.get(2016, 4, 25, 16, 0).timestamp * 1000
        cls.not_in_between_datetime = arrow.get(2015, 4, 25, 16, 0).timestamp * 1000
        cls.in_between_number = 42
        cls.not_in_between_number = 85
        cls.equal_to_datetime = arrow.get(2016, 4, 25, 16, 0).timestamp * 1000
        cls.not_equal_to_datetime = arrow.get(2015, 4, 25, 16, 0).timestamp * 1000
        cls.equal_to_number = 50
        cls.not_equal_to_number = 85
        cls.greater_than_or_equal_to_datetime = arrow.get(2016, 4, 25, 16, 0).timestamp * 1000
        cls.not_greater_than_or_equal_to_datetime = arrow.get(2015, 4, 25, 16, 0).timestamp * 1000
        cls.greater_than_or_equal_to_number = 50
        cls.not_greater_than_or_equal_to_number = 32
        cls.less_than_or_equal_to_datetime = arrow.get(2015, 4, 25, 16, 0).timestamp * 1000
        cls.not_less_than_or_equal_to_datetime = arrow.get(2016, 4, 25, 16, 0).timestamp * 1000
        cls.less_than_or_equal_to_number = 32
        cls.not_less_than_or_equal_to_number = 50
        cls.multi_condition_equal_to_number = 42
        cls.multi_condition_not_equal_to_number = 85
        cls.in_whitelist = 'bitsy'
        cls.not_in_whitelist = 'foobar'

    #
    # basic tests
    #

    def test_no_key_returns_control(self):
        """
        Tests that get_treatment returns control treatment if the key is None
        """
        self.assertEqual(CONTROL, self.client.get_treatment(
            None, 'test_in_segment'))

    def test_unknown_feature_returns_control(self):
        """
        Tests that get_treatment returns control treatment if feature is unknown
        """
        self.assertEqual(CONTROL, self.client.get_treatment(
            self.some_key, self.unknown_feature_name))

    #
    # test_between_datetime tests
    #

    def test_test_between_datetime_include_on_user(self):
        """
        Test that get_treatment returns on for the test_between_datetime feature using the user key
        included for on treatment
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_between_datetime',
            {self.attribute_name: self.in_between_datetime}))

    def test_test_between_datetime_include_on_user_no_attribute_match(self):
        """
        Test that get_treatment returns on for the test_between_datetime feature using the user key
        included for on treatment even while there is no attribute match
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_between_datetime',
            {self.attribute_name: self.not_in_between_datetime}))

    def test_test_between_datetime_include_off_user(self):
        """
        Test that get_treatment returns off for the test_between_datetime feature using the user key
        included for off treatment
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.fake_id_off_key, 'test_between_datetime',
            {self.attribute_name: self.in_between_datetime}))

    def test_test_between_datetime_some_key_attribute_match(self):
        """
        Test that get_treatment returns on for the test_between_datetime feature using the some key
        while the attribute matches (100% for on treatment)
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.some_key, 'test_between_datetime',
            {self.attribute_name: self.in_between_datetime}))

    def test_test_between_datetime_some_key_no_attribute_match(self):
        """
        Test that get_treatment returns off for the test_between_datetime feature using the some key
        while the attribute doesn't match (100% for on treatment)
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.some_key, 'test_between_datetime',
            {self.attribute_name: self.not_in_between_datetime}))

    def test_test_between_datetime_some_key_no_attributes(self):
        """
        Test that get_treatment returns off for the test_between_datetime feature using the some key
        and no attributes
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.some_key, 'test_between_datetime'))

    #
    # test_between_number tests
    #

    def test_test_between_number_include_on_user(self):
        """
        Test that get_treatment returns on for the test_between_number feature using the user key
        included for on treatment
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_between_number',
            {self.attribute_name: self.in_between_number}))

    def test_test_between_number_include_on_user_no_attribute_match(self):
        """
        Test that get_treatment returns on for the test_between_number feature using the user key
        included for on treatment even while there is no attribute match
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_between_number',
            {self.attribute_name: self.not_in_between_number}))

    def test_test_between_number_include_off_user(self):
        """
        Test that get_treatment returns off for the test_between_number feature using the user key
        included for off treatment
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.fake_id_off_key, 'test_between_number',
            {self.attribute_name: self.in_between_number}))

    def test_test_between_number_some_key_attribute_match(self):
        """
        Test that get_treatment returns on for the test_between_number feature using the some key
        while the attribute matches (100% for on treatment)
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.some_key, 'test_between_number',
            {self.attribute_name: self.in_between_number}))

    def test_test_between_number_some_key_no_attribute_match(self):
        """
        Test that get_treatment returns off for the test_between_number feature using the some key
        while the attribute doesn't match (100% for on treatment)
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.some_key, 'test_between_number',
            {self.attribute_name: self.not_in_between_number}))

    def test_test_between_number_some_key_no_attributes(self):
        """
        Test that get_treatment returns off for the test_between_number feature using the some key
        and no attributes
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.some_key, 'test_between_number'))

    #
    # test_equal_to_datetime tests
    #

    def test_test_equal_to_datetime_include_on_user(self):
        """
        Test that get_treatment returns on for the test_equal_to_datetime feature using the user key
        included for on treatment
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_equal_to_datetime',
            {self.attribute_name: self.equal_to_datetime}))

    def test_test_equal_to_datetime_include_on_user_no_attribute_match(self):
        """
        Test that get_treatment returns on for the test_equal_to_datetime feature using the user key
        included for on treatment even while there is no attribute match
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_equal_to_datetime',
            {self.attribute_name: self.not_equal_to_datetime}))

    def test_test_equal_to_datetime_include_off_user(self):
        """
        Test that get_treatment returns off for the test_equal_to_datetime feature using the user
        key included for off treatment
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.fake_id_off_key, 'test_equal_to_datetime',
            {self.attribute_name: self.equal_to_datetime}))

    def test_test_equal_to_datetime_some_key_attribute_match(self):
        """
        Test that get_treatment returns on for the test_equal_to_datetime feature using the some key
        while the attribute matches (100% for on treatment)
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.some_key, 'test_equal_to_datetime',
            {self.attribute_name: self.equal_to_datetime}))

    def test_test_equal_to_datetime_some_key_no_attribute_match(self):
        """
        Test that get_treatment returns off for the test_equal_to_datetime feature using the some
        key while the attribute doesn't match (100% for on treatment)
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.some_key, 'test_equal_to_datetime',
            {self.attribute_name: self.not_equal_to_datetime}))

    def test_test_equal_to_datetime_some_key_no_attributes(self):
        """
        Test that get_treatment returns off for the test_equal_to_datetime feature using the some
        key and no attributes
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.some_key, 'test_equal_to_datetime'))

    #
    # test_equal_to_number tests
    #

    def test_test_equal_to_number_include_on_user(self):
        """
        Test that get_treatment returns on for the test_equal_to_number feature using the user key
        included for on treatment
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_equal_to_number',
            {self.attribute_name: self.equal_to_number}))

    def test_test_equal_to_number_include_on_user_no_attribute_match(self):
        """
        Test that get_treatment returns on for the test_equal_to_number feature using the user key
        included for on treatment even while there is no attribute match
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_equal_to_number',
            {self.attribute_name: self.not_equal_to_number}))

    def test_test_equal_to_number_include_off_user(self):
        """
        Test that get_treatment returns off for the test_equal_to_number feature using the user key
        included for off treatment
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.fake_id_off_key, 'test_equal_to_number',
            {self.attribute_name: self.equal_to_number}))

    def test_test_equal_to_number_some_key_attribute_match(self):
        """
        Test that get_treatment returns on for the test_equal_to_number feature using the some key
        while the attribute matches (100% for on treatment)
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.some_key, 'test_equal_to_number',
            {self.attribute_name: self.equal_to_number}))

    def test_test_equal_to_number_some_key_no_attribute_match(self):
        """
        Test that get_treatment returns off for the test_equal_to_number feature using the some key
        while the attribute doesn't match (100% for on treatment)
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.some_key, 'test_equal_to_number',
            {self.attribute_name: self.not_equal_to_number}))

    def test_test_equal_to_number_some_key_no_attributes(self):
        """
        Test that get_treatment returns off for the test_equal_to_number feature using the some key
        and no attributes
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.some_key, 'test_equal_to_number'))

    #
    # test_greater_than_or_equal_to_datetime tests
    #

    def test_test_greater_than_or_equal_to_datetime_include_on_user(self):
        """
        Test that get_treatment returns on for the test_greater_than_or_equal_to_datetime feature
        using the user key included for on treatment
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_greatr_than_or_equal_to_datetime',
            {self.attribute_name: self.greater_than_or_equal_to_datetime}))

    def test_test_greater_than_or_equal_to_datetime_include_on_user_no_attribute_match(self):
        """
        Test that get_treatment returns on for the test_greater_than_or_equal_to_datetime feature
        using the user key included for on treatment even while there is no attribute match
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_greatr_than_or_equal_to_datetime',
            {self.attribute_name: self.not_greater_than_or_equal_to_datetime}))

    def test_test_greater_than_or_equal_to_datetime_include_off_user(self):
        """
        Test that get_treatment returns off for the test_greater_than_or_equal_to_datetime feature
        using the user key included for off treatment
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.fake_id_off_key, 'test_greatr_than_or_equal_to_datetime',
            {self.attribute_name: self.greater_than_or_equal_to_datetime}))

    def test_test_greater_than_or_equal_to_datetime_some_key_attribute_match(self):
        """
        Test that get_treatment returns on for the test_greater_than_or_equal_to_datetime feature
        using the some key while the attribute matches (100% for on treatment)
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.some_key, 'test_greatr_than_or_equal_to_datetime',
            {self.attribute_name: self.greater_than_or_equal_to_datetime}))

    def test_test_greater_than_or_equal_to_datetime_some_key_no_attribute_match(self):
        """
        Test that get_treatment returns off for the test_greater_than_or_equal_to_datetime feature
        using the some key while the attribute doesn't match (100% for on treatment)
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.some_key, 'test_greatr_than_or_equal_to_datetime',
            {self.attribute_name: self.not_greater_than_or_equal_to_datetime}))

    def test_test_greater_than_or_equal_to_datetime_some_key_no_attributes(self):
        """
        Test that get_treatment returns off for the test_greater_than_or_equal_to_datetime feature
        using the some key and no attributes
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.some_key, 'test_greatr_than_or_equal_to_datetime'))

    #
    # test_greater_than_or_equal_to_number tests
    #

    def test_test_greater_than_or_equal_to_number_include_on_user(self):
        """
        Test that get_treatment returns on for the test_greater_than_or_equal_to_number feature
        using the user key included for on treatment
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_greatr_than_or_equal_to_number',
            {self.attribute_name: self.greater_than_or_equal_to_number}))

    def test_test_greater_than_or_equal_to_number_include_on_user_no_attribute_match(self):
        """
        Test that get_treatment returns on for the test_greater_than_or_equal_to_number feature
        using the user key included for on treatment even while there is no attribute match
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_greatr_than_or_equal_to_number',
            {self.attribute_name: self.not_greater_than_or_equal_to_number}))

    def test_test_greater_than_or_equal_to_number_include_off_user(self):
        """
        Test that get_treatment returns off for the test_greater_than_or_equal_to_number feature
        using the user key included for off treatment
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.fake_id_off_key, 'test_greatr_than_or_equal_to_number',
            {self.attribute_name: self.greater_than_or_equal_to_datetime}))

    def test_test_greater_than_or_equal_to_number_some_key_attribute_match(self):
        """
        Test that get_treatment returns on for the test_greater_than_or_equal_to_number feature
        using the some key while the attribute matches (100% for on treatment)
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.some_key, 'test_greatr_than_or_equal_to_number',
            {self.attribute_name: self.greater_than_or_equal_to_datetime}))

    def test_test_greater_than_or_equal_to_number_some_key_no_attribute_match(self):
        """
        Test that get_treatment returns off for the test_greater_than_or_equal_to_number feature
        using the some key while the attribute doesn't match (100% for on treatment)
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.some_key, 'test_greatr_than_or_equal_to_number',
            {self.attribute_name: self.not_greater_than_or_equal_to_number}))

    def test_test_greater_than_or_equal_to_number_some_key_no_attributes(self):
        """
        Test that get_treatment returns off for the test_greater_than_or_equal_to_number feature
        using the some key and no attributes
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.some_key, 'test_greatr_than_or_equal_to_number'))

    #
    # test_less_than_or_equal_to_datetime tests
    #

    def test_test_less_than_or_equal_to_datetime_include_on_user(self):
        """
        Test that get_treatment returns on for the test_less_than_or_equal_to_datetime feature
        using the user key included for on treatment
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_less_than_or_equal_to_datetime',
            {self.attribute_name: self.less_than_or_equal_to_datetime}))

    def test_test_less_than_or_equal_to_datetime_include_on_user_no_attribute_match(self):
        """
        Test that get_treatment returns on for the test_less_than_or_equal_to_datetime feature
        using the user key included for on treatment even while there is no attribute match
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_less_than_or_equal_to_datetime',
            {self.attribute_name: self.not_less_than_or_equal_to_datetime}))

    def test_test_less_than_or_equal_to_datetime_include_off_user(self):
        """
        Test that get_treatment returns off for the test_less_than_or_equal_to_datetime feature
        using the user key included for off treatment
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.fake_id_off_key, 'test_less_than_or_equal_to_datetime',
            {self.attribute_name: self.less_than_or_equal_to_datetime}))

    def test_test_less_than_or_equal_to_datetime_some_key_attribute_match(self):
        """
        Test that get_treatment returns on for the test_less_than_or_equal_to_datetime feature
        using the some key while the attribute matches (100% for on treatment)
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.some_key, 'test_less_than_or_equal_to_datetime',
            {self.attribute_name: self.less_than_or_equal_to_datetime}))

    def test_test_less_than_or_equal_to_datetime_some_key_no_attribute_match(self):
        """
        Test that get_treatment returns off for the test_less_than_or_equal_to_datetime feature
        using the some key while the attribute doesn't match (100% for on treatment)
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.some_key, 'test_less_than_or_equal_to_datetime',
            {self.attribute_name: self.not_less_than_or_equal_to_datetime}))

    def test_test_less_than_or_equal_to_datetime_some_key_no_attributes(self):
        """
        Test that get_treatment returns off for the test_less_than_or_equal_to_datetime feature
        using the some key and no attributes
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.some_key, 'test_less_than_or_equal_to_datetime'))

    #
    # test_in_segment tests
    #

    def test_test_in_segment_include_on_user(self):
        """
        Test that get_treatment returns on for the test_in_segment feature using the user key
        included for on treatment
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_in_segment'))

    def test_test_in_segment_include_off_user(self):
        """
        Test that get_treatment returns off for the test_in_segment feature using the user key
        included for off treatment
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.fake_id_off_key, 'test_in_segment'))

    def test_test_in_segment_in_segment_key(self):
        """
        Test that get_treatment returns on for the test_in_segment feature using a key in the
        segment
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_in_segment, 'test_in_segment'))

    def test_test_in_segment_not_in_segment_key(self):
        """
        Test that get_treatment returns off for the test_in_segment feature using a key not in the
        segment
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.fake_id_not_in_segment, 'test_in_segment'))

    #
    # test_in_segment_multi_treatment tests
    #

    def test_test_in_segment_multi_treatment_include_on_user(self):
        """
        Test that get_treatment returns on for the test_in_segment_multi_treatment feature using
        the user key included for on treatment
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_in_segment_multi_treatment'))

    def test_test_in_segment_multi_treatment_include_off_user(self):
        """
        Test that get_treatment returns off for the test_in_segment_multi_treatment feature using
        the user key included for off treatment
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.fake_id_off_key, 'test_in_segment_multi_treatment'))

    def test_test_in_segment_multi_treatment_include_some_treatment_user(self):
        """
        Test that get_treatment returns on for the test_in_segment_multi_treatment feature using
        the user key included for some_treatment treatment
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_in_segment_multi_treatment'))

    def test_test_in_segment_multi_treatment_in_segment_key(self):
        """
        Test that get_treatment returns on for the test_in_segment_multi_treatment feature using a
        key in the segment
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_in_segment, 'test_in_segment_multi_treatment'))

    def test_test_in_segment_multi_treatment_not_in_segment_key(self):
        """
        Test that get_treatment returns off for the test_in_segment_multi_treatment feature using a
        key not in the segment
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.fake_id_not_in_segment, 'test_in_segment_multi_treatment'))

    #
    # test_multi_condition tests
    #

    def test_test_multi_condition_include_on_user(self):
        """
        Test that get_treatment returns on for the test_multi_condition feature using the user key
        included for on treatment
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_multi_condition',
            {self.attribute_name: self.multi_condition_not_equal_to_number}))

    def test_test_multi_condition_include_off_user(self):
        """
        Test that get_treatment returns off for the test_multi_condition feature using the user key
        included for off treatment
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.fake_id_off_key, 'test_multi_condition',
            {self.attribute_name: self.multi_condition_equal_to_number}))

    def test_test_multi_condition_in_segment_key_no_attribute_match(self):
        """
        Test that get_treatment returns on for the test_multi_condition feature using a key in the
        segment
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_in_segment, 'test_multi_condition',
            {self.attribute_name: self.multi_condition_not_equal_to_number}))

    def test_test_multi_condition_not_in_segment_key_attribute_match(self):
        """
        Test that get_treatment returns on for the test_multi_condition feature using a key not in
        the segment
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_not_in_segment, 'test_multi_condition',
            {self.attribute_name: self.multi_condition_equal_to_number}))

    def test_test_multi_condition_not_in_segment_key_no_attribute_match(self):
        """
        Test that get_treatment returns off for the test_multi_condition feature using a key not in
        the segment
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.fake_id_not_in_segment, 'test_multi_condition',
            {self.attribute_name: self.multi_condition_not_equal_to_number}))

    #
    # test_whitelist tests
    #

    def test_test_whitelist_include_on_user(self):
        """
        Test that get_treatment returns on for the test_whitelist feature using the user key
        included for on treatment
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_whitelist',
            {self.attribute_name: self.not_in_whitelist}))

    def test_test_whitelist_include_off_user(self):
        """
        Test that get_treatment returns off for the test_whitelist feature using the user key
        included for off treatment
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.fake_id_off_key, 'test_whitelist',
            {self.attribute_name: self.not_in_whitelist}))

    def test_test_whitelist_in_whitelist(self):
        """
        Test that get_treatment returns on for the test_whitelist feature using an attribute
        in the whitelist
        """
        self.assertEqual(self.on_treatment, self.client.get_treatment(
            self.fake_id_in_segment, 'test_whitelist',
            {self.attribute_name: self.in_whitelist}))

    def test_test_whitelist_not_in_whitelist(self):
        """
        Test that get_treatment returns off for the test_whitelist feature using an attribute not
        in the whitelist
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.fake_id_not_in_segment, 'test_whitelist',
            {self.attribute_name: self.not_in_whitelist}))

    #
    # test_killed tests
    #

    def test_test_killed_include_on_user(self):
        """
        Test that get_treatment returns off for the test_killed feature using the user key
        included for on treatment
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.fake_id_on_key, 'test_killed'))

    def test_test_killed_include_off_user(self):
        """
        Test that get_treatment returns off for the test_killed feature using the user key
        included for off treatment
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.fake_id_off_key, 'test_killed'))

    def test_test_killed_in_segment_key(self):
        """
        Test that get_treatment returns off for the test_killed feature using a key in the
        segment
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.fake_id_in_segment, 'test_killed'))

    def test_test_killed_not_in_segment_key(self):
        """
        Test that get_treatment returns off for the test_killed feature using a key not in the
        segment
        """
        self.assertEqual(self.off_treatment, self.client.get_treatment(
            self.fake_id_not_in_segment, 'test_killed'))
