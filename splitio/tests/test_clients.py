"""Unit tests for the matchers module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

import tempfile
import arrow

from unittest import TestCase
from os.path import dirname, join
from time import sleep

from splitio.clients import (Client, SelfRefreshingClient, randomize_interval, JSONFileClient,
                             LocalhostEnvironmentClient)
from splitio.exceptions import TimeoutException
from splitio.config import DEFAULT_CONFIG, MAX_INTERVAL, SDK_API_BASE_URL, EVENTS_API_BASE_URL
from splitio.treatments import CONTROL
from splitio.tests.utils import MockUtilsMixin


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
        self.some_algo = mock.MagicMock()
        self.some_split = mock.MagicMock()
        self.some_split.killed = False
        self.some_split.conditions.__iter__.return_value = self.some_conditions
        self.splitter_mock = self.patch('splitio.clients.Splitter')
        self.client = Client()
        self.get_split_fetcher_mock = self.patch_object(self.client, 'get_split_fetcher')
        self.get_treatment_log_mock = self.patch_object(self.client, 'get_treatment_log')
        self.get_metrics = self.patch_object(self.client, 'get_metrics')
        self.record_stats_mock = self.patch_object(self.client, '_record_stats')

    def test_get_splitter_returns_a_splitter(self):
        """Test that get_splitter returns a splitter"""
        self.assertEqual(self.splitter_mock.return_value, self.client.get_splitter())

    def test_get_treatment_returns_control_if_key_is_none(self):
        """Test that get_treatment returns CONTROL treatment if key is None"""
        self.assertEqual(CONTROL, self.client.get_treatment(None, self.some_feature,
                                                            self.some_attributes))

    def test_get_treatment_returns_control_if_feature_is_none(self):
        """Test that get_treatment returns CONTROL treatment if feature is None"""
        self.assertEqual(CONTROL, self.client.get_treatment(self.some_key, None,
                                                            self.some_attributes))

    def test_get_treatment_calls_split_fetcher_fetch(self):
        """Test that get_treatment calls split fetcher fetch"""
        self.client.get_treatment(self.some_key, self.some_feature, self.some_attributes)
        self.get_split_fetcher_mock.return_value.fetch.assert_called_once_with(self.some_feature)

    def test_get_treatment_returns_control_if_get_split_fetcher_raises_exception(self):
        """Test that get_treatment returns CONTROL treatment if get_split_fetcher raises an
        exception"""
        self.get_split_fetcher_mock.side_effect = Exception()
        self.assertEqual(CONTROL, self.client.get_treatment(self.some_key, self.some_feature,
                                                            self.some_attributes))

    def test_get_treatment_returns_control_if_fetch_raises_exception(self):
        """Test that get_treatment returns CONTROL treatment if fetch raises an exception"""
        self.get_split_fetcher_mock.return_value.fetch.side_effect = Exception()
        self.assertEqual(CONTROL, self.client.get_treatment(self.some_key, self.some_feature,
                                                            self.some_attributes))

    def test_get_treatment_returns_control_if_split_is_none(self):
        """Test that get_treatment returns CONTROL treatment if split is None"""
        self.get_split_fetcher_mock.return_value.fetch.return_value = None
        self.assertEqual(CONTROL, self.client.get_treatment(self.some_key, self.some_feature,
                                                            self.some_attributes))

    def test_get_treatment_returns_control_if_get_treatment_for_split_raises_exception(self):
        """Test that get_treatment returns CONTROL treatment _get_treatment_for_split raises an
        exception"""
        self.some_split.killed = False
        self.get_split_fetcher_mock.return_value.fetch.return_value = self.some_split
        self.patch_object(self.client, '_get_treatment_for_split', side_effect=Exception())
        self.assertEqual(CONTROL, self.client.get_treatment(self.some_key, self.some_feature,
                                                            self.some_attributes))

    def test_get_treatment_returns_default_treatment_if_feature_is_killed(self):
        """Test that get_treatment returns default treatment if split is Killed"""
        self.some_split.killed = True
        self.get_split_fetcher_mock.return_value.fetch.return_value = self.some_split
        self.assertEqual(self.some_split.default_treatment,
                         self.client.get_treatment(self.some_key, self.some_feature,
                                                            self.some_attributes))

    def test_get_treatment_returns_default_treatment_if_no_conditions_match(self):
        """Test that _get_treatment_for_split returns None if no split conditions_match"""
        self.some_conditions[0].matcher.match.return_value = False
        self.some_conditions[1].matcher.match.return_value = False
        self.some_conditions[2].matcher.match.return_value = False

        treatment, label = self.client._get_treatment_for_split(self.some_split, self.some_key,
                                                              self.some_feature)

        self.assertEqual(None, treatment)
        self.assertEqual(None, label)

    def test_get_treatment_calls_condition_matcher_match_with_short_circuit(self):
        """
        Test that _get_treatment_for_split calls the conditions matcher match method until a match
        is found
        """
        self.some_conditions[0].matcher.match.return_value = False
        self.some_conditions[1].matcher.match.return_value = True
        self.some_conditions[2].matcher.match.return_value = False
        self.client._get_treatment_for_split(self.some_split, self.some_key, self.some_key, self.some_attributes)
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
        self.client._get_treatment_for_split(self.some_split, self.some_key, self.some_key, self.some_attributes)
        self.splitter_mock.return_value.get_treatment.assert_called_once_with(
            self.some_key, self.some_split.seed, self.some_conditions[1].partitions,
            self.some_split.algo
        )

    def test_get_treatment_calls_record_stats(self):
        """Test that get_treatment calls get_split_fetcher"""
        get_treatment_for_split_mock = self.patch_object(self.client, '_get_treatment_for_split')
        self.client.get_treatment(self.some_key, self.some_feature, self.some_attributes)

        impression = self.client._build_impression(self.some_key, self.some_feature, 'some_treatment', 'some_label',
                                                   -1, self.some_key, mock.ANY)

        self.client._record_stats(impression, mock.ANY, 'sdk.getTreatment')

class ClientRecordStatsTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_feature = mock.MagicMock()
        self.some_treatment = mock.MagicMock()
        self.some_start = 123456000
        self.some_operation = mock.MagicMock()

        self.client = Client()
        self.get_treatment_log_mock = self.patch_object(self.client, 'get_treatment_log')
        self.get_metrics_mock = self.patch_object(self.client, 'get_metrics')
        self.arrow_mock = self.patch('splitio.clients.time')
        self.arrow_mock.utcnow.return_value.timestamp = 123457

    def test_record_stats_calls_treatment_log_log(self):
        """Test that _record_stats calls log on the treatment log"""

        impression = self.client._build_impression(self.some_key, self.some_feature, self.some_treatment, 'some_label',
                                                   -1, self.some_key, self.some_start)

        self.client._record_stats(impression, self.some_start, self.some_operation)

        self.get_treatment_log_mock.return_value.log.assert_called_once_with(impression)

    def test_record_stats_doesnt_raise_an_exception_if_log_does(self):
        """Test that _record_stats doesn't raise an exception if log does"""
        self.get_treatment_log_mock.return_value.log.side_effect = Exception()
        try:

            impression = self.client._build_impression(self.some_key, self.some_feature, self.some_treatment,
                                                       'some_label',
                                                       -1, self.some_key, self.some_start)

            self.client._record_stats(impression, self.some_start, self.some_operation)
        except:
            self.fail('Unexpected exception raised')

    def test_record_stats_calls_metrics_time(self):
        """Test that _record_stats calls time on the metrics object"""

        impression = self.client._build_impression(self.some_key, self.some_feature, self.some_treatment, 'some_label',
                                            -1, self.some_key, self.some_start)

        self.client._record_stats(impression, self.some_start, self.some_operation)

        self.get_metrics_mock.return_value.time.assert_called_once()


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
        self.init_config_mock = self.patch('splitio.clients.SelfRefreshingClient._init_config')
        self.build_sdk_api_mock = self.patch('splitio.clients.SelfRefreshingClient._build_sdk_api')
        self.build_split_fetcher_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_split_fetcher')
        self.build_treatment_log_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_treatment_log')
        self.build_metrics_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_metrics')
        self.start_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._start')

        self.some_api_key = mock.MagicMock()
        self.some_config = mock.MagicMock()

    def test_sets_api_key(self):
        """Test that __init__ sets api key to the given value"""
        client = SelfRefreshingClient(self.some_api_key)
        self.assertEqual(self.some_api_key, client._api_key)

    def test_calls_init_config(self):
        """Test that __init__ calls _init_config with the given config"""
        SelfRefreshingClient(self.some_api_key, config=self.some_config)
        self.init_config_mock.assert_called_once_with(self.some_config)

    def test_calls_build_sdk_api(self):
        """Test that __init__ calls _build_sdk_api"""
        client = SelfRefreshingClient(self.some_api_key)
        self.build_sdk_api_mock.assert_called_once_with()
        self.assertEqual(self.build_sdk_api_mock.return_value, client._sdk_api)

    def test_calls_build_split_fetcher(self):
        """Test that __init__ calls _build_split_fetcher"""
        client = SelfRefreshingClient(self.some_api_key)
        self.build_split_fetcher_mock.assert_called_once_with()
        self.assertEqual(self.build_split_fetcher_mock.return_value, client._split_fetcher)

    def test_calls_build_build_treatment_log(self):
        """Test that __init__ calls _build_treatment_log"""
        client = SelfRefreshingClient(self.some_api_key)
        self.build_treatment_log_mock.assert_called_once_with()
        self.assertEqual(self.build_treatment_log_mock.return_value, client._treatment_log)

    def test_calls_build_treatment_log(self):
        """Test that __init__ calls _build_treatment_log"""
        client = SelfRefreshingClient(self.some_api_key)
        self.build_treatment_log_mock.assert_called_once_with()
        self.assertEqual(self.build_treatment_log_mock.return_value, client._treatment_log)

    def test_calls_build_metrics(self):
        """Test that __init__ calls _build_metrics"""
        client = SelfRefreshingClient(self.some_api_key)
        self.build_metrics_mock.assert_called_once_with()
        self.assertEqual(self.build_metrics_mock.return_value, client._metrics)

    def test_calls_start(self):
        """Test that __init__ calls _start"""
        SelfRefreshingClient(self.some_api_key)
        self.start_mock.assert_called_once_with()


class SelfRefreshingClientStartTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.event_mock = self.patch('splitio.clients.Event')
        self.event_mock.return_value.wait.return_value = True
        self.thread_mock = self.patch('splitio.clients.Thread')
        self.build_sdk_api_mock = self.patch('splitio.clients.SelfRefreshingClient._build_sdk_api')
        self.build_split_fetcher_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_split_fetcher')
        self.build_treatment_log_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_treatment_log')
        self.build_metrics_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_metrics')
        self.fetch_splits_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._fetch_splits')

        self.some_api_key = mock.MagicMock()

    def test_calls_start_on_treatment_log_delegate(self):
        """Test that _start calls start on the treatment log delegate"""
        SelfRefreshingClient(self.some_api_key, config={'ready': 0})
        self.build_treatment_log_mock.return_value.delegate.start.assert_called_once_with()

    def test_calls_start_on_treatment_log_delegate_with_timeout(self):
        """Test that _start calls start on the treatment log delegate when a timeout is given"""
        SelfRefreshingClient(self.some_api_key, config={'ready': 10})
        self.build_treatment_log_mock.return_value.delegate.start.assert_called_once_with()

    def test_no_event_or_thread_created_if_timeout_is_zero(self):
        """Test that if timeout is zero, no threads or events are created"""
        SelfRefreshingClient(self.some_api_key, config={'ready': 0})
        self.event_mock.assert_not_called()
        self.thread_mock.assert_not_called()

    def test_split_fetcher_start_called_if_timeout_is_zero(self):
        """Test that if timeout is zero, start is called on the split fetcher"""
        SelfRefreshingClient(self.some_api_key, config={'ready': 0})
        self.build_split_fetcher_mock.assert_called_once_with()

    def test_event_created_if_timeout_is_non_zero(self):
        """Test that if timeout is non-zero, an event is created"""
        SelfRefreshingClient(self.some_api_key, config={'ready': 10})
        self.event_mock.assert_called_once_with()

    def test_wait_is_called_on_event_if_timeout_is_non_zero(self):
        """Test that if timeout is non-zero, wait is called on the event"""
        SelfRefreshingClient(self.some_api_key, config={'ready': 10})
        self.event_mock.return_value.wait.asser_called_once_with(10)

    def test_thread_created_if_timeout_is_non_zero(self):
        """Test that if timeout is non-zero, a thread with target _fetch_splits is created"""
        SelfRefreshingClient(self.some_api_key, config={'ready': 10})
        self.thread_mock.assert_called_once_with(target=self.fetch_splits_mock,
                                                 args=(self.event_mock.return_value,))
        self.thread_mock.return_value.start.asser_called_once_with()

    def test_if_event_flag_is_not_set_an_exception_is_raised(self):
        """Test that if the event flag is not set, a TimeoutException is raised"""
        self.event_mock.return_value.wait.return_value = False
        with self.assertRaises(TimeoutException):
            SelfRefreshingClient(self.some_api_key, config={'ready': 10})

    def test_if_event_flag_is_set_an_exception_is_not_raised(self):
        """Test that if the event flag is set, a TimeoutException is not raised"""
        try:
            SelfRefreshingClient(self.some_api_key, config={'ready': 10})
        except:
            self.fail('An unexpected exception was raised')


class SelfRefreshingClientFetchSplitsTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_event = mock.MagicMock()
        self.build_sdk_api_mock = self.patch('splitio.clients.SelfRefreshingClient._build_sdk_api')
        self.build_split_fetcher_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_split_fetcher')
        self.build_treatment_log_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_treatment_log')
        self.build_metrics_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_metrics')

        self.some_api_key = mock.MagicMock()
        self.client = SelfRefreshingClient(self.some_api_key, config={'ready': 10})
        self.build_split_fetcher_mock.reset_mock()

    def test_calls_refresh_splits_on_split_fetcher(self):
        """Test that _fetch_splits calls refresh_splits on split_fetcher"""
        self.client._fetch_splits(self.some_event)
        self.build_split_fetcher_mock.return_value.refresh_splits.assert_called_once_with(
            block_until_ready=True)

    def test_calls_start_on_split_fetcher(self):
        """Test that _fetch_splits calls start on split_fetcher"""
        self.client._fetch_splits(self.some_event)
        self.build_split_fetcher_mock.return_value.start.assert_called_once_with(
            delayed_update=True)

    def test_calls_set_on_event(self):
        """Test that _fetch_splits calls set on event"""
        self.client._fetch_splits(self.some_event)
        self.some_event.set.assert_called_once_with()


class SelfRefreshingClientInitConfigTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.build_sdk_api_mock = self.patch('splitio.clients.SelfRefreshingClient._build_sdk_api')
        self.build_split_fetcher_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_split_fetcher')
        self.build_treatment_log_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_treatment_log')
        self.build_metrics_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_metrics')
        self.start_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._start')
        self.some_api_key = mock.MagicMock()
        self.randomize_interval_side_effect = [mock.MagicMock(), mock.MagicMock(), mock.MagicMock()]
        self.randomize_interval_mock = self.patch(
            'splitio.clients.randomize_interval', side_effect=self.randomize_interval_side_effect)

        self.some_config = {
            'connectionTimeout': mock.MagicMock(),
            'readTimeout': mock.MagicMock(),
            'featuresRefreshRate': 31,
            'segmentsRefreshRate': 32,
            'metricsRefreshRate': 33,
            'impressionsRefreshRate': 34,
            'randomizeIntervals': False,
            'maxImpressionsLogSize': -1,
            'maxMetricsCallsBeforeFlush': -1,
            'ready': 10,
            'sdkApiBaseUrl': SDK_API_BASE_URL,
            'eventsApiBaseUrl': EVENTS_API_BASE_URL,
            'splitSdkMachineName': None,
            'splitSdkMachineIp': None,
            'redisHost': 'localhost',
            'redisPort': 6379,
            'redisDb': 0,
            'redisPassword':None,
            'redisSocketTimeout': None,
            'redisSocketConnectTimeout': None,
            'redisSocketKeepalive': None,
            'redisSocketKeepaliveOptions': None,
            'redisConnectionPool': None,
            'redisUnixSocketPath': None,
            'redisEncoding': 'utf-8',
            'redisEncodingErrors': 'strict',
            'redisCharset': None,
            'redisErrors': None,
            'redisDecodeResponses': False,
            'redisRetryOnTimeout': False,
            'redisSsl': False,
            'redisSslKeyfile': None,
            'redisSslCertfile': None,
            'redisSslCertReqs': None,
            'redisSslCaCerts': None,
            'redisMaxConnections': None
        }



        self.client = SelfRefreshingClient(self.some_api_key)

    def test_if_config_is_none_uses_default(self):
        """Test that if config is None _init_config uses the defaults"""
        self.client._init_config(config=None)
        self.assertDictEqual(DEFAULT_CONFIG, self.client._config)

    def test_it_uses_supplied_config(self):
        """Test that if config is not None, it uses the supplied config"""
        self.client._init_config(config=self.some_config)

        print('!1', self.some_config)
        print('!2', self.client._config)

        self.assertDictEqual(self.some_config, self.client._config)

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
        self.client._init_config(config=self.some_config)
        self.assertEqual(MAX_INTERVAL, self.client._split_fetcher_interval)
        self.assertEqual(MAX_INTERVAL, self.client._segment_fetcher_interval)
        self.assertEqual(MAX_INTERVAL, self.client._impressions_interval)

    def test_randomizes_intervales_if_randomize_intervals_is_true(self):
        """
        Tests that __init__ calls randomize_interval on intervals if randomizeIntervals is True
        """
        self.some_config['randomizeIntervals'] = True
        self.client._init_config(config=self.some_config)
        self.assertListEqual([mock.call(self.some_config['segmentsRefreshRate']),
                              mock.call(self.some_config['featuresRefreshRate']),
                              mock.call(self.some_config['impressionsRefreshRate'])],
                             self.randomize_interval_mock.call_args_list)
        self.assertEqual(self.randomize_interval_side_effect[0],
                         self.client._segment_fetcher_interval)
        self.assertEqual(self.randomize_interval_side_effect[1],
                         self.client._split_fetcher_interval)
        self.assertEqual(self.randomize_interval_side_effect[2],
                         self.client._impressions_interval)

    def test_sets_enabled_labels(self):
        """Test that sets labels enabled to the given value"""
        client = SelfRefreshingClient(self.some_api_key, config={'labelsEnabled': False})
        self.assertFalse(client._labels_enabled)

    def test_default_enabled_labels(self):
        """Test that sets labels enabled to the given value"""
        client = SelfRefreshingClient(self.some_api_key)
        self.assertTrue(client._labels_enabled)

class SelfRefreshingClientBuildSdkApiTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.sdk_api_mock = self.patch('splitio.clients.SdkApi')
        self.build_split_fetcher_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_split_fetcher')
        self.build_treatment_log_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_treatment_log')
        self.build_metrics_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_metrics')
        self.start_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._start')
        self.some_api_key = mock.MagicMock()
        self.client = SelfRefreshingClient(self.some_api_key)

    def test_calls_sdk_api_constructor(self):
        """Test that _build_sdk_api calls SdkApi constructor"""
        self.sdk_api_mock.assert_called_once_with(
            self.some_api_key, sdk_api_base_url=self.client._sdk_api_base_url,
            events_api_base_url=self.client._events_api_base_url,
            connect_timeout=self.client._connection_timeout, read_timeout=self.client._read_timeout
        )


class SelfRefreshingClientBuildSplitFetcherTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.build_sdk_api_mock = self.patch('splitio.clients.SelfRefreshingClient._build_sdk_api')
        self.build_treatment_log_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_treatment_log')
        self.build_metrics_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_metrics')
        self.start_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._start')
        self.some_api_key = mock.MagicMock()

        self.api_segment_change_fetcher_mock = self.patch('splitio.clients.ApiSegmentChangeFetcher')
        self.self_refreshing_segment_fetcher_mock = self.patch(
            'splitio.clients.SelfRefreshingSegmentFetcher')
        self.api_split_change_fetcher_mock = self.patch('splitio.clients.ApiSplitChangeFetcher')
        self.split_parser_mock = self.patch('splitio.clients.SplitParser')
        self.self_refreshing_split_fetcher_mock = self.patch(
            'splitio.clients.SelfRefreshingSplitFetcher')

        self.some_api_key = mock.MagicMock()
        self.client = SelfRefreshingClient(self.some_api_key)

    def test_builds_segment_change_fetcher(self):
        """Tests that _build_split_fetcher calls the ApiSegmentChangeFetcher constructor"""
        self.api_segment_change_fetcher_mock.assert_called_once_with(
            self.build_sdk_api_mock.return_value)

    def test_builds_segment_fetcher(self):
        """Tests that _build_split_fetcher calls the SelfRefreshingSegmentFetcher constructor"""
        self.self_refreshing_segment_fetcher_mock.assert_called_once_with(
            self.api_segment_change_fetcher_mock.return_value,
            interval=self.client._segment_fetcher_interval)

    def test_builds_split_change_fetcher(self):
        """Tests that _build_split_fetcher calls the ApiSplitChangeFetcher constructor"""
        self.api_split_change_fetcher_mock.assert_called_once_with(
            self.build_sdk_api_mock.return_value)

    def test_builds_split_parser(self):
        """Tests that _build_split_fetcher calls the SplitParser constructor"""
        self.split_parser_mock.assert_called_once_with(
            self.self_refreshing_segment_fetcher_mock.return_value)

    def test_builds_split_fetcher(self):
        """Tests that _build_split_fetcher calls the SplitParser constructor"""
        self.self_refreshing_split_fetcher_mock.assert_called_once_with(
            self.api_split_change_fetcher_mock.return_value, self.split_parser_mock.return_value,
            interval=self.client._split_fetcher_interval)

    def test_returns_split_fetcher(self):
        """Tests that _build_split_fetcher returns the result of calling the
        SelfRefreshingSplitFetcher constructor"""
        self.assertEqual(self.self_refreshing_split_fetcher_mock.return_value,
                         self.client._build_split_fetcher())


class SelfRefreshingClientBuildTreatmentLogTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.build_sdk_api_mock = self.patch('splitio.clients.SelfRefreshingClient._build_sdk_api')
        self.build_split_fetcher_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_split_fetcher')
        self.build_metrics_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_metrics')
        self.start_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._start')
        self.some_api_key = mock.MagicMock()

        self.self_updating_treatment_log_mock = self.patch(
            'splitio.clients.SelfUpdatingTreatmentLog')
        self.aync_treatment_log_mock = self.patch(
            'splitio.clients.AsyncTreatmentLog')
        self.some_api_key = mock.MagicMock()
        self.client = SelfRefreshingClient(self.some_api_key)

    def test_calls_self_updating_treatment_log_constructor(self):
        """Tests that _build_treatment_log calls SelfUpdatingTreatmentLog constructor"""
        self.self_updating_treatment_log_mock.assert_called_once_with(
            self.client._sdk_api, max_count=self.client._max_impressions_log_size,
            interval=self.client._impressions_interval)

    def test_calls_async_treatment_log_constructor(self):
        """Tests that _build_treatment_log calls AsyncTreatmentLog constructor"""
        self.aync_treatment_log_mock.assert_called_once_with(
            self.self_updating_treatment_log_mock.return_value)

    def test_returns_async_treatment_log(self):
        """Tests that _build_treatment_log returns an AsyncTreatmentLog"""
        self.assertEqual(self.aync_treatment_log_mock.return_value,
                         self.client._build_treatment_log())


class SelfRefreshingClientBuildMetricsTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.build_sdk_api_mock = self.patch('splitio.clients.SelfRefreshingClient._build_sdk_api')
        self.build_split_fetcher_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_split_fetcher')
        self.build_treatment_log_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._build_treatment_log')
        self.start_mock = self.patch(
            'splitio.clients.SelfRefreshingClient._start')
        self.some_api_key = mock.MagicMock()

        self.api_metrics_mock = self.patch(
            'splitio.clients.ApiMetrics')
        self.aync_metrics_mock = self.patch(
            'splitio.clients.AsyncMetrics')
        self.some_api_key = mock.MagicMock()
        self.client = SelfRefreshingClient(self.some_api_key)

    def test_calls_api_metrics_constructor(self):
        """Tests that _build_metrics calls ApiMetrics constructor"""
        self.api_metrics_mock.assert_called_once_with(
            self.client._sdk_api, max_call_count=self.client._metrics_max_call_count,
            max_time_between_calls=self.client._metrics_max_time_between_calls)

    def test_calls_async_metrics_constructor(self):
        """Tests that _build_metrics calls AsyncMetrics constructor"""
        self.aync_metrics_mock.assert_called_once_with(
            self.api_metrics_mock.return_value)

    def test_returns_async_treatment_log(self):
        """Tests that _build_metrics returns an AsyncMetrics"""
        self.assertEqual(self.aync_metrics_mock.return_value, self.client._build_metrics())


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
        cls.in_between_datetime = arrow.get(2016, 4, 25, 16, 0).timestamp
        cls.not_in_between_datetime = arrow.get(2015, 4, 25, 16, 0).timestamp
        cls.in_between_number = 42
        cls.not_in_between_number = 85
        cls.equal_to_datetime = arrow.get(2016, 4, 25, 16, 0).timestamp
        cls.not_equal_to_datetime = arrow.get(2015, 4, 25, 16, 0).timestamp
        cls.equal_to_number = 50
        cls.not_equal_to_number = 85
        cls.greater_than_or_equal_to_datetime = arrow.get(2016, 4, 25, 16, 0).timestamp
        cls.not_greater_than_or_equal_to_datetime = arrow.get(2015, 4, 25, 16, 0).timestamp
        cls.greater_than_or_equal_to_number = 50
        cls.not_greater_than_or_equal_to_number = 32
        cls.less_than_or_equal_to_datetime = arrow.get(2015, 4, 25, 16, 0).timestamp
        cls.not_less_than_or_equal_to_datetime = arrow.get(2016, 4, 25, 16, 0).timestamp
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


class LocalhostEnvironmentClientParseSplitFileTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_file_name = mock.MagicMock()
        self.all_keys_split_side_effect = [mock.MagicMock(), mock.MagicMock()]
        self.all_keys_split_mock = self.patch('splitio.clients.AllKeysSplit',
                                              side_effect=self.all_keys_split_side_effect)
        self.build_split_fetcher_mock = self.patch(
            'splitio.tests.test_clients.LocalhostEnvironmentClient._build_split_fetcher')

        self.open_mock = self.patch_builtin('open')
        self.client = LocalhostEnvironmentClient()

    def test_skips_comment_lines(self):
        """Test that _parse_split_file skips comment lines"""
        self.open_mock.return_value.__enter__.return_value.__iter__.return_value = [
            '#feature treatment']
        self.client._parse_split_file(self.some_file_name)
        self.all_keys_split_mock.assert_not_called()

    def test_skips_illegal_lines(self):
        """Test that _parse_split_file skips illegal lines"""
        self.open_mock.return_value.__enter__.return_value.__iter__.return_value = [
            '!feature treat$ment']
        self.client._parse_split_file(self.some_file_name)
        self.all_keys_split_mock.assert_not_called()

    def test_parses_definition_lines(self):
        """Test that _parse_split_file skips comment lines"""
        self.open_mock.return_value.__enter__.return_value.__iter__.return_value = [
            'feature1 treatment1', 'feature2 treatment2']
        self.client._parse_split_file(self.some_file_name)
        self.assertListEqual([mock.call('feature1', 'treatment1'),
                              mock.call('feature2', 'treatment2')],
                             self.all_keys_split_mock.call_args_list)

    def test_returns_dict_with_parsed_splits(self):
        """Test that _parse_split_file skips comment lines"""
        self.open_mock.return_value.__enter__.return_value.__iter__.return_value = [
            'feature1 treatment1', 'feature2 treatment2']
        self.assertDictEqual({'feature1': self.all_keys_split_side_effect[0],
                              'feature2': self.all_keys_split_side_effect[1]},
                             self.client._parse_split_file(self.some_file_name))

    def test_raises_value_error_if_ioerror_is_raised(self):
        """Raises a ValueError if an IOError is raised"""
        self.open_mock.side_effect = IOError()
        with self.assertRaises(ValueError):
            self.client._parse_split_file(self.some_file_name)


class LocalhostEnvironmentClientOffTheGrid(TestCase):
    '''
    Tests for LocalhostEnvironmentClient. Auto update config behaviour
    '''
    def test_auto_update_splits(self):
        '''
        Verifies that the split file is automatically re-parsed as soon as it's
        modified
        '''
        with tempfile.NamedTemporaryFile(mode='w') as split_file:
            split_file.write('a_test_split off\n')
            split_file.flush()

            client = LocalhostEnvironmentClient(
                split_definition_file_name=split_file.name
            )

            self.assertEqual(client.get_treatment('x', 'a_test_split'), 'off')

            split_file.truncate()
            split_file.write('a_test_split on\n')
            split_file.flush()
            sleep(1)

            self.assertEqual(client.get_treatment('x', 'a_test_split'), 'on')

