from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase
from collections import defaultdict

from splitio.version import __version__
from splitio.metrics import BUCKETS
from splitio.impressions import Impression
from splitio.tests.utils import MockUtilsMixin

from splitio.redis_support import (RedisSegmentCache, RedisSplitCache, RedisImpressionsCache,
                                   RedisMetricsCache, RedisSplitParser, RedisSplit,
                                   RedisSplitBasedSegment, get_redis)

from splitio.splits import (JSONFileSplitFetcher, SplitParser)
from splitio.segments import JSONFileSegmentFetcher


class RedisSegmentCacheTests(TestCase):
    def setUp(self):
        self.some_segment_name = mock.MagicMock()
        self.some_segment_name_str = 'some_segment_name'
        self.some_segment_keys = [mock.MagicMock(), mock.MagicMock()]
        self.some_key = mock.MagicMock()
        self.some_change_number = mock.MagicMock()
        self.some_redis = mock.MagicMock()
        self.a_segment_cache = RedisSegmentCache(self.some_redis)

    def test_unregister_segment_removes_segment_name_to_register_segments_set(self):
        """Test that unregister_segment removes segment name to registered segments set"""
        self.a_segment_cache.unregister_segment(self.some_segment_name)
        self.some_redis.srem.assert_called_once_with('SPLITIO.segments.registered',
                                                     self.some_segment_name)

    def test_get_registered_segments_returns_registered_segments_set_members(self):
        """Test that get_registered_segments returns the registered segments sets members"""
        self.assertEqual(self.some_redis.smembers.return_value,
                         self.a_segment_cache.get_registered_segments())
        self.some_redis.smembers.assert_called_once_with('SPLITIO.segments.registered')

    def test_add_keys_to_segment_adds_keys_to_segment_set(self):
        """Test that add_keys_to_segment adds the keys to the segment key set"""
        self.a_segment_cache.add_keys_to_segment(self.some_segment_name_str, self.some_segment_keys)
        self.some_redis.sadd.assert_called_once_with(
            'SPLITIO.segment.some_segment_name', self.some_segment_keys[0],
            self.some_segment_keys[1])

    def test_remove_keys_from_segment_remove_keys_from_segment_set(self):
        """Test that remove_keys_from_segment removes the keys to the segment key set"""
        self.a_segment_cache.remove_keys_from_segment(self.some_segment_name_str,
                                                      self.some_segment_keys)
        self.some_redis.srem.assert_called_once_with(
            'SPLITIO.segment.some_segment_name', self.some_segment_keys[0],
            self.some_segment_keys[1])

    def test_is_in_segment_tests_whether_a_key_is_in_a_segments_key_set(self):
        """Test that is_in_segment checks if a key is in a segment's key set"""
        self.assertEqual(self.some_redis.sismember.return_value,
                         self.a_segment_cache.is_in_segment(self.some_segment_name_str,
                                                            self.some_key))
        self.some_redis.sismember.assert_called_once_with(
            'SPLITIO.segment.some_segment_name', self.some_key)

    def test_set_change_number_sets_segment_change_number_key(self):
        """Test that set_change_number sets the segment's change number key"""
        self.a_segment_cache.set_change_number(self.some_segment_name_str, self.some_change_number)
        self.some_redis.set.assert_called_once_with(
            'SPLITIO.segment.some_segment_name.till', self.some_change_number)

    def test_get_change_number_gets_segment_change_number_key(self):
        """Test that get_change_number gets the segment's change number key"""
        self.some_redis.get.return_value = '1234'
        result = self.a_segment_cache.get_change_number(self.some_segment_name_str)
        self.assertEqual(int(self.some_redis.get.return_value), result)
        self.assertIsInstance(result, int)
        self.some_redis.get.assert_called_once_with(
            'SPLITIO.segment.some_segment_name.till')

    def test_get_change_number_returns_default_value_if_not_set(self):
        """Test that get_change_number returns -1 if the value is not set"""
        self.some_redis.get.return_value = None
        self.assertEqual(-1,
                         self.a_segment_cache.get_change_number(self.some_segment_name_str))


class RedisSplitCacheTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.decode_mock = self.patch('splitio.redis_support.decode')
        self.encode_mock = self.patch('splitio.redis_support.encode')
        self.some_split_name = mock.MagicMock()
        self.some_split_name_str = 'some_split_name'
        self.some_split = mock.MagicMock()
        self.some_change_number = mock.MagicMock()
        self.some_redis = mock.MagicMock()
        self.a_split_cache = RedisSplitCache(self.some_redis)

    def test_set_change_number_sets_change_number_key(self):
        """Test that set_change_number sets the change number key"""
        self.a_split_cache.set_change_number(self.some_change_number)
        self.some_redis.set.assert_called_once_with(
            'SPLITIO.splits.till', self.some_change_number, None)

    def test_get_change_number_gets_segment_change_number_key(self):
        """Test that get_change_number gets the change number key"""
        self.some_redis.get.return_value = '1234'
        result = self.a_split_cache.get_change_number()
        self.assertEqual(int(self.some_redis.get.return_value), result)
        self.assertIsInstance(result, int)
        self.some_redis.get.assert_called_once_with(
            'SPLITIO.splits.till')

    def test_get_change_number_returns_default_value_if_not_set(self):
        """Test that get_change_number returns -1 if the value is not set"""
        self.some_redis.get.return_value = None
        self.assertEqual(-1, self.a_split_cache.get_change_number())

    def test_add_split_sets_split_key_with_pickled_split(self):
        """Test that add_split sets the split key with pickled split"""
        self.a_split_cache.add_split(self.some_split_name_str, self.some_split)
        self.encode_mock.assert_called_once_with(self.some_split)
        self.some_redis.set.assert_called_once_with('SPLITIO.split.some_split_name',
                                                    self.encode_mock.return_value)

    def test_get_split_returns_none_if_not_cached(self):
        """Test that if a split is not cached get_split returns None"""
        self.some_redis.get.return_value = None
        self.assertEqual(None, self.a_split_cache.get_split(self.some_split_name_str))
        self.some_redis.get.assert_called_once_with('SPLITIO.split.some_split_name')

    def test_remove_split_deletes_split_key(self):
        """Test that remove_split deletes the split key"""
        self.a_split_cache.remove_split(self.some_split_name_str)
        self.some_redis.delete.assert_called_once_with('SPLITIO.split.some_split_name')


class RedisImpressionsCacheTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.encode_mock = self.patch('splitio.redis_support.encode')
        self.decode_mock = self.patch('splitio.redis_support.decode')
        self.some_impression = mock.MagicMock()
        self.some_redis = mock.MagicMock()
        self.an_impressions_cache = RedisImpressionsCache(self.some_redis)
        self.build_impressions_dict_mock = self.patch_object(self.an_impressions_cache,
                                                             '_build_impressions_dict')

    def test_fetch_all_doesnt_call_build_impressions_dict_if_no_impressions_cached(self):
        """Test that fetch_all doesn't call _build_impressions_dict if no impressions are cached"""
        self.some_redis.lrange.return_value = None
        self.assertDictEqual(dict(), self.an_impressions_cache.fetch_all())
        self.build_impressions_dict_mock.assert_not_called()

    def test_clear_deletes_impressions_key(self):
        """Test that clear deletes impressions key"""
        self.an_impressions_cache.clear()
        self.some_redis.eval.assert_called_once_with(
            "return redis.call('del', unpack(redis.call('keys', ARGV[1])))",
            0,
            RedisImpressionsCache._get_impressions_key('*')
        )

    def test_fetch_all_and_clear_calls_eval_with_fetch_and_clear_script(self):
        """Test that fetch_and_clear calls eval with the fetch and clear script"""
        self.an_impressions_cache.fetch_all_and_clear()
        self.some_redis.keys.assert_called_once_with(
            RedisImpressionsCache._get_impressions_key('*')
        )

    def test_fetch_all_and_clear_doesnt_call_build_impressions_dict_if_no_impressions_cached(self):
        """Test that fetch_all_and_clear doesn't call _build_impressions_dict if no impressions are
        cached"""
        self.some_redis.eval.return_value = None
        self.assertDictEqual(dict(), self.an_impressions_cache.fetch_all_and_clear())
        self.build_impressions_dict_mock.assert_not_called()


class RedisImpressionsCacheBuildImpressionsDictTests(TestCase):
    def setUp(self):
        self.some_redis = mock.MagicMock()
        self.an_impressions_cache = RedisImpressionsCache(self.some_redis)

    def _build_impression(self, feature_name):
        return Impression(matching_key=mock.MagicMock(), feature_name=feature_name,
                          treatment=mock.MagicMock(), label=mock.MagicMock(), time=mock.MagicMock(),
                          change_number=mock.MagicMock(), bucketing_key=mock.MagicMock())

    def test_build_impressions_dict(self):
        """Test that _build_impressions_dict builds the dictionary properly"""
        some_feature_name = mock.MagicMock()
        some_other_feature_name = mock.MagicMock()
        some_feature_impressions = [self._build_impression(some_feature_name)]
        some_other_feature_impressions = [self._build_impression(some_other_feature_name),
                                          self._build_impression(some_other_feature_name)]

        self.assertDictEqual({some_feature_name: some_feature_impressions,
                              some_other_feature_name: some_other_feature_impressions},
                             self.an_impressions_cache._build_impressions_dict(
                                 some_feature_impressions + some_other_feature_impressions))


class RedisMetricsCacheTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_redis = mock.MagicMock()
        self.some_count_str = 'some_count'
        self.some_delta = mock.MagicMock()
        self.some_operation_str = 'some_operation'
        self.some_bucket_index = 15
        self.some_gauge_str = 'some_gauge'
        self.some_value = mock.MagicMock()
        self.a_metrics_cache = RedisMetricsCache(self.some_redis)
        self.build_metrics_from_cache_response_mock = self.patch_object(
            self.a_metrics_cache, '_build_metrics_from_cache_response')

    def test_get_latency_calls_get(self):
        """Test that get_latency calls get in last position (22)"""
        self.a_metrics_cache.get_latency(self.some_operation_str)
        self.some_redis.get.assert_called_with(
            'SPLITIO/python-'+__version__+'/unknown/latency.some_operation.bucket.{}'
            .format(22)
        )

    def test_get_latency_returns_result(self):
        """Test that get_latency returns the result of calling get"""
        self.some_redis.get.return_value = mock.MagicMock()
        result = [self.some_redis.get.return_value] * 23
        self.assertListEqual(result,
                             self.a_metrics_cache.get_latency(self.some_operation_str))

    def test_get_latency_sets_empty_results_to_zero(self):
        """Test that get_latency sets the missing results from get to zero"""
        self.some_redis.get.return_value = None
        self.assertEqual(0, self.a_metrics_cache.get_latency(self.some_operation_str)[13])

    def test_get_latency_bucket_counter_calls_get(self):
        """Test that get_latency_bucket_counter calls get"""
        self.a_metrics_cache.get_latency_bucket_counter(self.some_operation_str,
                                                        self.some_bucket_index)
        self.some_redis.get.assert_called_once_with(
            'SPLITIO/python-'+__version__+'/unknown/latency.{0}.bucket.{1}'.format(
            self.some_operation_str, self.some_bucket_index))

    def test_get_latency_bucket_counter_returns_get_result(self):
        """Test that get_latency_bucket_counter returns the result of calling get"""
        self.assertEqual(1,
                         self.a_metrics_cache.get_latency_bucket_counter(self.some_operation_str,
                                                                         self.some_bucket_index))

    def test_get_latency_bucket_counter_returns_0_on_missing_value(self):
        """Test that get_latency_bucket_counter returns 0 if the bucket value is not cached"""
        self.some_redis.get.return_value = None
        self.assertEqual(0,
                         self.a_metrics_cache.get_latency_bucket_counter(self.some_operation_str,
                                                                         self.some_bucket_index))

    def test_set_gauge_calls_hset(self):
        """Test that set_gauge calls hset"""
        self.a_metrics_cache.set_gauge(self.some_gauge_str, self.some_value)
        self.some_redis.hset('SPLITIO.metrics.metric', 'gauge.some_gauge', self.some_value)

    def test_set_latency_bucket_counter_calls_set(self):
        """Test that set_latency_bucket_counter calls conditional eval with set latency bucket
        counter script"""
        self.a_metrics_cache.set_latency_bucket_counter(self.some_operation_str,
                                                        self.some_bucket_index,
                                                        self.some_value)
        self.some_redis.set.assert_called_once_with(
            self.a_metrics_cache._get_latency_bucket_key(
                self.some_operation_str, bucket_number=self.some_bucket_index),
            self.some_value)

    def test_increment_latency_bucket_counter_calls_delta(self):
        """Test that increment_latency_bucket_counter calls with the increment
        latency bucket counter script"""
        self.a_metrics_cache.increment_latency_bucket_counter(self.some_operation_str,
                                                              self.some_bucket_index,
                                                              self.some_delta)
        self.some_redis.incr.assert_called_once_with(
            self.a_metrics_cache._get_latency_bucket_key(
                self.some_operation_str, self.some_bucket_index),
            self.some_delta)

    def test_increment_latency_bucket_counter_calls_default_delta(self):
        """Test that increment_latency_bucket_counter calls conditional eval with the increment
        latency bucket counter script with the default delta"""
        self.a_metrics_cache.increment_latency_bucket_counter(self.some_operation_str,
                                                              self.some_bucket_index)
        self.some_redis.incr.assert_called_once_with(
            self.a_metrics_cache._get_latency_bucket_key(
                self.some_operation_str, self.some_bucket_index
            ),
            1
        )


class RedisMetricsCacheConditionalEvalTests(TestCase):
    def setUp(self):
        self.some_script = 'some_script'
        self.some_num_keys = mock.MagicMock()
        self.some_key = mock.MagicMock()
        self.some_other_key = mock.MagicMock()
        self.some_value = mock.MagicMock()
        self.some_redis = mock.MagicMock()
        self.a_metrics_cache = RedisMetricsCache(self.some_redis)


class RedisMetricsCacheBuildMetricsFromCacheResponseTests(TestCase):
    def setUp(self):
        self.some_redis = mock.MagicMock()
        self.a_metrics_cache = RedisMetricsCache(self.some_redis)

    def test_returns_default_empty_dict_on_none_response(self):
        """Test that _build_metrics_from_cache_response returns the empty default dict if response
        is None"""
        self.assertDictEqual({'count': [], 'gauge': []},
                             self.a_metrics_cache._build_metrics_from_cache_response(None))

    def test_returns_default_empty_dict_on_empty_response(self):
        """Test that _build_metrics_from_cache_response returns the empty default dict if response
        is empty"""
        self.assertDictEqual({'count': [], 'gauge': []},
                             self.a_metrics_cache._build_metrics_from_cache_response([]))

    def test_returns_count_metrics(self):
        """Test that _build_metrics_from_cache_response returns count metrics"""
        some_count = 'some_count'
        some_count_value = mock.MagicMock()
        some_other_count = 'some_other_count'
        some_other_count_value = mock.MagicMock()
        count_metrics = [
            'SPLITIO/python-'+__version__+'/unknown/count.some_count', some_count_value,
            'SPLITIO/python-'+__version__+'/unknown/count.some_other_count',
            some_other_count_value
        ]
        result_count_metrics = [{'name': some_count, 'delta': some_count_value},
                                {'name': some_other_count, 'delta': some_other_count_value}]
        self.assertDictEqual({'count': result_count_metrics, 'gauge': []},
                             self.a_metrics_cache._build_metrics_from_cache_response(count_metrics))

    def test_returns_time_metrics(self):
        """Test that _build_metrics_from_cache_response returns time metrics"""
        some_time = 'some_time'
        some_time_latencies = [0] * 23
        some_time_latencies[2] = mock.MagicMock()
        some_time_latencies[13] = mock.MagicMock()

        some_other_time = 'some_other_time'
        some_other_time_latencies = [0] * 23
        some_other_time_latencies[0] = mock.MagicMock()
        some_other_time_latencies[1] = mock.MagicMock()
        some_other_time_latencies[20] = mock.MagicMock()

        time_metrics = defaultdict(lambda: [0] * len(BUCKETS))

        time_metrics[some_time] = some_time_latencies
        time_metrics[some_other_time] = some_other_time_latencies

        result_time_metris = [{'name': some_other_time, 'latencies': some_other_time_latencies},
                              {'name': some_time, 'latencies': some_time_latencies}]

        self.assertListEqual(result_time_metris, self.a_metrics_cache._build_metrics_times_data(time_metrics))

    def test_returns_gauge_metrics(self):
        """Test that _build_metrics_from_cache_response returns gauge metrics"""
        some_gauge = 'some_gauge'
        some_gauge_value = mock.MagicMock()
        some_other_gauge = 'some_other_gauge'
        some_other_gauge_value = mock.MagicMock()
        gauge_metrics = [
            'SPLITIO/python-'+__version__+'/unknown/gauge.some_gauge', some_gauge_value,
            'SPLITIO/python-'+__version__+'/unknown/gauge.some_other_gauge',
            some_other_gauge_value
        ]
        result_gauge_metrics = [{'name': some_gauge, 'value': some_gauge_value},
                                {'name': some_other_gauge, 'value': some_other_gauge_value}]
        self.assertDictEqual({'count': [], 'gauge': result_gauge_metrics},
                             self.a_metrics_cache._build_metrics_from_cache_response(gauge_metrics))


class RedisSplitParserTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_matcher = mock.MagicMock()
        self.some_segment_cache = mock.MagicMock()
        self.split_parser = RedisSplitParser(self.some_segment_cache)
        self.redis_split_mock = self.patch('splitio.redis_support.RedisSplit')

        self.some_split = {
            'name': mock.MagicMock(),
            'seed': mock.MagicMock(),
            'killed': mock.MagicMock(),
            'defaultTreatment': mock.MagicMock(),
            'trafficTypeName': mock.MagicMock(),
            'status': mock.MagicMock(),
            'changeNumber': mock.MagicMock(),
            'algo': mock.MagicMock()
        }
        self.some_block_until_ready = mock.MagicMock()
        self.some_partial_split = mock.MagicMock()
        self.some_in_segment_matcher = {
            'matcherType': 'IN_SEGMENT',
            'userDefinedSegmentMatcherData': {
                'segmentName': mock.MagicMock()
            }
        }

    def test_parse_split_returns_redis_split(self):
        """Test that _parse_split returns a RedisSplit"""
        self.assertEqual(self.redis_split_mock.return_value,
                         self.split_parser._parse_split(
                             self.redis_split_mock, block_until_ready=self.some_block_until_ready))

    def test_parse_split_calls_redis_split_constructor(self):
        """Test that _parse_split calls RedisSplit constructor"""
        self.split_parser._parse_split(self.some_split,
                                       block_until_ready=self.some_block_until_ready)
        self.redis_split_mock.assert_called_once_with(
            self.some_split['name'], self.some_split['seed'], self.some_split['killed'],
            self.some_split['defaultTreatment'],self.some_split['trafficTypeName'],
            self.some_split['status'], self.some_split['changeNumber'], segment_cache=self.some_segment_cache,
            traffic_allocation=self.some_split.get('trafficAllocation'),
            traffic_allocation_seed=self.some_split.get('trafficAllocationSeed'),
            algo=self.some_split['algo']
        )

    def test_parse_matcher_in_segment_registers_segment(self):
        """Test that _parse_matcher_in_segment registers segment"""
        self.split_parser._parse_matcher_in_segment(self.some_partial_split,
                                                    self.some_in_segment_matcher,
                                                    block_until_ready=self.some_block_until_ready)
        self.some_segment_cache.register_segment.assert_called()
