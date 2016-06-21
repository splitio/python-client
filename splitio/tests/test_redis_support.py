from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase

from splitio.impressions import Impression
from splitio.tests.utils import MockUtilsMixin

from splitio.redis_support import (RedisSegmentCache, RedisSplitCache, RedisImpressionsCache,
                                   RedisMetricsCache, RedisSplitParser, RedisSplit,
                                   RedisSplitBasedSegment)


class RedisSegmentCacheTests(TestCase):
    def setUp(self):
        self.some_segment_name = mock.MagicMock()
        self.some_segment_name_str = 'some_segment_name'
        self.some_segment_keys = [mock.MagicMock(), mock.MagicMock()]
        self.some_key = mock.MagicMock()
        self.some_change_number = mock.MagicMock()
        self.some_redis = mock.MagicMock()
        self.a_segment_cache = RedisSegmentCache(self.some_redis)

    def test_disable_sets_disabled_key(self):
        """Test that disable sets the disabled key for segments"""
        self.a_segment_cache.disable()
        self.some_redis.setex.assert_called_once_with('SPLITIO.segments.__disabled__', 1,
                                                      self.a_segment_cache.disabled_period)

    def test_enable_deletes_disabled_key(self):
        """Test that enable deletes the disabled key for segments"""
        self.a_segment_cache.enable()
        self.some_redis.delete.assert_called_once_with('SPLITIO.segments.__disabled__')

    def test_is_enabled_returns_false_if_disabled_key_exists(self):
        """Test that is_enabled returns False if disabled key exists"""
        self.some_redis.exists.return_value = True
        self.assertFalse(self.a_segment_cache.is_enabled())
        self.some_redis.exists.assert_called_once_with('SPLITIO.segments.__disabled__')

    def test_is_enabled_returns_true_if_disabled_key_doesnt_exist(self):
        """Test that is_enabled returns True if disabled key doesn't exist"""
        self.some_redis.exists.return_value = False
        self.assertTrue(self.a_segment_cache.is_enabled())
        self.some_redis.exists.assert_called_once_with('SPLITIO.segments.__disabled__')

    def test_register_segment_adds_segment_name_to_register_segments_set(self):
        """Test that register_segment adds segment name to registered segments set"""
        self.a_segment_cache.register_segment(self.some_segment_name)
        self.some_redis.sadd.assert_called_once_with('SPLITIO.segments.__registered_segments__',
                                                     self.some_segment_name)

    def test_unregister_segment_removes_segment_name_to_register_segments_set(self):
        """Test that unregister_segment removes segment name to registered segments set"""
        self.a_segment_cache.unregister_segment(self.some_segment_name)
        self.some_redis.srem.assert_called_once_with('SPLITIO.segments.__registered_segments__',
                                                     self.some_segment_name)

    def test_get_registered_segments_returns_registered_segments_set_members(self):
        """Test that get_registered_segments returns the registered segments sets members"""
        self.assertEqual(self.some_redis.smembers.return_value,
                         self.a_segment_cache.get_registered_segments())
        self.some_redis.smembers.assert_called_once_with('SPLITIO.segments.__registered_segments__')

    def test_add_keys_to_segment_adds_keys_to_segment_set(self):
        """Test that add_keys_to_segment adds the keys to the segment key set"""
        self.a_segment_cache.add_keys_to_segment(self.some_segment_name_str, self.some_segment_keys)
        self.some_redis.sadd.assert_called_once_with(
            'SPLITIO.segments.segment.some_segment_name.key_set', self.some_segment_keys[0],
            self.some_segment_keys[1])

    def test_remove_keys_from_segment_remove_keys_from_segment_set(self):
        """Test that remove_keys_from_segment removes the keys to the segment key set"""
        self.a_segment_cache.remove_keys_from_segment(self.some_segment_name_str,
                                                      self.some_segment_keys)
        self.some_redis.srem.assert_called_once_with(
            'SPLITIO.segments.segment.some_segment_name.key_set', self.some_segment_keys[0],
            self.some_segment_keys[1])

    def test_is_in_segment_tests_whether_a_key_is_in_a_segments_key_set(self):
        """Test that is_in_segment checks if a key is in a segment's key set"""
        self.assertEqual(self.some_redis.sismember.return_value,
                         self.a_segment_cache.is_in_segment(self.some_segment_name_str,
                                                            self.some_key))
        self.some_redis.sismember.assert_called_once_with(
            'SPLITIO.segments.segment.some_segment_name.key_set', self.some_key)

    def test_set_change_number_sets_segment_change_number_key(self):
        """Test that set_change_number sets the segment's change number key"""
        self.a_segment_cache.set_change_number(self.some_segment_name_str, self.some_change_number)
        self.some_redis.set.assert_called_once_with(
            'SPLITIO.segments.segment.some_segment_name.change_number', self.some_change_number)

    def test_get_change_number_gets_segment_change_number_key(self):
        """Test that get_change_number gets the segment's change number key"""
        self.some_redis.get.return_value = '1234'
        result = self.a_segment_cache.get_change_number(self.some_segment_name_str)
        self.assertEqual(int(self.some_redis.get.return_value), result)
        self.assertIsInstance(result, int)
        self.some_redis.get.assert_called_once_with(
            'SPLITIO.segments.segment.some_segment_name.change_number')

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

    def test_disable_sets_disabled_key(self):
        """Test that disable sets the disabled key for splits"""
        self.a_split_cache.disable()
        self.some_redis.setex.assert_called_once_with('SPLITIO.splits.__disabled__', 1,
                                                      self.a_split_cache.disabled_period)

    def test_enable_deletes_disabled_key(self):
        """Test that enable deletes the disabled key for splits"""
        self.a_split_cache.enable()
        self.some_redis.delete.assert_called_once_with('SPLITIO.splits.__disabled__')

    def test_is_enabled_returns_false_if_disabled_key_exists(self):
        """Test that is_enabled returns False if disabled key exists"""
        self.some_redis.exists.return_value = True
        self.assertFalse(self.a_split_cache.is_enabled())
        self.some_redis.exists.assert_called_once_with('SPLITIO.splits.__disabled__')

    def test_is_enabled_returns_true_if_disabled_key_doesnt_exist(self):
        """Test that is_enabled returns True if disabled key doesn't exist"""
        self.some_redis.exists.return_value = False
        self.assertTrue(self.a_split_cache.is_enabled())
        self.some_redis.exists.assert_called_once_with('SPLITIO.splits.__disabled__')

    def test_set_change_number_sets_change_number_key(self):
        """Test that set_change_number sets the change number key"""
        self.a_split_cache.set_change_number(self.some_change_number)
        self.some_redis.set.assert_called_once_with(
            'SPLITIO.splits.__change_number__', self.some_change_number, None)

    def test_get_change_number_gets_segment_change_number_key(self):
        """Test that get_change_number gets the change number key"""
        self.some_redis.get.return_value = '1234'
        result = self.a_split_cache.get_change_number()
        self.assertEqual(int(self.some_redis.get.return_value), result)
        self.assertIsInstance(result, int)
        self.some_redis.get.assert_called_once_with(
            'SPLITIO.splits.__change_number__')

    def test_get_change_number_returns_default_value_if_not_set(self):
        """Test that get_change_number returns -1 if the value is not set"""
        self.some_redis.get.return_value = None
        self.assertEqual(-1, self.a_split_cache.get_change_number())

    def test_add_split_sets_split_key_with_pickled_split(self):
        """Test that add_split sets the split key with pickled split"""
        self.a_split_cache.add_split(self.some_split_name_str, self.some_split)
        self.encode_mock.assert_called_once_with(self.some_split)
        self.some_redis.set.assert_called_once_with('SPLITIO.splits.some_split_name',
                                                    self.encode_mock.return_value)

    def test_get_split_returns_unpickled_cached_split(self):
        """Test that if a split is cached get_split returns its unpickled form"""
        self.assertEqual(self.decode_mock.return_value,
                         self.a_split_cache.get_split(self.some_split_name_str))
        self.some_redis.get.assert_called_once_with('SPLITIO.splits.some_split_name')
        self.decode_mock.assert_called_once_with(self.some_redis.get.return_value)

    def test_get_split_returns_none_if_not_cached(self):
        """Test that if a split is not cached get_split returns None"""
        self.some_redis.get.return_value = None
        self.assertEqual(None, self.a_split_cache.get_split(self.some_split_name_str))
        self.some_redis.get.assert_called_once_with('SPLITIO.splits.some_split_name')

    def test_remove_split_deletes_split_key(self):
        """Test that remove_split deletes the split key"""
        self.a_split_cache.remove_split(self.some_split_name_str)
        self.some_redis.delete.assert_called_once_with('SPLITIO.splits.some_split_name')


class RedisImpressionsCacheTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.encode_mock = self.patch('splitio.redis_support.encode')
        self.decode_mock = self.patch('splitio.redis_support.decode')
        self.some_impression = mock.MagicMock()
        self.some_redis = mock.MagicMock()
        self.an_impressions_cache = RedisImpressionsCache(self.some_redis)
        self.build_impressions_dict_mock = self.patch_object(self.an_impressions_cache,
                                                             '_build_impressions_dict')

    def test_disable_sets_disabled_key(self):
        """Test that disable sets the disabled key for impressions"""
        self.an_impressions_cache.disable()
        self.some_redis.setex.assert_called_once_with('SPLITIO.impressions.__disabled__', 1,
                                                      self.an_impressions_cache.disabled_period)

    def test_enable_deletes_disabled_key(self):
        """Test that enable deletes the disabled key for impressions"""
        self.an_impressions_cache.enable()
        self.some_redis.delete.assert_called_once_with('SPLITIO.impressions.__disabled__')

    def test_is_enabled_returns_false_if_disabled_key_exists(self):
        """Test that is_enabled returns False if disabled key exists"""
        self.some_redis.exists.return_value = True
        self.assertFalse(self.an_impressions_cache.is_enabled())
        self.some_redis.exists.assert_called_once_with('SPLITIO.impressions.__disabled__')

    def test_is_enabled_returns_true_if_disabled_key_doesnt_exist(self):
        """Test that is_enabled returns True if disabled key doesn't exist"""
        self.some_redis.exists.return_value = False
        self.assertTrue(self.an_impressions_cache.is_enabled())
        self.some_redis.exists.assert_called_once_with('SPLITIO.impressions.__disabled__')

    def test_fetch_all_calls_lrange_on_impressions_key(self):
        """Test that fetch_all calls lrange on impressions key"""
        self.an_impressions_cache.fetch_all()
        self.some_redis.lrange.assert_called_once_with('SPLITIO.impressions.impressions', 0, -1)

    def test_fetch_all_returns_build_impressions_dict_result(self):
        """Test that fetch_all returns the result of calling _build_impressions_dict if there are
        impressions cached"""
        cached_impressions = [mock.MagicMock(), mock.MagicMock()]
        unpickled_cached_impressions = [mock.MagicMock(), mock.MagicMock()]
        self.some_redis.lrange.return_value = cached_impressions
        self.decode_mock.side_effect = unpickled_cached_impressions
        self.assertEqual(self.build_impressions_dict_mock.return_value,
                         self.an_impressions_cache.fetch_all())
        self.build_impressions_dict_mock.assert_called_once_with(unpickled_cached_impressions)

    def test_fetch_all_doesnt_call_build_impressions_dict_if_no_impressions_cached(self):
        """Test that fetch_all doesn't call _build_impressions_dict if no impressions are cached"""
        self.some_redis.lrange.return_value = None
        self.assertDictEqual(dict(), self.an_impressions_cache.fetch_all())
        self.build_impressions_dict_mock.assert_not_called()

    def test_clear_deletes_impressions_key(self):
        """Test that clear deletes impressions key"""
        self.an_impressions_cache.clear()
        self.some_redis.delete.assert_called_once_with('SPLITIO.impressions.impressions')

    def test_add_impression_calls_eval_with_add_impression_script(self):
        """Test that add_impression calls eval with the add impression script"""
        self.an_impressions_cache.add_impression(self.some_impression)
        self.some_redis.eval.assert_called_once_with(
            "if redis.call('EXISTS', KEYS[1]) == 0 then redis.call('LPUSH', KEYS[2], ARGV[1]) end",
            2, 'SPLITIO.impressions.__disabled__', 'SPLITIO.impressions.impressions',
            self.encode_mock.return_value)
        self.encode_mock.assert_called_once_with(self.some_impression)

    def test_fetch_all_and_clear_calls_eval_with_fetch_and_clear_script(self):
        """Test that fetch_and_clear calls eval with the fetch and clear script"""
        self.an_impressions_cache.fetch_all_and_clear()
        self.some_redis.eval.assert_called_once_with(
            "if redis.call('EXISTS', KEYS[1]) == 1 then redis.call('RENAME', KEYS[1], KEYS[2]); "
            "local impressions_to_clear = redis.call('LRANGE', KEYS[2], 0, -1); "
            "redis.call('DEL', KEYS[2]); return impressions_to_clear; end", 2,
            'SPLITIO.impressions.impressions', 'SPLITIO.impressions.impressions_to_clear'
        )

    def test_fetch_all_and_clear_returns_build_impressions_dict_result(self):
        """Test that fetch_all_and_clear returns the result of calling _build_impressions_dict if
        there are impressions cached"""
        cached_impressions = [mock.MagicMock(), mock.MagicMock()]
        unpickled_cached_impressions = [mock.MagicMock(), mock.MagicMock()]
        self.some_redis.eval.return_value = cached_impressions
        self.decode_mock.side_effect = unpickled_cached_impressions
        self.assertEqual(self.build_impressions_dict_mock.return_value,
                         self.an_impressions_cache.fetch_all_and_clear())
        self.build_impressions_dict_mock.assert_called_once_with(unpickled_cached_impressions)

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
        return Impression(key=mock.MagicMock(), feature_name=feature_name,
                          treatment=mock.MagicMock(), time=mock.MagicMock())

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
        self.conditional_eval_mock = self.patch_object(self.a_metrics_cache, '_conditional_eval')
        self.build_metrics_from_cache_response_mock = self.patch_object(
            self.a_metrics_cache, '_build_metrics_from_cache_response')

    def test_disable_sets_disabled_key(self):
        """Test that disable sets the disabled key for metrics"""
        self.a_metrics_cache.disable()
        self.some_redis.setex.assert_called_once_with('SPLITIO.metrics.__disabled__', 1,
                                                      self.a_metrics_cache.disabled_period)

    def test_enable_deletes_disabled_key(self):
        """Test that enable deletes the disabled key for metrics"""
        self.a_metrics_cache.enable()
        self.some_redis.delete.assert_called_once_with('SPLITIO.metrics.__disabled__')

    def test_is_enabled_returns_false_if_disabled_key_exists(self):
        """Test that is_enabled returns False if disabled key exists"""
        self.some_redis.exists.return_value = True
        self.assertFalse(self.a_metrics_cache.is_enabled())
        self.some_redis.exists.assert_called_once_with('SPLITIO.metrics.__disabled__')

    def test_is_enabled_returns_true_if_disabled_key_doesnt_exist(self):
        """Test that is_enabled returns True if disabled key doesn't exist"""
        self.some_redis.exists.return_value = False
        self.assertTrue(self.a_metrics_cache.is_enabled())
        self.some_redis.exists.assert_called_once_with('SPLITIO.metrics.__disabled__')

    def test_increment_count_calls_conditional_eval_with_increment_count_script(self):
        """Test that increment_count calls _conditional_eval with the increment count script"""
        self.a_metrics_cache.increment_count(self.some_count_str, self.some_delta)
        self.conditional_eval_mock.assert_called_once_with(
            "redis.call('HINCRBY', KEYS[1], ARGV[1], ARGV[2]);", 1, 'SPLITIO.metrics.metric',
            'count.some_count', self.some_delta)

    def test_increment_count_calls_conditional_eval_with_increment_count_script_default_delta(self):
        """Test that increment_count calls _conditional_eval with the increment count script with
        the default delta value"""
        self.a_metrics_cache.increment_count(self.some_count_str)
        self.conditional_eval_mock.assert_called_once_with(
            "redis.call('HINCRBY', KEYS[1], ARGV[1], ARGV[2]);", 1, 'SPLITIO.metrics.metric',
            'count.some_count', 1)

    def test_get_latency_calls_hmget(self):
        """Test that get_latency calls hmget"""
        self.a_metrics_cache.get_latency(self.some_operation_str)
        self.some_redis.hmget.assert_called_once_with(
            'SPLITIO.metrics.metric', *['time.some_operation.{}'.format(i) for i in range(23)])

    def test_get_latency_returns_hmget_result(self):
        """Test that get_latency returns the result of calling hmget"""
        self.some_redis.hmget.return_value = [mock.MagicMock() for _ in range(23)]
        self.assertListEqual(self.some_redis.hmget.return_value,
                             self.a_metrics_cache.get_latency(self.some_operation_str))

    def test_get_latency_sets_empty_results_to_zero(self):
        """Test that get_latency sets the missing results from hmget to zero"""
        self.some_redis.hmget.return_value = [mock.MagicMock() for _ in range(23)]
        self.some_redis.hmget.return_value[13] = None
        self.assertEqual(0, self.a_metrics_cache.get_latency(self.some_operation_str)[13])

    def test_get_latency_bucket_counter_calls_hget(self):
        """Test that get_latency_bucket_counter calls hget"""
        self.a_metrics_cache.get_latency_bucket_counter(self.some_operation_str,
                                                        self.some_bucket_index)
        self.some_redis.hget.assert_called_once_with('SPLITIO.metrics.metric',
                                                     'time.some_operation.15')

    def test_get_latency_bucket_counter_returns_hget_result(self):
        """Test that get_latency_bucket_counter returns the result of calling hget"""
        self.assertEqual(self.some_redis.hget.return_value,
                         self.a_metrics_cache.get_latency_bucket_counter(self.some_operation_str,
                                                                         self.some_bucket_index))

    def test_get_latency_bucket_counter_returns_0_on_missing_value(self):
        """Test that get_latency_bucket_counter returns 0 if the bucket value is not cached"""
        self.some_redis.hget.return_value = None
        self.assertEqual(0,
                         self.a_metrics_cache.get_latency_bucket_counter(self.some_operation_str,
                                                                         self.some_bucket_index))

    def test_set_gauge_calls_hset(self):
        """Test that set_gauge calls hset"""
        self.a_metrics_cache.set_gauge(self.some_gauge_str, self.some_value)
        self.some_redis.hset('SPLITIO.metrics.metric', 'gauge.some_gauge', self.some_value)

    def test_set_latency_bucket_counter_calls_conditional_eval(self):
        """Test that set_latency_bucket_counter calls conditional eval with set latency bucket
        counter script"""
        self.a_metrics_cache.set_latency_bucket_counter(self.some_operation_str,
                                                        self.some_bucket_index,
                                                        self.some_value)
        self.conditional_eval_mock.assert_called_once_with(
            "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]);", 1, 'SPLITIO.metrics.metric',
            'time.some_operation.15', self.some_value)

    def test_get_count_calls_hget(self):
        """Test that get_count calls hget"""
        self.assertEqual(self.some_redis.hget.return_value,
                         self.a_metrics_cache.get_count(self.some_count_str))
        self.some_redis.hget.assert_called_once_with('SPLITIO.metrics.metric', 'count.some_count')

    def test_set_count_calls_conditional_eval(self):
        """Test that set_count calls conditional eval with set count script"""
        self.a_metrics_cache.set_count(self.some_count_str, self.some_value)
        self.conditional_eval_mock.assert_called_once_with(
            "redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]);", 1, 'SPLITIO.metrics.metric',
            'count.some_count', self.some_value)

    def test_increment_latency_bucket_counter_calls_conditional_eval(self):
        """Test that increment_latency_bucket_counter calls conditional eval with the increment
        latency bucket counter script"""
        self.a_metrics_cache.increment_latency_bucket_counter(self.some_operation_str,
                                                              self.some_bucket_index,
                                                              self.some_delta)
        self.conditional_eval_mock.assert_called_once_with(
            "redis.call('HINCRBY', KEYS[1], ARGV[1], ARGV[2]);", 1, 'SPLITIO.metrics.metric',
            'time.some_operation.15', self.some_delta)

    def test_increment_latency_bucket_counter_calls_conditional_eval_default_delta(self):
        """Test that increment_latency_bucket_counter calls conditional eval with the increment
        latency bucket counter script with the default delta"""
        self.a_metrics_cache.increment_latency_bucket_counter(self.some_operation_str,
                                                              self.some_bucket_index)
        self.conditional_eval_mock.assert_called_once_with(
            "redis.call('HINCRBY', KEYS[1], ARGV[1], ARGV[2]);", 1, 'SPLITIO.metrics.metric',
            'time.some_operation.15', 1)

    def test_get_gauge_calls_hget(self):
        """Test that get_gauge calls hget"""
        self.assertEqual(self.some_redis.hget.return_value,
                         self.a_metrics_cache.get_gauge(self.some_gauge_str))
        self.some_redis.hget.assert_called_once_with('SPLITIO.metrics.metric', 'gauge.some_gauge')

    def test_fetch_all_and_clear_calls_eval(self):
        """Test that fetch_all_and_clear calls eval with fetch and clear script"""
        self.a_metrics_cache.fetch_all_and_clear()
        self.some_redis.eval.assert_called_once_with(
            "if redis.call('EXISTS', KEYS[1]) == 1 then redis.call('RENAME', KEYS[1], KEYS[2]); "
            "local metric = redis.call('HGETALL', KEYS[2]); redis.call('DEL', KEYS[2]); "
            "return metric; end", 2, 'SPLITIO.metrics.metric', 'SPLITIO.metrics.metric_to_clear'
        )

    def test_fetch_all_and_clear_returns_build_metrics_from_cache_response_result(self):
        """Test that fetch_all_and_clear returns the result of calling
        build_metrics_from_cache_response"""
        self.assertEqual(self.build_metrics_from_cache_response_mock.return_value,
                         self.a_metrics_cache.fetch_all_and_clear())
        self.build_metrics_from_cache_response_mock.assert_called_once_with(
            self.some_redis.eval.return_value)


class RedisMetricsCacheConditionalEvalTests(TestCase):
    def setUp(self):
        self.some_script = 'some_script'
        self.some_num_keys = mock.MagicMock()
        self.some_key = mock.MagicMock()
        self.some_other_key = mock.MagicMock()
        self.some_value = mock.MagicMock()
        self.some_redis = mock.MagicMock()
        self.a_metrics_cache = RedisMetricsCache(self.some_redis)

    def test_conditional_eval_calls_eval(self):
        """Test that _conditional eval calls eval with the supplied script sorrounded by the
        conditional clause"""
        self.a_metrics_cache._conditional_eval(self.some_script, self.some_num_keys, self.some_key,
                                               self.some_other_key, self.some_value)
        self.some_redis.eval.assert_called_once_with(
            "if redis.call('EXISTS', 'SPLITIO.metrics.__disabled__') == 0 then some_script end",
            self.some_num_keys, self.some_key, self.some_other_key, self.some_value)

    def test_conditional_eval_returns_eval_result(self):
        """Test that _conditional_eval returns the result of calling eval"""
        self.assertEqual(self.some_redis.eval.return_value,
                         self.a_metrics_cache._conditional_eval(self.some_script,
                                                                self.some_num_keys, self.some_key,
                                                                self.some_other_key,
                                                                self.some_value))


class RedisMetricsCacheBuildMetricsFromCacheResponseTests(TestCase):
    def setUp(self):
        self.some_redis = mock.MagicMock()
        self.a_metrics_cache = RedisMetricsCache(self.some_redis)

    def test_returns_default_empty_dict_on_none_response(self):
        """Test that _build_metrics_from_cache_response returns the empty default dict if response
        is None"""
        self.assertDictEqual({'time': [], 'count': [], 'gauge': []},
                             self.a_metrics_cache._build_metrics_from_cache_response(None))

    def test_returns_default_empty_dict_on_empty_response(self):
        """Test that _build_metrics_from_cache_response returns the empty default dict if response
        is empty"""
        self.assertDictEqual({'time': [], 'count': [], 'gauge': []},
                             self.a_metrics_cache._build_metrics_from_cache_response([]))

    def test_returns_count_metrics(self):
        """Test that _build_metrics_from_cache_response returns count metrics"""
        some_count = 'some_count'
        some_count_value = mock.MagicMock()
        some_other_count = 'some_other_count'
        some_other_count_value = mock.MagicMock()
        count_metrics = ['count.some_count', some_count_value, 'count.some_other_count',
                         some_other_count_value]
        result_count_metrics = [{'name': some_count, 'delta': some_count_value},
                                {'name': some_other_count, 'delta': some_other_count_value}]
        self.assertDictEqual({'time': [], 'count': result_count_metrics, 'gauge': []},
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

        time_metrics = ['time.some_time.2', some_time_latencies[2], 'time.some_time.13',
                        some_time_latencies[13], 'time.some_other_time.0',
                        some_other_time_latencies[0], 'time.some_other_time.1',
                        some_other_time_latencies[1], 'time.some_other_time.20',
                        some_other_time_latencies[20]]
        result_time_metris = [{'name': some_other_time, 'latencies': some_other_time_latencies},
                              {'name': some_time, 'latencies': some_time_latencies}]
        self.assertDictEqual({'time': result_time_metris, 'count': [], 'gauge': []},
                             self.a_metrics_cache._build_metrics_from_cache_response(time_metrics))

    def test_returns_gauge_metrics(self):
        """Test that _build_metrics_from_cache_response returns gauge metrics"""
        some_gauge = 'some_gauge'
        some_gauge_value = mock.MagicMock()
        some_other_gauge = 'some_other_gauge'
        some_other_gauge_value = mock.MagicMock()
        gauge_metrics = ['gauge.some_gauge', some_gauge_value, 'gauge.some_other_gauge',
                         some_other_gauge_value]
        result_gauge_metrics = [{'name': some_gauge, 'value': some_gauge_value},
                                {'name': some_other_gauge, 'value': some_other_gauge_value}]
        self.assertDictEqual({'time': [], 'count': [], 'gauge': result_gauge_metrics},
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
            'defaultTreatment': mock.MagicMock()
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
            self.some_split['defaultTreatment'], segment_cache=self.some_segment_cache)

    def test_parse_matcher_in_segment_registers_segment(self):
        """Test that _parse_matcher_in_segment registers segment"""
        self.split_parser._parse_matcher_in_segment(self.some_partial_split,
                                                    self.some_in_segment_matcher,
                                                    block_until_ready=self.some_block_until_ready)
        self.some_segment_cache.register_segment.assert_called()
