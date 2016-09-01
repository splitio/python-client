"""This module contains everything related to redis cache implementations"""
from __future__ import absolute_import, division, print_function, unicode_literals

import re

from collections import defaultdict
from builtins import zip
from itertools import groupby, islice

try:
    from jsonpickle import decode, encode
    from redis import StrictRedis
except ImportError:
    def missing_redis_dependencies(*args, **kwargs):
        raise NotImplementedError('Missing Redis support dependencies.')
    decode = encode = StrictRedis = missing_redis_dependencies

from six import iteritems

from splitio.config import import_from_string
from splitio.cache import SegmentCache, SplitCache, ImpressionsCache, MetricsCache
from splitio.matchers import UserDefinedSegmentMatcher
from splitio.metrics import BUCKETS
from splitio.segments import Segment
from splitio.splits import Split, SplitParser

# Template for Split.io related Cache keys
_SPLITIO_CACHE_KEY_TEMPLATE = 'SPLITIO.{suffix}'


class RedisSegmentCache(SegmentCache):
    _KEY_TEMPLATE = _SPLITIO_CACHE_KEY_TEMPLATE.format(suffix='segments.{suffix}')
    _DISABLED_KEY = _KEY_TEMPLATE.format(suffix='__disabled__')
    _SEGMENT_KEY_SET_KEY_TEMPLATE = _KEY_TEMPLATE.format(suffix='segment.{segment_name}.key_set')
    _SEGMENT_CHANGE_NUMBER_KEY_TEMPLATE = _KEY_TEMPLATE.format(
        suffix='segment.{segment_name}.change_number')

    def __init__(self, redis, disabled_period=300):
        """A Segment Cache implementation that uses Redis as its back-end
        :param redis: The redis client
        :rtype redis: StringRedis
        :param disabled_period: The expiration period for the disabled key.
        :param disabled_period: int
        """
        self._redis = redis
        self._disabled_period = disabled_period

    @property
    def disabled_period(self):
        return self._disabled_period

    @disabled_period.setter
    def disabled_period(self, disabled_period):
        self._disabled_period = disabled_period

    def disable(self):
        """Disables the automatic update process. This method will be called if the update fails
        for some reason. Use enable to re-enable the update process."""
        self._redis.setex(RedisSegmentCache._DISABLED_KEY, 1, self._disabled_period)

    def enable(self):
        """Enables the automatic update process."""
        self._redis.delete(RedisSegmentCache._DISABLED_KEY)

    def is_enabled(self):
        """
        :return: Whether the update process is enabled or not.
        :rtype: bool
        """
        return not self._redis.exists(RedisSegmentCache._DISABLED_KEY)

    def register_segment(self, segment_name):
        """Register a segment for inclusion in the automatic update process.
        :param segment_name: Name of the segment.
        :type segment_name: str
        """
        self._redis.sadd(RedisSegmentCache._KEY_TEMPLATE.format(suffix='__registered_segments__'),
                         segment_name)

    def unregister_segment(self, segment_name):
        """Unregister a segment from the automatic update process.
        :param segment_name: Name of the segment.
        :type segment_name: str
        """
        self._redis.srem(RedisSegmentCache._KEY_TEMPLATE.format(suffix='__registered_segments__'),
                         segment_name)

    def get_registered_segments(self):
        """
        :return: All segments included in the automatic update process.
        :rtype: set
        """
        return self._redis.smembers(RedisSegmentCache._KEY_TEMPLATE.format(
            suffix='__registered_segments__'))

    def _get_segment_key_set_key(self, segment_name):
        """Build cache key for a given segment key set.
        :param segment_name: The name of the segment
        :type segment_name: str
        :return: The cache key for the segment key set
        :rtype: str
        """
        return RedisSegmentCache._SEGMENT_KEY_SET_KEY_TEMPLATE.format(
            segment_name=segment_name)

    def _get_segment_change_number_key(self, segment_name):
        """Build cache key for a given segment change_number.
        :param segment_name: The name of the segment
        :type segment_name: str
        :return: The cache key for the segment change number
        :rtype: str
        """
        return RedisSegmentCache._SEGMENT_CHANGE_NUMBER_KEY_TEMPLATE.format(
            segment_name=segment_name)

    def add_keys_to_segment(self, segment_name, segment_keys):
        self._redis.sadd(self._get_segment_key_set_key(segment_name), *segment_keys)

    def remove_keys_from_segment(self, segment_name, segment_keys):
        self._redis.srem(self._get_segment_key_set_key(segment_name), *segment_keys)

    def is_in_segment(self, segment_name, key):
        return self._redis.sismember(self._get_segment_key_set_key(segment_name), key)

    def set_change_number(self, segment_name, change_number):
        self._redis.set(self._get_segment_change_number_key(segment_name), change_number)

    def get_change_number(self, segment_name):
        change_number = self._redis.get(self._get_segment_change_number_key(segment_name))
        return int(change_number) if change_number is not None else -1


class RedisSplitCache(SplitCache):
    _KEY_TEMPLATE = _SPLITIO_CACHE_KEY_TEMPLATE.format(suffix='splits.{suffix}')
    _DISABLED_KEY = _KEY_TEMPLATE.format(suffix='__disabled__')

    def __init__(self, redis, disabled_period=300):
        """A SplitCache implementation that uses Redis as its back-end.
        :param redis: The redis client
        :type redis: StrictRedis
        :param disabled_period: The expiration period for the disabled key.
        :param disabled_period: int
        """
        self._redis = redis
        self._disabled_period = disabled_period

    @property
    def disabled_period(self):
        return self._disabled_period

    @disabled_period.setter
    def disabled_period(self, disabled_period):
        self._disabled_period = disabled_period

    def _get_split_key(self, split_name):
        """Builds a Redis key cache for a given split (feature) name,
        :param split_name: Name of the split (feature)
        :type split_name: str
        :return: The split key
        :rtype: str
        """
        return RedisSplitCache._KEY_TEMPLATE.format(suffix=split_name)

    def disable(self):
        """Disables the automatic split update process for the specified disabled period. This
        method will be called if there's an exception while updating the splits."""
        self._redis.setex(RedisSplitCache._DISABLED_KEY, 1, self._disabled_period)

    def enable(self):
        """Enables the automatic split update process."""
        self._redis.delete(RedisSplitCache._DISABLED_KEY)

    def is_enabled(self):
        """
        :return: Whether the update process is enabled or not.
        :rtype: bool
        """
        return not self._redis.exists(RedisSplitCache._DISABLED_KEY)

    def get_change_number(self):
        change_number = self._redis.get(RedisSplitCache._KEY_TEMPLATE.format(
            suffix='__change_number__'))
        return int(change_number) if change_number is not None else -1

    def set_change_number(self, change_number):
        self._redis.set(RedisSplitCache._KEY_TEMPLATE.format(suffix='__change_number__'),
                        change_number, None)

    def add_split(self, split_name, split):
        self._redis.set(self._get_split_key(split_name), encode(split))

    def get_split(self, split_name):
        split_dump = self._redis.get(self._get_split_key(split_name))

        if split_dump is not None:
            split = decode(split_dump)
            split._segment_cache = RedisSegmentCache(self._redis)
            return split

        return None

    def remove_split(self, split_name):
        self._redis.delete(self._get_split_key(split_name))


class RedisImpressionsCache(ImpressionsCache):
    _KEY_TEMPLATE = _SPLITIO_CACHE_KEY_TEMPLATE.format(suffix='impressions.{suffix}')
    _IMPRESSIONS_KEY = _KEY_TEMPLATE.format(suffix='impressions')
    _IMPRESSIONS_TO_CLEAR_KEY = _KEY_TEMPLATE.format(suffix='impressions_to_clear')
    _DISABLED_KEY = _KEY_TEMPLATE.format(suffix='__disabled__')

    def __init__(self, redis, disabled_period=300):
        """An ImpressionsCache implementation that uses Redis as its back-end
        :param redis: The redis client
        :type redis: StrictRedis
        :param disabled_period: The expiration period for the disabled key.
        :param disabled_period: int
        """
        self._redis = redis
        self._disabled_period = disabled_period

    @property
    def disabled_period(self):
        return self._disabled_period

    @disabled_period.setter
    def disabled_period(self, disabled_period):
        self._disabled_period = disabled_period

    def enable(self):
        """Enables the automatic impressions report process and the registration of impressions."""
        self._redis.delete(RedisImpressionsCache._DISABLED_KEY)

    def disable(self):
        """Disables the automatic impressions report process and the registration of any
        impressions for the specificed disabled period. This method will be called if there's an
        exception while trying to send the impressions back to Split."""
        self._redis.setex(RedisImpressionsCache._DISABLED_KEY, 1, self._disabled_period)

    def is_enabled(self):
        """
        :return: Whether the automatic report process and impressions registration are enabled.
        :rtype: bool
        """
        return not self._redis.exists(RedisImpressionsCache._DISABLED_KEY)

    def _build_impressions_dict(self, impressions):
        """Buils a dictionary of impressions that groups them based on their feature name.
        :param impressions: List of impression tuples
        :type impressions: list
        :return: Dictionary of impressions grouped by feature name
        :rtype: dict
        """
        sorted_impressions = sorted(impressions, key=lambda impression: impression.feature_name)
        grouped_impressions = groupby(sorted_impressions,
                                      key=lambda impression: impression.feature_name)
        return dict((feature_name, list(group)) for feature_name, group in grouped_impressions)

    def fetch_all(self):
        """Fetches all impressions from the cache. It returns a dictionary with the impressions
        grouped by feature name.
        :return: All cached impressions so far grouped by feature name
        :rtype: dict
        """
        impressions = self._redis.lrange(RedisImpressionsCache._IMPRESSIONS_KEY, 0, -1)

        if impressions is None:
            return dict()

        return self._build_impressions_dict(
            [decode(impression) for impression in impressions])

    def clear(self):
        """Clears all cached impressions"""
        self._redis.delete(RedisImpressionsCache._IMPRESSIONS_KEY)

    def add_impression(self, impression):
        """Adds an impression to the log if it is enabled, otherwise the impression is dropped.
        :param impression: The impression tuple
        :type impression: Impression
        """
        self._redis.eval(
            "if redis.call('EXISTS', KEYS[1]) == 0 then "
            "redis.call('LPUSH', KEYS[2], ARGV[1]) end", 2, RedisImpressionsCache._DISABLED_KEY,
            self._IMPRESSIONS_KEY, encode(impression))

    def fetch_all_and_clear(self):
        """Fetches all impressions from the cache and clears it. It returns a dictionary with the
        impressions grouped by feature name.
        :return: All cached impressions so far grouped by feature name
        :rtype: dict
        """
        impressions = self._redis.eval(
            "if redis.call('EXISTS', KEYS[1]) == 1 then "
            "redis.call('RENAME', KEYS[1], KEYS[2]); "
            "local impressions_to_clear = redis.call('LRANGE', KEYS[2], 0, -1); "
            "redis.call('DEL', KEYS[2]); "
            "return impressions_to_clear; "
            "end", 2,
            RedisImpressionsCache._IMPRESSIONS_KEY, RedisImpressionsCache._IMPRESSIONS_TO_CLEAR_KEY)

        if impressions is None:
            return dict()

        return self._build_impressions_dict(
            [decode(impression) for impression in impressions])


class RedisMetricsCache(MetricsCache):
    _KEY_TEMPLATE = _SPLITIO_CACHE_KEY_TEMPLATE.format(suffix='metrics.{suffix}')
    _DISABLED_KEY = _KEY_TEMPLATE.format(suffix='__disabled__')
    _METRIC_KEY = _KEY_TEMPLATE.format(suffix='metric')
    _METRIC_TO_CLEAR_KEY = _KEY_TEMPLATE.format(suffix='metric_to_clear')
    _COUNT_FIELD_TEMPLATE = 'count.{counter}'
    _TIME_FIELD_TEMPLATE = 'time.{operation}.{bucket_index}'
    _GAUGE_FIELD_TEMPLATE = 'gauge.{gauge}'

    _COUNT_FIELD_RE = re.compile('^count\.(?P<counter>.+)$')
    _TIME_FIELD_RE = re.compile('^time\.(?P<operation>.+)\.(?P<bucket_index>.+)$')
    _GAUGE_FIELD_RE = re.compile('^gauge\.(?P<gauge>.+)$')

    _CONDITIONAL_EVAL_SCRIPT_TEMPLATE = "if redis.call('EXISTS', '{disabled_key}') == 0 " \
                                        "then {script} end"

    def __init__(self, redis, disabled_period=300):
        """A MetricsCache implementation that uses Redis as its back-end
        :param redis: The redis client
        :type redis: StrictRedis
        :param disabled_period: The expiration period for the disabled key.
        :param disabled_period: int
        """
        super(RedisMetricsCache, self).__init__()
        self._redis = redis
        self._disabled_period = disabled_period

    @property
    def disabled_period(self):
        return self._disabled_period

    @disabled_period.setter
    def disabled_period(self, disabled_period):
        self._disabled_period = disabled_period

    def enable(self):
        """Enables the automatic metrics report process and the registration of new metrics."""
        self._redis.delete(RedisMetricsCache._DISABLED_KEY)

    def disable(self):
        """Disables the automatic metrics report process and the registration of any
        metrics for the specified disabled period. This method will be called if there's an
        exception while trying to send the metrics back to Split."""
        self._redis.setex(RedisMetricsCache._DISABLED_KEY, 1, self._disabled_period)

    def is_enabled(self):
        """
        :return: Whether the automatic report process and metrics registration are enabled.
        :rtype: bool
        """
        return not self._redis.exists(RedisMetricsCache._DISABLED_KEY)

    def _get_count_field(self, counter):
        """Builds the field name for a counter on the metrics redis hash.
        :param counter: Name of the counter
        :type counter: str
        :return: Name of the field on the metrics hash for the given counter
        :rtype: str
        """
        return RedisMetricsCache._COUNT_FIELD_TEMPLATE.format(counter=counter)

    def _get_time_field(self, operation, bucket_index):
        """Builds the field name for a latency counting bucket ont the metrics redis hash.
        :param operation: Name of the operation
        :type operation: str
        :param bucket_index: Latency bucket index as returned by get_latency_bucket_index
        :type bucket_index: int
        :return: Name of the field on the metrics hash for the latency bucket counter
        :rtype: str
        """
        return RedisMetricsCache._TIME_FIELD_TEMPLATE.format(operation=operation,
                                                             bucket_index=bucket_index)

    def _get_all_buckets_time_fields(self, operation):
        """ Builds a list of all the fields in the metrics hash for the latency buckets for a given
        operation.
        :param operation: Name of the operation
        :type operation: str
        :return: List of field names
        :rtype: list
        """
        return [self._get_time_field(operation, bucket) for bucket in range(0, len(BUCKETS))]

    def _get_gauge_field(self, gauge):
        """Builds the field name for a gauge on the metrics redis hash.
        :param gauge: Name of the gauge
        :type gauge: str
        :return: Name of the field on the metrics hash for the given gauge
        :rtype: str
        """
        return RedisMetricsCache._GAUGE_FIELD_TEMPLATE.format(gauge=gauge)

    def _conditional_eval(self, script, num_keys, *keys_and_args):
        """Evaluates the given script on Redis depending on whether is_enabled is True or not.
        :param script: The script to evaluate
        :type script: str
        :param num_keys: Number of keys included in the keys_and_args list
        :type num_keys: int
        :param keys_and_args: List of keys and arguments to be given to redis "eval" method
        :type keys_and_args: list
        :return: The result of evaluating the script
        """
        return self._redis.eval(
            RedisMetricsCache._CONDITIONAL_EVAL_SCRIPT_TEMPLATE.format(
                disabled_key=RedisMetricsCache._DISABLED_KEY, script=script), num_keys,
            *keys_and_args)

    def _build_metrics_counter_data(self, count_metrics):
        """Build metrics counter data in the format expected by the API from the contents of the
        cache.
        :param count_metrics: A dictionary of name/value counter metrics
        :param count_metrics: dict
        :return: A list of of counter metrics
        :rtype: list
        """
        return [{'name': name, 'delta': delta} for name, delta in iteritems(count_metrics)]

    def _build_metrics_times_data(self, time_metrics):
        """Build metrics times data in the format expected by the API from the contents of the
        cache.
        :param time_metrics: A dictionary of name/latencies time metrics
        :param time_metrics: dict
        :return: A list of of time metrics
        :rtype: list
        """
        return [{'name': name, 'latencies': latencies}
                for name, latencies in iteritems(time_metrics)]

    def _build_metrics_gauge_data(self, gauge_metrics):
        """Build metrics gauge data in the format expected by the API from the contents of the
        cache.
        :param gauge_metrics: A dictionary of name/value gauge metrics
        :param gauge_metrics: dict
        :return: A list of of gauge metrics
        :rtype: list
        """
        return [{'name': name, 'value': value} for name, value in iteritems(gauge_metrics)]

    def _build_metrics_from_cache_response(self, response):
        """Builds a dictionary with time, count and gauge metrics based on the result of calling
        fetch_all_and_clear (list of name/value pairs). Each entry in the dictionary is in the
        format accepted by the events API.
        :param response: Response given by the fetch_all_and_clear method
        :type response: lsit
        :return: Dictionary with time, count and gauge metrics
        :rtype: dict
        """
        if response is None:
            return {'time': [], 'count': [], 'gauge': []}

        time = defaultdict(lambda: [0] * len(BUCKETS))
        count = dict()
        gauge = dict()

        for field, value in zip(islice(response, 0, None, 2), islice(response, 1, None, 2)):
            count_match = RedisMetricsCache._COUNT_FIELD_RE.match(field)
            if count_match is not None:
                count[count_match.group('counter')] = value
                continue

            time_match = RedisMetricsCache._TIME_FIELD_RE.match(field)
            if time_match is not None:
                time[time_match.group('operation')][int(time_match.group('bucket_index'))] = value
                continue

            gauge_match = RedisMetricsCache._GAUGE_FIELD_RE.match(field)
            if gauge_match is not None:
                gauge[gauge_match.group('gauge')] = value
                continue

        return {
            'time': self._build_metrics_times_data(time),
            'count': self._build_metrics_counter_data(count),
            'gauge': self._build_metrics_gauge_data(gauge)
        }

    def increment_count(self, counter, delta=1):
        self._conditional_eval("redis.call('HINCRBY', KEYS[1], ARGV[1], ARGV[2]);",
                               1, RedisMetricsCache._METRIC_KEY,
                               self._get_count_field(counter), delta)

    def get_latency(self, operation):
        return [
            0 if count is None else count
            for count in self._redis.hmget(
                RedisMetricsCache._METRIC_KEY, *self._get_all_buckets_time_fields(operation))]

    def get_latency_bucket_counter(self, operation, bucket_index):
        count = self._redis.hget(RedisMetricsCache._METRIC_KEY, self._get_time_field(operation,
                                                                                     bucket_index))
        return count if count is not None else 0

    def set_gauge(self, gauge, value):
        self._redis.hset(RedisMetricsCache._METRIC_KEY, self._get_gauge_field(gauge), value)

    def set_latency_bucket_counter(self, operation, bucket_index, value):
        self._conditional_eval("redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]);", 1,
                               RedisMetricsCache._METRIC_KEY,
                               self._get_time_field(operation, bucket_index), value)

    def get_count(self, counter):
        count = self._redis.hget(RedisMetricsCache._METRIC_KEY, self._get_count_field(counter))
        return count if count is not None else 0

    def set_count(self, counter, value):
        self._conditional_eval("redis.call('HSET', KEYS[1], ARGV[1], ARGV[2]);", 1,
                               RedisMetricsCache._METRIC_KEY,
                               self._get_count_field(counter), value)

    def increment_latency_bucket_counter(self, operation, bucket_index, delta=1):
        self._conditional_eval("redis.call('HINCRBY', KEYS[1], ARGV[1], ARGV[2]);", 1,
                               RedisMetricsCache._METRIC_KEY,
                               self._get_time_field(operation, bucket_index), delta)

    def get_gauge(self, gauge):
        return self._redis.hget(RedisMetricsCache._METRIC_KEY, self._get_gauge_field(gauge))

    def fetch_all_and_clear(self):
        response = self._redis.eval("if redis.call('EXISTS', KEYS[1]) == 1 then "
                                    "redis.call('RENAME', KEYS[1], KEYS[2]); "
                                    "local metric = redis.call('HGETALL', KEYS[2]); "
                                    "redis.call('DEL', KEYS[2]); "
                                    "return metric; end", 2, RedisMetricsCache._METRIC_KEY,
                                    RedisMetricsCache._METRIC_TO_CLEAR_KEY)
        return self._build_metrics_from_cache_response(response)


class RedisSplitParser(SplitParser):
    def __init__(self, segment_cache):
        """
        A SplitParser implementation that registers the segments with the redis segment cache
        implementation upon parsing an IN_SEGMENT matcher.
        """
        super(RedisSplitParser, self).__init__(None)
        self._segment_cache = segment_cache

    def _parse_split(self, split, block_until_ready=False):
        return RedisSplit(split['name'], split['seed'], split['killed'], split['defaultTreatment'],
                          segment_cache=self._segment_cache)

    def _parse_matcher_in_segment(self, partial_split, matcher, block_until_ready=False, *args,
                                  **kwargs):
        matcher_data = self._get_matcher_attribute('userDefinedSegmentMatcherData', matcher)
        segment = RedisSplitBasedSegment(matcher_data['segmentName'], partial_split)
        delegate = UserDefinedSegmentMatcher(segment)
        self._segment_cache.register_segment(delegate.segment.name)
        return delegate


class RedisSplit(Split):
    def __init__(self, name, seed, killed, default_treatment, conditions=None, segment_cache=None):
        """A split implementation that mantains a reference to the segment cache so segments can
        be easily pickled and unpickled.
        :param name: Name of the feature
        :type name: unicode
        :param seed: Seed
        :type seed: int
        :param killed: Whether the split is killed or not
        :type killed: bool
        :param default_treatment: Default treatment for the split
        :type default_treatment: str
        :param conditions: Set of conditions to test
        :type conditions: list
        :param segment_cache: A segment cache
        :type segment_cache: SegmentCache
        """
        super(RedisSplit, self).__init__(name, seed, killed, default_treatment, conditions)
        self._segment_cache = segment_cache

    @property
    def segment_cache(self):
        return self._segment_cache

    @segment_cache.setter
    def segment_cache(self, segment_cache):
        self._segment_cache = segment_cache

    def __getstate__(self):
        old_dict = self.__dict__.copy()
        del old_dict['_segment_cache']
        return old_dict

    def __setstate__(self, dict):
        self.__dict__.update(dict)
        self._segment_cache = None


class RedisSplitBasedSegment(Segment):
    def __init__(self, name, split):
        """A Segment that uses a reference to a RedisSplit redis' instance to check if a key
        is in a segment
        :param name: The name of the segment
        :type name: str
        :param split: A RedisSplit instance
        :type split: RedisSplit
        """
        super(RedisSplitBasedSegment, self).__init__(name)
        self._split = split

    def contains(self, key):
        return self._split.segment_cache.is_in_segment(self.name, key)


def get_redis(config):
    """Build a redis client based on the configuration.
    :param config: Dictionary with the contents of the config file.
    :type config: dict
    :return: A redis client
    """
    if 'redisFactory' in config:
        redis_factory = import_from_string(config['redisFactory'])
        return redis_factory()

    return default_redis_factory(config)


def default_redis_factory(config):
    """Default redis client factory.
    :param config: A dict with the Redis configuration parameters
    :type config: dict
    :return: A StrictRedis object using the provided config values
    :rtype: StrictRedis
    """
    host = config.get('redisHost', 'localhost')
    port = config.get('redisPort', 6379)
    db = config.get('redisDb', 0)
    redis = StrictRedis(host=host, port=port, db=db)
    return redis
