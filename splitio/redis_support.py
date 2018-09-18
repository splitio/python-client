'''This module contains everything related to redis cache implementations'''
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import re
import logging

from builtins import zip
from itertools import groupby, islice

try:
    from jsonpickle import decode, encode
    from redis import StrictRedis
    from redis.sentinel import Sentinel
except ImportError:
    def missing_redis_dependencies(*args, **kwargs):
        raise NotImplementedError('Missing Redis support dependencies.')
    decode = encode = StrictRedis = missing_redis_dependencies

from six import iteritems

from splitio.config import import_from_string
from splitio.cache import SegmentCache, SplitCache, ImpressionsCache, \
    MetricsCache
from splitio.matchers import UserDefinedSegmentMatcher
from splitio.metrics import BUCKETS
from splitio.segments import Segment
from splitio.splits import Split, SplitParser
from splitio.impressions import Impression
from splitio.utils import bytes_to_string
from splitio.prefix_decorator import PrefixDecorator
from splitio.version import __version__ as _SDK_VERSION
# Template for Split.io related Cache keys
_SPLITIO_CACHE_KEY_TEMPLATE = 'SPLITIO.{suffix}'
_GLOBAL_KEY_PARAMETERS = {
    'sdk-language-version': 'python-{version}'.format(version=_SDK_VERSION),
    'instance-id': 'unknown',
    'ip-address': 'unknown',
}


class SentinelConfigurationException(Exception):
    pass


class RedisSegmentCache(SegmentCache):
    '''
    '''
    _KEY_TEMPLATE = _SPLITIO_CACHE_KEY_TEMPLATE.format(
        suffix='segments.{suffix}'
    )
    _DISABLED_KEY = _KEY_TEMPLATE.format(suffix='__disabled__')
    _SEGMENT_KEY_SET_KEY_TEMPLATE = _SPLITIO_CACHE_KEY_TEMPLATE.format(
        suffix='segment.{segment_name}'
    )
    _SEGMENT_CHANGE_NUMBER_KEY_TEMPLATE = _SPLITIO_CACHE_KEY_TEMPLATE.format(
        suffix='segment.{segment_name}.till'
    )

    def __init__(self, redis):
        '''
        A Segment Cache implementation that uses Redis as its back-end
        :param redis: The redis client
        :rtype redis: StringRedis
        '''
        self._redis = redis

    def register_segment(self, segment_name):
        '''
        Register a segment for inclusion in the automatic update process.
        :param segment_name: Name of the segment.
        :type segment_name: str
        '''
        # self._redis.sadd(
        #     RedisSegmentCache._KEY_TEMPLATE.format(suffix='registered'),
        #     segment_name
        # )
        # @TODO The Segment logic for redis should be removed.
        pass

    def unregister_segment(self, segment_name):
        '''
        Unregister a segment from the automatic update process.
        :param segment_name: Name of the segment.
        :type segment_name: str
        '''
        # self._redis.srem(
        #     RedisSegmentCache._KEY_TEMPLATE.format(suffix='registered'),
        #     segment_name
        # )
        # @TODO The Segment logic for redis should be removed.
        pass

    def get_registered_segments(self):
        '''
        :return: All segments included in the automatic update process.
        :rtype: set
        '''
        return self._redis.smembers(
            RedisSegmentCache._KEY_TEMPLATE.format(suffix='registered')
        )

    def _get_segment_key_set_key(self, segment_name):
        '''
        Build cache key for a given segment key set.
        :param segment_name: The name of the segment
        :type segment_name: str
        :return: The cache key for the segment key set
        :rtype: str
        '''
        segment_name = bytes_to_string(segment_name)
        return RedisSegmentCache._SEGMENT_KEY_SET_KEY_TEMPLATE.format(
            segment_name=segment_name
        )

    def _get_segment_change_number_key(self, segment_name):
        '''
        Build cache key for a given segment change_number.
        :param segment_name: The name of the segment
        :type segment_name: str
        :return: The cache key for the segment change number
        :rtype: str
        '''
        segment_name = bytes_to_string(segment_name)
        return RedisSegmentCache._SEGMENT_CHANGE_NUMBER_KEY_TEMPLATE.format(
            segment_name=segment_name
        )

    def add_keys_to_segment(self, segment_name, segment_keys):
        self._redis.sadd(
            self._get_segment_key_set_key(segment_name),
            *segment_keys
        )

    def remove_keys_from_segment(self, segment_name, segment_keys):
        self._redis.srem(
            self._get_segment_key_set_key(segment_name),
            *segment_keys
        )

    def is_in_segment(self, segment_name, key):
        return self._redis.sismember(
            self._get_segment_key_set_key(segment_name),
            key
        )

    def set_change_number(self, segment_name, change_number):
        self._redis.set(
            self._get_segment_change_number_key(segment_name),
            change_number
        )

    def get_change_number(self, segment_name):
        change_number = self._redis.get(
            self._get_segment_change_number_key(segment_name)
        )
        return int(change_number) if change_number is not None else -1


class RedisSplitCache(SplitCache):
    _KEY_TEMPLATE = _SPLITIO_CACHE_KEY_TEMPLATE.format(suffix='split.{suffix}')
    _KEY_TILL_TEMPLATE = _SPLITIO_CACHE_KEY_TEMPLATE.format(
        suffix='splits.{suffix}'
    )

    def __init__(self, redis):
        '''
        A SplitCache implementation that uses Redis as its back-end.
        :param redis: The redis client
        :type redis: StrictRedis
        '''
        self._redis = redis
        self._logger = logging.getLogger(self.__class__.__name__)

    def get_splits_keys(self):
        return self._redis.keys(
            RedisSplitCache._KEY_TEMPLATE.format(suffix='*')
        )

    def _get_split_key(self, split_name):
        '''
        Builds a Redis key cache for a given split (feature) name,
        :param split_name: Name of the split (feature)
        :type split_name: str
        :return: The split key
        :rtype: str
        '''
        return RedisSplitCache._KEY_TEMPLATE.format(suffix=split_name)

    def get_change_number(self):
        change_number = self._redis.get(
            RedisSplitCache._KEY_TILL_TEMPLATE.format(suffix='till')
        )
        return int(change_number) if change_number is not None else -1

    def set_change_number(self, change_number):
        self._redis.set(
            RedisSplitCache._KEY_TILL_TEMPLATE.format(suffix='till'),
            change_number,
            None
        )

    def add_split(self, split_name, split):
        self._redis.set(self._get_split_key(split_name), encode(split))

    def get_split(self, split_name):

        to_decode = self._redis.get(self._get_split_key(split_name))
        if to_decode is None:
            return None

        to_decode = bytes_to_string(to_decode)

        split_dump = decode(to_decode)

        if split_dump is not None:
            segment_cache = RedisSegmentCache(self._redis)
            split_parser = RedisSplitParser(segment_cache)
            split = split_parser.parse(split_dump)
            return split

        return None

    def get_splits(self):
        keys = self.get_splits_keys()

        splits = self._redis.mget(keys)

        to_return = []

        segment_cache = RedisSegmentCache(self._redis)
        split_parser = RedisSplitParser(segment_cache)

        for split in splits:
            try:
                split = bytes_to_string(split)
                split_dump = decode(split)
                if split_dump is not None:
                    to_return.append(split_parser.parse(split_dump))
            except:
                self._logger.error(
                    'Error decoding/parsing fetched split or invalid split'
                    ' format: %s' % split
                )

        return to_return

    def remove_split(self, split_name):
        self._redis.delete(self._get_split_key(split_name))


class RedisEventsCache(ImpressionsCache):
    _KEY_TEMPLATE = (
        'SPLITIO.events'
    )

    def __init__(self, redis):
        '''
        An ImpressionsCache implementation that uses Redis as its back-end
        :param redis: The redis client
        :type redis: StrictRedis
        '''
        self._logger = logging.getLogger(self.__class__.__name__)
        self._redis = redis

    def log_event(self, event):
        """
        Adds an event to the redis storage
        """
        key = self._KEY_TEMPLATE
        to_store = {
            'e': dict(event._asdict()),
            'm': {
                's': _GLOBAL_KEY_PARAMETERS['sdk-language-version'],
                'n': _GLOBAL_KEY_PARAMETERS['instance-id'],
                'i': _GLOBAL_KEY_PARAMETERS['ip-address'],
            }
        }
        try:
            res = self._redis.rpush(key, encode(to_store))
            return True
        except Exception:
            self._logger.exception("Something went wrong when trying to add event to redis")
            return False


class RedisImpressionsCache(ImpressionsCache):
    _KEY_TEMPLATE = (
        'SPLITIO/{sdk-language-version}/{instance-id}/impressions.{feature}'
    )

    @classmethod
    def _get_impressions_key(cls, feature_name):
        '''
        '''
        return cls._KEY_TEMPLATE.format(
            **dict(_GLOBAL_KEY_PARAMETERS, feature=feature_name)
        )

    @classmethod
    def _get_impressions_clear_key(cls):
        '''
        '''
        return cls._get_impressions_key('impressions_to_clear')

    def __init__(self, redis):
        '''
        An ImpressionsCache implementation that uses Redis as its back-end
        :param redis: The redis client
        :type redis: StrictRedis
        '''
        self._redis = redis

        key_params = _GLOBAL_KEY_PARAMETERS.copy()
        key_params['suffix'] = '{feature_name}'

    def _build_impressions_dict(self, impressions):
        '''
        Buils a dictionary of impressions that groups them based on their
        feature name.
        :param impressions: List of impression tuples
        :type impressions: list
        :return: Dictionary of impressions grouped by feature name
        :rtype: dict
        '''
        sorted_impressions = sorted(
            impressions,
            key=lambda impression: impression.feature_name
        )
        grouped_impressions = groupby(
            sorted_impressions,
            key=lambda impression: impression.feature_name
        )
        return dict(
            (feature_name, list(group))
            for feature_name, group in grouped_impressions
        )

    def fetch_all(self):
        '''
        Fetches all impressions from the cache. It returns a dictionary with the
        impressions  grouped by feature name.
        :return: All cached impressions so far grouped by feature name
        :rtype: dict
        '''
        impressions_list = list()
        impressions_keys = self._redis.keys(self._get_impressions_key('*'))

        for impression_key in impressions_keys:
            impression_key = bytes_to_string(impression_key)
            if (impression_key.replace(self._get_impressions_key(''), '') == 'impressions'):
                continue

            feature_name = impression_key.replace(
                self._get_impressions_key(''),
                ''
            )

            for impression in self._redis.smembers(impression_key):
                impression = bytes_to_string(impression)
                impression_decoded = decode(impression)
                impression_tuple = Impression(
                    key=impression_decoded['keyName'],
                    feature_name=feature_name,
                    treatment=impression_decoded['treatment'],
                    time=impression_decoded['time']
                )
                impressions_list.append(impression_tuple)

        if not impressions_list:
            return dict()

        return self._build_impressions_dict(impressions_list)

    def clear(self):
        '''
        Clears all cached impressions
        '''
        self._redis.eval(
            "return redis.call('del', unpack(redis.call('keys', ARGV[1])))",
            0,
            self._get_impressions_key('*')
        )

    def add_impression(self, impression):
        '''
        Adds an impression to the log if it is enabled, otherwise the impression
        is dropped.
        :param impression: The impression tuple
        :type impression: Impression
        '''
        cache_impression = {
            'keyName': impression.matching_key,
            'treatment': impression.treatment,
            'time': impression.time,
            'changeNumber': impression.change_number,
            'label': impression.label,
            'bucketingKey': impression.bucketing_key
        }
        self._redis.sadd(
            self._get_impressions_key(impression.feature_name),
            encode(cache_impression)
        )

    def fetch_all_and_clear(self):
        '''
        Fetches all impressions from the cache and clears it.
        It returns a dictionary with the impressions grouped by feature name.
        :return: All cached impressions so far grouped by feature name
        :rtype: dict
        '''
        impressions_list = list()
        impressions_keys = self._redis.keys(self._get_impressions_key('*'))

        for impression_key in impressions_keys:

            impression_key = bytes_to_string(impression_key)

            if (impression_key.replace(self._get_impressions_key(''), '') == 'impressions'):
                continue

            feature_name = impression_key.replace(
                self._get_impressions_key(''),
                ''
            )

            to_remove = list()
            for impression in self._redis.smembers(impression_key):
                to_remove.append(impression)

                impression = bytes_to_string(impression)

                impression_decoded = decode(impression)

                label = ''
                if 'label' in impression_decoded:
                    label = impression_decoded['label']

                change_number = -1
                if 'changeNumber' in impression_decoded:
                    change_number = impression_decoded['changeNumber']

                bucketing_key = ''
                if 'bucketingKey' in impression_decoded:
                    bucketing_key = impression_decoded['bucketingKey']

                impression_tuple = Impression(
                    matching_key=impression_decoded['keyName'],
                    feature_name=feature_name,
                    treatment=impression_decoded['treatment'],
                    label=label,
                    change_number=change_number,
                    bucketing_key=bucketing_key,
                    time=impression_decoded['time']
                )
                impressions_list.append(impression_tuple)

            self._redis.srem(impression_key, *set(to_remove))

        if not impressions_list:
            return dict()

        return self._build_impressions_dict(impressions_list)


class RedisMetricsCache(MetricsCache):
    _KEY_TEMPLATE = _SPLITIO_CACHE_KEY_TEMPLATE.format(
        suffix='metrics.{suffix}'
    )

    _KEY_LATENCY = (
        'SPLITIO/{sdk-language-version}/{instance-id}/'
        'latency.{metric_name}.bucket.{bucket_number}'
    )

    _KEY_COUNT = 'SPLITIO/{sdk-language-version}/{instance-id}/count.{counter}'
    _KEY_GAUGE = 'SPLITIO/{sdk-language-version}/{instance-id}/gauge.{gauge}'

    @classmethod
    def _get_latency_bucket_key(cls, metric_name, bucket_number):
        '''
        Returns the latency bucket
        '''
        return cls._KEY_LATENCY.format(**dict(
            _GLOBAL_KEY_PARAMETERS,
            metric_name=metric_name,
            bucket_number=bucket_number
        ))

    @classmethod
    def _get_count_key(cls, counter):
        '''
        Returns the count key
        '''
        return cls._KEY_COUNT.format(**dict(
            _GLOBAL_KEY_PARAMETERS,
            counter=counter
        ))

    @classmethod
    def _get_gauge_key(cls, gauge):
        '''
        Returns the gauge key
        '''
        return cls._KEY_GAUGE.format(**dict(
            _GLOBAL_KEY_PARAMETERS,
            gauge=gauge
        ))

    @classmethod
    def _get_latency_field_re(cls):
        return ("^%s$" % (cls._KEY_LATENCY
                          .replace('.', '\.')
                          .replace('{metric_name}', '(?P<operation>.+)')
                          .replace('{bucket_number}', '(?P<bucket_index>.+)'))
                ).format(**_GLOBAL_KEY_PARAMETERS)

    @classmethod
    def _get_count_field_re(cls):
        return ("^%s$" % (cls._KEY_COUNT
                          .replace('.', '\.')
                          .replace('{counter}', '(?P<counter>.+)'))
                ).format(**_GLOBAL_KEY_PARAMETERS)

    @classmethod
    def _get_gauge_field_re(cls):
        return ("^%s$" % (cls._KEY_GAUGE
                          .replace('.', '\.')
                          .replace('{gauge}', '(?P<gauge>.+)'))
                ).format(**_GLOBAL_KEY_PARAMETERS)

    def __init__(self, redis):
        '''
        A MetricsCache implementation that uses Redis as its back-end
        :param redis: The redis client
        :type redis: StrictRedis
        '''
        super(RedisMetricsCache, self).__init__()
        self._redis = redis

    def _get_time_field(self, operation, bucket_index):
        '''
        Builds the field name for a latency counting bucket ont the metrics
        redis hash.
        :param operation: Name of the operation
        :type operation: str
        :param bucket_index: Latency bucket index as returned by
            get_latency_bucket_index
        :type bucket_index: int
        :return: Name of the field on the metrics hash for the latency bucket
            counter
        :rtype: str
        '''
        return RedisMetricsCache._TIME_FIELD_TEMPLATE.format(
            operation=operation,
            bucket_index=bucket_index
        )

    def _get_all_buckets_time_fields(self, operation):
        '''
        Builds a list of all the fields in the metrics hash for the latency
        buckets for a given operation.
        :param operation: Name of the operation
        :type operation: str
        :return: List of field names
        :rtype: list
        '''
        return [
            self._get_time_field(operation, bucket)
            for bucket in range(0, len(BUCKETS))
        ]

    def _build_metrics_counter_data(self, count_metrics):
        '''
        Build metrics counter data in the format expected by the API from the
        contents of the cache.
        :param count_metrics: A dictionary of name/value counter metrics
        :param count_metrics: dict
        :return: A list of of counter metrics
        :rtype: list
        '''
        return [{'name': name, 'delta': delta}
                for name, delta in iteritems(count_metrics)]

    def _build_metrics_times_data(self, time_metrics):
        '''
        Build metrics times data in the format expected by the API from the
        contents of the cache.
        :param time_metrics: A dictionary of name/latencies time metrics
        :param time_metrics: dict
        :return: A list of of time metrics
        :rtype: list
        '''
        return [{'name': name, 'latencies': latencies}
                for name, latencies in iteritems(time_metrics)]

    def _build_metrics_gauge_data(self, gauge_metrics):
        '''
        Build metrics gauge data in the format expected by the API from the
        contents of the cache.
        :param gauge_metrics: A dictionary of name/value gauge metrics
        :param gauge_metrics: dict
        :return: A list of of gauge metrics
        :rtype: list
        '''
        return [{'name': name, 'value': value}
                for name, value in iteritems(gauge_metrics)]

    def _build_metrics_from_cache_response(self, response):
        '''
        Builds a dictionary with time, count and gauge metrics based on the
        result of calling fetch_all_and_clear (list of name/value pairs).
        Each entry in the dictionary is in the format accepted by the events
        API.
        :param response: Response given by the fetch_all_and_clear method
        :type response: lsit
        :return: Dictionary with time, count and gauge metrics
        :rtype: dict
        '''
        if response is None:
            return {'count': [], 'gauge': []}

        count = dict()
        gauge = dict()

        for field, value in zip(islice(response, 0, None, 2), islice(response, 1, None, 2)):
            count_match = re.match(
                RedisMetricsCache._get_count_field_re(), field
            )
            if count_match is not None:
                count[count_match.group('counter')] = value
                continue

            gauge_match = re.match(
                RedisMetricsCache._get_gauge_field_re(), field
            )
            if gauge_match is not None:
                gauge[gauge_match.group('gauge')] = value
                continue

        return {
            'count': self._build_metrics_counter_data(count),
            'gauge': self._build_metrics_gauge_data(gauge)
        }

    def increment_count(self, counter, delta=1):
        '''
        '''
        return self._redis.incr(self._get_count_key(counter), delta)

    def get_latency(self, operation):
        return [
            0 if count is None else count
            for count in (self._redis.get(self._get_latency_bucket_key(operation, bucket))
                          for bucket in range(0, len(BUCKETS)))]

    def get_latency_bucket_counter(self, operation, bucket_index):
        count = self._redis.get(
            self._get_latency_bucket_key(operation, bucket_index)
        )
        return int(count) if count is not None else 0

    def set_gauge(self, gauge, value):
        return self._redis.set(self._get_gauge_key(gauge), value)

    def set_latency_bucket_counter(self, operation, bucket_index, value):
        self._redis.set(
            self._get_latency_bucket_key(operation, bucket_index), value
        )

    def get_count(self, counter):
        count = self._redis.get(self._get_count_key(counter))
        return count if count else 0

    def set_count(self, counter, value):
        return self._redis.set(self._get_count_key(counter), value)

    def increment_latency_bucket_counter(self, operation, bucket_index, delta=1):
        self._redis.incr(
            self._get_latency_bucket_key(operation, bucket_index),
            delta
        )

    def get_gauge(self, gauge):
        return self._redis.get(self._get_gauge_key(gauge))


class RedisSplitParser(SplitParser):
    def __init__(self, segment_cache):
        '''
        A SplitParser implementation that registers the segments with the redis
        segment cache implementation upon parsing an IN_SEGMENT matcher.
        '''
        super(RedisSplitParser, self).__init__(None)
        self._segment_cache = segment_cache

    def _parse_split(self, split, block_until_ready=False):
        return RedisSplit(
            split['name'], split['seed'], split['killed'],
            split['defaultTreatment'], split['trafficTypeName'],
            split['status'], split['changeNumber'],
            segment_cache=self._segment_cache,
            algo=split.get('algo'),
            traffic_allocation=split.get('trafficAllocation'),
            traffic_allocation_seed=split.get('trafficAllocationSeed')
        )

    def _parse_matcher_in_segment(self, partial_split, matcher,
                                  block_until_ready=False, *args, **kwargs):
        matcher_data = self._get_matcher_attribute(
            'userDefinedSegmentMatcherData',
            matcher
        )
        segment = RedisSplitBasedSegment(
            matcher_data['segmentName'],
            partial_split
        )
        delegate = UserDefinedSegmentMatcher(segment)
        self._segment_cache.register_segment(delegate.segment.name)
        return delegate


class RedisSplit(Split):
    def __init__(self, name, seed, killed, default_treatment, traffic_type_name,
                 status, change_number, conditions=None, segment_cache=None,
                 algo=None, traffic_allocation=None,
                 traffic_allocation_seed=None):
        '''
        A split implementation that mantains a reference to the segment cache
        so segments can be easily pickled and unpickled.
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
        '''
        super(RedisSplit, self).__init__(
            name, seed, killed, default_treatment, traffic_type_name, status,
            change_number, conditions, algo, traffic_allocation,
            traffic_allocation_seed
        )

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
        '''
        A Segment that uses a reference to a RedisSplit redis' instance to check
        if a key is in a segment
        :param name: The name of the segment
        :type name: str
        :param split: A RedisSplit instance
        :type split: RedisSplit
        '''
        super(RedisSplitBasedSegment, self).__init__(name)
        self._split = split

    def contains(self, key):
        return self._split.segment_cache.is_in_segment(self.name, key)


def setup_instance_id(instance_id):
    '''
    Setup the correct parameters once the redis-client has been instantiated
    with a configuration from the client
    '''
    if instance_id:
        _GLOBAL_KEY_PARAMETERS['instance-id'] = instance_id
    else:
        _GLOBAL_KEY_PARAMETERS['instance-id'] = 'unknown'


def get_redis(config):
    '''
    Build a redis client based on the configuration.
    :param config: Dictionary with the contents of the config file.
    :type config: dict
    :return: A redis client
    '''
    setup_instance_id(config.get('instance-id'))
    if 'redisFactory' in config:
        redis_factory = import_from_string(
            config['redisFactory'], 'redisFactory'
        )
        return redis_factory()
    else:
        if 'redisSentinels' in config:
            return default_redis_sentinel_factory(config)
        else:
            return default_redis_factory(config)


def default_redis_factory(config):
    '''
    Default redis client factory.
    :param config: A dict with the Redis configuration parameters
    :type config: dict
    :return: A StrictRedis object using the provided config values
    :rtype: StrictRedis
    '''
    host = config.get('redisHost', 'localhost')
    port = config.get('redisPort', 6379)
    db = config.get('redisDb', 0)
    password = config.get('redisPassword', None)
    socket_timeout = config.get('redisSocketTimeout', None)
    socket_connect_timeout = config.get('redisSocketConnectTimeout', None)
    socket_keepalive = config.get('redisSocketKeepalive', None)
    socket_keepalive_options = config.get('redisSocketKeepaliveOptions', None)
    connection_pool = config.get('redisConnectionPool', None)
    unix_socket_path = config.get('redisUnixSocketPath', None)
    encoding = config.get('redisEncoding', 'utf-8')
    encoding_errors = config.get('redisEncodingErrors', 'strict')
    charset = config.get('redisCharset', None)
    errors = config.get('redisErrors', None)
    decode_responses = config.get('redisDecodeResponses', False)
    retry_on_timeout = config.get('redisRetryOnTimeout', False)
    ssl = config.get('redisSsl', False)
    ssl_keyfile = config.get('redisSslKeyfile', None)
    ssl_certfile = config.get('redisSslCertfile', None)
    ssl_cert_reqs = config.get('redisSslCertReqs', None)
    ssl_ca_certs = config.get('redisSslCaCerts', None)
    max_connections = config.get('redisMaxConnections', None)
    prefix = config.get('redisPrefix')

    redis = StrictRedis(
        host=host,
        port=port,
        db=db,
        password=password,
        socket_timeout=socket_timeout,
        socket_connect_timeout=socket_connect_timeout,
        socket_keepalive=socket_keepalive,
        socket_keepalive_options=socket_keepalive_options,
        connection_pool=connection_pool,
        unix_socket_path=unix_socket_path,
        encoding=encoding,
        encoding_errors=encoding_errors,
        charset=charset,
        errors=errors,
        decode_responses=decode_responses,
        retry_on_timeout=retry_on_timeout,
        ssl=ssl,
        ssl_keyfile=ssl_keyfile,
        ssl_certfile=ssl_certfile,
        ssl_cert_reqs=ssl_cert_reqs,
        ssl_ca_certs=ssl_ca_certs,
        max_connections=max_connections
    )
    return PrefixDecorator(redis, prefix=prefix)


def default_redis_sentinel_factory(config):
    '''
    Default redis client factory for sentinel mode.
    :param config: A dict with the Redis configuration parameters
    :type config: dict
    :return: A Sentinel object using the provided config values
    :rtype: Sentinel
    '''
    sentinels = config.get('redisSentinels')

    if (sentinels is None):
        raise SentinelConfigurationException('redisSentinels must be specified.')
    if (not isinstance(sentinels, list)):
        raise SentinelConfigurationException('Sentinels must be an array of elements in the form of'
                                             ' [(ip, port)].')
    if (len(sentinels) == 0):
        raise SentinelConfigurationException('It must be at least one sentinel.')
    if not all(isinstance(s, tuple) for s in sentinels):
        raise SentinelConfigurationException('Sentinels must respect the tuple structure'
                                             '[(ip, port)].')

    master_service = config.get('redisMasterService')

    if (master_service is None):
        raise SentinelConfigurationException('redisMasterService must be specified.')

    db = config.get('redisDb', 0)
    password = config.get('redisPassword', None)
    socket_timeout = config.get('redisSocketTimeout', None)
    socket_connect_timeout = config.get('redisSocketConnectTimeout', None)
    socket_keepalive = config.get('redisSocketKeepalive', None)
    socket_keepalive_options = config.get('redisSocketKeepaliveOptions', None)
    connection_pool = config.get('redisConnectionPool', None)
    unix_socket_path = config.get('redisUnixSocketPath', None)
    encoding = config.get('redisEncoding', 'utf-8')
    encoding_errors = config.get('redisEncodingErrors', 'strict')
    charset = config.get('redisCharset', None)
    errors = config.get('redisErrors', None)
    decode_responses = config.get('redisDecodeResponses', False)
    retry_on_timeout = config.get('redisRetryOnTimeout', False)
    ssl = config.get('redisSsl', False)
    ssl_keyfile = config.get('redisSslKeyfile', None)
    ssl_certfile = config.get('redisSslCertfile', None)
    ssl_cert_reqs = config.get('redisSslCertReqs', None)
    ssl_ca_certs = config.get('redisSslCaCerts', None)
    max_connections = config.get('redisMaxConnections', None)
    prefix = config.get('redisPrefix')

    sentinel = Sentinel(
        sentinels,
        0,
        {
            'db': db,
            'password': password,
            'socket_timeout': socket_timeout,
            'socket_connect_timeout': socket_connect_timeout,
            'socket_keepalive': socket_keepalive,
            'socket_keepalive_options': socket_keepalive_options,
            'connection_pool': connection_pool,
            'unix_socket_path': unix_socket_path,
            'encoding': encoding,
            'encoding_errors': encoding_errors,
            'charset': charset,
            'errors': errors,
            'decode_responses': decode_responses,
            'retry_on_timeout': retry_on_timeout,
            'ssl': ssl,
            'ssl_keyfile': ssl_keyfile,
            'ssl_certfile': ssl_certfile,
            'ssl_cert_reqs': ssl_cert_reqs,
            'ssl_ca_certs': ssl_ca_certs,
            'max_connections': max_connections
        }
    )

    redis = sentinel.master_for(master_service)
    return PrefixDecorator(redis, prefix=prefix)
