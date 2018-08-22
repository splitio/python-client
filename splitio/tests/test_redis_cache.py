from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from os.path import dirname, join
from unittest import TestCase
from splitio.tests.utils import MockUtilsMixin
from json import load

from splitio.redis_support import (RedisSplitCache, RedisSegmentCache, get_redis)
from redis import StrictRedis
from splitio.clients import Client
from splitio.prefix_decorator import PrefixDecorator
from splitio.brokers import RedisBroker

class CacheInterfacesTests(TestCase):
    def setUp(self):
        self._segment_changes_file_name = join(dirname(__file__), 'segmentChanges.json')
        self._split_changes_file_name = join(dirname(__file__), 'splitChanges.json')

        self._redis = get_redis({'redisPrefix': 'test'})
        self._redis_split_cache = RedisSplitCache(self._redis)
        self._redis_segment_cache = RedisSegmentCache(self._redis)

    def test_split_cache_interface(self):

        with open(self._split_changes_file_name) as f:
            self._json = load(f)
            split_definition = self._json['splits'][0]
            split_name = split_definition['name']

        #Add and get Split
        self._redis_split_cache.add_split(split_name, split_definition)
        self.assertEqual(split_definition['name'], self._redis_split_cache.get_split(split_name).name)
        self.assertEqual(split_definition['killed'], self._redis_split_cache.get_split(split_name).killed)
        self.assertEqual(split_definition['seed'], self._redis_split_cache.get_split(split_name).seed)

        #Remove Split
        self._redis_split_cache.remove_split(split_name)
        self.assertIsNone(self._redis_split_cache.get_split(split_name))

        #Change Number
        self._redis_split_cache.set_change_number(1212)
        self.assertEqual(1212, self._redis_split_cache.get_change_number())

    # @TODO This tests should be removed regarding that this is not supported by redis now.
    # def testSegmentCacheInterface(self):
    #     with open(self._segment_changes_file_name) as f:
    #         self._json = load(f)
    #         segment_name = self._json['name']
    #         segment_change_number = self._json['till']
    #         segment_keys = self._json['added']

    #     self._redis_segment_cache.set_change_number(segment_name, segment_change_number)
    #     self.assertEqual(segment_change_number, self._redis_segment_cache.get_change_number(segment_name))

    #     self._redis_segment_cache.add_keys_to_segment(segment_name, segment_keys)
    #     self.assertTrue(self._redis_segment_cache.is_in_segment(segment_name, segment_keys[0]))

    #     self._redis_segment_cache.remove_keys_from_segment(segment_name, [segment_keys[0]])
    #     self.assertFalse(self._redis_segment_cache.is_in_segment(segment_name, segment_keys[0]))

class ReadOnlyRedisMock(PrefixDecorator):

    def __init__(self, *args, **kwargs):
        """
        Bases on PrefixDecorator.
        """
        PrefixDecorator.__init__(self, *args, **kwargs)

    def sadd(self, name, *values):
        """
        Decorated sadd to simulate read only exception error.
        """
        raise Exception('ReadOnlyError')

class RedisReadOnlyTest(TestCase, MockUtilsMixin):
    def setUp(self):
        self._split_changes_file_name = join(dirname(__file__), 'splitChangesReadOnly.json')
        
        with open(self._split_changes_file_name) as f:
            self._json = load(f)
            split_definition = self._json['splits'][0]
            split_name = split_definition['name']

        self._redis = get_redis({'redisPrefix': 'test'})

        self._mocked_redis = ReadOnlyRedisMock(self._redis)
        self._redis_split_cache = RedisSplitCache(self._redis)
        self._redis_split_cache.add_split(split_name, split_definition)
        self._client = Client(RedisBroker(self._mocked_redis))

        self._impression = mock.MagicMock()
        self._start = mock.MagicMock()
        self._operation = mock.MagicMock()

    def test_redis_read_only_mode(self):
        self.assertEqual(self._client.get_treatment('valid', 'test_read_only_1'), 'on')
        self.assertEqual(self._client.get_treatment('invalid', 'test_read_only_1'), 'off')
        self.assertEqual(self._client.get_treatment('valid', 'test_read_only_1_invalid'), 'control')