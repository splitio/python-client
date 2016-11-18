from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from os.path import dirname, join
from unittest import TestCase
from json import load

from splitio.redis_support import (RedisSplitCache, RedisSegmentCache, get_redis)
from splitio.splits import (JSONFileSplitFetcher, SplitParser)
from splitio.segments import JSONFileSegmentFetcher


class CacheInterfacesTests(TestCase):
    def setUp(self):
        self._segment_changes_file_name = join(dirname(__file__), 'segmentChanges.json')
        self._split_changes_file_name = join(dirname(__file__), 'splitChanges.json')

        self._redis = get_redis(dict()) #default config
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



    def testSegmentCacheInterface(self):
        with open(self._segment_changes_file_name) as f:
            self._json = load(f)
            segment_name = self._json['name']
            segment_change_number = self._json['till']
            segment_keys = self._json['added']

        self._redis_segment_cache.set_change_number(segment_name, segment_change_number)
        self.assertEqual(segment_change_number, self._redis_segment_cache.get_change_number(segment_name))

        self._redis_segment_cache.add_keys_to_segment(segment_name, segment_keys)
        self.assertTrue(self._redis_segment_cache.is_in_segment(segment_name, segment_keys[0]))

        self._redis_segment_cache.remove_keys_from_segment(segment_name, [segment_keys[0]])
        self.assertFalse(self._redis_segment_cache.is_in_segment(segment_name, segment_keys[0]))
