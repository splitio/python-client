from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase

from splitio import get_factory
from splitio.redis_support import SentinelConfigurationException


class RedisSentinelFactory(TestCase):
    def test_redis_factory_with_empty_sentinels_array(self):
        config = {
            'redisDb': 0,
            'redisPrefix': 'test',
            'redisSentinels': [],
            'redisMasterService': 'mymaster',
            'redisSocketTimeout': 3
        }

        with self.assertRaises(SentinelConfigurationException):
            get_factory('abc', config=config)

    def test_redis_factory_with_wrong_type_in_sentinels_array(self):
        config = {
            'redisDb': 0,
            'redisPrefix': 'test',
            'redisSentinels': 'abc',
            'redisMasterService': 'mymaster',
            'redisSocketTimeout': 3
        }

        with self.assertRaises(SentinelConfigurationException):
            get_factory('abc', config=config)

    def test_redis_factory_with_wrong_data_in_sentinels_array(self):
        config = {
            'redisDb': 0,
            'redisPrefix': 'test',
            'redisSentinels': ['asdasd'],
            'redisMasterService': 'mymaster',
            'redisSocketTimeout': 3
        }

        with self.assertRaises(SentinelConfigurationException):
            get_factory('abc', config=config)

    def test_redis_factory_with_without_master_service(self):
        config = {
            'redisDb': 0,
            'redisPrefix': 'test',
            'redisSentinels': [('test', 1234)],
            'redisSocketTimeout': 3
        }

        with self.assertRaises(SentinelConfigurationException):
            get_factory('abc', config=config)
