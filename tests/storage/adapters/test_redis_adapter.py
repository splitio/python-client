"""Redis storage adapter test module."""

import pytest
from splitio.storage.adapters import redis
from redis import StrictRedis, Redis
from redis.sentinel import Sentinel


class RedisStorageAdapterTests(object):
    """Redis storage adapter test cases."""

    def test_forwarding(self, mocker):
        """Test that all redis functions forward prefix appropriately."""
        redis_mock = mocker.Mock(StrictRedis)
        adapter = redis.RedisAdapter(redis_mock, 'some_prefix')

        redis_mock.keys.return_value = ['some_prefix.key1', 'some_prefix.key2']
        adapter.keys('*')
        assert redis_mock.keys.mock_calls[0] == mocker.call('some_prefix.*')

        adapter.set('key1', 'value1')
        assert redis_mock.set.mock_calls[0] == mocker.call('some_prefix.key1', 'value1')

        adapter.get('some_key')
        assert redis_mock.get.mock_calls[0] == mocker.call('some_prefix.some_key')

        adapter.setex('some_key', 123, 'some_value')
        assert redis_mock.setex.mock_calls[0] == mocker.call('some_prefix.some_key', 123, 'some_value')

        adapter.delete('some_key')
        assert redis_mock.delete.mock_calls[0] == mocker.call('some_prefix.some_key')

        redis_mock.mget.return_value = ['value1', 'value2', 'value3']
        adapter.mget(['key1', 'key2', 'key3'])
        assert redis_mock.mget.mock_calls[0] == mocker.call(['some_prefix.key1', 'some_prefix.key2', 'some_prefix.key3'])

        adapter.sadd('s1', 'value1', 'value2')
        assert redis_mock.sadd.mock_calls[0] == mocker.call('some_prefix.s1', 'value1', 'value2')

        adapter.srem('s1', 'value1', 'value2')
        assert redis_mock.srem.mock_calls[0] == mocker.call('some_prefix.s1', 'value1', 'value2')

        adapter.sismember('s1', 'value1')
        assert redis_mock.sismember.mock_calls[0] == mocker.call('some_prefix.s1', 'value1')

        adapter.eval('script', 3, 'key1', 'key2', 'key3')
        assert redis_mock.eval.mock_calls[0] == mocker.call('script', 3, 'some_prefix.key1', 'some_prefix.key2', 'some_prefix.key3')

        adapter.hset('key1', 'name', 'value')
        assert redis_mock.hset.mock_calls[0] == mocker.call('some_prefix.key1', 'name', 'value')

        adapter.hget('key1', 'name')
        assert redis_mock.hget.mock_calls[0] == mocker.call('some_prefix.key1', 'name')

        adapter.incr('key1')
        assert redis_mock.incr.mock_calls[0] == mocker.call('some_prefix.key1', 1)

        adapter.getset('key1', 'new_value')
        assert redis_mock.getset.mock_calls[0] == mocker.call('some_prefix.key1', 'new_value')

        adapter.rpush('key1', 'value1', 'value2')
        assert redis_mock.rpush.mock_calls[0] == mocker.call('some_prefix.key1', 'value1', 'value2')

        adapter.expire('key1', 10)
        assert redis_mock.expire.mock_calls[0] == mocker.call('some_prefix.key1', 10)

        adapter.rpop('key1')
        assert redis_mock.rpop.mock_calls[0] == mocker.call('some_prefix.key1')

        adapter.ttl('key1')
        assert redis_mock.ttl.mock_calls[0] == mocker.call('some_prefix.key1')

    def test_adapter_building(self, mocker):
        """Test buildin different types of client according to parameters received."""
        strict_redis_mock = mocker.Mock(spec=StrictRedis)
        sentinel_mock = mocker.Mock(spec=Sentinel)
        mocker.patch('splitio.storage.adapters.redis.StrictRedis', new=strict_redis_mock)
        mocker.patch('splitio.storage.adapters.redis.Sentinel', new=sentinel_mock)

        config = {
            'redisHost': 'some_host',
            'redisPort': 1234,
            'redisDb': 0,
            'redisPassword': 'some_password',
            'redisSocketTimeout': 123,
            'redisSocketConnectTimeout': 456,
            'redisSocketKeepalive': 789,
            'redisSocketKeepaliveOptions': 10,
            'redisConnectionPool': 20,
            'redisUnixSocketPath': '/tmp/socket',
            'redisEncoding': 'utf-8',
            'redisEncodingErrors': 'strict',
            'redisErrors': 'abc',
            'redisDecodeResponses': True,
            'redisRetryOnTimeout': True,
            'redisSsl': True,
            'redisSslKeyfile': '/ssl.cert',
            'redisSslCertfile': '/ssl2.cert',
            'redisSslCertReqs': 'abc',
            'redisSslCaCerts': 'def',
            'redisMaxConnections': 5,
            'redisPrefix': 'some_prefix'
        }

        redis.build(config)
        assert strict_redis_mock.mock_calls[0] == mocker.call(
            host='some_host',
            port=1234,
            db=0,
            password='some_password',
            socket_timeout=123,
            socket_connect_timeout=456,
            socket_keepalive=789,
            socket_keepalive_options=10,
            connection_pool=20,
            unix_socket_path='/tmp/socket',
            encoding='utf-8',
            encoding_errors='strict',
            errors='abc',
            decode_responses=True,
            retry_on_timeout=True,
            ssl=True,
            ssl_keyfile='/ssl.cert',
            ssl_certfile='/ssl2.cert',
            ssl_cert_reqs='abc',
            ssl_ca_certs='def',
            max_connections=5
        )

        config = {
            'redisSentinels': [('123.123.123.123', 1), ('456.456.456.456', 2), ('789.789.789.789', 3)],
            'redisMasterService': 'some_master',
            'redisDb': 0,
            'redisPassword': 'some_password',
            'redisSocketTimeout': 123,
            'redisSocketConnectTimeout': 456,
            'redisSocketKeepalive': 789,
            'redisSocketKeepaliveOptions': 10,
            'redisConnectionPool': 20,
            'redisUnixSocketPath': '/tmp/socket',
            'redisEncoding': 'utf-8',
            'redisEncodingErrors': 'strict',
            'redisErrors': 'abc',
            'redisDecodeResponses': True,
            'redisRetryOnTimeout': True,
            'redisSsl': False,
            'redisSslKeyfile': '/ssl.cert',
            'redisSslCertfile': '/ssl2.cert',
            'redisSslCertReqs': 'abc',
            'redisSslCaCerts': 'def',
            'redisMaxConnections': 5,
            'redisPrefix': 'some_prefix'
        }

        redis.build(config)
        assert sentinel_mock.mock_calls[0] == mocker.call(
            [('123.123.123.123', 1), ('456.456.456.456', 2), ('789.789.789.789', 3)],
            db=0,
            password='some_password',
            socket_timeout=123,
            socket_connect_timeout=456,
            socket_keepalive=789,
            socket_keepalive_options=10,
            connection_pool=20,
            encoding='utf-8',
            encoding_errors='strict',
            decode_responses=True,
            retry_on_timeout=True,
            max_connections=5
        )

    def test_sentinel_ssl_fails(self):
        """Test that SSL/TLS & Sentinel don't return a valid client."""
        with pytest.raises(redis.SentinelConfigurationException) as exc:
            redis.build({
                'redisSentinels': ['a', 'b'],
                'redisSsl': True,
            })


class RedisPipelineAdapterTests(object):
    """Redis pipelined adapter test cases."""

    def test_forwarding(self, mocker):
        """Test that all redis functions forward prefix appropriately."""
        redis_mock = mocker.Mock(StrictRedis)
        redis_mock_2 = mocker.Mock(Redis)
        redis_mock.pipeline.return_value = redis_mock_2
        prefix_helper = redis.PrefixHelper('some_prefix')
        adapter = redis.RedisPipelineAdapter(redis_mock, prefix_helper)

        adapter.rpush('key1', 'value1', 'value2')
        assert redis_mock_2.rpush.mock_calls[0] == mocker.call('some_prefix.key1', 'value1', 'value2')

        adapter.incr('key1')
        assert redis_mock_2.incr.mock_calls[0] == mocker.call('some_prefix.key1', 1)
