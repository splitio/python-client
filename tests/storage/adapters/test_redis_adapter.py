"""Redis storage adapter test module."""

import pytest
from redis.asyncio.client import Redis as aioredis
from splitio.storage.adapters import redis
from splitio.storage.adapters.redis import _build_default_client_async, _build_sentinel_client_async
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

        adapter.hincrby('key1', 'name1')
        assert redis_mock.hincrby.mock_calls[0] == mocker.call('some_prefix.key1', 'name1', 1)

        adapter.hincrby('key1', 'name1', 5)
        assert redis_mock.hincrby.mock_calls[1] == mocker.call('some_prefix.key1', 'name1', 5)

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
            'redisUsername': 'redis_user',
            'redisPassword': 'some_password',
            'redisSocketTimeout': 123,
            'redisSocketConnectTimeout': 456,
            'redisSocketKeepalive': 789,
            'redisSocketKeepaliveOptions': 10,
            'redisConnectionPool': 20,
            'redisUnixSocketPath': '/tmp/socket',
            'redisEncoding': 'utf-8',
            'redisEncodingErrors': 'strict',
#            'redisErrors': 'abc',
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
            username='redis_user',
            password='some_password',
            socket_timeout=123,
            socket_connect_timeout=456,
            socket_keepalive=789,
            socket_keepalive_options=10,
            connection_pool=20,
            unix_socket_path='/tmp/socket',
            encoding='utf-8',
            encoding_errors='strict',
#            errors='abc',
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
            'redisUsername': 'redis_user',
            'redisPassword': 'some_password',
            'redisSocketTimeout': 123,
            'redisSocketConnectTimeout': 456,
            'redisSocketKeepalive': 789,
            'redisSocketKeepaliveOptions': 10,
            'redisConnectionPool': 20,
            'redisUnixSocketPath': '/tmp/socket',
            'redisEncoding': 'utf-8',
            'redisEncodingErrors': 'strict',
#            'redisErrors': 'abc',
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
            username='redis_user',
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


class RedisStorageAdapterAsyncTests(object):
    """Redis storage adapter test cases."""

    @pytest.mark.asyncio
    async def test_forwarding(self, mocker):
        """Test that all redis functions forward prefix appropriately."""
        redis_mock = await aioredis.from_url("redis://localhost")
        adapter = redis.RedisAdapterAsync(redis_mock, 'some_prefix')

        self.arg = None
        async def keys(sel, args):
            self.arg = args
            return ['some_prefix.key1', 'some_prefix.key2']
        mocker.patch('redis.asyncio.client.Redis.keys', new=keys)
        await adapter.keys('*')
        assert self.arg == 'some_prefix.*'

        self.key = None
        self.value = None
        async def set(sel, key, value):
            self.key = key
            self.value = value
        mocker.patch('redis.asyncio.client.Redis.set', new=set)
        await adapter.set('key1', 'value1')
        assert self.key == 'some_prefix.key1'
        assert self.value == 'value1'

        self.key = None
        async def get(sel, key):
            self.key = key
            return 'value1'
        mocker.patch('redis.asyncio.client.Redis.get', new=get)
        await adapter.get('some_key')
        assert self.key == 'some_prefix.some_key'

        self.key = None
        self.value = None
        self.exp = None
        async def setex(sel, key, exp, value):
            self.key = key
            self.value = value
            self.exp = exp
        mocker.patch('redis.asyncio.client.Redis.setex', new=setex)
        await adapter.setex('some_key', 123, 'some_value')
        assert self.key == 'some_prefix.some_key'
        assert self.exp == 123
        assert self.value == 'some_value'

        self.key = None
        async def delete(sel, key):
            self.key = key
        mocker.patch('redis.asyncio.client.Redis.delete', new=delete)
        await adapter.delete('some_key')
        assert self.key == 'some_prefix.some_key'

        self.keys = None
        async def mget(sel, keys):
            self.keys = keys
            return ['value1', 'value2', 'value3']
        mocker.patch('redis.asyncio.client.Redis.mget', new=mget)
        await adapter.mget(['key1', 'key2', 'key3'])
        assert self.keys == ['some_prefix.key1', 'some_prefix.key2', 'some_prefix.key3']

        self.key = None
        self.value = None
        self.value2 = None
        async def sadd(sel, key, value, value2):
            self.key = key
            self.value = value
            self.value2 = value2
        mocker.patch('redis.asyncio.client.Redis.sadd', new=sadd)
        await adapter.sadd('s1', 'value1', 'value2')
        assert self.key == 'some_prefix.s1'
        assert self.value == 'value1'
        assert self.value2 == 'value2'

        self.key = None
        self.value = None
        self.value2 = None
        async def srem(sel, key, value, value2):
            self.key = key
            self.value = value
            self.value2 = value2
        mocker.patch('redis.asyncio.client.Redis.srem', new=srem)
        await adapter.srem('s1', 'value1', 'value2')
        assert self.key == 'some_prefix.s1'
        assert self.value == 'value1'
        assert self.value2 == 'value2'

        self.key = None
        self.value = None
        async def sismember(sel, key, value):
            self.key = key
            self.value = value
        mocker.patch('redis.asyncio.client.Redis.sismember', new=sismember)
        await adapter.sismember('s1', 'value1')
        assert self.key == 'some_prefix.s1'
        assert self.value == 'value1'

        self.key = None
        self.key2 = None
        self.key3 = None
        self.script = None
        self.value = None
        async def eval(sel, script, value, key, key2, key3):
            self.key = key
            self.key2 = key2
            self.key3 = key3
            self.script = script
            self.value = value
        mocker.patch('redis.asyncio.client.Redis.eval', new=eval)
        await adapter.eval('script', 3, 'key1', 'key2', 'key3')
        assert self.script == 'script'
        assert self.value == 3
        assert self.key == 'some_prefix.key1'
        assert self.key2 == 'some_prefix.key2'
        assert self.key3 == 'some_prefix.key3'

        self.key = None
        self.value = None
        self.name = None
        async def hset(sel, key, name, value):
            self.key = key
            self.value = value
            self.name = name
        mocker.patch('redis.asyncio.client.Redis.hset', new=hset)
        await adapter.hset('key1', 'name', 'value')
        assert self.key == 'some_prefix.key1'
        assert self.name == 'name'
        assert self.value == 'value'

        self.key = None
        self.name = None
        async def hget(sel, key, name):
            self.key = key
            self.name = name
        mocker.patch('redis.asyncio.client.Redis.hget', new=hget)
        await adapter.hget('key1', 'name')
        assert self.key == 'some_prefix.key1'
        assert self.name == 'name'

        self.key = None
        self.value = None
        async def incr(sel, key, value):
            self.key = key
            self.value = value
        mocker.patch('redis.asyncio.client.Redis.incr', new=incr)
        await adapter.incr('key1')
        assert self.key == 'some_prefix.key1'
        assert self.value == 1

        self.key = None
        self.value = None
        self.name = None
        async def hincrby(sel, key, name, value):
            self.key = key
            self.value = value
            self.name = name
        mocker.patch('redis.asyncio.client.Redis.hincrby', new=hincrby)
        await adapter.hincrby('key1', 'name1')
        assert self.key == 'some_prefix.key1'
        assert self.name == 'name1'
        assert self.value == 1

        await adapter.hincrby('key1', 'name1', 5)
        assert self.key == 'some_prefix.key1'
        assert self.name == 'name1'
        assert self.value == 5

        self.key = None
        self.value = None
        async def getset(sel, key, value):
            self.key = key
            self.value = value
        mocker.patch('redis.asyncio.client.Redis.getset', new=getset)
        await adapter.getset('key1', 'new_value')
        assert self.key == 'some_prefix.key1'
        assert self.value == 'new_value'

        self.key = None
        self.value = None
        self.value2 = None
        async def rpush(sel, key, value, value2):
            self.key = key
            self.value = value
            self.value2 = value2
        mocker.patch('redis.asyncio.client.Redis.rpush', new=rpush)
        await adapter.rpush('key1', 'value1', 'value2')
        assert self.key == 'some_prefix.key1'
        assert self.value == 'value1'
        assert self.value2 == 'value2'

        self.key = None
        self.exp = None
        async def expire(sel, key, exp):
            self.key = key
            self.exp = exp
        mocker.patch('redis.asyncio.client.Redis.expire', new=expire)
        await adapter.expire('key1', 10)
        assert self.key == 'some_prefix.key1'
        assert self.exp == 10

        self.key = None
        async def rpop(sel, key):
            self.key = key
        mocker.patch('redis.asyncio.client.Redis.rpop', new=rpop)
        await adapter.rpop('key1')
        assert self.key == 'some_prefix.key1'

        self.key = None
        async def ttl(sel, key):
            self.key = key
        mocker.patch('redis.asyncio.client.Redis.ttl', new=ttl)
        await adapter.ttl('key1')
        assert self.key == 'some_prefix.key1'

    @pytest.mark.asyncio
    async def test_adapter_building(self, mocker):
        """Test buildin different types of client according to parameters received."""

        config = {
            'redisHost': 'some_host',
            'redisPort': 1234,
            'redisDb': 0,
            'redisPassword': 'some_password',
            'redisSocketTimeout': 123,
            'redisSocketKeepalive': 789,
            'redisSocketKeepaliveOptions': 10,
            'redisUnixSocketPath': '/tmp/socket',
            'redisEncoding': 'utf-8',
            'redisEncodingErrors': 'strict',
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

        def redis_init(se, connection_pool,
                socket_connect_timeout,
                socket_keepalive,
                socket_keepalive_options,
                unix_socket_path,
                encoding_errors,
                retry_on_timeout,
                ssl,
                ssl_keyfile,
                ssl_certfile,
                ssl_cert_reqs,
                ssl_ca_certs):
            self.connection_pool=connection_pool
            self.socket_connect_timeout=socket_connect_timeout
            self.socket_keepalive=socket_keepalive
            self.socket_keepalive_options=socket_keepalive_options
            self.unix_socket_path=unix_socket_path
            self.encoding_errors=encoding_errors
            self.retry_on_timeout=retry_on_timeout
            self.ssl=ssl
            self.ssl_keyfile=ssl_keyfile
            self.ssl_certfile=ssl_certfile
            self.ssl_cert_reqs=ssl_cert_reqs
            self.ssl_ca_certs=ssl_ca_certs
        mocker.patch('redis.asyncio.client.Redis.__init__', new=redis_init)

        redis_mock = await _build_default_client_async(config)

        assert self.connection_pool.connection_kwargs['host'] == 'some_host'
        assert self.connection_pool.connection_kwargs['port'] == 1234
        assert self.connection_pool.connection_kwargs['db'] == 0
        assert self.connection_pool.connection_kwargs['password'] == 'some_password'
        assert self.connection_pool.connection_kwargs['encoding'] == 'utf-8'
        assert self.connection_pool.connection_kwargs['decode_responses'] == True

        assert self.socket_keepalive == 789
        assert self.socket_keepalive_options == 10
        assert self.unix_socket_path == '/tmp/socket'
        assert self.encoding_errors == 'strict'
        assert self.retry_on_timeout == True
        assert self.ssl == True
        assert self.ssl_keyfile == '/ssl.cert'
        assert self.ssl_certfile == '/ssl2.cert'
        assert self.ssl_cert_reqs == 'abc'
        assert self.ssl_ca_certs == 'def'

        def create_sentinel(se,
                sentinels,
                db,
                password,
                encoding,
                max_connections,
                encoding_errors,
                decode_responses,
                connection_pool,
                socket_connect_timeout):
            self.sentinels=sentinels
            self.db=db
            self.password=password
            self.encoding=encoding
            self.max_connections=max_connections
            self.encoding_errors=encoding_errors,
            self.decode_responses=decode_responses,
            self.connection_pool=connection_pool,
            self.socket_connect_timeout=socket_connect_timeout
        mocker.patch('redis.asyncio.sentinel.Sentinel.__init__', new=create_sentinel)

        def master_for(se,
            master_service,
            socket_timeout,
            socket_keepalive,
            socket_keepalive_options,
            encoding_errors,
            retry_on_timeout,
            ssl):
            self.master_service = master_service,
            self.socket_timeout = socket_timeout,
            self.socket_keepalive = socket_keepalive,
            self.socket_keepalive_options = socket_keepalive_options,
            self.encoding_errors = encoding_errors,
            self.retry_on_timeout = retry_on_timeout,
            self.ssl = ssl
        mocker.patch('redis.asyncio.sentinel.Sentinel.master_for', new=master_for)

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
            'redisMaxConnections': 5,
            'redisPrefix': 'some_prefix'
        }
        await _build_sentinel_client_async(config)
        assert self.sentinels == [('123.123.123.123', 1), ('456.456.456.456', 2), ('789.789.789.789', 3)]
        assert self.db == 0
        assert self.password == 'some_password'
        assert self.encoding == 'utf-8'
        assert self.max_connections == 5
        assert self.ssl == False
        assert self.master_service == ('some_master',)
        assert self.socket_timeout == (123,)
        assert self.socket_keepalive == (789,)
        assert self.socket_keepalive_options == (10,)
        assert self.encoding_errors == ('strict',)
        assert self.retry_on_timeout == (True,)


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

        adapter.hincrby('key1', 'name1')
        assert redis_mock_2.hincrby.mock_calls[0] == mocker.call('some_prefix.key1', 'name1', 1)

        adapter.hincrby('key1', 'name1', 5)
        assert redis_mock_2.hincrby.mock_calls[1] == mocker.call('some_prefix.key1', 'name1', 5)


class RedisPipelineAdapterAsyncTests(object):
    """Redis pipelined adapter test cases."""

    @pytest.mark.asyncio
    async def test_forwarding(self, mocker):
        """Test that all redis functions forward prefix appropriately."""
        redis_mock = await aioredis.from_url("redis://localhost")
        prefix_helper = redis.PrefixHelper('some_prefix')
        adapter = redis.RedisPipelineAdapterAsync(redis_mock, prefix_helper)

        self.key = None
        self.value = None
        self.value2 = None
        def rpush(sel, key, value, value2):
            self.key = key
            self.value = value
            self.value2 = value2
        mocker.patch('redis.asyncio.client.Pipeline.rpush', new=rpush)
        adapter.rpush('key1', 'value1', 'value2')
        assert self.key == 'some_prefix.key1'
        assert self.value == 'value1'
        assert self.value2 == 'value2'

        self.key = None
        self.value = None
        def incr(sel, key, value):
            self.key = key
            self.value = value
        mocker.patch('redis.asyncio.client.Pipeline.incr', new=incr)
        adapter.incr('key1')
        assert self.key == 'some_prefix.key1'
        assert self.value == 1

        self.key = None
        self.value = None
        self.name = None
        def hincrby(sel, key, name, value):
            self.key = key
            self.value = value
            self.name = name
        mocker.patch('redis.asyncio.client.Pipeline.hincrby', new=hincrby)
        adapter.hincrby('key1', 'name1')
        assert self.key == 'some_prefix.key1'
        assert self.name == 'name1'
        assert self.value == 1

        adapter.hincrby('key1', 'name1', 5)
        assert self.key == 'some_prefix.key1'
        assert self.name == 'name1'
        assert self.value == 5

        self.called = False
        async def execute(*_):
            self.called = True
        mocker.patch('redis.asyncio.client.Pipeline.execute', new=execute)
        await adapter.execute()
        assert self.called
