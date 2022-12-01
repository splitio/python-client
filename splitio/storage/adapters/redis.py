"""Redis client wrapper with prefix support."""
from builtins import str

try:
    from redis import StrictRedis
    from redis.sentinel import Sentinel
    from redis.exceptions import RedisError
except ImportError:
    def missing_redis_dependencies(*_, **__):
        """Fail if missing dependencies are used."""
        raise NotImplementedError(
            'Missing Redis support dependencies. '
            'Please use `pip install splitio_client[redis]` to install the sdk with redis support'
        )
    StrictRedis = Sentinel = missing_redis_dependencies


class RedisAdapterException(Exception):
    """Exception to be thrown when a redis command fails with an exception."""

    def __init__(self, message, original_exception=None):
        """
        Exception constructor.

        :param message: Custom exception message.
        :type message: str
        :param original_exception: Original exception object.
        :type original_exception: Exception
        """
        Exception.__init__(self, message)
        self._original_exception = original_exception

    @property
    def original_exception(self):
        """Return original exception."""
        return self._original_exception


class SentinelConfigurationException(Exception):
    """Exception to be raised when sentinel config options are incorrect."""

    pass


class PrefixHelper(object):
    """PrefixHelper generator."""

    def __init__(self, prefix=None):
        """
        Class constructor.

        :param prefix: User prefix to add.
        """
        self._prefix = prefix

    def add_prefix(self, k):
        """
        Add a prefix to the contents of k.

        :param k: single (string).
        :returns: Key(s) with prefix if applicable
        """
        if self._prefix:
            if isinstance(k, str):
                return '{prefix}.{key}'.format(prefix=self._prefix, key=k)
            elif isinstance(k, list) and k:
                if isinstance(k[0], bytes):
                    return [
                        '{prefix}.{key}'.format(prefix=self._prefix, key=key.decode("utf8"))
                        for key in k
                    ]
                elif isinstance(k[0], str):
                    return [
                        '{prefix}.{key}'.format(prefix=self._prefix, key=key)
                        for key in k
                    ]
        else:
            return k

        raise RedisAdapterException(
            "Cannot append prefix correctly. Wrong type for key(s) provided"
        )

    def remove_prefix(self, k):
        """
        Remove the user prefix from a key before handling it back to the requester.

        Similar to _add_prefix, this class will handle single strings as well
        as lists. If no _prefix is set, the original key/keys will be returned.

        :param k: key(s) whose prefix will be removed.
        :returns: prefix-less key(s)
        """
        if self._prefix:
            if isinstance(k, str):
                return k[len(self._prefix)+1:]
            elif isinstance(k, list):
                return [key[len(self._prefix)+1:] for key in k]
        else:
            return k

        raise RedisAdapterException(
            "Cannot remove prefix correctly. Wrong type for key(s) provided"
        )


class RedisAdapter(object):  # pylint: disable=too-many-public-methods
    """
    Instance decorator for Redis clients such as StrictRedis.

    Adds an extra layer handling addition/removal of user prefix when handling
    keys
    """

    def __init__(self, decorated, prefix=None):
        """
        Store the user prefix and the redis client instance.

        :param decorated: Instance of redis cache client to decorate.
        :param prefix: User prefix to add.
        """
        self._decorated = decorated
        self._prefix_helper = PrefixHelper(prefix)

    # Below starts a list of methods that implement the interface of a standard
    # redis client.

    def keys(self, pattern):
        """Mimic original redis function but using user custom prefix."""
        try:
            return [
                key
                for key in self._prefix_helper.remove_prefix(self._decorated.keys(self._prefix_helper.add_prefix(pattern)))
            ]
        except RedisError as exc:
            raise RedisAdapterException('Failed to execute keys operation') from exc

    def set(self, name, value, *args, **kwargs):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.set(
                self._prefix_helper.add_prefix(name), value, *args, **kwargs
            )
        except RedisError as exc:
            raise RedisAdapterException('Failed to execute set operation') from exc

    def get(self, name):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.get(self._prefix_helper.add_prefix(name))
        except RedisError as exc:
            raise RedisAdapterException('Error executing get operation') from exc

    def setex(self, name, time, value):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.setex(self._prefix_helper.add_prefix(name), time, value)
        except RedisError as exc:
            raise RedisAdapterException('Error executing setex operation') from exc

    def delete(self, *names):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.delete(*self._prefix_helper.add_prefix(list(names)))
        except RedisError as exc:
            raise RedisAdapterException('Error executing delete operation') from exc

    def exists(self, name):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.exists(self._prefix_helper.add_prefix(name))
        except RedisError as exc:
            raise RedisAdapterException('Error executing exists operation') from exc

    def lrange(self, key, start, end):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.lrange(self._prefix_helper.add_prefix(key), start, end)
        except RedisError as exc:
            raise RedisAdapterException('Error executing exists operation') from exc

    def mget(self, names):
        """Mimic original redis function but using user custom prefix."""
        try:
            return [
                item
                for item in self._decorated.mget(self._prefix_helper.add_prefix(names))
            ]
        except RedisError as exc:
            raise RedisAdapterException('Error executing mget operation') from exc

    def smembers(self, name):
        """Mimic original redis function but using user custom prefix."""
        try:
            return [
                item
                for item in self._decorated.smembers(self._prefix_helper.add_prefix(name))
            ]
        except RedisError as exc:
            raise RedisAdapterException('Error executing smembers operation') from exc

    def sadd(self, name, *values):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.sadd(self._prefix_helper.add_prefix(name), *values)
        except RedisError as exc:
            raise RedisAdapterException('Error executing sadd operation') from exc

    def srem(self, name, *values):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.srem(self._prefix_helper.add_prefix(name), *values)
        except RedisError as exc:
            raise RedisAdapterException('Error executing srem operation') from exc

    def sismember(self, name, value):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.sismember(self._prefix_helper.add_prefix(name), value)
        except RedisError as exc:
            raise RedisAdapterException('Error executing sismember operation') from exc

    def eval(self, script, number_of_keys, *keys):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.eval(script, number_of_keys, *self._prefix_helper.add_prefix(list(keys)))
        except RedisError as exc:
            raise RedisAdapterException('Error executing eval operation') from exc

    def hset(self, name, key, value):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.hset(self._prefix_helper.add_prefix(name), key, value)
        except RedisError as exc:
            raise RedisAdapterException('Error executing hset operation') from exc

    def hget(self, name, key):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.hget(self._prefix_helper.add_prefix(name), key)
        except RedisError as exc:
            raise RedisAdapterException('Error executing hget operation') from exc

    def hincrby(self, name, key, amount=1):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.hincrby(self._prefix_helper.add_prefix(name), key, amount)
        except RedisError as exc:
            raise RedisAdapterException('Error executing hincrby operation') from exc

    def incr(self, name, amount=1):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.incr(self._prefix_helper.add_prefix(name), amount)
        except RedisError as exc:
            raise RedisAdapterException('Error executing incr operation') from exc

    def getset(self, name, value):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.getset(self._prefix_helper.add_prefix(name), value)
        except RedisError as exc:
            raise RedisAdapterException('Error executing getset operation') from exc

    def rpush(self, key, *values):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.rpush(self._prefix_helper.add_prefix(key), *values)
        except RedisError as exc:
            raise RedisAdapterException('Error executing rpush operation') from exc

    def expire(self, key, value):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.expire(self._prefix_helper.add_prefix(key), value)
        except RedisError as exc:
            raise RedisAdapterException('Error executing expire operation') from exc

    def rpop(self, key):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.rpop(self._prefix_helper.add_prefix(key))
        except RedisError as exc:
            raise RedisAdapterException('Error executing rpop operation') from exc

    def ttl(self, key):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.ttl(self._prefix_helper.add_prefix(key))
        except RedisError as exc:
            raise RedisAdapterException('Error executing ttl operation') from exc

    def lpop(self, key):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.lpop(self._prefix_helper.add_prefix(key))
        except RedisError as exc:
            raise RedisAdapterException('Error executing lpop operation') from exc

    def pipeline(self):
        """Mimic original redis pipeline."""
        try:
            return RedisPipelineAdapter(self._decorated, self._prefix_helper)
        except RedisError as exc:
            raise RedisAdapterException('Error executing ttl operation') from exc


class RedisPipelineAdapter(object):
    """
    Instance decorator for Redis Pipeline.

    Adds an extra layer handling addition/removal of user prefix when handling
    keys
    """
    def __init__(self, decorated, prefix_helper):
        """
        Store the user prefix and the redis client instance.

        :param decorated: Instance of redis cache client to decorate.
        :param _prefix_helper: PrefixHelper utility
        """
        self._prefix_helper = prefix_helper
        self._pipe = decorated.pipeline()

    def rpush(self, key, *values):
        """Mimic original redis function but using user custom prefix."""
        self._pipe.rpush(self._prefix_helper.add_prefix(key), *values)

    def incr(self, name, amount=1):
        """Mimic original redis function but using user custom prefix."""
        self._pipe.incr(self._prefix_helper.add_prefix(name), amount)

    def hincrby(self, name, key, amount=1):
        """Mimic original redis function but using user custom prefix."""
        self._pipe.hincrby(self._prefix_helper.add_prefix(name), key, amount)

    def execute(self):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._pipe.execute()
        except RedisError as exc:
            raise RedisAdapterException('Error executing pipeline operation') from exc


def _build_default_client(config):  # pylint: disable=too-many-locals
    """
    Build a redis adapter.

    :param config: Redis configuration properties
    :type config: dict

    :return: A wrapped StrictRedis object
    :rtype: splitio.storage.adapters.redis.RedisAdapter
    """
    host = config.get('redisHost', 'localhost')
    port = config.get('redisPort', 6379)
    database = config.get('redisDb', 0)
    password = config.get('redisPassword', None)
    socket_timeout = config.get('redisSocketTimeout', None)
    socket_connect_timeout = config.get('redisSocketConnectTimeout', None)
    socket_keepalive = config.get('redisSocketKeepalive', None)
    socket_keepalive_options = config.get('redisSocketKeepaliveOptions', None)
    connection_pool = config.get('redisConnectionPool', None)
    unix_socket_path = config.get('redisUnixSocketPath', None)
    encoding = config.get('redisEncoding', 'utf-8')
    encoding_errors = config.get('redisEncodingErrors', 'strict')
    errors = config.get('redisErrors', None)
    decode_responses = config.get('redisDecodeResponses', True)
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
        db=database,
        password=password,
        socket_timeout=socket_timeout,
        socket_connect_timeout=socket_connect_timeout,
        socket_keepalive=socket_keepalive,
        socket_keepalive_options=socket_keepalive_options,
        connection_pool=connection_pool,
        unix_socket_path=unix_socket_path,
        encoding=encoding,
        encoding_errors=encoding_errors,
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
    return RedisAdapter(redis, prefix=prefix)


def _build_sentinel_client(config):  # pylint: disable=too-many-locals
    """
    Build a redis client with sentinel replication.

    :param config: Redis configuration properties.
    :type config: dict

    :return: A Wrapped redis-sentinel client
    :rtype: splitio.storage.adapters.redis.RedisAdapter
    """
    sentinels = config.get('redisSentinels')

    if config.get('redisSsl', False):
        raise SentinelConfigurationException('Redis Sentinel cannot be used with SSL/TLS.')

    if sentinels is None:
        raise SentinelConfigurationException('redisSentinels must be specified.')
    if not isinstance(sentinels, list):
        raise SentinelConfigurationException('Sentinels must be an array of elements in the form of'
                                             ' [(ip, port)].')
    if not sentinels:
        raise SentinelConfigurationException('It must be at least one sentinel.')
    if not all(isinstance(s, tuple) for s in sentinels):
        raise SentinelConfigurationException('Sentinels must respect the tuple structure'
                                             '[(ip, port)].')

    master_service = config.get('redisMasterService')

    if master_service is None:
        raise SentinelConfigurationException('redisMasterService must be specified.')

    database = config.get('redisDb', 0)
    password = config.get('redisPassword', None)
    socket_timeout = config.get('redisSocketTimeout', None)
    socket_connect_timeout = config.get('redisSocketConnectTimeout', None)
    socket_keepalive = config.get('redisSocketKeepalive', None)
    socket_keepalive_options = config.get('redisSocketKeepaliveOptions', None)
    connection_pool = config.get('redisConnectionPool', None)
    encoding = config.get('redisEncoding', 'utf-8')
    encoding_errors = config.get('redisEncodingErrors', 'strict')
    decode_responses = config.get('redisDecodeResponses', True)
    retry_on_timeout = config.get('redisRetryOnTimeout', False)
    max_connections = config.get('redisMaxConnections', None)
    prefix = config.get('redisPrefix')

    sentinel = Sentinel(
        sentinels,
        db=database,
        password=password,
        socket_timeout=socket_timeout,
        socket_connect_timeout=socket_connect_timeout,
        socket_keepalive=socket_keepalive,
        socket_keepalive_options=socket_keepalive_options,
        connection_pool=connection_pool,
        encoding=encoding,
        encoding_errors=encoding_errors,
        decode_responses=decode_responses,
        retry_on_timeout=retry_on_timeout,
        max_connections=max_connections
    )

    redis = sentinel.master_for(master_service)
    return RedisAdapter(redis, prefix=prefix)


def build(config):
    """
    Build a redis storage according to the configuration received.

    :param config: SDK Configuration parameters with redis properties.
    :type config: dict.

    :return: A redis client.
    :rtype: splitio.storage.adapters.redis.RedisAdapter
    """
    if 'redisSentinels' in config:
        return _build_sentinel_client(config)
    return _build_default_client(config)
