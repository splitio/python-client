"""Redis client wrapper with prefix support."""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

from builtins import str
from six import string_types, binary_type
from future.utils import raise_from

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


def _bytes_to_string(maybe_bytes, encode='utf-8'):
    if type(maybe_bytes).__name__ == 'bytes':
        return str(maybe_bytes, encode)
    return maybe_bytes


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
    pass


class RedisAdapter(object):  #pylint: disable=too-many-public-methods
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
        self._prefix = prefix
        self._decorated = decorated

    def _add_prefix(self, k):
        """
        Add a prefix to the contents of k.

        'k' may be:
            - a single key (of type string or unicode in python2, or type string
            in python 3. In which case we simple add a prefix with a dot.
            - a list, in which the prefix is applied to element.
        If no user prefix is stored, the key/list of keys will be returned as is

        :param k: single (string) or list of (list) keys.
        :returns: Key(s) with prefix if applicable
        """
        if self._prefix:
            if isinstance(k, string_types):
                return '{prefix}.{key}'.format(prefix=self._prefix, key=k)
            elif isinstance(k, list) and k:
                if isinstance(k[0], binary_type):
                    return [
                        '{prefix}.{key}'.format(prefix=self._prefix, key=key.decode("utf8"))
                        for key in k
                    ]
                elif isinstance(k[0], string_types):
                    return [
                        '{prefix}.{key}'.format(prefix=self._prefix, key=key)
                        for key in k
                    ]
        else:
            return k

        raise RedisAdapterException(
            "Cannot append prefix correctly. Wrong type for key(s) provided"
        )

    def _remove_prefix(self, k):
        """
        Remove the user prefix from a key before handling it back to the requester.

        Similar to _add_prefix, this class will handle single strings as well
        as lists. If no _prefix is set, the original key/keys will be returned.

        :param k: key(s) whose prefix will be removed.
        :returns: prefix-less key(s)
        """
        if self._prefix:
            if isinstance(k, string_types):
                return k[len(self._prefix)+1:]
            elif isinstance(k, list):
                return [key[len(self._prefix)+1:] for key in k]
        else:
            return k

        raise RedisAdapterException(
            "Cannot remove prefix correctly. Wrong type for key(s) provided"
        )

    # Below starts a list of methods that implement the interface of a standard
    # redis client.

    def keys(self, pattern):
        """Mimic original redis function but using user custom prefix."""
        try:
            return [
                _bytes_to_string(key)
                for key in self._remove_prefix(self._decorated.keys(self._add_prefix(pattern)))
            ]
        except RedisError as exc:
            raise_from(RedisAdapterException('Failed to execute keys operation'), exc)

    def set(self, name, value, *args, **kwargs):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.set(
                self._add_prefix(name), value, *args, **kwargs
            )
        except RedisError as exc:
            raise RedisAdapterException('Failed to execute set operation', exc)

    def get(self, name):
        """Mimic original redis function but using user custom prefix."""
        try:
            return _bytes_to_string(self._decorated.get(self._add_prefix(name)))
        except RedisError as exc:
            raise RedisAdapterException('Error executing get operation', exc)

    def setex(self, name, time, value):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.setex(self._add_prefix(name), time, value)
        except RedisError as exc:
            raise RedisAdapterException('Error executing setex operation', exc)

    def delete(self, *names):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.delete(*self._add_prefix(list(names)))
        except RedisError as exc:
            raise_from(RedisAdapterException('Error executing delete operation'), exc)

    def exists(self, name):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.exists(self._add_prefix(name))
        except RedisError as exc:
            raise_from(RedisAdapterException('Error executing exists operation'), exc)

    def lrange(self, key, start, end):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.lrange(self._add_prefix(key), start, end)
        except RedisError as exc:
            raise_from(RedisAdapterException('Error executing exists operation'), exc)

    def mget(self, names):
        """Mimic original redis function but using user custom prefix."""
        try:
            return [
                _bytes_to_string(item)
                for item in self._decorated.mget(self._add_prefix(names))
            ]
        except RedisError as exc:
            raise_from(RedisAdapterException('Error executing mget operation'), exc)

    def smembers(self, name):
        """Mimic original redis function but using user custom prefix."""
        try:
            return [
                _bytes_to_string(item)
                for item in self._decorated.smembers(self._add_prefix(name))
            ]
        except RedisError as exc:
            raise_from(RedisAdapterException('Error executing smembers operation'), exc)

    def sadd(self, name, *values):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.sadd(self._add_prefix(name), *values)
        except RedisError as exc:
            raise_from(RedisAdapterException('Error executing sadd operation'), exc)

    def srem(self, name, *values):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.srem(self._add_prefix(name), *values)
        except RedisError as exc:
            raise_from(RedisAdapterException('Error executing srem operation'), exc)

    def sismember(self, name, value):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.sismember(self._add_prefix(name), value)
        except RedisError as exc:
            raise_from(RedisAdapterException('Error executing sismember operation'), exc)

    def eval(self, script, number_of_keys, *keys):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.eval(script, number_of_keys, *self._add_prefix(list(keys)))
        except RedisError as exc:
            raise_from(RedisAdapterException('Error executing eval operation'), exc)

    def hset(self, name, key, value):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.hset(self._add_prefix(name), key, value)
        except RedisError as exc:
            raise_from(RedisAdapterException('Error executing hset operation'), exc)

    def hget(self, name, key):
        """Mimic original redis function but using user custom prefix."""
        try:
            return _bytes_to_string(self._decorated.hget(self._add_prefix(name), key))
        except RedisError as exc:
            raise_from(RedisAdapterException('Error executing hget operation'), exc)

    def incr(self, name, amount=1):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.incr(self._add_prefix(name), amount)
        except RedisError as exc:
            raise_from(RedisAdapterException('Error executing incr operation'), exc)

    def getset(self, name, value):
        """Mimic original redis function but using user custom prefix."""
        try:
            return _bytes_to_string(self._decorated.getset(self._add_prefix(name), value))
        except RedisError as exc:
            raise_from(RedisAdapterException('Error executing getset operation'), exc)

    def rpush(self, key, *values):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.rpush(self._add_prefix(key), *values)
        except RedisError as exc:
            raise_from(RedisAdapterException('Error executing rpush operation'), exc)

    def expire(self, key, value):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.expire(self._add_prefix(key), value)
        except RedisError as exc:
            raise_from(RedisAdapterException('Error executing expire operation'), exc)

    def rpop(self, key):
        """Mimic original redis function but using user custom prefix."""
        try:
            return _bytes_to_string(self._decorated.rpop(self._add_prefix(key)))
        except RedisError as exc:
            raise_from(RedisAdapterException('Error executing rpop operation'), exc)

    def ttl(self, key):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.ttl(self._add_prefix(key))
        except RedisError as exc:
            raise_from(RedisAdapterException('Error executing ttl operation'), exc)

    def lpop(self, key):
        """Mimic original redis function but using user custom prefix."""
        try:
            return self._decorated.lpop(self._add_prefix(key))
        except RedisError as exc:
            raise_from(RedisAdapterException('Error executing lpop operation'), exc)


def _build_default_client(config):  #pylint: disable=too-many-locals
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
    return RedisAdapter(redis, prefix=prefix)


def _build_sentinel_client(config):  #pylint: disable=too-many-locals
    """
    Build a redis client with sentinel replication.

    :param config: Redis configuration properties.
    :type config: dict

    :return: A Wrapped redis-sentinel client
    :rtype: splitio.storage.adapters.redis.RedisAdapter
    """
    sentinels = config.get('redisSentinels')

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
    decode_responses = config.get('redisDecodeResponses', False)
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
