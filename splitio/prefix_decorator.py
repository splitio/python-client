from six import string_types, binary_type


class PrefixDecorator:
    '''
    Instance decorator for Redis clients such as StrictRedis.
    Adds an extra layer handling addition/removal of user prefix when handling
    keys
    '''

    def __init__(self, decorated, prefix=None):
        '''
        Stores the user prefix and the redis client instance.

        :param decorated: Instance of redis cache client to decorate.
        :param prefix: User prefix to add.
        '''
        self._prefix = prefix
        self._decorated = decorated

    def _add_prefix(self, k):
        '''
        Add a prefix to the contents of k.
        'k' may be:
            - a single key (of type string or unicode in python2, or type string
            in python 3. In which case we simple add a prefix with a dot.
            - a list, in which the prefix is applied to element.
        If no user prefix is stored, the key/list of keys will be returned as is

        :param k: single (string) or list of (list) keys.
        :returns: Key(s) with prefix if applicable
        '''
        if self._prefix:
            if isinstance(k, string_types):
                return '{prefix}.{key}'.format(prefix=self._prefix, key=k)
            elif isinstance(k, list) and len(k) > 0:
                if isinstance(k[0], binary_type):
                    return [
                        '{prefix}.{key}'.format(prefix=self._prefix, key=key.decode("utf8"))
                        for key in k
                    ]
                elif isinstance(k[0], string_types):
                    return [
                        '{prefix}.{key}'.format(prefix=self._prefix, key=key.decode("utf8"))
                        for key in k
                    ]

        else:
            return k

    def _remove_prefix(self, k):
        '''
        Removes the user prefix from a key before handling it back
        to the requester.
        Similar to _add_prefix, this class will handle single strings as well
        as lists. If no _prefix is set, the original key/keys will be returned.

        :param k: key(s) whose prefix will be removed.
        :returns: prefix-less key(s)
        '''
        if self._prefix:
            if isinstance(k, string_types):
                return k[len(self._prefix)+1:]
            elif isinstance(k, list):
                return [key[len(self._prefix)+1:] for key in k]
        else:
            return k

    # Below starts a list of methods that implement the interface of a standard
    # redis client.

    def keys(self, pattern):
        return self._remove_prefix(
            self._decorated.keys(self._add_prefix(pattern))
        )

    def set(self, name, value, *args, **kwargs):
        return self._decorated.set(
            self._add_prefix(name), value, *args, **kwargs
        )

    def get(self, name):
        return self._decorated.get(self._add_prefix(name))

    def setex(self, name, time, value):
        return self._decorated.setex(self._add_prefix(name), time, value)

    def delete(self, names):
        return self._decorated.delete(self._add_prefix(names))

    def exists(self, name):
        return self._decorated.exists(self._add_prefix(name))

    def mget(self, names):
        return self._decorated.mget(self._add_prefix(names))

    def smembers(self, name):
        return self._decorated.smembers(self._add_prefix(name))

    def sadd(self, name, *values):
        return self._decorated.sadd(self._add_prefix(name), *values)

    def srem(self, name, *values):
        return self._decorated.srem(self._add_prefix(name), *values)

    def sismember(self, name, value):
        return self._decorated.sismember(self._add_prefix(name), value)

    def eval(self, *args):
        script = args[0]
        num_keys = args[1]
        keys = list(args[2:])
        return self._decorated.eval(script, num_keys, *self._add_prefix(keys))

    def hset(self, name, key, value):
        return self._decorated.hset(self._add_prefix(name), key, value)

    def hget(self, name, key):
        return self._decorated.hget(self._add_prefix(name), key)

    def incr(self, name, amount=1):
        return self._decorated.incr(self._add_prefix(name), amount)

    def getset(self, name, value):
        return self._decorated.getset(self._add_prefix(name), value)

    def rpush(self, key, value):
        return self._decorated.rpush(self._add_prefix(key), value)
