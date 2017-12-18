'''
Unit tests for the prefix decorator class
'''

from __future__ import absolute_import, division, print_function, \
    unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase
from splitio.prefix_decorator import PrefixDecorator


class PrefixDecoratorTests(TestCase):

    def setUp(self):
        self._prefix = 'test'
        self._pattern = 'some_pattern'
        self._prefixed_mock = mock.Mock()
        self._unprefixed_mock = mock.Mock()
        self._prefixed = PrefixDecorator(self._prefixed_mock, self._prefix)
        self._unprefixed = PrefixDecorator(self._unprefixed_mock)

    def test_keys(self):
        self._prefixed.keys('some_pattern')
        self._prefixed_mock.keys.assert_called_once_with('test.some_pattern')
        self._unprefixed.keys('some_pattern')
        self._unprefixed_mock.keys.assert_called_once_with('some_pattern')

    def test_set(self):
        self._prefixed.set('key1', 1)
        self._prefixed_mock.set.assert_called_once_with('test.key1', 1)
        self._unprefixed.set('key1', 1)
        self._unprefixed_mock.set.assert_called_once_with('key1', 1)

    def test_get(self):
        self._prefixed.get('key1')
        self._prefixed_mock.get.assert_called_once_with('test.key1')
        self._unprefixed.get('key1')
        self._unprefixed_mock.get.assert_called_once_with('key1')

    def test_setex(self):
        self._prefixed.setex('key1', 1, 100)
        self._prefixed_mock.setex.assert_called_once_with('test.key1', 1, 100)
        self._unprefixed.setex('key1', 1, 100)
        self._unprefixed_mock.setex.assert_called_once_with('key1', 1, 100)

    def test_delete(self):
        self._prefixed.delete('key1')
        self._prefixed_mock.delete.assert_called_once_with('test.key1')
        self._unprefixed.delete('key1')
        self._unprefixed_mock.delete.assert_called_once_with('key1')

    def test_exists(self):
        self._prefixed.exists('key1')
        self._prefixed_mock.exists.assert_called_once_with('test.key1')
        self._unprefixed.exists('key1')
        self._unprefixed_mock.exists.assert_called_once_with('key1')

    def test_mget(self):
        self._prefixed.mget(['key1', 'key2'])
        self._prefixed_mock.mget.assert_called_once_with(
            ['test.key1', 'test.key2']
        )
        self._unprefixed.mget(['key1', 'key2'])
        self._unprefixed_mock.mget.assert_called_once_with(['key1', 'key2'])

    def test_smembers(self):
        self._prefixed.smembers('set1')
        self._prefixed_mock.smembers.assert_called_once_with('test.set1')
        self._unprefixed.smembers('set1')
        self._unprefixed_mock.smembers.assert_called_once_with('set1')

    def test_sadd(self):
        self._prefixed.sadd('set1', 1, 2, 3)
        self._prefixed_mock.sadd.assert_called_once_with(
            'test.set1',
            1,
            2,
            3
        )
        self._unprefixed.sadd('set1', 1, 2, 3)
        self._unprefixed_mock.sadd.assert_called_once_with('set1', 1, 2, 3)

    def test_srem(self):
        self._prefixed.srem('set1', 1)
        self._prefixed_mock.srem.assert_called_once_with('test.set1', 1)
        self._unprefixed.srem('set1', 1)
        self._unprefixed_mock.srem.assert_called_once_with('set1', 1)

    def test_sismember(self):
        self._prefixed.sismember('set1', 1)
        self._prefixed_mock.sismember.assert_called_once_with('test.set1', 1)
        self._unprefixed.sismember('set1', 1)
        self._unprefixed_mock.sismember.assert_called_once_with('set1', 1)

    def test_eval(self):
        self._prefixed.eval('some_lua_script', 2, 'key1', 'key2')
        self._prefixed_mock.eval.assert_called_once_with(
            'some_lua_script', 2, 'test.key1', 'test.key2'
        )
        self._unprefixed.eval('some_lua_script', 2, 'key1', 'key2')
        self._unprefixed_mock.eval.assert_called_once_with(
            'some_lua_script', 2, 'key1', 'key2'
        )

    def test_hset(self):
        self._prefixed.hset('hash1', 'key', 1)
        self._prefixed_mock.hset.assert_called_once_with('test.hash1', 'key', 1)
        self._unprefixed.hset('hash1', 'key', 1)
        self._unprefixed_mock.hset.assert_called_once_with('hash1', 'key', 1)

    def test_hget(self):
        self._prefixed.hget('hash1', 'key')
        self._prefixed_mock.hget.assert_called_once_with('test.hash1', 'key')
        self._unprefixed.hget('hash1', 'key')
        self._unprefixed_mock.hget.assert_called_once_with('hash1', 'key')

    def test_incr(self):
        self._prefixed.incr('key1')
        self._prefixed_mock.incr.assert_called_once_with('test.key1', 1)
        self._unprefixed.incr('key1')
        self._unprefixed_mock.incr.assert_called_once_with('key1', 1)

    def test_getset(self):
        self._prefixed.getset('key1', 12)
        self._prefixed_mock.getset.assert_called_once_with('test.key1', 12)
        self._unprefixed.getset('key1', 12)
        self._unprefixed_mock.getset.assert_called_once_with('key1', 12)
