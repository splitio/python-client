"""Cache testing module."""
#pylint: disable=protected-access,no-self-use,line-too-long
import time
from random import choice

import pytest

from splitio.storage.adapters import cache_trait
from splitio.optional.loaders import asyncio

class  CacheTraitTests(object):
    """Cache trait test cases."""

    def test_lru_behaviour(self, mocker):
        """Test LRU cache functionality."""
        user_func = mocker.Mock()
        user_func.side_effect = lambda *p, **kw: len(p[0])
        key_func = mocker.Mock()
        key_func.side_effect = lambda *p, **kw: p[0]
        cache = cache_trait.LocalMemoryCache(key_func, user_func, 1, 5)

        # add one element & validate basic function calls
        assert cache.get('some') == 4
        assert key_func.mock_calls == [mocker.call('some')]
        assert user_func.mock_calls == [mocker.call('some')]

        # validate _lru & _mru references are updated correctly
        assert cache._lru.key == 'some'
        assert cache._lru.value == 4
        assert cache._lru.next is None
        assert cache._lru.previous is None
        assert cache._mru.key == 'some'
        assert cache._mru.value == 4
        assert cache._mru.next is None
        assert cache._mru.previous is None

        # add another element & validate references.
        assert cache.get('some_other') == 10
        assert cache._lru.key == 'some'
        assert cache._lru.value == 4
        assert cache._lru.next is cache._mru
        assert cache._lru.previous is None
        assert cache._mru.key == 'some_other'
        assert cache._mru.value == 10
        assert cache._mru.next is None
        assert cache._mru.previous is cache._lru

        # add 3 more elements to reach max_size
        assert cache.get('another') == 7
        assert cache.get('some_another') == 12
        assert cache.get('yet_another') == 11
        assert len(cache._data) == 5
        assert cache._lru.key == 'some'

        # add one more element to force LRU eviction and check structure integrity.
        assert cache.get('overrun') == 7
        assert 'some' not in cache._data
        assert len(cache._data) == 5
        assert cache._lru.key == 'some_other'
        for node in cache._data.values():
            if node != cache._mru:
                assert node.next is not None

        # `some_other` should be the next key to be evicted.
        # if we issue a get call for it, it should be marked as the MRU,
        # and `another` should become the new LRU.
        assert cache.get('some_other') == 10
        assert len(cache._data) == 5
        assert cache._mru.key == 'some_other'
        assert cache._lru.key == 'another'

    def test_intensive_usage_behavior(self, mocker):
        """Test fetches with random repeated strings."""
        user_func = mocker.Mock()
        user_func.side_effect = lambda *p, **kw: len(p[0])
        key_func = mocker.Mock()
        key_func.side_effect = lambda *p, **kw: p[0]
        cache = cache_trait.LocalMemoryCache(key_func, user_func, 1, 5)

        strings = ['a', 'bb', 'ccc', 'dddd', 'eeeee', 'ffffff', 'ggggggg', 'hhhhhhhh',
                   'jjjjjjjjj', 'kkkkkkkkkk']
        for _ in range(0, 100000):
            chosen = choice(strings)
            assert cache.get(chosen) == len(chosen)
            assert cache._lru is not None

    def test_expiration_behaviour(self, mocker):
        """Test time expiration works as expected."""
        user_func = mocker.Mock()
        k = 0
        user_func.side_effect = lambda *p, **kw: len(p[0]) + k
        key_func = mocker.Mock()
        key_func.side_effect = lambda *p, **kw: p[0]
        cache = cache_trait.LocalMemoryCache(key_func, user_func, 1, 5)

        assert cache.get('key') == 3
        assert cache.get('other') == 5

        k = 1
        assert cache.get('key') == 3  # cached key retains value until it's expired
        assert cache.get('other') == 5  # cached key retains value until it's expired
        assert cache.get('kkk') == 4  # non-cached key calls function anyway and gets new result.

        time.sleep(1)
        assert cache.get('key') == 4
        assert cache.get('other') == 6

    def test_decorate(self, mocker):
        """Test decorator maker function."""
        local_memory_cache_mock = mocker.Mock(spec=cache_trait.LocalMemoryCache)
        returned_instance_mock = mocker.Mock(spec=cache_trait.LocalMemoryCache)
        local_memory_cache_mock.return_value = returned_instance_mock
        update_wrapper_mock = mocker.Mock()
        mocker.patch('splitio.storage.adapters.cache_trait.update_wrapper', new=update_wrapper_mock)
        mocker.patch('splitio.storage.adapters.cache_trait.LocalMemoryCache', new=local_memory_cache_mock)
        key_func = mocker.Mock()
        user_func = mocker.Mock()

        cache_trait.decorate(key_func)(user_func)
        assert update_wrapper_mock.mock_calls == [mocker.call(mocker.ANY, user_func)]
        assert local_memory_cache_mock.mock_calls == [
            mocker.call(key_func, user_func, cache_trait.DEFAULT_MAX_AGE, cache_trait.DEFAULT_MAX_SIZE)
        ]

        with pytest.raises(TypeError):
            cache_trait.decorate(key_func, -1)

        with pytest.raises(TypeError):
            cache_trait.decorate(key_func, 10, -1)

        assert cache_trait.decorate(key_func, 0, 10)(user_func) is user_func
        assert cache_trait.decorate(key_func, 10, 0)(user_func) is user_func
        assert cache_trait.decorate(key_func, 0, 0)(user_func) is user_func

    @pytest.mark.asyncio
    async def test_async_add_and_get_key(self, mocker):
        cache = cache_trait.LocalMemoryCache(None, None, 1, 1)
        await cache.add_key('split', {'split_name': 'split'})
        assert await cache.get_key('split') == {'split_name': 'split'}
        await asyncio.sleep(1)
        assert await cache.get_key('split') == None
