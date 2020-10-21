"""LRU Cache unit tests."""

from splitio.engine.cache.lru import SimpleLruCache

class SimpleLruCacheTests(object):
    """Test SimpleLruCache."""

    def test_basic_usage(self, mocker):
        """Test that a missing split logs and returns CONTROL."""
        cache = SimpleLruCache(5)
        assert cache.test_and_set('a', 1) is None
        assert cache.test_and_set('b', 2) is None
        assert cache.test_and_set('c', 3) is None
        assert cache.test_and_set('d', 4) is None
        assert cache.test_and_set('e', 5) is None

        assert cache.test_and_set('a', 10) is 1
        assert cache.test_and_set('b', 20) is 2
        assert cache.test_and_set('c', 30) is 3
        assert cache.test_and_set('d', 40) is 4
        assert cache.test_and_set('e', 50) is 5
        assert len(cache._data) is 5

    def test_lru_eviction(self, mocker):
        """Test that a missing split logs and returns CONTROL."""
        cache = SimpleLruCache(5)
        assert cache.test_and_set('a', 1) is None
        assert cache.test_and_set('b', 2) is None
        assert cache.test_and_set('c', 3) is None
        assert cache.test_and_set('d', 4) is None
        assert cache.test_and_set('e', 5) is None
        assert cache.test_and_set('f', 6) is None
        assert cache.test_and_set('g', 7) is None
        assert cache.test_and_set('h', 8) is None
        assert cache.test_and_set('i', 9) is None
        assert cache.test_and_set('j', 0) is None
        assert len(cache._data) is 5
        assert set(cache._data.keys()) == set(['f', 'g', 'h', 'i', 'j'])
