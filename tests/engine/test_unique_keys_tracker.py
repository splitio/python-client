"""BloomFilter unit tests."""

import threading
from splitio.engine.impressions.unique_keys_tracker import UniqueKeysTracker
from splitio.engine.filters import BloomFilter

class UniqueKeysTrackerTests(object):
    """StandardRecorderTests test cases."""

    def test_adding_and_removing_keys(self, mocker):
        tracker = UniqueKeysTracker()

        assert(tracker._cache_size > 0)
        assert(tracker._current_cache_size == 0)
        assert(tracker._cache == {})
        assert(isinstance(tracker._filter, BloomFilter))

        key1 = 'key1'
        key2 = 'key2'
        key3 = 'key3'
        split1= 'feature1'
        split2= 'feature2'

        assert(tracker.track(key1, split1))
        assert(tracker.track(key3, split1))
        assert(not tracker.track(key1, split1))
        assert(tracker.track(key2, split2))

        assert(tracker._filter.contains(split1+key1))
        assert(not tracker._filter.contains(split1+key2))
        assert(tracker._filter.contains(split2+key2))
        assert(not tracker._filter.contains(split2+key1))
        assert(key1 in tracker._cache[split1])
        assert(key3 in tracker._cache[split1])
        assert(key2 in tracker._cache[split2])
        assert(not key3 in tracker._cache[split2])

        tracker.clear_filter()
        assert(not tracker._filter.contains(split1+key1))
        assert(not tracker._filter.contains(split2+key2))

        cache_backup = tracker._cache.copy()
        cache_size_backup = tracker._current_cache_size
        cache, cache_size = tracker.get_cache_info_and_pop_all()
        assert(cache_backup == cache)
        assert(cache_size_backup == cache_size)
        assert(tracker._current_cache_size == 0)
        assert(tracker._cache == {})

    def test_cache_size(self, mocker):
        cache_size = 10
        tracker = UniqueKeysTracker(cache_size)

        split1= 'feature1'
        for x in range(1, cache_size + 1):
            tracker.track('key' + str(x), split1)
        split2= 'feature2'
        for x in range(1, int(cache_size / 2) + 1):
            tracker.track('key' + str(x), split2)

        assert(tracker._current_cache_size == (cache_size + (cache_size / 2)))
        assert(len(tracker._cache[split1]) == cache_size)
        assert(len(tracker._cache[split2]) == cache_size / 2)
