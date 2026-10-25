"""Regression coverage for replacing cached impression timestamps."""
from splitio.engine.cache.lru import SimpleLruCache
from splitio.engine.impressions.manager import Observer
from splitio.models.impressions import Impression


class LruUpdateTests(object):
    def test_repeated_updates_return_latest_value(self):
        cache = SimpleLruCache(2)
        assert cache.test_and_set('key', 1) is None
        assert cache.test_and_set('key', 2) == 1
        assert cache.test_and_set('key', 3) == 2

    def test_update_promotes_and_replaces_lru(self):
        cache = SimpleLruCache(2)
        cache.test_and_set('a', 1)
        cache.test_and_set('b', 2)
        assert cache.test_and_set('a', 3) == 1
        cache.test_and_set('c', 4)
        assert cache.test_and_set('a', 5) == 3
        assert cache.test_and_set('b', 6) is None

    def test_observer_reports_immediately_previous_time(self):
        observer = Observer(2)
        def impression(timestamp):
            return Impression('key', 'flag', 'on', 'rule', 1, None, timestamp, None, None)
        assert observer.test_and_set(impression(100)).previous_time is None
        assert observer.test_and_set(impression(200)).previous_time == 100
        assert observer.test_and_set(impression(300)).previous_time == 200

    def test_optimized_strategy_deduplicates_after_hour_boundary(self, mocker):
        from splitio.engine.impressions.strategies import StrategyOptimizedMode
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=3600200)
        strategy = StrategyOptimizedMode()
        def impression(timestamp):
            return Impression('key', 'flag', 'on', 'rule', 1, None, timestamp, None, None)
        stored, _, _, _ = strategy.process_impressions([(impression(1000), None)])
        assert len(stored) == 1
        stored, _, _, _ = strategy.process_impressions([(impression(3600100), None)])
        assert len(stored) == 1
        stored, _, _, _ = strategy.process_impressions([(impression(3600200), None)])
        assert stored == []
