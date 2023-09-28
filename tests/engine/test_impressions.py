"""Impression manager, observer & hasher tests."""
from datetime import datetime
import unittest.mock as mock
import pytest
from splitio.engine.impressions.impressions import Manager, ImpressionsMode
from splitio.engine.impressions.manager import Hasher, Observer, Counter, truncate_time
from splitio.engine.impressions.strategies import StrategyDebugMode, StrategyOptimizedMode, StrategyNoneMode
from splitio.models.impressions import Impression
from splitio.client.listener import ImpressionListenerWrapper
import splitio.models.telemetry as ModelTelemetry
from splitio.engine.telemetry import TelemetryStorageProducer
from splitio.storage.inmemmory import InMemoryTelemetryStorage

def utctime_ms_reimplement():
    """Re-implementation of utctime_ms to avoid conflicts with mock/patching."""
    return int((datetime.utcnow() - datetime(1970, 1, 1)).total_seconds() * 1000)


class ImpressionHasherTests(object):
    """Test ImpressionHasher behavior."""

    def test_changes_are_reflected(self):
        """Test that change in any field changes the resulting hash."""
        total = set()
        hasher = Hasher()
        total.add(hasher.process(Impression('key1', 'feature1', 'on', 'killed', 123, None, 456)))
        total.add(hasher.process(Impression('key2', 'feature1', 'on', 'killed', 123, None, 456)))
        total.add(hasher.process(Impression('key1', 'feature2', 'on', 'killed', 123, None, 456)))
        total.add(hasher.process(Impression('key1', 'feature1', 'off', 'killed', 123, None, 456)))
        total.add(hasher.process(Impression('key1', 'feature1', 'on', 'not killed', 123, None, 456)))
        total.add(hasher.process(Impression('key1', 'feature1', 'on', 'killed', 321, None, 456)))
        assert len(total) == 6

        # Re-adding the first-one should not increase the number of different hashes
        total.add(hasher.process(Impression('key1', 'feature1', 'on', 'killed', 123, None, 456)))
        assert len(total) == 6


class ImpressionObserverTests(object):
    """Test impression observer behaviour."""

    def test_previous_time_properly_calculated(self):
        """Test that the previous time is properly set."""
        observer = Observer(5)
        assert (observer.test_and_set(Impression('key1', 'f1', 'on', 'killed', 123, None, 456))
                == Impression('key1', 'f1', 'on', 'killed', 123, None, 456))
        assert (observer.test_and_set(Impression('key1', 'f1', 'on', 'killed', 123, None, 457))
                == Impression('key1', 'f1', 'on', 'killed', 123, None, 457, 456))

        # Add 5 new impressions to evict the first one and check that previous time is None again
        assert (observer.test_and_set(Impression('key2', 'f1', 'on', 'killed', 123, None, 456))
                == Impression('key2', 'f1', 'on', 'killed', 123, None, 456))
        assert (observer.test_and_set(Impression('key3', 'f1', 'on', 'killed', 123, None, 456))
                == Impression('key3', 'f1', 'on', 'killed', 123, None, 456))
        assert (observer.test_and_set(Impression('key4', 'f1', 'on', 'killed', 123, None, 456))
                == Impression('key4', 'f1', 'on', 'killed', 123, None, 456))
        assert (observer.test_and_set(Impression('key5', 'f1', 'on', 'killed', 123, None, 456))
                == Impression('key5', 'f1', 'on', 'killed', 123, None, 456))
        assert (observer.test_and_set(Impression('key6', 'f1', 'on', 'killed', 123, None, 456))
                == Impression('key6', 'f1', 'on', 'killed', 123, None, 456))

        # Re-process the first-one
        assert (observer.test_and_set(Impression('key1', 'f1', 'on', 'killed', 123, None, 456))
                == Impression('key1', 'f1', 'on', 'killed', 123, None, 456))


class ImpressionCounterTests(object):
    """Impression counter test cases."""

    def test_tracking_and_popping(self):
        """Test adding impressions counts and popping them."""
        counter = Counter()
        utc_now = utctime_ms_reimplement()
        utc_1_hour_after = utc_now + (3600 * 1000)
        counter.track([Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now),
                       Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now),
                       Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now)])

        counter.track([Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now),
                       Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now)])

        counter.track([Impression('k1', 'f1', 'on', 'l1', 123, None, utc_1_hour_after),
                       Impression('k1', 'f2', 'on', 'l1', 123, None, utc_1_hour_after)])

        assert set(counter.pop_all()) == set([
            Counter.CountPerFeature('f1', truncate_time(utc_now), 3),
            Counter.CountPerFeature('f2', truncate_time(utc_now), 2),
            Counter.CountPerFeature('f1', truncate_time(utc_1_hour_after), 1),
            Counter.CountPerFeature('f2', truncate_time(utc_1_hour_after), 1)])
        assert len(counter._data) == 0
        assert set(counter.pop_all()) == set()


class ImpressionManagerTests(object):
    """Test impressions manager in all of its configurations."""

    def test_standalone_optimized(self, mocker):
        """Test impressions manager in optimized mode with sdk in standalone mode."""

        # Mock utc_time function to be able to play with the clock
        utc_now = truncate_time(utctime_ms_reimplement()) + 1800 * 1000
        utc_time_mock = mocker.Mock()
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        manager = Manager(StrategyOptimizedMode(Counter()), telemetry_runtime_producer)  # no listener
        assert manager._strategy._counter is not None
        assert manager._strategy._observer is not None
        assert manager._listener is None
        assert isinstance(manager._strategy, StrategyOptimizedMode)

        # An impression that hasn't happened in the last hour (pt = None) should be tracked
        imps, deduped = manager.process_impressions([
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3), None),
            (Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3), None)
        ])

        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3),
                        Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3)]
        assert deduped == 0

        # Tracking the same impression a ms later should be empty
        imps, deduped = manager.process_impressions([
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-2), None)
        ])
        assert imps == []
        assert deduped == 1

        # Tracking an impression with a different key makes it to the queue
        imps, deduped = manager.process_impressions([
            (Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1), None)
        ])
        assert imps == [Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1)]
        assert deduped == 0

        # Advance the perceived clock one hour
        old_utc = utc_now  # save it to compare captured impressions
        utc_now += 3600 * 1000
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        # Track the same impressions but "one hour later"
        imps, deduped = manager.process_impressions([
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1), None),
            (Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2), None)
        ])
        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1, old_utc-3),
                        Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2, old_utc-1)]
        assert deduped == 0

        assert len(manager._strategy._observer._cache._data) == 3  # distinct impressions seen
        assert len(manager._strategy._counter._data) == 2  # 2 distinct features. 1 seen in 2 different timeframes

        assert set(manager._strategy._counter.pop_all()) == set([
            Counter.CountPerFeature('f1', truncate_time(old_utc), 1),
            Counter.CountPerFeature('f1', truncate_time(utc_now), 2)
        ])

        # Test counting only from the second impression
        imps, deduped = manager.process_impressions([
            (Impression('k3', 'f3', 'on', 'l1', 123, None, utc_now-1), None)
        ])
        assert set(manager._strategy._counter.pop_all()) == set([])
        assert deduped == 0

        imps, deduped = manager.process_impressions([
            (Impression('k3', 'f3', 'on', 'l1', 123, None, utc_now-1), None)
        ])
        assert set(manager._strategy._counter.pop_all()) == set([
            Counter.CountPerFeature('f3', truncate_time(utc_now), 1)
        ])
        assert deduped == 1

    def test_standalone_debug(self, mocker):
        """Test impressions manager in debug mode with sdk in standalone mode."""

        # Mock utc_time function to be able to play with the clock
        utc_now = truncate_time(utctime_ms_reimplement()) + 1800 * 1000
        utc_time_mock = mocker.Mock()
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        manager = Manager(StrategyDebugMode(), mocker.Mock())  # no listener
        assert manager._strategy._observer is not None
        assert manager._listener is None
        assert isinstance(manager._strategy, StrategyDebugMode)

        # An impression that hasn't happened in the last hour (pt = None) should be tracked
        imps, deduped = manager.process_impressions([
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3), None),
            (Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3), None)
        ])
        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3),
                        Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3)]

        # Tracking the same impression a ms later should return the impression
        imps, deduped = manager.process_impressions([
            (Impression('k1',  'f1', 'on', 'l1', 123, None, utc_now-2), None)
        ])
        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-2, utc_now-3)]

        # Tracking a in impression with a different key makes it to the queue
        imps, deduped = manager.process_impressions([
            (Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1), None)
        ])
        assert imps == [Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1)]

        # Advance the perceived clock one hour
        old_utc = utc_now  # save it to compare captured impressions
        utc_now += 3600 * 1000
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        # Track the same impressions but "one hour later"
        imps, deduped = manager.process_impressions([
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1), None),
            (Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2), None)
        ])
        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1, old_utc-3),
                        Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2, old_utc-1)]

        assert len(manager._strategy._observer._cache._data) == 3  # distinct impressions seen

    def test_standalone_none(self, mocker):
        """Test impressions manager in none mode with sdk in standalone mode."""

        # Mock utc_time function to be able to play with the clock
        utc_now = truncate_time(utctime_ms_reimplement()) + 1800 * 1000
        utc_time_mock = mocker.Mock()
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        manager = Manager(StrategyNoneMode(Counter()), mocker.Mock())  # no listener
        assert manager._strategy._counter is not None
        assert manager._listener is None
        assert isinstance(manager._strategy, StrategyNoneMode)

        # no impressions are tracked, only counter and mtk
        imps, deduped = manager.process_impressions([
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3), None),
            (Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3), None)
        ])
        assert imps == []
        assert [Counter.CountPerFeature(k.feature, k.timeframe, v)
                for (k, v) in manager._strategy._counter._data.items()] == [
            Counter.CountPerFeature('f1', truncate_time(utc_now-3), 1),
            Counter.CountPerFeature('f2', truncate_time(utc_now-3), 1)]
        assert manager._strategy.get_unique_keys_tracker()._cache == {
            'f1': set({'k1'}),
            'f2': set({'k1'})}

        # Tracking the same impression a ms later should not return the impression and no change on mtk cache
        imps, deduped = manager.process_impressions([
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-2), None)
        ])
        assert imps == []
        assert manager._strategy.get_unique_keys_tracker()._cache == {'f1': set({'k1'}), 'f2': set({'k1'})}

        # Tracking an impression with a different key, will only increase mtk
        imps, deduped = manager.process_impressions([
            (Impression('k3', 'f1', 'on', 'l1', 123, None, utc_now-1), None)
        ])
        assert imps == []
        assert manager._strategy.get_unique_keys_tracker()._cache == {
            'f1': set({'k1', 'k3'}),
            'f2': set({'k1'})}

        # Advance the perceived clock one hour
        old_utc = utc_now  # save it to compare captured impressions
        utc_now += 3600 * 1000
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        # Track the same impressions but "one hour later", no changes on mtk
        imps, deduped = manager.process_impressions([
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1), None),
            (Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2), None)
        ])
        assert imps == []
        assert manager._strategy.get_unique_keys_tracker()._cache == {
            'f1': set({'k1', 'k3', 'k2'}),
            'f2': set({'k1'})}

        assert len(manager._strategy._counter._data) == 3  # 2 distinct features. 1 seen in 2 different timeframes

        assert set(manager._strategy._counter.pop_all()) == set([
            Counter.CountPerFeature('f1', truncate_time(old_utc), 3),
            Counter.CountPerFeature('f2', truncate_time(old_utc), 1),
            Counter.CountPerFeature('f1', truncate_time(utc_now), 2)
        ])

    def test_standalone_optimized_listener(self, mocker):
        """Test impressions manager in optimized mode with sdk in standalone mode."""

        # Mock utc_time function to be able to play with the clock
        utc_now = truncate_time(utctime_ms_reimplement()) + 1800 * 1000
        utc_time_mock = mocker.Mock()
        utc_time_mock.return_value = utc_now
#        mocker.patch('splitio.util.time.utctime_ms', return_value=utc_time_mock)
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        listener = mocker.Mock(spec=ImpressionListenerWrapper)
        manager = Manager(StrategyOptimizedMode(Counter()), mocker.Mock(), listener=listener)
        assert manager._strategy._counter is not None
        assert manager._strategy._observer is not None
        assert manager._listener is not None
        assert isinstance(manager._strategy, StrategyOptimizedMode)

        # An impression that hasn't happened in the last hour (pt = None) should be tracked
        imps, deduped = manager.process_impressions([
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3), None),
            (Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3), None)
        ])
        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3),
                        Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3)]
        assert deduped == 0

        # Tracking the same impression a ms later should return empty
        imps, deduped = manager.process_impressions([
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-2), None)
        ])
        assert imps == []
        assert deduped == 1

        # Tracking a in impression with a different key makes it to the queue
        imps, deduped = manager.process_impressions([
            (Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1), None)
        ])
        assert imps == [Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1)]
        assert deduped == 0

        # Advance the perceived clock one hour
        old_utc = utc_now  # save it to compare captured impressions
        utc_now += 3600 * 1000
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        # Track the same impressions but "one hour later"
        imps, deduped = manager.process_impressions([
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1), None),
            (Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2), None)
        ])
        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1, old_utc-3),
                        Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2, old_utc-1)]
        assert deduped == 0

        assert len(manager._strategy._observer._cache._data) == 3  # distinct impressions seen
        assert len(manager._strategy._counter._data) == 2  # 2 distinct features. 1 seen in 2 different timeframes

        assert set(manager._strategy._counter.pop_all()) == set([
            Counter.CountPerFeature('f1', truncate_time(old_utc), 1),
            Counter.CountPerFeature('f1', truncate_time(utc_now), 2)
        ])

        assert listener.log_impression.mock_calls == [
            mocker.call(Impression('k1', 'f1', 'on', 'l1', 123, None, old_utc-3), None),
            mocker.call(Impression('k1', 'f2', 'on', 'l1', 123, None, old_utc-3), None),
            mocker.call(Impression('k1', 'f1', 'on', 'l1', 123, None, old_utc-2, old_utc-3), None),
            mocker.call(Impression('k2', 'f1', 'on', 'l1', 123, None, old_utc-1), None),
            mocker.call(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1, old_utc-3), None),
            mocker.call(Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2, old_utc-1), None)
        ]

        # Test counting only from the second impression
        imps, deduped = manager.process_impressions([
            (Impression('k3', 'f3', 'on', 'l1', 123, None, utc_now-1), None)
        ])
        assert set(manager._strategy._counter.pop_all()) == set([])
        assert deduped == 0

        imps, deduped = manager.process_impressions([
            (Impression('k3', 'f3', 'on', 'l1', 123, None, utc_now-1), None)
        ])
        assert set(manager._strategy._counter.pop_all()) == set([
            Counter.CountPerFeature('f3', truncate_time(utc_now), 1)
        ])
        assert deduped == 1

    def test_standalone_debug_listener(self, mocker):
        """Test impressions manager in optimized mode with sdk in standalone mode."""

        # Mock utc_time function to be able to play with the clock
        utc_now = truncate_time(utctime_ms_reimplement()) + 1800 * 1000
        utc_time_mock = mocker.Mock()
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        imps = []
        listener = mocker.Mock(spec=ImpressionListenerWrapper)
        manager = Manager(StrategyDebugMode(), mocker.Mock(), listener=listener)
        assert manager._listener is not None
        assert isinstance(manager._strategy, StrategyDebugMode)

        # An impression that hasn't happened in the last hour (pt = None) should be tracked
        imps, deduped = manager.process_impressions([
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3), None),
            (Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3), None)
        ])
        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3),
                        Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3)]

        # Tracking the same impression a ms later should return the imp
        imps, deduped = manager.process_impressions([
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-2), None)
        ])
        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-2, utc_now-3)]

        # Tracking a in impression with a different key makes it to the queue
        imps, deduped = manager.process_impressions([
            (Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1), None)
        ])
        assert imps == [Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1)]

        # Advance the perceived clock one hour
        old_utc = utc_now  # save it to compare captured impressions
        utc_now += 3600 * 1000
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        # Track the same impressions but "one hour later"
        imps, deduped = manager.process_impressions([
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1), None),
            (Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2), None)
        ])
        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1, old_utc-3),
                        Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2, old_utc-1)]

        assert len(manager._strategy._observer._cache._data) == 3  # distinct impressions seen

        assert listener.log_impression.mock_calls == [
            mocker.call(Impression('k1', 'f1', 'on', 'l1', 123, None, old_utc-3), None),
            mocker.call(Impression('k1', 'f2', 'on', 'l1', 123, None, old_utc-3), None),
            mocker.call(Impression('k1', 'f1', 'on', 'l1', 123, None, old_utc-2, old_utc-3), None),
            mocker.call(Impression('k2', 'f1', 'on', 'l1', 123, None, old_utc-1), None),
            mocker.call(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1, old_utc-3), None),
            mocker.call(Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2, old_utc-1), None)
        ]

    def test_standalone_none_listener(self, mocker):
        """Test impressions manager in none mode with sdk in standalone mode."""

        # Mock utc_time function to be able to play with the clock
        utc_now = truncate_time(utctime_ms_reimplement()) + 1800 * 1000
        utc_time_mock = mocker.Mock()
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        listener = mocker.Mock(spec=ImpressionListenerWrapper)
        manager = Manager(StrategyNoneMode(Counter()), mocker.Mock(), listener=listener)
        assert manager._strategy._counter is not None
        assert manager._listener is not None
        assert isinstance(manager._strategy, StrategyNoneMode)

        # An impression that hasn't happened in the last hour (pt = None) should not be tracked
        imps, deduped = manager.process_impressions([
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3), None),
            (Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3), None)
        ])
        assert imps == []
        assert [Counter.CountPerFeature(k.feature, k.timeframe, v)
                for (k, v) in manager._strategy._counter._data.items()] == [
            Counter.CountPerFeature('f1', truncate_time(utc_now-3), 1),
            Counter.CountPerFeature('f2', truncate_time(utc_now-3), 1)]
        assert manager._strategy.get_unique_keys_tracker()._cache == {
            'f1': set({'k1'}),
            'f2': set({'k1'})}

        # Tracking the same impression a ms later should return empty, no updates on mtk
        imps, deduped = manager.process_impressions([
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-2), None)
        ])
        assert imps == []
        assert manager._strategy.get_unique_keys_tracker()._cache == {
            'f1': set({'k1'}),
            'f2': set({'k1'})}

        # Tracking a in impression with a different key update mtk
        imps, deduped = manager.process_impressions([
            (Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1), None)
        ])
        assert imps == []
        assert manager._strategy.get_unique_keys_tracker()._cache == {
            'f1': set({'k1', 'k2'}),
            'f2': set({'k1'})}

        # Advance the perceived clock one hour
        old_utc = utc_now  # save it to compare captured impressions
        utc_now += 3600 * 1000
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        # Track the same impressions but "one hour later"
        imps, deduped = manager.process_impressions([
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1), None),
            (Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2), None)
        ])
        assert imps == []
        assert manager._strategy.get_unique_keys_tracker()._cache == {
            'f1': set({'k1', 'k2'}),
            'f2': set({'k1'})}

        assert len(manager._strategy._counter._data) == 3  # 2 distinct features. 1 seen in 2 different timeframes

        assert set(manager._strategy._counter.pop_all()) == set([
            Counter.CountPerFeature('f1', truncate_time(old_utc), 3),
            Counter.CountPerFeature('f2', truncate_time(old_utc), 1),
            Counter.CountPerFeature('f1', truncate_time(utc_now), 2)
        ])

        assert listener.log_impression.mock_calls == [
            mocker.call(Impression('k1', 'f1', 'on', 'l1', 123, None, old_utc-3), None),
            mocker.call(Impression('k1', 'f2', 'on', 'l1', 123, None, old_utc-3), None),
            mocker.call(Impression('k1', 'f1', 'on', 'l1', 123, None, old_utc-2, None), None),
            mocker.call(Impression('k2', 'f1', 'on', 'l1', 123, None, old_utc-1), None),
            mocker.call(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1, None), None),
            mocker.call(Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2, None), None)
        ]