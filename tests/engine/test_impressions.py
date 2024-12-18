"""Impression manager, observer & hasher tests."""
from datetime import datetime
import unittest.mock as mock
import pytest
from splitio.engine.impressions.impressions import Manager, ImpressionsMode
from splitio.engine.impressions.manager import Hasher, Observer, Counter, truncate_time
from splitio.engine.impressions.strategies import StrategyDebugMode, StrategyOptimizedMode, StrategyNoneMode
from splitio.models.impressions import Impression, ImpressionDecorated
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

        manager = Manager(StrategyOptimizedMode(), StrategyNoneMode(), telemetry_runtime_producer)  # no listener
        assert manager._strategy._observer is not None
        assert isinstance(manager._strategy, StrategyOptimizedMode)
        assert isinstance(manager._none_strategy, StrategyNoneMode)

        # An impression that hasn't happened in the last hour (pt = None) should be tracked
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3), True), None),
            (ImpressionDecorated(Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3), True), None)
        ])

        assert for_unique_keys_tracker == []
        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3),
                        Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3)]
        assert deduped == 0

        # Tracking the same impression a ms later should be empty
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-2), True), None)
        ])
        assert imps == []
        assert deduped == 1
        assert for_unique_keys_tracker == []

        # Tracking an impression with a different key makes it to the queue
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1), True), None)
        ])
        assert imps == [Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1)]
        assert deduped == 0

        # Advance the perceived clock one hour
        old_utc = utc_now  # save it to compare captured impressions
        utc_now += 3600 * 1000
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        # Track the same impressions but "one hour later"
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1), True), None),
            (ImpressionDecorated(Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2), True), None)
        ])
        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1, old_utc-3),
                        Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2, old_utc-1)]
        assert deduped == 0
        assert for_unique_keys_tracker == []

        assert len(manager._strategy._observer._cache._data) == 3  # distinct impressions seen
        assert for_counter == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1, old_utc-3),
                        Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2, old_utc-1)]

        # Test counting only from the second impression
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k3', 'f3', 'on', 'l1', 123, None, utc_now-1), True), None)
        ])
        assert for_counter == []
        assert deduped == 0
        assert for_unique_keys_tracker == []

        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k3', 'f3', 'on', 'l1', 123, None, utc_now-1), True), None)
        ])
        assert for_counter == [Impression('k3', 'f3', 'on', 'l1', 123, None, utc_now-1, utc_now-1)]
        assert deduped == 1
        assert for_unique_keys_tracker == []

    def test_standalone_debug(self, mocker):
        """Test impressions manager in debug mode with sdk in standalone mode."""

        # Mock utc_time function to be able to play with the clock
        utc_now = truncate_time(utctime_ms_reimplement()) + 1800 * 1000
        utc_time_mock = mocker.Mock()
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        manager = Manager(StrategyDebugMode(), StrategyNoneMode(), mocker.Mock())  # no listener
        assert manager._strategy._observer is not None
        assert isinstance(manager._strategy, StrategyDebugMode)

        # An impression that hasn't happened in the last hour (pt = None) should be tracked
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3), True), None),
            (ImpressionDecorated(Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3), True), None)
        ])
        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3),
                        Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3)]
        assert for_counter == []
        assert for_unique_keys_tracker == []

        # Tracking the same impression a ms later should return the impression
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1',  'f1', 'on', 'l1', 123, None, utc_now-2), True), None)
        ])
        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-2, utc_now-3)]
        assert for_counter == []
        assert for_unique_keys_tracker == []

        # Tracking a in impression with a different key makes it to the queue
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1), True), None)
        ])
        assert imps == [Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1)]
        assert for_counter == []
        assert for_unique_keys_tracker == []

        # Advance the perceived clock one hour
        old_utc = utc_now  # save it to compare captured impressions
        utc_now += 3600 * 1000
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        # Track the same impressions but "one hour later"
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1), True), None),
            (ImpressionDecorated(Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2), True), None)
        ])
        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1, old_utc-3),
                        Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2, old_utc-1)]
        assert for_counter == []
        assert for_unique_keys_tracker == []

        assert len(manager._strategy._observer._cache._data) == 3  # distinct impressions seen

    def test_standalone_none(self, mocker):
        """Test impressions manager in none mode with sdk in standalone mode."""

        # Mock utc_time function to be able to play with the clock
        utc_now = truncate_time(utctime_ms_reimplement()) + 1800 * 1000
        utc_time_mock = mocker.Mock()
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        manager = Manager(StrategyNoneMode(), StrategyNoneMode(), mocker.Mock())  # no listener
        assert isinstance(manager._strategy, StrategyNoneMode)

        # no impressions are tracked, only counter and mtk
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3), True), None),
            (ImpressionDecorated(Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3), True), None)
        ])
        assert imps == []
        assert for_counter == [
            Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3),
            Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3)
        ]
        assert for_unique_keys_tracker == [('k1', 'f1'), ('k1', 'f2')]

        # Tracking the same impression a ms later should not return the impression and no change on mtk cache
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-2), True), None)
        ])
        assert imps == []

        # Tracking an impression with a different key, will only increase mtk
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k3', 'f1', 'on', 'l1', 123, None, utc_now-1), True), None)
        ])
        assert imps == []
        assert for_unique_keys_tracker == [('k3', 'f1')]
        assert for_counter == [
            Impression('k3', 'f1', 'on', 'l1', 123, None, utc_now-1)
        ]

        # Advance the perceived clock one hour
        old_utc = utc_now  # save it to compare captured impressions
        utc_now += 3600 * 1000
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        # Track the same impressions but "one hour later", no changes on mtk
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1), True), None),
            (ImpressionDecorated(Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2), True), None)
        ])
        assert imps == []
        assert for_counter == [
            Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1),
            Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2)
        ]

    def test_standalone_optimized_listener(self, mocker):
        """Test impressions manager in optimized mode with sdk in standalone mode."""

        # Mock utc_time function to be able to play with the clock
        utc_now = truncate_time(utctime_ms_reimplement()) + 1800 * 1000
        utc_time_mock = mocker.Mock()
        utc_time_mock.return_value = utc_now
#        mocker.patch('splitio.util.time.utctime_ms', return_value=utc_time_mock)
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        manager = Manager(StrategyOptimizedMode(), StrategyNoneMode(), mocker.Mock())
        assert manager._strategy._observer is not None
        assert isinstance(manager._strategy, StrategyOptimizedMode)

        # An impression that hasn't happened in the last hour (pt = None) should be tracked
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3), True), None),
            (ImpressionDecorated(Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3), True), None)
        ])
        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3),
                        Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3)]
        assert deduped == 0
        assert listen == [(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3), None),
                        (Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3), None)]
        assert for_unique_keys_tracker == []

        # Tracking the same impression a ms later should return empty
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-2), True), None)
        ])
        assert imps == []
        assert deduped == 1
        assert listen == [(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-2, utc_now-3), None)]
        assert for_unique_keys_tracker == []

        # Tracking a in impression with a different key makes it to the queue
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1), True), None)
        ])
        assert imps == [Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1)]
        assert deduped == 0
        assert listen == [(Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1), None)]
        assert for_unique_keys_tracker == []

        # Advance the perceived clock one hour
        old_utc = utc_now  # save it to compare captured impressions
        utc_now += 3600 * 1000
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        # Track the same impressions but "one hour later"
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1), True), None),
            (ImpressionDecorated(Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2), True), None)
        ])
        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1, old_utc-3),
                        Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2, old_utc-1)]
        assert deduped == 0
        assert listen == [
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1, old_utc-3), None),
            (Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2, old_utc-1), None),
        ]
        assert for_unique_keys_tracker == []
        assert len(manager._strategy._observer._cache._data) == 3  # distinct impressions seen
        assert for_counter == [
            Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1, old_utc-3),
            Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2, old_utc-1)
        ]

        # Test counting only from the second impression
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k3', 'f3', 'on', 'l1', 123, None, utc_now-1), True), None)
        ])
        assert for_counter == []
        assert deduped == 0
        assert for_unique_keys_tracker == []

        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k3', 'f3', 'on', 'l1', 123, None, utc_now-1), True), None)
        ])
        assert for_counter == [
            Impression('k3', 'f3', 'on', 'l1', 123, None, utc_now-1, utc_now-1)
        ]
        assert deduped == 1
        assert for_unique_keys_tracker == []

    def test_standalone_debug_listener(self, mocker):
        """Test impressions manager in optimized mode with sdk in standalone mode."""

        # Mock utc_time function to be able to play with the clock
        utc_now = truncate_time(utctime_ms_reimplement()) + 1800 * 1000
        utc_time_mock = mocker.Mock()
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        imps = []
        listener = mocker.Mock(spec=ImpressionListenerWrapper)
        manager = Manager(StrategyDebugMode(), StrategyNoneMode(), mocker.Mock())
        assert isinstance(manager._strategy, StrategyDebugMode)

        # An impression that hasn't happened in the last hour (pt = None) should be tracked
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3), True), None),
            (ImpressionDecorated(Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3), True), None)
        ])
        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3),
                        Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3)]

        assert listen == [(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3), None),
                        (Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3), None)]

        # Tracking the same impression a ms later should return the imp
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-2), True), None)
        ])
        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-2, utc_now-3)]
        assert listen == [(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-2, utc_now-3), None)]
        assert for_counter == []
        assert for_unique_keys_tracker == []

        # Tracking a in impression with a different key makes it to the queue
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1), True), None)
        ])
        assert imps == [Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1)]
        assert listen == [(Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1), None)]
        assert for_counter == []
        assert for_unique_keys_tracker == []

        # Advance the perceived clock one hour
        old_utc = utc_now  # save it to compare captured impressions
        utc_now += 3600 * 1000
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        # Track the same impressions but "one hour later"
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1), True), None),
            (ImpressionDecorated(Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2), True), None)
        ])
        assert imps == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1, old_utc-3),
                        Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2, old_utc-1)]
        assert listen == [
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1, old_utc-3), None),
            (Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2, old_utc-1), None)
        ]
        assert len(manager._strategy._observer._cache._data) == 3  # distinct impressions seen
        assert for_counter == []
        assert for_unique_keys_tracker == []

    def test_standalone_none_listener(self, mocker):
        """Test impressions manager in none mode with sdk in standalone mode."""

        # Mock utc_time function to be able to play with the clock
        utc_now = truncate_time(utctime_ms_reimplement()) + 1800 * 1000
        utc_time_mock = mocker.Mock()
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        manager = Manager(StrategyNoneMode(), StrategyNoneMode(), mocker.Mock())
        assert isinstance(manager._strategy, StrategyNoneMode)

        # An impression that hasn't happened in the last hour (pt = None) should not be tracked
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3), True), None),
            (ImpressionDecorated(Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3), True), None)
        ])
        assert imps == []
        assert listen == [(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3), None),
                        (Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3), None)]

        assert for_counter == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3),
                               Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3)]
        assert for_unique_keys_tracker == [('k1', 'f1'), ('k1', 'f2')]

        # Tracking the same impression a ms later should return empty, no updates on mtk
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-2), True), None)
        ])
        assert imps == []
        assert listen == [(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-2, None), None)]
        assert for_counter == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-2)]
        assert for_unique_keys_tracker == [('k1', 'f1')]

        # Tracking a in impression with a different key update mtk
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1), True), None)
        ])
        assert imps == []
        assert listen == [(Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1), None)]
        assert for_counter == [Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-1)]
        assert for_unique_keys_tracker == [('k2', 'f1')]

        # Advance the perceived clock one hour
        old_utc = utc_now  # save it to compare captured impressions
        utc_now += 3600 * 1000
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())

        # Track the same impressions but "one hour later"
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1), True), None),
            (ImpressionDecorated(Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2), True), None)
        ])
        assert imps == []
        assert for_counter == [Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1),
                               Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2)]
        assert listen == [
            (Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-1, None), None),
            (Impression('k2', 'f1', 'on', 'l1', 123, None, utc_now-2, None), None)
        ]
        assert for_unique_keys_tracker == [('k1', 'f1'), ('k2', 'f1')]

    def test_impression_toggle_optimized(self, mocker):
        """Test impressions manager in optimized mode with sdk in standalone mode."""

        # Mock utc_time function to be able to play with the clock
        utc_now = truncate_time(utctime_ms_reimplement()) + 1800 * 1000
        utc_time_mock = mocker.Mock()
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        manager = Manager(StrategyOptimizedMode(), StrategyNoneMode(), telemetry_runtime_producer)  # no listener
        assert manager._strategy._observer is not None
        assert isinstance(manager._strategy, StrategyOptimizedMode)
        assert isinstance(manager._none_strategy, StrategyNoneMode)

        # An impression that hasn't happened in the last hour (pt = None) should be tracked
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3), False), None),
            (ImpressionDecorated(Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3), True), None)
        ])

        assert for_unique_keys_tracker == [('k1', 'f1')]
        assert imps == [Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3)]
        assert deduped == 1

    def test_impression_toggle_debug(self, mocker):
        """Test impressions manager in optimized mode with sdk in standalone mode."""

        # Mock utc_time function to be able to play with the clock
        utc_now = truncate_time(utctime_ms_reimplement()) + 1800 * 1000
        utc_time_mock = mocker.Mock()
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        manager = Manager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)  # no listener
        assert manager._strategy._observer is not None

        # An impression that hasn't happened in the last hour (pt = None) should be tracked
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3), False), None),
            (ImpressionDecorated(Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3), True), None)
        ])

        assert for_unique_keys_tracker == [('k1', 'f1')]
        assert imps == [Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3)]
        assert deduped == 1

    def test_impression_toggle_none(self, mocker):
        """Test impressions manager in optimized mode with sdk in standalone mode."""

        # Mock utc_time function to be able to play with the clock
        utc_now = truncate_time(utctime_ms_reimplement()) + 1800 * 1000
        utc_time_mock = mocker.Mock()
        utc_time_mock.return_value = utc_now
        mocker.patch('splitio.engine.impressions.strategies.utctime_ms', return_value=utc_time_mock())
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        strategy = StrategyNoneMode()
        manager = Manager(strategy, strategy, telemetry_runtime_producer)  # no listener

        # An impression that hasn't happened in the last hour (pt = None) should be tracked
        imps, deduped, listen, for_counter, for_unique_keys_tracker = manager.process_impressions([
            (ImpressionDecorated(Impression('k1', 'f1', 'on', 'l1', 123, None, utc_now-3), False), None),
            (ImpressionDecorated(Impression('k1', 'f2', 'on', 'l1', 123, None, utc_now-3), True), None)
        ])

        assert for_unique_keys_tracker == [('k1', 'f1'), ('k1', 'f2')]
        assert imps == []
        assert deduped == 2
