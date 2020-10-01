"""Impression manager, observer & hasher tests."""

from splitio.engine.impmanager import Hasher, Observer, Manager
from splitio.models.impressions import Impression


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
