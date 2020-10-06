"""Split evaluator module."""
import logging
import threading
from collections import defaultdict, namedtuple
from enum import Enum

import six

from splitio.models.impressions import Impression
from splitio.engine.hashfns import murmur_128
from splitio.engine.cache.lru import SimpleLruCache
from splitio import util


_TIME_INTERVAL_MS = 3600 * 1000
_IMPRESSION_OBSERVER_CACHE_SIZE = 500000


class ImpressionsMode(Enum):
    """Impressions tracking mode."""

    OPTIMIZED = "OPTIMIZED"
    DEBUG = "DEBUG"


def truncate_time(timestamp_ms):
    """
    Truncate a timestamp in milliseconds to have hour granularity.

    :param timestamp_ms: timestamp generated in the impression.
    :type timestamp_ms: int

    :returns: a timestamp with hour, min, seconds, and ms set to 0.
    :rtype: int
    """
    return timestamp_ms - (timestamp_ms % _TIME_INTERVAL_MS)


class Hasher(object):
    """Impression hasher."""

    _PATTERN = "%s:%s:%s:%s:%d"

    def __init__(self, hash_fn=murmur_128, seed=0):
        """
        Class constructor.

        :param hash_fn: Hash function to apply (str, int) -> int
        :type hash_fn: callable

        :param seed: seed to be provided when hashing
        :type seed: int
        """
        self._hash_fn = hash_fn
        self._seed = seed

    def _stringify(self, impression):
        """
        Stringify an impression.

        :param impression: Impression to stringify using _PATTERN
        :type impression: splitio.models.impressions.Impression

        :returns: a string representation of the impression
        :rtype: str
        """
        return self._PATTERN % (impression.matching_key,
                                impression.feature_name,
                                impression.treatment,
                                impression.label,
                                impression.change_number)

    def process(self, impression):
        """
        Hash an impression.

        :param impression: Impression to hash.
        :type impression: splitio.models.impressions.Impression

        :returns: a hash of the supplied impression's relevant fields.
        :rtype: int
        """
        return self._hash_fn(self._stringify(impression), self._seed)


class Observer(object):
    """Observe impression and add a previous time if applicable."""

    def __init__(self, size):
        """Class constructor."""
        self._hasher = Hasher()
        self._cache = SimpleLruCache(size)

    def test_and_set(self, impression):
        """
        Examine an impression to determine and set it's previous time accordingly.

        :param impression: Impression to track
        :type impression: splitio.models.impressions.Impression

        :returns: Impression with populated previous time
        :rtype: splitio.models.impressions.Impression
        """
        previous_time = self._cache.test_and_set(self._hasher.process(impression), impression.time)
        return Impression(impression.matching_key,
                          impression.feature_name,
                          impression.treatment,
                          impression.label,
                          impression.change_number,
                          impression.bucketing_key,
                          impression.time,
                          previous_time)


class Counter(object):
    """Class that counts impressions per timeframe."""

    CounterKey = namedtuple('Count', ['feature', 'timeframe'])
    CountPerFeature = namedtuple('Count', ['feature', 'timeframe', 'count'])

    def __init__(self):
        """Class constructor."""
        self._data = defaultdict(lambda: 0)
        self._lock = threading.Lock()

    def track(self, impressions, inc=1):
        """
        Register N new impressions for a feature in a specific timeframe.

        :param impressions: generated impressions
        :type impressions: list[splitio.models.impressions.Impression]

        :param inc: amount to increment (defaults to 1)
        :type inc: int
        """
        keys = [Counter.CounterKey(i.feature_name, truncate_time(i.time)) for i in impressions]
        with self._lock:
            for key in keys:
                self._data[key] += inc

    def pop_all(self):
        """
        Clear and return all the counters currently stored.

        :returns: List of count per feature/timeframe objects
        :rtype: list[ImpressionCounter.CountPerFeature]
        """
        with self._lock:
            old = self._data
            self._data = defaultdict(lambda: 0)

        return [Counter.CountPerFeature(k.feature, k.timeframe, v)
                for (k, v) in six.iteritems(old)]


class Manager(object):
    """Impression manager."""

    def __init__(self, forwarder, mode=ImpressionsMode.OPTIMIZED, standalone=True, listener=None):
        """
        Construct a manger to track and forward impressions to the queue.

        :param forwarder: function accepting a list of impressions to be added to the queue.
        :type forwarder: callable[list[splitio.models.impressions.Impression]]

        :param mode: Impressions capturing mode.
        :type mode: ImpressionsMode

        :param standalone: whether the SDK is running in standalone sending impressions by itself
        :type standalone: bool

        :param listener: Optional impressions listener that will capture all seen impressions.
        :type listener: splitio.client.listener.ImpressionListenerWrapper
        """
        self._forwarder = forwarder
        self._observer = Observer(_IMPRESSION_OBSERVER_CACHE_SIZE) if standalone else None
        self._counter = Counter() if standalone and mode == ImpressionsMode.OPTIMIZED else None
        self._listener = listener

    def track(self, impressions):
        """TODO."""
        imps = [self._observer.test_and_set(i) for i in impressions] if self._observer \
                else impressions

        if self._counter:
            self._counter.track(imps)

        if self._listener:
            for imp in imps:
                self._listener.log_impression(imp)

        this_hour = truncate_time(util.utctime_ms())
        self._forwarder(imps if self._counter is None
                        else [i for i in imps if i.previous_time is None or i.previous_time < this_hour])
