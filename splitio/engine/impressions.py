"""Split evaluator module."""
import abc
import threading
from collections import defaultdict, namedtuple
from enum import Enum

from splitio.models.impressions import Impression
from splitio.engine.hashfns import murmur_128
from splitio.engine.cache.lru import SimpleLruCache
from splitio.client.listener import ImpressionListenerException
from splitio import util

import logging
_LOGGER = logging.getLogger(__name__)

_TIME_INTERVAL_MS = 3600 * 1000  # one hour
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


class Hasher(object):  # pylint:disable=too-few-public-methods
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
        return self._PATTERN % (impression.matching_key if impression.matching_key else 'UNKNOWN',
                                impression.feature_name if impression.feature_name else 'UNKNOWN',
                                impression.treatment if impression.treatment else 'UNKNOWN',
                                impression.label if impression.label else 'UNKNOWN',
                                impression.change_number if impression.change_number else 0)

    def process(self, impression):
        """
        Hash an impression.

        :param impression: Impression to hash.
        :type impression: splitio.models.impressions.Impression

        :returns: a hash of the supplied impression's relevant fields.
        :rtype: int
        """
        return self._hash_fn(self._stringify(impression), self._seed)


class Observer(object):  # pylint:disable=too-few-public-methods
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
    CountPerFeature = namedtuple('CountPerFeature', ['feature', 'timeframe', 'count'])

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
                for (k, v) in old.items()]

class BaseStrategy(object, metaclass=abc.ABCMeta):
    """Strategy interface."""

    @abc.abstractmethod
    def process_impressions(self):
        """
        Return a list(impressions) object

        """
        pass

    @abc.abstractmethod
    def truncate_impressions_time(self):
        """
        Return list(impressions) object

        """
        pass

    def get_counts(self):
        """
        Return A list of counter objects.

        """
        pass

class StrategyOptimizedMode(BaseStrategy):
    """Optimized mode strategy."""

    def __init__(self, standalone=True):
        """
        Construct a strategy instance for optimized mode.

        """
        self._standalone = standalone
        self._counter = Counter() if self._standalone else None
        self._observer =  Observer(_IMPRESSION_OBSERVER_CACHE_SIZE) if self._standalone else None

    def process_impressions(self, impressions):
        """
        Process impressions.

        Impressions are analyzed to see if they've been seen before and counted.

        :param impressions: List of impression objects with attributes
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]

        :returns: Observed list of impressions
        :rtype: list[tuple[splitio.models.impression.Impression, dict]]
        """
        imps = [(self._observer.test_and_set(imp), attrs) for imp, attrs in impressions] \
        if self._observer else impressions
        if self._counter is not None:
            self._counter.track([imp for imp, _ in imps])
        return imps

    def truncate_impressions_time(self, imps):
        """
        Process impressions.

        Impressions are truncated based on time

        :param impressions: List of impression objects with attributes
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]

        :returns: truncated list of impressions
        :rtype: list[splitio.models.impression.Impression]
        """
        this_hour = truncate_time(util.utctime_ms())
        return [imp for imp, _ in imps] if self._counter is None \
            else [i for i, _ in imps if i.previous_time is None or i.previous_time < this_hour]

    def get_counts(self):
        """
        Return counts of impressions per features.

        :returns: A list of counter objects.
        :rtype: list[Counter.CountPerFeature]
        """
        return self._counter.pop_all() if self._counter is not None else []

class StrategyDebugMode(BaseStrategy):
    """Debug mode strategy."""

    def __init__(self, standalone=True):
        """
        Construct a strategy instance for debug mode.

        """
        self._standalone = standalone
        self._observer =  Observer(_IMPRESSION_OBSERVER_CACHE_SIZE) if self._standalone else None

    def process_impressions(self, impressions):
        """
        Process impressions.

        Impressions are analyzed to see if they've been seen before.

        :param impressions: List of impression objects with attributes
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]

        :returns: Observed list of impressions
        :rtype: list[tuple[splitio.models.impression.Impression, dict]]
        """
        imps = [(self._observer.test_and_set(imp), attrs) for imp, attrs in impressions] if self._observer is not None else impressions
        return imps

    def truncate_impressions_time(self, imps):
        """
        No counter implemented, return same impresisons passed.

        :returns: list of impressions
        :rtype: list[splitio.models.impression.Impression]
        """
        return [imp for imp, _ in imps]

    def get_counts(self):
        """
        No counter implemented, return empty array

        :returns: empty list
        :rtype: list[]
        """
        return []

class Manager(object):  # pylint:disable=too-few-public-methods
    """Impression manager."""

    def __init__(self, mode=ImpressionsMode.OPTIMIZED, standalone=True, listener=None):
        """
        Construct a manger to track and forward impressions to the queue.

        :param mode: Impressions capturing mode.
        :type mode: ImpressionsMode

        :param standalone: whether the SDK is running in standalone sending impressions by itself
        :type standalone: bool

        :param listener: Optional impressions listener that will capture all seen impressions.
        :type listener: splitio.client.listener.ImpressionListenerWrapper
        """
        self._strategy = self.get_strategy(mode, standalone)
        self._listener = listener

    def get_strategy(self, mode, standalone):
        """
        Return a strategy object based on mode value.

        :returns: A strategy object
        :rtype: (BaseStrategy)
        """
        return StrategyOptimizedMode(standalone) if mode == ImpressionsMode.OPTIMIZED else StrategyDebugMode(standalone)

    def process_impressions(self, impressions):
        """
        Process impressions.

        Impressions are analyzed to see if they've been seen before and counted.

        :param impressions: List of impression objects with attributes
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]
        """
        imps = self._strategy.process_impressions(impressions)
        self._send_impressions_to_listener(imps)
        return self._strategy.truncate_impressions_time(imps)

    def get_counts(self):
        """
        Return counts of impressions per features.

        :returns: A list of counter objects.
        :rtype: list[Counter.CountPerFeature]
        """
        return self._strategy.get_counts()

    def _send_impressions_to_listener(self, impressions):
        """
        Send impression result to custom listener.

        :param impressions: List of impression objects with attributes
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]
        """
        if self._listener is not None:
            try:
                for impression, attributes in impressions:
                    self._listener.log_impression(impression, attributes)
            except ImpressionListenerException:
                pass
#                self._logger.error('An exception was raised while calling user-custom impression listener')
#                self._logger.debug('Error', exc_info=True)
