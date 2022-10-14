import abc

from splitio.engine.impressions.manager import Observer, truncate_impressions_time, Counter, truncate_time
from splitio.engine.impressions.unique_keys_tracker import UniqueKeysTracker
from splitio import util

_IMPRESSION_OBSERVER_CACHE_SIZE = 500000
_UNIQUE_KEYS_CACHE_SIZE = 30000

class BaseStrategy(object, metaclass=abc.ABCMeta):
    """Strategy interface."""

    @abc.abstractmethod
    def process_impressions(self):
        """
        Return a list(impressions) object

        """
        pass

class StrategyDebugMode(BaseStrategy):
    """Debug mode strategy."""

    def __init__(self):
        """
        Construct a strategy instance for debug mode.

        """
        self._observer = Observer(_IMPRESSION_OBSERVER_CACHE_SIZE)

    def process_impressions(self, impressions):
        """
        Process impressions.

        Impressions are analyzed to see if they've been seen before.

        :param impressions: List of impression objects with attributes
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]

        :returns: Observed list of impressions
        :rtype: list[tuple[splitio.models.impression.Impression, dict]]
        """
        imps = [(self._observer.test_and_set(imp), attrs) for imp, attrs in impressions]
        return [i for i, _ in imps], imps

class StrategyNoneMode(BaseStrategy):
    """Debug mode strategy."""

    def __init__(self, counter):
        """
        Construct a strategy instance for none mode.

        """
        self._counter = counter
        self._unique_keys_tracker = UniqueKeysTracker(_UNIQUE_KEYS_CACHE_SIZE)

    def process_impressions(self, impressions):
        """
        Process impressions.

        Impressions are analyzed to see if they've been seen before and counted.
        Unique keys tracking are updated.

        :param impressions: List of impression objects with attributes
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]

        :returns: Empty list, no impressions to post
        :rtype: list[]
        """
        self._counter.track([imp for imp, _ in impressions])
        for i, _ in impressions:
            self._unique_keys_tracker.track(i.matching_key, i.feature_name)
        return [], impressions

    def get_unique_keys_tracker(self):
        return self._unique_keys_tracker

class StrategyOptimizedMode(BaseStrategy):
    """Optimized mode strategy."""

    def __init__(self, counter):
        """
        Construct a strategy instance for optimized mode.

        """
        self._observer = Observer(_IMPRESSION_OBSERVER_CACHE_SIZE)
        self._counter = counter

    def process_impressions(self, impressions):
        """
        Process impressions.

        Impressions are analyzed to see if they've been seen before and counted.

        :param impressions: List of impression objects with attributes
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]

        :returns: Observed list of impressions
        :rtype: list[tuple[splitio.models.impression.Impression, dict]]
        """
        imps = [(self._observer.test_and_set(imp), attrs) for imp, attrs in impressions]
        self._counter.track([imp for imp, _ in imps if imp.previous_time != None])
        this_hour = truncate_time(util.utctime_ms())
        return [i for i, _ in imps if i.previous_time is None or i.previous_time < this_hour], imps
