import abc

from splitio.engine.impressions.manager import Observer, truncate_time
from splitio.util.time import utctime_ms

_IMPRESSION_OBSERVER_CACHE_SIZE = 500000

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

        :returns: Tuple of to be stored, observed and counted impressions, and unique keys tuple
        :rtype: list[tuple[splitio.models.impression.Impression, dict]], list[], list[], list[]
        """
        imps = [(self._observer.test_and_set(imp), attrs) for imp, attrs in impressions]
        return [i for i, _ in imps], imps, [], []

class StrategyNoneMode(BaseStrategy):
    """Debug mode strategy."""

    def process_impressions(self, impressions):
        """
        Process impressions.

        Impressions are analyzed to see if they've been seen before and counted.
        Unique keys tracking are updated.

        :param impressions: List of impression objects with attributes
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]

        :returns: Tuple of to be stored, observed and counted impressions, and unique keys tuple
        :rtype: list[[], dict]], list[splitio.models.impression.Impression], list[splitio.models.impression.Impression], list[(str, str)]
        """
        counter_imps = [imp for imp, _ in impressions]
        unique_keys_tracker = []
        for i, _ in impressions:
            unique_keys_tracker.append((i.matching_key, i.feature_name))
        return [], impressions, counter_imps, unique_keys_tracker

class StrategyOptimizedMode(BaseStrategy):
    """Optimized mode strategy."""

    def __init__(self):
        """
        Construct a strategy instance for optimized mode.

        """
        self._observer = Observer(_IMPRESSION_OBSERVER_CACHE_SIZE)

    def process_impressions(self, impressions):
        """
        Process impressions.

        Impressions are analyzed to see if they've been seen before and counted.

        :param impressions: List of impression objects with attributes
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]

        :returns: Tuple of to be stored, observed and counted impressions, and unique keys tuple
        :rtype: list[tuple[splitio.models.impression.Impression, dict]], list[splitio.models.impression.Impression], list[splitio.models.impression.Impression], list[]
        """
        imps = [(self._observer.test_and_set(imp), attrs) for imp, attrs in impressions]
        counter_imps = [imp for imp, _ in imps if imp.previous_time != None]
        this_hour = truncate_time(utctime_ms())
        return [i for i, _ in imps if i.previous_time is None or i.previous_time < this_hour], imps, counter_imps, []
