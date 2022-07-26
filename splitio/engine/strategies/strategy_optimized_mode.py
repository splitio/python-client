from splitio.engine.strategies.base_strategy import BaseStrategy
from splitio.engine.strategies import Observer, Counter, truncate_time
from splitio import util

_IMPRESSION_OBSERVER_CACHE_SIZE = 500000

class StrategyOptimizedMode(BaseStrategy):
    """Optimized mode strategy."""

    def __init__(self, standalone=True):
        """
        Construct a strategy instance for optimized mode.

        """
        self._observer = Observer(_IMPRESSION_OBSERVER_CACHE_SIZE) if standalone else None

    def process_impressions(self, impressions, counter):
        """
        Process impressions.

        Impressions are analyzed to see if they've been seen before and counted.

        :param impressions: List of impression objects with attributes
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]

        :returns: Observed list of impressions
        :rtype: list[tuple[splitio.models.impression.Impression, dict]]
        """
        imps = [(self._observer.test_and_set(imp), attrs) for imp, attrs in impressions] if self._observer else impressions
        counter.track([imp for imp, _ in imps])
        this_hour = truncate_time(util.utctime_ms())
        return [i for i, _ in imps if i.previous_time is None or i.previous_time < this_hour], imps
