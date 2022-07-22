from splitio.engine.strategies.base_strategy import BaseStrategy
from splitio.engine.strategies import Observer, Counter, truncate_impressions_time
from splitio import util

class StrategyOptimizedMode(BaseStrategy):
    """Optimized mode strategy."""

    def __init__(self, counter=None, observer=None, standalone=True):
        """
        Construct a strategy instance for optimized mode.

        """
        self._standalone = standalone
        self._counter = counter
        self._observer =  observer

    def process_impressions(self, impressions):
        """
        Process impressions.

        Impressions are analyzed to see if they've been seen before and counted.

        :param impressions: List of impression objects with attributes
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]

        :returns: Observed list of impressions
        :rtype: list[tuple[splitio.models.impression.Impression, dict]]
        """
        forListener = [(self._observer.test_and_set(imp), attrs) for imp, attrs in impressions] if self._observer else impressions
        if self._counter is not None:
            self._counter.track([imp for imp, _ in forListener])
        return truncate_impressions_time(forListener, self._counter), forListener
