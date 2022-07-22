from splitio.engine.strategies.base_strategy import BaseStrategy
from splitio.engine.strategies import Observer, truncate_impressions_time

_IMPRESSION_OBSERVER_CACHE_SIZE = 500000

class StrategyDebugMode(BaseStrategy):
    """Debug mode strategy."""

    def __init__(self, observer=None, standalone=True):
        """
        Construct a strategy instance for debug mode.

        """
        self._standalone = standalone
        self._observer =  observer

    def process_impressions(self, impressions):
        """
        Process impressions.

        Impressions are analyzed to see if they've been seen before.

        :param impressions: List of impression objects with attributes
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]

        :returns: Observed list of impressions
        :rtype: list[tuple[splitio.models.impression.Impression, dict]]
        """
        for_listener = [(self._observer.test_and_set(imp), attrs) for imp, attrs in impressions] if self._observer is not None else impressions
        return truncate_impressions_time(for_listener, None), for_listener