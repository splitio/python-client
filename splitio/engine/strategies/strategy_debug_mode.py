from splitio.engine.strategies.base_strategy import BaseStrategy
from splitio.engine.strategies import Observer

_IMPRESSION_OBSERVER_CACHE_SIZE = 500000

class StrategyDebugMode(BaseStrategy):
    """Debug mode strategy."""

    def __init__(self, observer=None, standalone=True):
        """
        Construct a strategy instance for debug mode.

        """
        self._standalone = standalone
        self._observer = observer # Observer(_IMPRESSION_OBSERVER_CACHE_SIZE) if self._standalone else None

    def process_impressions(self, impressions):
        """
        Process impressions.

        Impressions are analyzed to see if they've been seen before.

        :param impressions: List of impression objects with attributes
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]

        :returns: Observed list of impressions
        :rtype: list[tuple[splitio.models.impression.Impression, dict]]
        """
        forLog = [(self._observer.test_and_set(imp), attrs) for imp, attrs in impressions] if self._observer is not None else impressions
        return forLog, forLog

    # def truncate_impressions_time(self, imps):
    #     """
    #     No counter implemented, return same impresisons passed.

    #     :returns: list of impressions
    #     :rtype: list[splitio.models.impression.Impression]
    #     """
    #     return [imp for imp, _ in imps]

    # def get_counts(self):
    #     """
    #     No counter implemented, return empty array

    #     :returns: empty list
    #     :rtype: list[]
    #     """
    #     return []
