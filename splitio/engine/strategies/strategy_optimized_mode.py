from splitio.engine.strategies.base_strategy import BaseStrategy
from splitio.engine.strategies import Observer, Counter, truncate_time
from splitio import util

_IMPRESSION_OBSERVER_CACHE_SIZE = 500000

class StrategyOptimizedMode(BaseStrategy):
    """Optimized mode strategy."""

    def __init__(self, counter=None, observer=None, standalone=True):
        """
        Construct a strategy instance for optimized mode.

        """
        self._standalone = standalone
        self._counter = counter # Counter() if self._standalone else None
        self._observer = observer # Observer(_IMPRESSION_OBSERVER_CACHE_SIZE) if self._standalone else None

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
            self._counter.track([imp for imp, _ in forLog])

        forLog = self.truncate_impressions_time(impressions)
        
        return forLog, forListener

    def _truncate_impressions_time(self, imps):
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

    # def get_counts(self):
    #     """
    #     Return counts of impressions per features.

    #     :returns: A list of counter objects.
    #     :rtype: list[Counter.CountPerFeature]
    #     """
    #     return self._counter.pop_all() if self._counter is not None else []
