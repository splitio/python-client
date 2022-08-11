from splitio.engine.strategies.base_strategy import BaseStrategy
from splitio.engine.strategies import Observer, Counter, truncate_time
from splitio.engine.unique_keys_tracker import UniqueKeysTracker
from splitio import util

_IMPRESSION_OBSERVER_CACHE_SIZE = 500000
_UNIQUE_KEYS_CACHE_SIZE = 30000

class StrategyNoneMode(BaseStrategy):
    """Debug mode strategy."""

    def __init__(self, counter=None):
        """
        Construct a strategy instance for none mode.

        """
        self._observer = Observer(_IMPRESSION_OBSERVER_CACHE_SIZE)
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
        imps = [(self._observer.test_and_set(imp), attrs) for imp, attrs in impressions]
        self._counter.track([imp for imp, _ in imps])
        this_hour = truncate_time(util.utctime_ms())
        [self._unique_keys_tracker(i.matching_key, i.feature_name) for i, _ in imps if i.previous_time is None or i.previous_time < this_hour], imps

        return []
