from splitio.engine.strategies.base_strategy import BaseStrategy
from splitio.engine.strategies import  Counter, truncate_time
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
        [self._unique_keys_tracker.track(i.matching_key, i.feature_name) for i, _ in impressions]
        return [], impressions
