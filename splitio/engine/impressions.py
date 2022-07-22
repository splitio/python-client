"""Split evaluator module."""
from enum import Enum

from splitio.client.listener import ImpressionListenerException
from splitio.engine.strategies import Observer
from splitio.engine.strategies.strategy_debug_mode import StrategyDebugMode
from splitio.engine.strategies.strategy_optimized_mode import StrategyOptimizedMode

import logging
_LOGGER = logging.getLogger(__name__)

class ImpressionsMode(Enum):
    """Impressions tracking mode."""

    OPTIMIZED = "OPTIMIZED"
    DEBUG = "DEBUG"

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

        observer = Observer(_IMPRESSION_OBSERVER_CACHE_SIZE) if self._standalone else None

        if mode == ImpressionsMode.OPTIMIZED:
            self._counter = Counter() if self._standalone else None
            return StrategyOptimizedMode(self._counter, observer, standalone)
        

        return StrategyDebugMode(observer, standalone)

    def process_impressions(self, impressions):
        """
        Process impressions.

        Impressions are analyzed to see if they've been seen before and counted.

        :param impressions: List of impression objects with attributes
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]
        """

        forLog, forListener = self._strategy.process_impressions(impressions)
        self._send_impressions_to_listener(forListener)

        return forLog
    def get_counts(self):
        """
        Return counts of impressions per features.

        :returns: A list of counter objects.
        :rtype: list[Counter.CountPerFeature]
        """
        return self._counter.pop_all() if self._counter is not None else []

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
