"""Split evaluator module."""
from enum import Enum

class ImpressionsMode(Enum):
    """Impressions tracking mode."""

    OPTIMIZED = "OPTIMIZED"
    DEBUG = "DEBUG"
    NONE = "NONE"

class Manager(object):  # pylint:disable=too-few-public-methods
    """Impression manager."""

    def __init__(self, strategy, telemetry_runtime_producer):
        """
        Construct a manger to track and forward impressions to the queue.

        :param listener: Optional impressions listener that will capture all seen impressions.
        :type listener: splitio.client.listener.ImpressionListenerWrapper

        :param strategy: Impressions stragetgy instance
        :type strategy: (BaseStrategy)
        """

        self._strategy = strategy
        self._telemetry_runtime_producer = telemetry_runtime_producer

    def process_impressions(self, impressions):
        """
        Process impressions.

        Impressions are analyzed to see if they've been seen before and counted.

        :param impressions: List of impression objects with attributes
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]

        :return: processed and deduped impressions.
        :rtype: tuple(list[tuple[splitio.models.impression.Impression, dict]], list(int))
        """
        for_log, for_listener, for_counter, for_unique_keys_tracker = self._strategy.process_impressions(impressions)
        return for_log, len(impressions) - len(for_log), for_listener, for_counter, for_unique_keys_tracker
