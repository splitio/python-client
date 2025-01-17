"""Split evaluator module."""
from enum import Enum

class ImpressionsMode(Enum):
    """Impressions tracking mode."""

    OPTIMIZED = "OPTIMIZED"
    DEBUG = "DEBUG"
    NONE = "NONE"

class Manager(object):  # pylint:disable=too-few-public-methods
    """Impression manager."""

    def __init__(self, strategy, none_strategy, telemetry_runtime_producer):
        """
        Construct a manger to track and forward impressions to the queue.

        :param listener: Optional impressions listener that will capture all seen impressions.
        :type listener: splitio.client.listener.ImpressionListenerWrapper

        :param strategy: Impressions stragetgy instance
        :type strategy: (BaseStrategy)
        """

        self._strategy = strategy
        self._none_strategy = none_strategy
        self._telemetry_runtime_producer = telemetry_runtime_producer

    def process_impressions(self, impressions_decorated):
        """
        Process impressions.

        Impressions are analyzed to see if they've been seen before and counted.

        :param impressions_decorated: List of impression objects with attributes
        :type impressions_decorated: list[tuple[splitio.models.impression.ImpressionDecorated, dict]]

        :return: processed and deduped impressions.
        :rtype: tuple(list[tuple[splitio.models.impression.Impression, dict]], list(int))
        """
        for_listener_all = []
        for_log_all = []
        for_counter_all = []
        for_unique_keys_tracker_all = []
        for impression_decorated, att in impressions_decorated:
            if impression_decorated.disabled:
                for_log, for_listener, for_counter, for_unique_keys_tracker = self._none_strategy.process_impressions([(impression_decorated.Impression, att)])
            else:
                for_log, for_listener, for_counter, for_unique_keys_tracker = self._strategy.process_impressions([(impression_decorated.Impression, att)])
            for_listener_all.extend(for_listener)
            for_log_all.extend(for_log)
            for_counter_all.extend(for_counter)
            for_unique_keys_tracker_all.extend(for_unique_keys_tracker)

        return for_log_all, len(impressions_decorated) - len(for_log_all), for_listener_all, for_counter_all, for_unique_keys_tracker_all
