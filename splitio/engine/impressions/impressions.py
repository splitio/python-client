"""Split evaluator module."""
from enum import Enum

from splitio.client.listener import ImpressionListenerException

class ImpressionsMode(Enum):
    """Impressions tracking mode."""

    OPTIMIZED = "OPTIMIZED"
    DEBUG = "DEBUG"
    NONE = "NONE"

class Manager(object):  # pylint:disable=too-few-public-methods
    """Impression manager."""

    def __init__(self, listener=None, strategy=None):
        """
        Construct a manger to track and forward impressions to the queue.

        :param listener: Optional impressions listener that will capture all seen impressions.
        :type listener: splitio.client.listener.ImpressionListenerWrapper

        :param strategy: Impressions stragetgy instance
        :type strategy: (BaseStrategy)
        """

        self._strategy = strategy
        self._listener = listener

    def process_impressions(self, impressions):
        """
        Process impressions.

        Impressions are analyzed to see if they've been seen before and counted.

        :param impressions: List of impression objects with attributes
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]
        """
        for_log, for_listener = self._strategy.process_impressions(impressions)
        self._send_impressions_to_listener(for_listener)
        return for_log

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
