"""Events Manager."""
import logging

from splitio.events import EventsDeliveryInterface

_LOGGER = logging.getLogger(__name__)

class EventsDelivery(EventsDeliveryInterface):
    """Events Manager class."""

    def __init__(self):
        """
        Construct Events Manager instance.
        """

    def deliver(self, sdk_event, event_metadata, event_handler):
        try:
            event_handler(event_metadata)
        except Exception as ex:
            _LOGGER.error("Exception when calling handler for Sdk Event %s", sdk_event)
            _LOGGER.error(ex)
