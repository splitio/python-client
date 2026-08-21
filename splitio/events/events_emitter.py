"""Events Manager."""
import logging

from splitio_commons.events.events_metadata import EventsMetadata
from splitio_commons.models.notification import SdkInternalEventNotification

_LOGGER = logging.getLogger(__name__)

class EventsEmitter(object):
    """Events Emitter class."""

    def __init__(self, internal_event_queue):
        """
        Construct Events Emitter instance.
        """
        self._internal_event_queue = internal_event_queue

    def emit(self, sdk_internal_event_type, sdk_event_type, event_metadata={}):
        metadata = None
        if sdk_event_type != None:
            _LOGGER.debug("Emitting SDKEventType %s", sdk_event_type)
            metadata = EventsMetadata(sdk_event_type, event_metadata)
            
        self._internal_event_queue.put(
            SdkInternalEventNotification(
                sdk_internal_event_type, metadata))
        
    async def emit_async(self, sdk_internal_event_type, sdk_event_type, event_metadata={}):
        metadata = None
        if sdk_event_type != None:
            _LOGGER.debug("Emitting SDKEventType %s", sdk_event_type)
            metadata = EventsMetadata(sdk_event_type, event_metadata)

        await self._internal_event_queue.put(
            SdkInternalEventNotification(
                sdk_internal_event_type,
                metadata))
