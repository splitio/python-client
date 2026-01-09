"""EventsManager test module."""
from splitio.models.events import SdkEvent, SdkInternalEvent
from splitio.events.events_metadata import EventsMetadata
from splitio.events.events_delivery import EventsDelivery
from splitio.events.events_metadata import SdkEventType

class EventsDeliveryTests(object):
    """Tests for EventsManager."""

    sdk_ready_flag = False
    metadata = None
    
    def test_firing_events(self):
        events_delivery = EventsDelivery()
        
        metadata = EventsMetadata(SdkEventType.FLAG_UPDATE, { "feature1" })
        events_delivery.deliver(SdkEvent.SDK_READY, metadata, self._sdk_ready_callback)
        assert self.sdk_ready_flag
        self._verify_metadata(metadata)
                    
    def _sdk_ready_callback(self, metadata):
        self.sdk_ready_flag = True
        self.metadata = metadata

    def _verify_metadata(self, metadata):
        assert metadata.get_type() == self.metadata.get_type()
        assert metadata.get_names() == self.metadata.get_names()