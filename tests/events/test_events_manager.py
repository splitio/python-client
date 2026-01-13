"""EventsManager test module."""
import pytest
from splitio.models.events import SdkEvent, SdkInternalEvent
from splitio.events.events_metadata import EventsMetadata
from splitio.events.events_manager_config import EventsManagerConfig
from splitio.events.events_delivery import EventsDelivery
from splitio.events.events_manager import EventsManager
from splitio.events.events_metadata import SdkEventType

class EventsManagerTests(object):
    """Tests for EventsManager."""

    sdk_ready_flag = False
    sdk_timed_out_flag = False
    sdk_update_flag = False
    metadata = None
    
    def test_firing_events(self):
        events_manager = EventsManager(EventsManagerConfig(), EventsDelivery())
        events_manager.register(SdkEvent.SDK_READY, self._sdk_ready_callback)
        events_manager.register(SdkEvent.SDK_UPDATE, self._sdk_update_callback)
        
        metadata = EventsMetadata(SdkEventType.FLAG_UPDATE, { "feature1" })
        events_manager.notify_internal_event(SdkInternalEvent.FLAGS_UPDATED, metadata)    
        events_manager.notify_internal_event(SdkInternalEvent.FLAG_KILLED_NOTIFICATION, metadata)    
        events_manager.notify_internal_event(SdkInternalEvent.RB_SEGMENTS_UPDATED, metadata)    
        events_manager.notify_internal_event(SdkInternalEvent.SEGMENTS_UPDATED, metadata)    
        assert not self.sdk_ready_flag
        assert not self.sdk_timed_out_flag
        assert not self.sdk_update_flag
        
        self._reset_flags()
        events_manager.notify_internal_event(SdkInternalEvent.SDK_TIMED_OUT, metadata)    
        assert not self.sdk_ready_flag
        assert not self.sdk_timed_out_flag # not registered yet
        assert not self.sdk_update_flag
        
        events_manager.register(SdkEvent.SDK_READY_TIMED_OUT, self._sdk_timeout_callback)
        events_manager.notify_internal_event(SdkInternalEvent.SDK_TIMED_OUT, metadata)    
        assert not self.sdk_ready_flag
        assert self.sdk_timed_out_flag
        assert not self.sdk_update_flag
        self._verify_metadata(metadata)

        self._reset_flags()
        events_manager.notify_internal_event(SdkInternalEvent.SDK_READY, metadata)    
        assert self.sdk_ready_flag
        assert not self.sdk_timed_out_flag
        assert not self.sdk_update_flag
        self._verify_metadata(metadata)

        self._reset_flags()
        events_manager.notify_internal_event(SdkInternalEvent.RB_SEGMENTS_UPDATED, metadata)    
        assert not self.sdk_ready_flag
        assert not self.sdk_timed_out_flag
        assert self.sdk_update_flag
        self._verify_metadata(metadata)
        
        self._reset_flags()
        events_manager.notify_internal_event(SdkInternalEvent.FLAG_KILLED_NOTIFICATION, metadata)    
        assert not self.sdk_ready_flag
        assert not self.sdk_timed_out_flag
        assert self.sdk_update_flag
        self._verify_metadata(metadata)

        self._reset_flags()
        events_manager.notify_internal_event(SdkInternalEvent.FLAGS_UPDATED, metadata)    
        assert not self.sdk_ready_flag
        assert not self.sdk_timed_out_flag
        assert self.sdk_update_flag
        self._verify_metadata(metadata)

        self._reset_flags()
        events_manager.notify_internal_event(SdkInternalEvent.SEGMENTS_UPDATED, metadata)    
        assert not self.sdk_ready_flag
        assert not self.sdk_timed_out_flag
        assert self.sdk_update_flag
        self._verify_metadata(metadata)
    
    def _reset_flags(self):
        self.sdk_ready_flag = False
        self.sdk_timed_out_flag = False
        self.sdk_update_flag = False
        self.metadata = None
        
    def _sdk_ready_callback(self, metadata):
        self.sdk_ready_flag = True
        self.metadata = metadata

    def _sdk_update_callback(self, metadata):
        self.sdk_update_flag = True
        self.metadata = metadata

    def _sdk_timeout_callback(self, metadata):
        self.sdk_timed_out_flag = True
        self.metadata = metadata

    def _verify_metadata(self, metadata):
        assert metadata.get_type() == self.metadata.get_type()
        assert metadata.get_names() == self.metadata.get_names()