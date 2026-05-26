"""EventsManager test module."""
import pytest
import asyncio

from harness_commons.models.events import SdkEvent, SdkInternalEvent
from splitio.events.events_metadata import EventsMetadata
from splitio.events.events_manager_config import EventsManagerConfig
from splitio.events.events_delivery import EventsDelivery
from splitio.events.events_manager import EventsManager, EventsManagerAsync
from splitio.events.events_metadata import SdkEventType

class EventsManagerTests(object):
    """Tests for EventsManager."""

    sdk_ready_flag = False
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
        assert not self.sdk_update_flag
        
        self._reset_flags()
        events_manager.notify_internal_event(SdkInternalEvent.SDK_READY, metadata)    
        assert self.sdk_ready_flag
        assert not self.sdk_update_flag
        self._verify_metadata(metadata)

        self._reset_flags()
        events_manager.notify_internal_event(SdkInternalEvent.RB_SEGMENTS_UPDATED, metadata)    
        assert not self.sdk_ready_flag
        assert self.sdk_update_flag
        self._verify_metadata(metadata)
        
        self._reset_flags()
        events_manager.notify_internal_event(SdkInternalEvent.FLAG_KILLED_NOTIFICATION, metadata)    
        assert not self.sdk_ready_flag
        assert self.sdk_update_flag
        self._verify_metadata(metadata)

        self._reset_flags()
        events_manager.notify_internal_event(SdkInternalEvent.FLAGS_UPDATED, metadata)    
        assert not self.sdk_ready_flag
        assert self.sdk_update_flag
        self._verify_metadata(metadata)

        self._reset_flags()
        events_manager.notify_internal_event(SdkInternalEvent.SEGMENTS_UPDATED, metadata)    
        assert not self.sdk_ready_flag
        assert self.sdk_update_flag
        self._verify_metadata(metadata)
    
    def _reset_flags(self):
        self.sdk_ready_flag = False
        self.sdk_update_flag = False
        self.metadata = None
        
    def _sdk_ready_callback(self, metadata):
        self.sdk_ready_flag = True
        self.metadata = metadata

    def _sdk_update_callback(self, metadata):
        self.sdk_update_flag = True
        self.metadata = metadata

    def _verify_metadata(self, metadata):
        assert metadata.get_type() == self.metadata.get_type()
        assert metadata.get_names() == self.metadata.get_names()
        
class EventsManagerAsyncTests(object):
    """Tests for EventsManagerAsync."""

    sdk_ready_flag = False
    sdk_update_flag = False
    metadata = None
    
    @pytest.mark.asyncio
    async def test_firing_events(self):
        events_manager = EventsManagerAsync(EventsManagerConfig(), EventsDelivery())
        await events_manager.register(SdkEvent.SDK_READY, self._sdk_ready_callback)
        await events_manager.register(SdkEvent.SDK_UPDATE, self._sdk_update_callback)
        
        metadata = EventsMetadata(SdkEventType.FLAG_UPDATE, { "feature1" })
        await events_manager.notify_internal_event(SdkInternalEvent.FLAGS_UPDATED, metadata)    
        await events_manager.notify_internal_event(SdkInternalEvent.FLAG_KILLED_NOTIFICATION, metadata)    
        await events_manager.notify_internal_event(SdkInternalEvent.RB_SEGMENTS_UPDATED, metadata)    
        await events_manager.notify_internal_event(SdkInternalEvent.SEGMENTS_UPDATED, metadata)    
        assert not self.sdk_ready_flag
        assert not self.sdk_update_flag
        
        self._reset_flags()
        await events_manager.notify_internal_event(SdkInternalEvent.SDK_READY, metadata)    
        await asyncio.sleep(.3)
        assert self.sdk_ready_flag
        assert not self.sdk_update_flag
        self._verify_metadata(metadata)

        self._reset_flags()
        await events_manager.notify_internal_event(SdkInternalEvent.RB_SEGMENTS_UPDATED, metadata)    
        await asyncio.sleep(.3)
        assert not self.sdk_ready_flag
        assert self.sdk_update_flag
        self._verify_metadata(metadata)
        
        self._reset_flags()
        await events_manager.notify_internal_event(SdkInternalEvent.FLAG_KILLED_NOTIFICATION, metadata)    
        await asyncio.sleep(.3)
        assert not self.sdk_ready_flag
        assert self.sdk_update_flag
        self._verify_metadata(metadata)

        self._reset_flags()
        await events_manager.notify_internal_event(SdkInternalEvent.FLAGS_UPDATED, metadata)    
        await asyncio.sleep(.3)
        assert not self.sdk_ready_flag
        assert self.sdk_update_flag
        self._verify_metadata(metadata)

        self._reset_flags()
        await events_manager.notify_internal_event(SdkInternalEvent.SEGMENTS_UPDATED, metadata)    
        await asyncio.sleep(.3)
        assert not self.sdk_ready_flag
        assert self.sdk_update_flag
        self._verify_metadata(metadata)
    
    def _reset_flags(self):
        self.sdk_ready_flag = False
        self.sdk_update_flag = False
        self.metadata = None
        
    async def _sdk_ready_callback(self, metadata):
        self.sdk_ready_flag = True
        self.metadata = metadata

    async def _sdk_update_callback(self, metadata):
        self.sdk_update_flag = True
        self.metadata = metadata

    def _verify_metadata(self, metadata):
        assert metadata.get_type() == self.metadata.get_type()
        assert metadata.get_names() == self.metadata.get_names()