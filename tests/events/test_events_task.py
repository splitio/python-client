"""EventsManager test module."""
import pytest
import queue
import time
import asyncio

from splitio.models.events import SdkInternalEvent
from splitio.models.notification import SdkInternalEventNotification
from splitio.events.events_metadata import EventsMetadata
from splitio.events.events_metadata import SdkEventType
from splitio.events.events_task import EventsTask, EventsTaskAsync


class EventsTaskTests(object):
    """Tests for EventsTask."""

    internal_event = None
    metadata = None
    
    def test_firing_events(self):
        events_queue = queue.Queue()
        events_task = EventsTask(self._event_callback, events_queue)

        events_task.start()        
        assert events_task.is_running()
        
        metadata = EventsMetadata(SdkEventType.FLAG_UPDATE, { "feature1" })
        events_queue.put(SdkInternalEventNotification(SdkInternalEvent.SDK_READY, metadata))
        time.sleep(.5)
        assert self.internal_event == SdkInternalEvent.SDK_READY
        self._verify_metadata(metadata)
        
        self._reset_flags()
        events_queue.put(SdkInternalEventNotification(SdkInternalEvent.RB_SEGMENTS_UPDATED, metadata))
        time.sleep(.5)
        assert self.internal_event == SdkInternalEvent.RB_SEGMENTS_UPDATED
        self._verify_metadata(metadata)
    
        events_task.stop()
        time.sleep(.5)
        assert not events_task.is_running()

    def test_on_error(self):
        events_queue = queue.Queue()

        def handler_sync(internal_event, metadata):
            raise Exception('some')

        events_task = EventsTask(handler_sync, events_queue)
        events_task.start()        
        assert events_task.is_running()

        events_queue.put(SdkInternalEventNotification(SdkInternalEvent.SDK_READY, None))

        with pytest.raises(Exception):
            events_task._handler()

        assert events_task.is_running()
        events_task.stop()
        time.sleep(1)
        assert not events_task.is_running()
                
    def _reset_flags(self):
        self.internal_event = None
        self.metadata = None
        
    def _event_callback(self, internal_event, metadata):
        self.internal_event = internal_event
        self.metadata = metadata

    def _verify_metadata(self, metadata):
        assert metadata.get_type() == self.metadata.get_type()
        assert metadata.get_names() == self.metadata.get_names()
        

class EventsTaskAsyncTests(object):
    """Tests for EventsTaskAsyncr."""

    internal_event = None
    metadata = None
    
    @pytest.mark.asyncio
    async def test_firing_events(self):
        events_queue = asyncio.Queue()
        events_task = EventsTaskAsync(self._event_callback, events_queue)

        events_task.start()        
        assert events_task.is_running()
        
        metadata = EventsMetadata(SdkEventType.FLAG_UPDATE, { "feature1" })
        await events_queue.put(SdkInternalEventNotification(SdkInternalEvent.SDK_READY, metadata))
        await asyncio.sleep(.5)
        assert self.internal_event == SdkInternalEvent.SDK_READY
        self._verify_metadata(metadata)
        
        self._reset_flags()
        await events_queue.put(SdkInternalEventNotification(SdkInternalEvent.RB_SEGMENTS_UPDATED, metadata))
        await asyncio.sleep(.5)
        assert self.internal_event == SdkInternalEvent.RB_SEGMENTS_UPDATED
        self._verify_metadata(metadata)
    
        await events_task.stop()
        await asyncio.sleep(.5)
        assert not events_task.is_running()

    @pytest.mark.asyncio
    async def test_on_error(self):
        events_queue = asyncio.Queue()

        async def handler_sync(internal_event, metadata):
            raise Exception('some')

        events_task = EventsTaskAsync(handler_sync, events_queue)
        events_task.start()        
        assert events_task.is_running()

        await events_queue.put(SdkInternalEventNotification(SdkInternalEvent.SDK_READY, None))

        with pytest.raises(Exception):
            events_task._handler()

        assert events_task.is_running()
        await events_task.stop()
        await asyncio.sleep(1)
        assert not events_task.is_running()
                
    def _reset_flags(self):
        self.internal_event = None
        self.metadata = None
        
    async def _event_callback(self, internal_event, metadata):
        self.internal_event = internal_event
        self.metadata = metadata

    def _verify_metadata(self, metadata):
        assert metadata.get_type() == self.metadata.get_type()
        assert metadata.get_names() == self.metadata.get_names()
        
    