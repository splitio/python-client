"""Impressions synchronization task test module."""

import threading
import time
import pytest

from splitio.api.client import HttpResponse
from splitio.tasks import events_sync
from splitio.storage import EventStorage
from splitio.models.events import Event
from splitio.api.events import EventsAPI
from splitio.sync.event import EventSynchronizer, EventSynchronizerAsync
from splitio.optional.loaders import asyncio


class EventsSyncTests(object):
    """Impressions Syncrhonization task test cases."""

    def test_normal_operation(self, mocker):
        """Test that the task works properly under normal circumstances."""
        storage = mocker.Mock(spec=EventStorage)
        events = [
            Event('key1', 'user', 'purchase', 5.3, 123456, None),
            Event('key2', 'user', 'purchase', 5.3, 123456, None),
            Event('key3', 'user', 'purchase', 5.3, 123456, None),
            Event('key4', 'user', 'purchase', 5.3, 123456, None),
            Event('key5', 'user', 'purchase', 5.3, 123456, None),
        ]

        storage.pop_many.return_value = events
        api = mocker.Mock(spec=EventsAPI)
        api.flush_events.return_value = HttpResponse(200, '', {})
        event_synchronizer = EventSynchronizer(api, storage, 5)
        task = events_sync.EventsSyncTask(event_synchronizer.synchronize_events, 1)
        assert task._LOGGER.name == 'splitio.tasks.events_sync'
        task.start()
        time.sleep(2)
        assert task.is_running()
        assert storage.pop_many.mock_calls[0] == mocker.call(5)
        assert api.flush_events.mock_calls[0] == mocker.call(events)
        stop_event = threading.Event()
        calls_now = len(api.flush_events.mock_calls)
        task.stop(stop_event)
        stop_event.wait(5)
        assert stop_event.is_set()
        assert len(api.flush_events.mock_calls) > calls_now


class EventsSyncAsyncTests(object):
    """Impressions Syncrhonization task async test cases."""

    @pytest.mark.asyncio
    async def test_normal_operation(self, mocker):
        """Test that the task works properly under normal circumstances."""
        self.events = [
            Event('key1', 'user', 'purchase', 5.3, 123456, None),
            Event('key2', 'user', 'purchase', 5.3, 123456, None),
            Event('key3', 'user', 'purchase', 5.3, 123456, None),
            Event('key4', 'user', 'purchase', 5.3, 123456, None),
            Event('key5', 'user', 'purchase', 5.3, 123456, None),
        ]
        storage = mocker.Mock(spec=EventStorage)
        self.called = False
        async def pop_many(*args):
            self.called = True
            return self.events
        storage.pop_many = pop_many

        api = mocker.Mock(spec=EventsAPI)
        self.flushed_events = None
        self.count = 0
        async def flush_events(events):
            self.count += 1
            self.flushed_events = events
            return HttpResponse(200, '', {})
        api.flush_events = flush_events

        event_synchronizer = EventSynchronizerAsync(api, storage, 5)
        task = events_sync.EventsSyncTaskAsync(event_synchronizer.synchronize_events, 1)
        assert task._LOGGER.name == 'asyncio'
        task.start()
        await asyncio.sleep(2)

        assert task.is_running()
        assert self.called
        assert self.flushed_events == self.events

        calls_now = self.count
        await task.stop()
        assert not task.is_running()
        assert self.count > calls_now
