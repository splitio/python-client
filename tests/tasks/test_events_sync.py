"""Impressions synchronization task test module."""

import threading
import time
from splitio.api.client import HttpResponse
from splitio.tasks import events_sync
from splitio.storage import EventStorage
from splitio.models.events import Event
from splitio.api.events import EventsAPI


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
        api.flush_events.return_value = HttpResponse(200, '')
        task =events_sync.EventsSyncTask(api, storage, 1, 5)
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
