"""Split Worker tests."""

import threading
import time
import pytest

from splitio.api.client import HttpResponse
from splitio.api import APIException
from splitio.storage import EventStorage
from splitio.models.events import Event
from splitio.sync.event import EventSynchronizer, EventSynchronizerAsync


class EventsSynchronizerTests(object):
    """Events synchronizer test cases."""

    def test_synchronize_events_error(self, mocker):
        storage = mocker.Mock(spec=EventStorage)
        storage.pop_many.return_value = [
            Event('key1', 'user', 'purchase', 5.3, 123456, None),
            Event('key2', 'user', 'purchase', 5.3, 123456, None),
        ]

        api = mocker.Mock()

        def run(x):
            raise APIException("something broke")

        api.flush_events.side_effect = run
        event_synchronizer = EventSynchronizer(api, storage, 5)
        assert event_synchronizer._LOGGER.name == 'splitio.sync.event'
        event_synchronizer.synchronize_events()
        assert event_synchronizer._failed.qsize() == 2

    def test_synchronize_events_empty(self, mocker):
        storage = mocker.Mock(spec=EventStorage)
        storage.pop_many.return_value = []

        api = mocker.Mock()

        def run(x):
            run._called += 1

        run._called = 0
        api.flush_events.side_effect = run
        event_synchronizer = EventSynchronizer(api, storage, 5)
        event_synchronizer.synchronize_events()
        assert run._called == 0

    def test_synchronize_impressions(self, mocker):
        storage = mocker.Mock(spec=EventStorage)
        storage.pop_many.return_value = [
            Event('key1', 'user', 'purchase', 5.3, 123456, None),
            Event('key2', 'user', 'purchase', 5.3, 123456, None),
        ]

        api = mocker.Mock()

        def run(x):
            run._called += 1
            return HttpResponse(200, '', {})

        api.flush_events.side_effect = run
        run._called = 0

        event_synchronizer = EventSynchronizer(api, storage, 5)
        event_synchronizer.synchronize_events()
        assert run._called == 1
        assert event_synchronizer._failed.qsize() == 0


class EventsSynchronizerAsyncTests(object):
    """Events synchronizer async test cases."""

    @pytest.mark.asyncio
    async def test_synchronize_events_error(self, mocker):
        storage = mocker.Mock(spec=EventStorage)
        async def pop_many(*args):
            return [
                Event('key1', 'user', 'purchase', 5.3, 123456, None),
                Event('key2', 'user', 'purchase', 5.3, 123456, None),
            ]
        storage.pop_many = pop_many

        api = mocker.Mock()
        async def run(x):
            raise APIException("something broke")

        api.flush_events = run
        event_synchronizer = EventSynchronizerAsync(api, storage, 5)
        assert event_synchronizer._LOGGER.name == 'asyncio'
        await event_synchronizer.synchronize_events()
        assert event_synchronizer._failed.qsize() == 2

    @pytest.mark.asyncio
    async def test_synchronize_events_empty(self, mocker):
        storage = mocker.Mock(spec=EventStorage)
        async def pop_many(*args):
            return []
        storage.pop_many = pop_many

        api = mocker.Mock()
        async def run(x):
            run._called += 1

        run._called = 0
        api.flush_events = run
        event_synchronizer = EventSynchronizerAsync(api, storage, 5)
        await event_synchronizer.synchronize_events()
        assert run._called == 0

    @pytest.mark.asyncio
    async def test_synchronize_impressions(self, mocker):
        storage = mocker.Mock(spec=EventStorage)
        async def pop_many(*args):
            return [
                Event('key1', 'user', 'purchase', 5.3, 123456, None),
                Event('key2', 'user', 'purchase', 5.3, 123456, None),
            ]
        storage.pop_many = pop_many

        api = mocker.Mock()
        async def run(x):
            run._called += 1
            return HttpResponse(200, '', {})

        api.flush_events.side_effect = run
        run._called = 0

        event_synchronizer = EventSynchronizerAsync(api, storage, 5)
        await event_synchronizer.synchronize_events()
        assert run._called == 1
        assert event_synchronizer._failed.qsize() == 0
