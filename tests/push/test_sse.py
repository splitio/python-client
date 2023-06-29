"""SSEClient unit tests."""

import time
import threading
import pytest

from splitio.push.sse import SSEClient, SSEEvent, SSEClientAsync
from splitio.optional.loaders import asyncio
from tests.helpers.mockserver import SSEMockServer

class SSEClientTests(object):
    """SSEClient test cases."""

    def test_sse_client_disconnects(self):
        """Test correct initialization. Client ends the connection."""
        server = SSEMockServer()
        server.start()

        events = []
        def callback(event):
            """Callback."""
            events.append(event)

        client = SSEClient(callback)

        def runner():
            """SSE client runner thread."""
            assert client.start('http://127.0.0.1:' + str(server.port()))
        client_task = threading.Thread(target=runner, daemon=True)
        client_task.setName('client')
        client_task.start()
        with pytest.raises(RuntimeError):
            client_task.start()

        server.publish({'id': '1'})
        server.publish({'id': '2', 'event': 'message', 'data': 'abc'})
        server.publish({'id': '3', 'event': 'message', 'data': 'def'})
        server.publish({'id': '4', 'event': 'message', 'data': 'ghi'})
        time.sleep(1)
        client.shutdown()
        time.sleep(1)

        assert events == [
            SSEEvent('1', None, None, None),
            SSEEvent('2', 'message', None, 'abc'),
            SSEEvent('3', 'message', None, 'def'),
            SSEEvent('4', 'message', None, 'ghi')
        ]

        assert client._conn is None
        server.publish(server.GRACEFUL_REQUEST_END)
        server.stop()

    def test_sse_server_disconnects(self):
        """Test correct initialization. Server ends connection."""
        server = SSEMockServer()
        server.start()

        events = []
        def callback(event):
            """Callback."""
            events.append(event)

        client = SSEClient(callback)

        def runner():
            """SSE client runner thread."""
            assert client.start('http://127.0.0.1:' + str(server.port()))
        client_task = threading.Thread(target=runner, daemon=True)
        client_task.setName('client')
        client_task.start()

        server.publish({'id': '1'})
        server.publish({'id': '2', 'event': 'message', 'data': 'abc'})
        server.publish({'id': '3', 'event': 'message', 'data': 'def'})
        server.publish({'id': '4', 'event': 'message', 'data': 'ghi'})
        time.sleep(1)
        server.publish(server.GRACEFUL_REQUEST_END)
        server.stop()
        time.sleep(1)

        assert events == [
            SSEEvent('1', None, None, None),
            SSEEvent('2', 'message', None, 'abc'),
            SSEEvent('3', 'message', None, 'def'),
            SSEEvent('4', 'message', None, 'ghi')
        ]

        assert client._conn is None

    def test_sse_server_disconnects_abruptly(self):
        """Test correct initialization. Server ends connection."""
        server = SSEMockServer()
        server.start()

        events = []
        def callback(event):
            """Callback."""
            events.append(event)

        client = SSEClient(callback)

        def runner():
            """SSE client runner thread."""
            assert client.start('http://127.0.0.1:' + str(server.port()))
        client_task = threading.Thread(target=runner, daemon=True)
        client_task.setName('client')
        client_task.start()

        server.publish({'id': '1'})
        server.publish({'id': '2', 'event': 'message', 'data': 'abc'})
        server.publish({'id': '3', 'event': 'message', 'data': 'def'})
        server.publish({'id': '4', 'event': 'message', 'data': 'ghi'})
        time.sleep(1)
        server.publish(server.VIOLENT_REQUEST_END)
        server.stop()
        time.sleep(1)

        assert events == [
            SSEEvent('1', None, None, None),
            SSEEvent('2', 'message', None, 'abc'),
            SSEEvent('3', 'message', None, 'def'),
            SSEEvent('4', 'message', None, 'ghi')
        ]

        assert client._conn is None

class SSEClientAsyncTests(object):
    """SSEClient test cases."""

    @pytest.mark.asyncio
    async def test_sse_client_disconnects(self):
        """Test correct initialization. Client ends the connection."""
        server = SSEMockServer()
        server.start()
        client = SSEClientAsync('http://127.0.0.1:' + str(server.port()))
        sse_events_loop = client.start()

        server.publish({'id': '1'})
        server.publish({'id': '2', 'event': 'message', 'data': 'abc'})
        server.publish({'id': '3', 'event': 'message', 'data': 'def'})
        server.publish({'id': '4', 'event': 'message', 'data': 'ghi'})

        await asyncio.sleep(1)
        event1 = await sse_events_loop.__anext__()
        event2 = await sse_events_loop.__anext__()
        event3 = await sse_events_loop.__anext__()
        event4 = await sse_events_loop.__anext__()
        await client.shutdown()
        await asyncio.sleep(1)

        assert event1 == SSEEvent('1', None, None, None)
        assert event2 == SSEEvent('2', 'message', None, 'abc')
        assert event3 == SSEEvent('3', 'message', None, 'def')
        assert event4 == SSEEvent('4', 'message', None, 'ghi')
        assert client._conn.closed

        server.publish(server.GRACEFUL_REQUEST_END)
        server.stop()

    @pytest.mark.asyncio
    async def test_sse_server_disconnects(self):
        """Test correct initialization. Server ends connection."""
        server = SSEMockServer()
        server.start()
        client = SSEClientAsync('http://127.0.0.1:' + str(server.port()))
        sse_events_loop = client.start()

        server.publish({'id': '1'})
        server.publish({'id': '2', 'event': 'message', 'data': 'abc'})
        server.publish({'id': '3', 'event': 'message', 'data': 'def'})
        server.publish({'id': '4', 'event': 'message', 'data': 'ghi'})

        await asyncio.sleep(1)
        event1 = await sse_events_loop.__anext__()
        event2 = await sse_events_loop.__anext__()
        event3 = await sse_events_loop.__anext__()
        event4 = await sse_events_loop.__anext__()

        server.publish(server.GRACEFUL_REQUEST_END)
        try:
            await sse_events_loop.__anext__()
        except StopAsyncIteration:
            pass

        server.stop()
        await asyncio.sleep(1)
        assert event1 == SSEEvent('1', None, None, None)
        assert event2 == SSEEvent('2', 'message', None, 'abc')
        assert event3 == SSEEvent('3', 'message', None, 'def')
        assert event4 == SSEEvent('4', 'message', None, 'ghi')
        assert client._conn is None

    @pytest.mark.asyncio
    async def test_sse_server_disconnects_abruptly(self):
        """Test correct initialization. Server ends connection."""
        server = SSEMockServer()
        server.start()
        client = SSEClientAsync('http://127.0.0.1:' + str(server.port()))
        sse_events_loop = client.start()

        server.publish({'id': '1'})
        server.publish({'id': '2', 'event': 'message', 'data': 'abc'})
        server.publish({'id': '3', 'event': 'message', 'data': 'def'})
        server.publish({'id': '4', 'event': 'message', 'data': 'ghi'})

        await asyncio.sleep(1)
        event1 = await sse_events_loop.__anext__()
        event2 = await sse_events_loop.__anext__()
        event3 = await sse_events_loop.__anext__()
        event4 = await sse_events_loop.__anext__()

        server.publish(server.VIOLENT_REQUEST_END)
        try:
            await sse_events_loop.__anext__()
        except StopAsyncIteration:
            pass

        server.stop()

        await asyncio.sleep(1)
        assert event1 == SSEEvent('1', None, None, None)
        assert event2 == SSEEvent('2', 'message', None, 'abc')
        assert event3 == SSEEvent('3', 'message', None, 'def')
        assert event4 == SSEEvent('4', 'message', None, 'ghi')
        assert client._conn is None
