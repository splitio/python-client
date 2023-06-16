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

#    @pytest.mark.asyncio
    async def test_sse_client_disconnects(self):
        """Test correct initialization. Client ends the connection."""
        server = SSEMockServer()
        server.start()

        events = []
        async def callback(event):
            """Callback."""
            events.append(event)

        client = SSEClientAsync(callback)

        async def connect_split_sse_client():
            await client.start('http://127.0.0.1:' + str(server.port()))

        self._client_task = asyncio.gather(connect_split_sse_client())
        server.publish({'id': '1'})
        server.publish({'id': '2', 'event': 'message', 'data': 'abc'})
        server.publish({'id': '3', 'event': 'message', 'data': 'def'})
        server.publish({'id': '4', 'event': 'message', 'data': 'ghi'})
        await asyncio.sleep(1)
        await client.shutdown()
        self._client_task.cancel()
        await asyncio.sleep(1)

        assert events == [
            SSEEvent('1', None, None, None),
            SSEEvent('2', 'message', None, 'abc'),
            SSEEvent('3', 'message', None, 'def'),
            SSEEvent('4', 'message', None, 'ghi')
        ]
        assert client._conn is None
        server.publish(server.GRACEFUL_REQUEST_END)
        server.stop()

    async def test_sse_server_disconnects(self):
        """Test correct initialization. Server ends connection."""
        server = SSEMockServer()
        server.start()

        events = []
        async def callback(event):
            """Callback."""
            events.append(event)

        client = SSEClientAsync(callback)

        async def start_client():
            await client.start('http://127.0.0.1:' + str(server.port()))

        asyncio.gather(start_client())
        server.publish({'id': '1'})
        server.publish({'id': '2', 'event': 'message', 'data': 'abc'})
        server.publish({'id': '3', 'event': 'message', 'data': 'def'})
        server.publish({'id': '4', 'event': 'message', 'data': 'ghi'})
        server.publish(server.GRACEFUL_REQUEST_END)

        await asyncio.sleep(1)
        server.stop()
        await asyncio.sleep(1)

        assert events == [
            SSEEvent('1', None, None, None),
            SSEEvent('2', 'message', None, 'abc'),
            SSEEvent('3', 'message', None, 'def'),
            SSEEvent('4', 'message', None, 'ghi')
        ]

        assert client._conn is None

    async def test_sse_server_disconnects_abruptly(self):
        """Test correct initialization. Server ends connection."""
        server = SSEMockServer()
        server.start()

        events = []
        async def callback(event):
            """Callback."""
            events.append(event)

        client = SSEClientAsync(callback)

        async def runner():
            """SSE client runner thread."""
            await client.start('http://127.0.0.1:' + str(server.port()))

        client_task = asyncio.gather(runner())

        server.publish({'id': '1'})
        server.publish({'id': '2', 'event': 'message', 'data': 'abc'})
        server.publish({'id': '3', 'event': 'message', 'data': 'def'})
        server.publish({'id': '4', 'event': 'message', 'data': 'ghi'})
        await asyncio.sleep(1)
        server.publish(server.VIOLENT_REQUEST_END)
        server.stop()
        await asyncio.sleep(1)

        assert events == [
            SSEEvent('1', None, None, None),
            SSEEvent('2', 'message', None, 'abc'),
            SSEEvent('3', 'message', None, 'def'),
            SSEEvent('4', 'message', None, 'ghi')
        ]

        assert client._conn is None
