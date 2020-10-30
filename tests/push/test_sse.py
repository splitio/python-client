"""SSEClient unit tests."""

import time
import threading
import pytest
from splitio.push.sse import SSEClient, SSEEvent
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
        client_task = threading.Thread(target=runner)
        client_task.setDaemon(True)
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
        client_task = threading.Thread(target=runner)
        client_task.setDaemon(True)
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
        client_task = threading.Thread(target=runner)
        client_task.setDaemon(True)
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
