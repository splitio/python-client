"""SSEClient unit tests."""

import time
import threading
import pytest
from contextlib import suppress

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
        client_task = threading.Thread(target=runner)
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
            assert not client.start('http://127.0.0.1:' + str(server.port()))
        client_task = threading.Thread(target=runner)
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
            assert not client.start('http://127.0.0.1:' + str(server.port()))
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

    def test_sse_client_uses_explicit_proxy(self, monkeypatch):
        """Explicit proxy_host/proxy_port args tunnel through the given proxy."""
        monkeypatch.delenv('HTTPS_PROXY', raising=False)
        captured = {}

        class FakeConn:
            def __init__(self, host, port, timeout=None):
                captured['host'] = host
                captured['port'] = port
                self.sock = None

            def set_tunnel(self, host, port):
                captured['tunnel_host'] = host
                captured['tunnel_port'] = port

            def request(self, *args, **kwargs):
                captured['request_args'] = args
                captured['request_headers'] = kwargs.get('headers')

            def getresponse(self):
                raise RuntimeError('stop reading')

            def close(self):
                captured['closed'] = True

        monkeypatch.setattr('splitio.push.sse.HTTPConnection', FakeConn)

        client = SSEClient(lambda e: None)
        client.start(
            'http://target-host:9999/path?token=abc',
            proxy_host='proxyhost',
            proxy_port=8080,
        )

        assert captured['host'] == 'proxyhost'
        assert captured['port'] == 8080
        assert captured['tunnel_host'] == 'target-host'
        assert captured['tunnel_port'] == 9999
        assert captured.get('closed') is True

    def test_sse_client_uses_https_proxy_env_var(self, monkeypatch):
        """When no proxy is passed, HTTPS_PROXY env var is used to configure the proxy."""
        monkeypatch.setenv('HTTPS_PROXY', 'http://envproxy:3128')
        captured = {}

        class FakeConn:
            def __init__(self, host, port, timeout=None):
                captured['host'] = host
                captured['port'] = port
                self.sock = None

            def set_tunnel(self, host, port):
                captured['tunnel_host'] = host
                captured['tunnel_port'] = port

            def request(self, *args, **kwargs):
                pass

            def getresponse(self):
                raise RuntimeError('stop reading')

            def close(self):
                pass

        monkeypatch.setattr('splitio.push.sse.HTTPConnection', FakeConn)

        client = SSEClient(lambda e: None)
        client.start('http://target-host:9999/path?token=abc')

        assert captured['host'] == 'envproxy'
        assert captured['port'] == 3128
        assert captured['tunnel_host'] == 'target-host'
        assert captured['tunnel_port'] == 9999

    def test_sse_client_env_proxy_defaults_port_80(self, monkeypatch):
        """HTTPS_PROXY without an explicit port falls back to port 80."""
        monkeypatch.setenv('HTTPS_PROXY', 'http://envproxy')
        captured = {}

        class FakeConn:
            def __init__(self, host, port, timeout=None):
                captured['host'] = host
                captured['port'] = port
                self.sock = None

            def set_tunnel(self, host, port):
                pass

            def request(self, *args, **kwargs):
                pass

            def getresponse(self):
                raise RuntimeError('stop reading')

            def close(self):
                pass

        monkeypatch.setattr('splitio.push.sse.HTTPConnection', FakeConn)

        client = SSEClient(lambda e: None)
        client.start('http://target-host:9999/path?token=abc')

        assert captured['host'] == 'envproxy'
        assert captured['port'] == 80

    def test_sse_client_no_proxy_direct_connection(self, monkeypatch):
        """Without proxy args or env var, connect directly to target host."""
        monkeypatch.delenv('HTTPS_PROXY', raising=False)
        captured = {}

        class FakeConn:
            def __init__(self, host, port=None, timeout=None):
                captured['host'] = host
                captured['port'] = port
                captured['tunneled'] = False
                self.sock = None

            def set_tunnel(self, host, port):
                captured['tunneled'] = True

            def request(self, *args, **kwargs):
                pass

            def getresponse(self):
                raise RuntimeError('stop reading')

            def close(self):
                pass

        monkeypatch.setattr('splitio.push.sse.HTTPConnection', FakeConn)

        client = SSEClient(lambda e: None)
        client.start('http://target-host:9999/path?token=abc')

        assert captured['host'] == 'target-host'
        assert captured['port'] == 9999
        assert captured['tunneled'] is False

class SSEClientAsyncTests(object):
    """SSEClient test cases."""

    @pytest.mark.asyncio
    async def test_sse_client_disconnects(self):
        """Test correct initialization. Client ends the connection."""
        server = SSEMockServer()
        server.start()
        client = SSEClientAsync()
        sse_events_loop = client.start(f"http://127.0.0.1:{str(server.port())}?token=abc123$%^&(")

        server.publish({'id': '1'})
        server.publish({'id': '2', 'event': 'message', 'data': 'abc'})
        server.publish({'id': '3', 'event': 'message', 'data': 'def'})
        server.publish({'id': '4', 'event': 'message', 'data': 'ghi'})

        event1 = await sse_events_loop.__anext__()
        event2 = await sse_events_loop.__anext__()
        event3 = await sse_events_loop.__anext__()
        event4 = await sse_events_loop.__anext__()

        # Since generators are meant to be iterated, we need to consume them all until StopIteration occurs
        # to do this, connection must be closed in another coroutine, while the current one is still consuming events.
        shutdown_task = asyncio.get_running_loop().create_task(client.shutdown())
        with pytest.raises(StopAsyncIteration): await sse_events_loop.__anext__()
        await shutdown_task

        assert event1 == SSEEvent('1', None, None, None)
        assert event2 == SSEEvent('2', 'message', None, 'abc')
        assert event3 == SSEEvent('3', 'message', None, 'def')
        assert event4 == SSEEvent('4', 'message', None, 'ghi')
        assert client._response == None

        server.publish(server.GRACEFUL_REQUEST_END)
        server.stop()

    @pytest.mark.asyncio
    async def test_sse_server_disconnects(self):
        """Test correct initialization. Server ends connection."""
        server = SSEMockServer()
        server.start()
        client = SSEClientAsync()
        sse_events_loop = client.start('http://127.0.0.1:' + str(server.port()))

        server.publish({'id': '1'})
        server.publish({'id': '2', 'event': 'message', 'data': 'abc'})
        server.publish({'id': '3', 'event': 'message', 'data': 'def'})
        server.publish({'id': '4', 'event': 'message', 'data': 'ghi'})

        event1 = await sse_events_loop.__anext__()
        event2 = await sse_events_loop.__anext__()
        event3 = await sse_events_loop.__anext__()
        event4 = await sse_events_loop.__anext__()

        server.publish(server.GRACEFUL_REQUEST_END)

        # after the connection ends, any subsequent read sohould fail and iteration should stop
        with pytest.raises(StopAsyncIteration): await sse_events_loop.__anext__()

        assert event1 == SSEEvent('1', None, None, None)
        assert event2 == SSEEvent('2', 'message', None, 'abc')
        assert event3 == SSEEvent('3', 'message', None, 'def')
        assert event4 == SSEEvent('4', 'message', None, 'ghi')
        assert client._response == None

        await client._done.wait() # to ensure `start()` has finished
        assert client._response is None

#        server.stop()


    @pytest.mark.asyncio
    async def test_sse_server_disconnects_abruptly(self):
        """Test correct initialization. Server ends connection."""
        server = SSEMockServer()
        server.start()
        client = SSEClientAsync()
        sse_events_loop = client.start('http://127.0.0.1:' + str(server.port()))

        server.publish({'id': '1'})
        server.publish({'id': '2', 'event': 'message', 'data': 'abc'})
        server.publish({'id': '3', 'event': 'message', 'data': 'def'})
        server.publish({'id': '4', 'event': 'message', 'data': 'ghi'})

        event1 = await sse_events_loop.__anext__()
        event2 = await sse_events_loop.__anext__()
        event3 = await sse_events_loop.__anext__()
        event4 = await sse_events_loop.__anext__()

        server.publish(server.VIOLENT_REQUEST_END)
        with pytest.raises(StopAsyncIteration): await sse_events_loop.__anext__()

        server.stop()

        assert event1 == SSEEvent('1', None, None, None)
        assert event2 == SSEEvent('2', 'message', None, 'abc')
        assert event3 == SSEEvent('3', 'message', None, 'def')
        assert event4 == SSEEvent('4', 'message', None, 'ghi')

        await client._done.wait() # to ensure `start()` has finished
        assert client._response is None
