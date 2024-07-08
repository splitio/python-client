"""SSEClient unit tests."""
# pylint:disable=no-self-use,line-too-long
import time
from queue import Queue
import pytest

from splitio.models.token import Token
from splitio.push.splitsse import SplitSSEClient, SplitSSEClientAsync
from splitio.push.sse import SSEEvent, SSE_EVENT_ERROR

from tests.helpers.mockserver import SSEMockServer
from splitio.client.util import SdkMetadata
from splitio.optional.loaders import asyncio

class SSESplitClientTests(object):
    """SSEClient test cases."""

    def test_split_sse_success(self):
        """Test correct initialization. Client ends the connection."""
        events = []
        def handler(event):
            """Handler."""
            events.append(event)

        status = {
            'on_connect': False,
            'on_disconnect': False,
        }

        def on_connect():
            """On connect handler."""
            status['on_connect'] = True

        def on_disconnect():
            """On disconnect handler."""
            status['on_disconnect'] = True

        request_queue = Queue()
        server = SSEMockServer(request_queue)
        server.start()

        client = SplitSSEClient(handler, SdkMetadata('1.0', 'some', '1.2.3.4'), on_connect, on_disconnect,
                                'abcd', base_url='http://localhost:' + str(server.port()))

        token = Token(True, 'some', {'chan1': ['subscribe'], 'chan2': ['subscribe', 'channel-metadata:publishers']},
                      1, 2)

        server.publish({'id': '1'})  # send a non-error event early to unblock start
        assert client.start(token)
        with pytest.raises(Exception):
            client.start(token)

        server.publish({'id': '1', 'data': 'a', 'retry': '1', 'event': 'message'})
        server.publish({'id': '2', 'data': 'a', 'retry': '1', 'event': 'message'})
        time.sleep(1)
        client.stop(True)

        request = request_queue.get(1)
        assert request.path == '/event-stream?v=1.1&accessToken=some&channels=chan1,[?occupancy=metrics.publishers]chan2'
        assert request.headers['accept'] == 'text/event-stream'
        assert request.headers['SplitSDKVersion'] == '1.0'
        assert request.headers['SplitSDKMachineIP'] == '1.2.3.4'
        assert request.headers['SplitSDKMachineName'] == 'some'
        assert request.headers['SplitSDKClientKey'] == 'abcd'

        assert events == [
            SSEEvent('1', 'message', '1', 'a'),
            SSEEvent('2', 'message', '1', 'a')
        ]

        server.publish(SSEMockServer.VIOLENT_REQUEST_END)
        server.stop()

        assert status['on_connect']
        assert status['on_disconnect']

    def test_split_sse_error(self):
        """Test correct initialization. Client ends the connection."""
        events = []
        def handler(event):
            """Handler."""
            events.append(event)

        request_queue = Queue()
        server = SSEMockServer(request_queue)
        server.start()

        status = {
            'on_connect': False,
            'on_disconnect': False,
        }

        def on_connect():
            """On connect handler."""
            status['on_connect'] = True

        def on_disconnect():
            """On disconnect handler."""
            status['on_disconnect'] = True

        client = SplitSSEClient(handler, SdkMetadata('1.0', 'some', '1.2.3.4'), on_connect, on_disconnect,
                                "abcd", base_url='http://localhost:' + str(server.port()))

        token = Token(True, 'some', {'chan1': ['subscribe'], 'chan2': ['subscribe', 'channel-metadata:publishers']},
                      1, 2)

        server.publish({'event': 'error'})  # send an error event early to unblock start
        assert not client.start(token)

        request = request_queue.get(1)
        assert request.path == '/event-stream?v=1.1&accessToken=some&channels=chan1,[?occupancy=metrics.publishers]chan2'
        assert request.headers['accept'] == 'text/event-stream'
        assert request.headers['SplitSDKVersion'] == '1.0'
        assert request.headers['SplitSDKMachineIP'] == '1.2.3.4'
        assert request.headers['SplitSDKMachineName'] == 'some'
        assert request.headers['SplitSDKClientKey'] == 'abcd'

        server.publish(SSEMockServer.VIOLENT_REQUEST_END)
        server.stop()

        time.sleep(1)

        assert status['on_connect']
        assert status['on_disconnect']


class SSESplitClientAsyncTests(object):
    """SSEClientAsync test cases."""

    @pytest.mark.asyncio
    async def test_split_sse_success(self):
        """Test correct initialization. Client ends the connection."""
        request_queue = Queue()
        server = SSEMockServer(request_queue)
        server.start()

        client = SplitSSEClientAsync(SdkMetadata('1.0', 'some', '1.2.3.4'),
                                'abcd', base_url='http://localhost:' + str(server.port()))

        token = Token(True, 'some', {'chan1': ['subscribe'], 'chan2': ['subscribe', 'channel-metadata:publishers']},
                      1, 2)

        events_source = client.start(token)
        server.publish({'id': '1'})  # send a non-error event early to unblock start
        server.publish({'id': '1', 'data': 'a', 'retry': '1', 'event': 'message'})
        server.publish({'id': '2', 'data': 'a', 'retry': '1', 'event': 'message'})

        first_event = await events_source.__anext__()
        assert first_event.event != SSE_EVENT_ERROR


        event2 = await events_source.__anext__()
        event3 = await events_source.__anext__()

        # Since generators are meant to be iterated, we need to consume them all until StopIteration occurs
        # to do this, connection must be closed in another coroutine, while the current one is still consuming events.
        shutdown_task = asyncio.get_running_loop().create_task(client.stop())
        with pytest.raises(StopAsyncIteration): await events_source.__anext__()
        await shutdown_task


        request = request_queue.get(1)
        assert request.path == '/event-stream?v=1.1&accessToken=some&channels=chan1,%5B?occupancy=metrics.publishers%5Dchan2'
        assert request.headers['accept'] == 'text/event-stream'
        assert request.headers['SplitSDKVersion'] == '1.0'
        assert request.headers['SplitSDKMachineIP'] == '1.2.3.4'
        assert request.headers['SplitSDKMachineName'] == 'some'
        assert request.headers['SplitSDKClientKey'] == 'abcd'

        assert event2 == SSEEvent('1', 'message', '1', 'a')
        assert event3 == SSEEvent('2', 'message', '1', 'a')

        server.publish(SSEMockServer.VIOLENT_REQUEST_END)
        server.stop()
        await asyncio.sleep(1)

        assert client.status == SplitSSEClient._Status.IDLE


    @pytest.mark.asyncio
    async def test_split_sse_error(self):
        """Test correct initialization. Client ends the connection."""
        request_queue = Queue()
        server = SSEMockServer(request_queue)
        server.start()

        client = SplitSSEClientAsync(SdkMetadata('1.0', 'some', '1.2.3.4'),
                                'abcd', base_url='http://localhost:' + str(server.port()))

        token = Token(True, 'some', {'chan1': ['subscribe'], 'chan2': ['subscribe', 'channel-metadata:publishers']},
                      1, 2)

        events_source = client.start(token)
        server.publish({'event': 'error'})  # send an error event early to unblock start


        with pytest.raises(StopAsyncIteration): await events_source.__anext__()

        assert client.status == SplitSSEClient._Status.IDLE

        request = request_queue.get(1)
        assert request.path == '/event-stream?v=1.1&accessToken=some&channels=chan1,%5B?occupancy=metrics.publishers%5Dchan2'
        assert request.headers['accept'] == 'text/event-stream'
        assert request.headers['SplitSDKVersion'] == '1.0'
        assert request.headers['SplitSDKMachineIP'] == '1.2.3.4'
        assert request.headers['SplitSDKMachineName'] == 'some'
        assert request.headers['SplitSDKClientKey'] == 'abcd'

        server.publish(SSEMockServer.VIOLENT_REQUEST_END)
        server.stop()
