"""SSEClient unit tests."""
# pylint:disable=no-self-use,line-too-long
import time
from queue import Queue
import pytest

from splitio.models.token import Token

from splitio.push.splitsse import SplitSSEClient
from splitio.push.sse import SSEEvent

from tests.helpers.mockserver import SSEMockServer

from splitio.client.util import SdkMetadata


class SSEClientTests(object):
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
