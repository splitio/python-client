"""SSE mock server."""
import json
from collections import namedtuple
import queue
import threading

from http.server import HTTPServer, BaseHTTPRequestHandler


Request = namedtuple('Request', ['method', 'path', 'headers', 'body'])


class SSEMockServer(object):
    """SSE server for testing purposes."""

    protocol_version = 'HTTP/1.1'

    GRACEFUL_REQUEST_END = 'REQ-END'
    VIOLENT_REQUEST_END = 'REQ-KILL'

    def __init__(self, req_queue=None):
        """Consruct a mock server."""
        self._queue = queue.Queue()
        self._server = HTTPServer(('localhost', 0),
                                  lambda *xs: SSEHandler(self._queue, *xs, req_queue=req_queue))
        self._server_thread = threading.Thread(target=self._blocking_run)
        self._server_thread.setDaemon(True)
        self._done_event = threading.Event()

    def _blocking_run(self):
        """Execute."""
        self._server.serve_forever()
        self._done_event.set()

    def port(self):
        """Return the assigned port."""
        return self._server.server_port

    def publish(self, event):
        """Publish an event."""
        self._queue.put(event, block=False)

    def start(self):
        """Start the server asyncrhonously."""
        self._server_thread.start()

    def wait(self, timeout=None):
        """Wait for the server to shutdown."""
        return self._done_event.wait(timeout)

    def stop(self):
        """Stop the server."""
        self._server.shutdown()


class SSEHandler(BaseHTTPRequestHandler):
    """Handler."""

    def __init__(self, event_queue, *args, **kwargs):
        """Construct a handler."""
        self._queue = event_queue
        self._req_queue = kwargs.get('req_queue')
        BaseHTTPRequestHandler.__init__(self, *args)

    def do_GET(self):  #pylint:disable=invalid-name
        """Respond to a GET request."""
        self.send_response(200)
        self.send_header("Content-type", "text/event-stream")
        self.send_header("Transfer-Encoding", "chunked")
        self.send_header("Connection", "keep-alive")
        self.end_headers()

        if self._req_queue is not None:
            headers = dict(zip(self.headers.keys(), self.headers.values()))
            self._req_queue.put(Request('GET', self.path, headers, None))

        def write_chunk(chunk):
            """Write an event/chunk."""
            tosend = '%X\r\n%s\r\n'%(len(chunk), chunk)
            self.wfile.write(tosend.encode('utf-8'))

        while True:
            event = self._queue.get()
            if event == SSEMockServer.GRACEFUL_REQUEST_END:
                break
            elif event == SSEMockServer.VIOLENT_REQUEST_END:
                raise Exception('exploding')

            chunk = ''
            chunk += 'id: % s\n' % event['id'] if 'id' in event else ''
            chunk += 'event: % s\n' % event['event'] if 'event' in event else ''
            chunk += 'retry: % s\n' % event['retry'] if 'retry' in event else ''
            chunk += 'data: % s\n' % event['data'] if 'data' in event else ''
            if chunk != '':
                write_chunk(chunk + '\r\n')

        self.wfile.write('0\r\n\r\n'.encode('utf-8'))


class SplitMockServer(object):
    """SDK server mock for testing purposes."""

    protocol_version = 'HTTP/1.1'

    def __init__(self, split_changes=None, segment_changes=None, req_queue=None,
                 auth_response=None):
        """
        Consruct a mock server.

        :param changes: mapping of changeNumbers to splitChanges responses
        :type changes: dict
        """
        split_changes = split_changes if split_changes is not None else {}
        segment_changes = segment_changes if segment_changes is not None else {}
        self._server = HTTPServer(('localhost', 0),
                                  lambda *xs: SDKHandler(split_changes, segment_changes, *xs,
                                                         req_queue=req_queue,
                                                         auth_response=auth_response))
        self._server_thread = threading.Thread(target=self._blocking_run, name="SplitMockServer")
        self._server_thread.setDaemon(True)
        self._done_event = threading.Event()

    def _blocking_run(self):
        """Execute."""
        self._server.serve_forever()
        self._done_event.set()

    def port(self):
        """Return the assigned port."""
        return self._server.server_port

    def start(self):
        """Start the server asyncrhonously."""
        self._server_thread.start()

    def wait(self, timeout=None):
        """Wait for the server to shutdown."""
        return self._done_event.wait(timeout)

    def stop(self):
        """Stop the server."""
        self._server.shutdown()


class SDKHandler(BaseHTTPRequestHandler):
    """Handler."""

    def __init__(self, split_changes, segment_changes, *args, **kwargs):
        """Construct a handler."""
        self._req_queue = kwargs.get('req_queue')
        self._auth_response = kwargs.get('auth_response')
        self._split_changes = split_changes
        self._segment_changes = segment_changes
        BaseHTTPRequestHandler.__init__(self, *args)

    def _parse_qs(self):
        raw_query = self.path.split('?')[1] if '?' in self.path else ''
        return dict([item.split('=') for item in raw_query.split('&')])

    def _handle_segment_changes(self):
        qstring = self._parse_qs()
        since = int(qstring.get('since', -1))
        name = self.path.split('/')[-1].split('?')[0]
        if name is None:
            self.send_response(400)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write('{}'.encode('utf-8'))
            return

        to_send = self._segment_changes.get((name, since,))
        if to_send is None:
            self.send_response(404)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write('{}'.encode('utf-8'))
            return

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(to_send).encode('utf-8'))

    def _handle_split_changes(self):
        qstring = self._parse_qs()
        since = int(qstring.get('since', -1))
        to_send = self._split_changes.get(since)
        if to_send is None:
            self.send_response(404)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write('{}'.encode('utf-8'))
            return

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(to_send).encode('utf-8'))

    def _handle_auth(self):
        if not self._auth_response:
            self.send_response(401)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write('{}'.encode('utf-8'))
            return

        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        self.wfile.write(json.dumps(self._auth_response).encode('utf-8'))

    def do_GET(self):  #pylint:disable=invalid-name
        """Respond to a GET request."""
        if self._req_queue is not None:
            headers = self._format_headers()
            self._req_queue.put(Request('GET', self.path, headers, None))

        if self.path.startswith('/api/splitChanges'):
            self._handle_split_changes()
        elif self.path.startswith('/api/segmentChanges'):
            self._handle_segment_changes()
        elif self.path.startswith('/api/v2/auth'):
            self._handle_auth()
        else:
            self.send_response(404)
            self.send_header("Content-type", "application/json")
            self.end_headers()

    def do_POST(self):  #pylint:disable=invalid-name
        """Respond to a GET request."""
        if self._req_queue is not None:
            headers = self._format_headers()
            length = int(headers.get('content-length'))
            body = self.rfile.read(length) if length else None
            self._req_queue.put(Request('POST', self.path, headers, body))

        if self.path in set(['/api/testImpressions/bulk', '/testImpressions/count',
                             '/api/events/bulk']):

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
        else:
            self.send_response(404)
            self.send_header("Content-type", "application/json")
            self.end_headers()

    def _format_headers(self):
        """Format headers and return them as a dict."""
        return dict(zip([k.lower() for k in self.headers.keys()], self.headers.values()))
