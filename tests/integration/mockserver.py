"""SDK mock server."""
from collections import namedtuple
import threading
import json

from http.server import HTTPServer, BaseHTTPRequestHandler


Request = namedtuple('Request', ['method', 'path', 'headers', 'body'])


class SplitMockServer(object):
    """SDK server mock for testing purposes."""

    protocol_version = 'HTTP/1.1'

    def __init__(self, split_changes=None, segment_changes=None, req_queue=None):
        """
        Consruct a mock server.

        :param changes: mapping of changeNumbers to splitChanges responses
        :type changes: dict
        """
        split_changes = split_changes if split_changes is not None else {}
        segment_changes = segment_changes if segment_changes is not None else {}
        self._server = HTTPServer(('localhost', 0),
                                  lambda *xs: SDKHandler(split_changes, segment_changes, *xs,
                                                         req_queue=req_queue))  # pylint:disable=line-too-long
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
        self._split_changes = split_changes
        self._segment_changes = segment_changes
        BaseHTTPRequestHandler.__init__(self, *args)

    def _parse_qs(self):
        raw_query = self.path.split('?')[1] if '?' in self.path else ''
        return dict([item.split('=') for item in raw_query.split('&')])

    def _handle_segment_changes(self):
        qstring = self._parse_qs()
        since = int(qstring.get('since', -1))
        name = qstring.get('name')
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

    def do_GET(self):  #pylint:disable=invalid-name
        """Respond to a GET request."""
        if self._req_queue is not None:
            headers = dict(zip(self.headers.keys(), self.headers.values()))
            self._req_queue.put(Request('GET', self.path, headers, None))

        if self.path.startswith('/api/splitChanges'):
            self._handle_split_changes()
        elif self.path.startswith('/api/segmentChanges'):
            self._handle_segment_changes()
        else:
            self.send_response(404)
            self.send_header("Content-type", "application/json")
            self.end_headers()

    def do_POST(self):  #pylint:disable=invalid-name
        """Respond to a GET request."""
        if self._req_queue is not None:
            length = int(self.headers.getheader('content-length'))
            body = self.rfile.read(length) if length else None
            headers = dict(zip(self.headers.keys(), self.headers.values()))
            self._req_queue.put(Request('GET', self.path, headers, body))

        if self.path in set(['/api/testImpressions/bulk', '/testImpressions/count',
                             '/api/events/bulk', '/metrics/times', '/metrics/count',
                             '/metrics/gauge']):

            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
        else:
            self.send_response(404)
            self.send_header("Content-type", "application/json")
            self.end_headers()
