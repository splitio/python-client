"""SSE mock server."""
import queue
import threading

from http.server import HTTPServer, BaseHTTPRequestHandler


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
            self._req_queue.put(self.path)

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
