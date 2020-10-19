"""Low-level SSE Client."""
import logging
import socket
from collections import namedtuple

try:  # try to import python3 names. fallback to python2
    from http.client import HTTPConnection, HTTPSConnection
    from urllib.parse import urlparse
except ImportError:
    import urlparse
    from httplib import HTTPConnection, HTTPSConnection


_LOGGER = logging.getLogger(__name__)


SSE_EVENT_ERROR = 'error'
SSE_EVENT_MESSAGE = 'message'


SSEEvent = namedtuple('SSEEvent', ['event_id', 'event', 'retry', 'data'])


class EventBuilder(object):
    """Event builder class."""

    _SEPARATOR = b':'

    def __init__(self):
        """Construct a builder."""
        self._lines = {}

    def process_line(self, line):
        """
        Process a new line.

        :param line: Line to process
        :type line: bytes
        """
        try:
            key, val = line.split(self._SEPARATOR, 1)
            self._lines[key.decode('utf8').strip()] = val.decode('utf8').strip()
        except ValueError:  # key without a value
            self._lines[line.decode('utf8').strip()] = None

    def build(self):
        """Construct an event with relevant fields."""
        return SSEEvent(self._lines.get('id'), self._lines.get('event'),
                        self._lines.get('retry'), self._lines.get('data'))


class SSEClient(object):
    """SSE Client implementation."""

    _DEFAULT_HEADERS = {'Accept': 'text/event-stream'}
    _EVENT_SEPARATORS = set([b'\n', b'\r\n'])

    def __init__(self, callback):
        """
        Construct an SSE client.

        :param callback: function to call when an event is received
        :type callback: callable
        """
        self._connection = None
        self._event_callback = callback
        self._shutdown_requested = False

    def _read_events(self):
        """
        Read events from the supplied connection.

        :returns: True if the connection was ended by us. False if it was closed by the serve.
        :rtype: bool
        """
        try:
            response = self._connection.getresponse()
            event_builder = EventBuilder()
            while True:
                line = response.readline()
                if line is None or len(line) <= 0:  # connection ended
                    _LOGGER.info("sse connection has ended.")
                    break
                elif line.startswith(b':'):  # comment. Skip
                    _LOGGER.debug("skipping sse comment")
                    continue
                elif line in self._EVENT_SEPARATORS:
                    event = event_builder.build()
                    _LOGGER.debug("dispatching event: %s", event)
                    self._event_callback(event)
                    event_builder = EventBuilder()
                else:
                    event_builder.process_line(line)
        except Exception:  #pylint:disable=broad-except
            _LOGGER.info('sse connection ended.')
            _LOGGER.debug(exc_info=True)
        finally:
            self._connection.close()
            self._connection = None  # clear so it can be started again

        return self._shutdown_requested

    def start(self, url, headers=None):  #pylint:disable=dangerous-default-value
        """
        Connect and start listening for events.

        :param url: url to connect to
        :type url: str

        :param headers: additional headers
        :type headers: dict[str, str]

        :returns: True if the connection was ended by us. False if it was closed by the serve.
        :rtype: bool
        """
        if self._connection is not None:
            raise RuntimeError('Client already started.')

        url = urlparse(url)
        headers = self._DEFAULT_HEADERS.copy()
        headers.update(headers if headers is not None else {})
        self._connection = HTTPSConnection(url.hostname, url.port) if url.scheme == 'https' \
            else HTTPConnection(url.hostname, port=url.port)

        self._connection.request('GET', '%s?%s' % (url.path, url.query), headers=headers)
        return self._read_events()

    def shutdown(self):
        """Shutdown the current connection."""
        if self._connection is None:
            _LOGGER.warn("no sse connection has been started on this SSEClient instance. Ignoring")
            return

        if self._shutdown_requested:
            _LOGGER.warn("shutdown already requested")
            return

        self._shutdown_requested = True
        self._connection.sock.shutdown(socket.SHUT_RDWR)
