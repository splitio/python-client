"""Low-level SSE Client."""
import logging
import socket
from collections import namedtuple
from http.client import HTTPConnection, HTTPSConnection
from urllib.parse import urlparse

from splitio.optional.loaders import asyncio, aiohttp

_LOGGER = logging.getLogger(__name__)

SSE_EVENT_ERROR = 'error'
SSE_EVENT_MESSAGE = 'message'
_DEFAULT_HEADERS = {'accept': 'text/event-stream'}
_EVENT_SEPARATORS = set([b'\n', b'\r\n'])
_DEFAULT_SOCKET_READ_TIMEOUT = 70

SSEEvent = namedtuple('SSEEvent', ['event_id', 'event', 'retry', 'data'])


__ENDING_CHARS = set(['\n', ''])

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

    def __init__(self, callback):
        """
        Construct an SSE client.

        :param callback: function to call when an event is received
        :type callback: callable
        """
        self._conn = None
        self._event_callback = callback
        self._shutdown_requested = False

    def _read_events(self):
        """
        Read events from the supplied connection.

        :returns: True if the connection was ended by us. False if it was closed by the serve.
        :rtype: bool
        """
        try:
            response = self._conn.getresponse()
            event_builder = EventBuilder()
            while True:
                line = response.readline()
                if line is None or len(line) <= 0:  # connection ended
                    break
                elif line.startswith(b':'):  # comment. Skip
                    _LOGGER.debug("skipping sse comment")
                    continue
                elif line in _EVENT_SEPARATORS:
                    event = event_builder.build()
                    _LOGGER.debug("dispatching event: %s", event)
                    self._event_callback(event)
                    event_builder = EventBuilder()
                else:
                    event_builder.process_line(line)
        except Exception:  # pylint:disable=broad-except
            _LOGGER.debug('sse connection ended.')
            _LOGGER.debug('stack trace: ', exc_info=True)
        finally:
            self._conn.close()
            self._conn = None  # clear so it can be started again

        return self._shutdown_requested

    def start(self, url, extra_headers=None, timeout=socket._GLOBAL_DEFAULT_TIMEOUT):  # pylint:disable=protected-access
        """
        Connect and start listening for events.

        :param url: url to connect to
        :type url: str

        :param extra_headers: additional headers
        :type extra_headers: dict[str, str]

        :param timeout: connection & read timeout
        :type timeout: float

        :returns: True if the connection was ended by us. False if it was closed by the serve.
        :rtype: bool
        """
        if self._conn is not None:
            raise RuntimeError('Client already started.')

        self._shutdown_requested = False
        url, headers = urlparse(url), get_headers(extra_headers)
        self._conn = (HTTPSConnection(url.hostname, url.port, timeout=timeout)
                      if url.scheme == 'https'
                      else HTTPConnection(url.hostname, port=url.port, timeout=timeout))

        self._conn.request('GET', '%s?%s' % (url.path, url.query), headers=headers)
        return self._read_events()

    def shutdown(self):
        """Shutdown the current connection."""
        if self._conn is None or self._conn.sock is None:
            _LOGGER.warning("no sse connection has been started on this SSEClient instance. Ignoring")
            return

        if self._shutdown_requested:
            _LOGGER.warning("shutdown already requested")
            return

        self._shutdown_requested = True
        self._conn.sock.shutdown(socket.SHUT_RDWR)


class SSEClientAsync(object):
    """SSE Client implementation."""

    def __init__(self, socket_read_timeout=_DEFAULT_SOCKET_READ_TIMEOUT):
        """
        Construct an SSE client.

        :param url: url to connect to
        :type url: str

        :param extra_headers: additional headers
        :type extra_headers: dict[str, str]

        :param timeout: connection & read timeout
        :type timeout: float
        """
        self._socket_read_timeout = socket_read_timeout + socket_read_timeout * .3
        self._response = None
        self._done = asyncio.Event()
        client_timeout = aiohttp.ClientTimeout(total=0, sock_read=self._socket_read_timeout)
        self._sess = aiohttp.ClientSession(timeout=client_timeout)

    async def start(self, url, extra_headers=None):  # pylint:disable=protected-access
        """
        Connect and start listening for events.

        :returns: yield event when received
        :rtype: SSEEvent
        """
        _LOGGER.debug("Async SSEClient Started")
        if self._response is not None:
            raise RuntimeError('Client already started.')

        self._done.clear()
        try:
            async with self._sess.get(url, headers=get_headers(extra_headers)) as response:
                self._response = response
                event_builder = EventBuilder()
                async for line in response.content:
                    if line.startswith(b':'):
                        _LOGGER.debug("skipping emtpy line / comment")
                        continue
                    elif line in _EVENT_SEPARATORS:
                        _LOGGER.debug("dispatching event: %s", event_builder.build())
                        yield event_builder.build()
                        event_builder = EventBuilder()
                    else:
                        event_builder.process_line(line)

        except Exception as exc:  # pylint:disable=broad-except
            if self._is_conn_closed_error(exc):
                _LOGGER.debug('sse connection ended.')
                return

            _LOGGER.error('http client is throwing exceptions')
            _LOGGER.error('stack trace: ', exc_info=True)

        finally:
            self._response = None
            self._done.set()

    async def shutdown(self):
        """Close connection"""
        if self._response:
            self._response.close()
        # catching exception to avoid task hanging if a canceled exception occurred
        try:
            await self._done.wait()
        except asyncio.CancelledError:
            _LOGGER.error("Exception waiting for SSE connection to end")
            _LOGGER.debug('stack trace: ', exc_info=True)
            pass

    @staticmethod
    def _is_conn_closed_error(exc):
        """Check if the ReadError is caused by the connection being closed."""
        return isinstance(exc, aiohttp.ClientConnectionError) and str(exc) == "Connection closed"

    async def close_session(self):
        if not self._sess.closed:
            await self._sess.close()

def get_headers(extra=None):
    """
    Return default headers with added custom ones if specified.

    :param extra: additional headers
    :type extra: dict[str, str]

    :returns: processed Headers
    :rtype: dict
    """
    headers = _DEFAULT_HEADERS.copy()
    headers.update(extra if extra is not None else {})
    return headers
