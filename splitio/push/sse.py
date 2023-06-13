"""Low-level SSE Client."""
import logging
import socket
import abc
from collections import namedtuple
from http.client import HTTPConnection, HTTPSConnection
from urllib.parse import urlparse

from splitio.optional.loaders import asyncio, aiohttp
from splitio.api.client import HttpClientException

_LOGGER = logging.getLogger(__name__)

SSE_EVENT_ERROR = 'error'
SSE_EVENT_MESSAGE = 'message'
_DEFAULT_HEADERS = {'accept': 'text/event-stream'}
_EVENT_SEPARATORS = set([b'\n', b'\r\n'])
_DEFAULT_ASYNC_TIMEOUT = 300

SSEEvent = namedtuple('SSEEvent', ['event_id', 'event', 'retry', 'data'])


__ENDING_CHARS = set(['\n', ''])

def _get_request_parameters(url, extra_headers):
    """
    Parse URL and headers

    :param url: url to connect to
    :type url: str

    :param extra_headers: additional headers
    :type extra_headers: dict[str, str]

    :returns: processed URL and Headers
    :rtype: str, dict
    """
    url = urlparse(url)
    headers = _DEFAULT_HEADERS.copy()
    headers.update(extra_headers if extra_headers is not None else {})
    return url, headers

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

class SSEClientBase(object, metaclass=abc.ABCMeta):
    """Worker template."""

    @abc.abstractmethod
    def start(self, url, extra_headers, timeout):  # pylint:disable=protected-access
        """Connect and start listening for events."""

    @abc.abstractmethod
    def shutdown(self):
        """Shutdown the current connection."""

class SSEClient(SSEClientBase):
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
        url, headers = _get_request_parameters(url, extra_headers)
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

class SSEClientAsync(SSEClientBase):
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

    async def _read_events(self, response):
        """
        Read events from the supplied connection.

        :returns: True if the connection was ended by us. False if it was closed by the serve.
        :rtype: bool
        """
        try:
            event_builder = EventBuilder()
            while not self._shutdown_requested:
                line = await response.readline()
                if line is None or len(line) <= 0:  # connection ended
                    break
                elif line.startswith(b':'):  # comment. Skip
                    _LOGGER.debug("skipping sse comment")
                    continue
                elif line in _EVENT_SEPARATORS:
                    event = event_builder.build()
                    _LOGGER.debug("dispatching event: %s", event)
                    await self._event_callback(event)
                    event_builder = EventBuilder()
                else:
                    event_builder.process_line(line)
        except asyncio.CancelledError:
           _LOGGER.debug("Cancellation request, proceeding to cancel.")
           raise
        except Exception:  # pylint:disable=broad-except
            _LOGGER.debug('sse connection ended.')
            _LOGGER.debug('stack trace: ', exc_info=True)
        finally:
            await self._conn.close()
            self._conn = None  # clear so it can be started again

        return self._shutdown_requested

    async def start(self, url, extra_headers=None, timeout=_DEFAULT_ASYNC_TIMEOUT):  # pylint:disable=protected-access
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
        _LOGGER.debug("Async SSEClient Started")
        if self._conn is not None:
            raise RuntimeError('Client already started.')

        self._shutdown_requested = False
        url = urlparse(url)
        headers = _DEFAULT_HEADERS.copy()
        headers.update(extra_headers if extra_headers is not None else {})
        parsed_url =  url[0] + "://" + url[1] + url[2]
        params=url[4]
        try:
            self._conn = aiohttp.connector.TCPConnector()
            async with aiohttp.client.ClientSession(
                connector=self._conn,
                headers={'accept': 'text/event-stream'},
                timeout=aiohttp.ClientTimeout(timeout)
                ) as self._session:
                reader = await self._session.request(
                    "GET",
                    parsed_url,
                    params=params
                )
                return await self._read_events(reader.content)
        except aiohttp.ClientError as exc:  # pylint: disable=broad-except
            _LOGGER.error(str(exc))
            raise HttpClientException('http client is throwing exceptions') from exc

    async def shutdown(self):
        """Shutdown the current connection."""
        _LOGGER.debug("Async SSEClient Shutdown")
        if self._conn is None:
            _LOGGER.warning("no sse connection has been started on this SSEClient instance. Ignoring")
            return

        if self._shutdown_requested:
            _LOGGER.warning("shutdown already requested")
            return

        self._shutdown_requested = True
        sock = self._session.connector._loop._ssock
        sock.shutdown(socket.SHUT_RDWR)
        await self._conn.close()
        for task in asyncio.Task.all_tasks():
            if not task.done():
                if task._coro.cr_code.co_name == 'connect_split_sse_client':
                    task.cancel()