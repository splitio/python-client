"""An SSE client wrapper to be used with split endpoint."""
import logging
import threading
from enum import Enum
import abc
import sys

from splitio.push.sse import SSEClient, SSEClientAsync, SSE_EVENT_ERROR
from splitio.util.threadutil import EventGroup
from splitio.api import headers_from_metadata
from splitio.optional.loaders import asyncio

if sys.version_info.major == 3 and sys.version_info.minor < 10:
  from splitio.optional.loaders import _anext as anext

_LOGGER = logging.getLogger(__name__)

class SplitSSEClientBase(object, metaclass=abc.ABCMeta):
    """Split streaming endpoint SSE base client."""

    KEEPALIVE_TIMEOUT = 70

    class _Status(Enum):
        IDLE = 0
        CONNECTING = 1
        ERRORED = 2
        CONNECTED = 3

    def __init__(self, base_url):
        """
        Construct a split sse client.

        :param base_url: scheme + :// + host
        :type base_url: str
        """
        self._base_url = base_url

    @staticmethod
    def _format_channels(channels):
        """
        Format channels into a list from the raw object retrieved in the token.

        :param channels: object as extracted from the JWT capabilities.
        :type channels: dict[str,list[str]]

        :returns: channels as a list of strings.
        :rtype: list[str]
        """
        regular = [k for (k, v) in channels.items() if v == ['subscribe']]
        occupancy = ['[?occupancy=metrics.publishers]' + k
                     for (k, v) in channels.items()
                     if 'channel-metadata:publishers' in v]
        return regular + occupancy

    def _build_url(self, token):
        """
        Build the url to connect to and return it as a string.

        :param token: (parsed) JWT
        :type token: splitio.models.token.Token

        :returns: true if the connection was successful. False otherwise.
        :rtype: bool
        """
        return '{base}/event-stream?v=1.1&accessToken={token}&channels={channels}'.format(
            base=self._base_url,
            token=token.token,
            channels=','.join(self._format_channels(token.channels)))

    @abc.abstractmethod
    def start(self, token):
        """Open a connection to start listening for events."""

    @abc.abstractmethod
    def stop(self, blocking=False, timeout=None):
        """Abort the ongoing connection."""


class SplitSSEClient(SplitSSEClientBase):  # pylint: disable=too-many-instance-attributes
    """Split streaming endpoint SSE client."""

    def __init__(self, event_callback, sdk_metadata, first_event_callback=None,
                 connection_closed_callback=None, client_key=None,
                 base_url='https://streaming.split.io'):
        """
        Construct a split sse client.

        :param callback: fuction to call when an event is received.
        :type callback: callable

        :param sdk_metadata: SDK version & machine name & IP.
        :type sdk_metadata: splitio.client.util.SdkMetadata

        :param first_event_callback: function to call when the first event is received.
        :type first_event_callback: callable

        :param connection_closed_callback: funciton to call when the connection ends.
        :type connection_closed_callback: callable

        :param base_url: scheme + :// + host
        :type base_url: str

        :param client_key: client key.
        :type client_key: str
        """
        SplitSSEClientBase.__init__(self, base_url)
        self._client = SSEClient(self._raw_event_handler)
        self._callback = event_callback
        self._on_connected = first_event_callback
        self._on_disconnected = connection_closed_callback
        self._status = SplitSSEClient._Status.IDLE
        self._sse_first_event = None
        self._sse_connection_closed = None
        self._metadata = headers_from_metadata(sdk_metadata, client_key)

    def _raw_event_handler(self, event):
        """
        Handle incoming raw sse event.

        :param event: Incoming raw sse event.
        :type event: splitio.push.sse.SSEEvent
        """
        if self._status == SplitSSEClient._Status.CONNECTING:
            self._status = SplitSSEClient._Status.CONNECTED if event.event != SSE_EVENT_ERROR \
                else SplitSSEClient._Status.ERRORED
            self._sse_first_event.set()
            if self._on_connected is not None:
                self._on_connected()

        if event.data is not None:
            self._callback(event)

    def start(self, token):
        """
        Open a connection to start listening for events.

        :param token: (parsed) JWT
        :type token: splitio.models.token.Token

        :returns: true if the connection was successful. False otherwise.
        :rtype: bool
        """
        if self._status != SplitSSEClient._Status.IDLE:
            raise Exception('SseClient already started.')

        self._status = SplitSSEClient._Status.CONNECTING

        event_group = EventGroup()
        self._sse_first_event = event_group.make_event()
        self._sse_connection_closed = event_group.make_event()

        def connect(url):
            """Connect to sse in a blocking manner."""
            try:
                self._client.start(url, timeout=self.KEEPALIVE_TIMEOUT,
                                   extra_headers=self._metadata)
            finally:
                self._status = SplitSSEClient._Status.IDLE
                self._sse_connection_closed.set()
                self._on_disconnected()

        url = self._build_url(token)
        task = threading.Thread(target=connect, name='SSEConnection', args=(url,), daemon=True)
        task.start()
        event_group.wait()
        return self._status == SplitSSEClient._Status.CONNECTED

    def stop(self, blocking=False, timeout=None):
        """Abort the ongoing connection."""
        if self._status == SplitSSEClient._Status.IDLE:
            _LOGGER.warning('sse already closed. ignoring')
            return

        self._client.shutdown()
        if blocking:
            self._sse_connection_closed.wait(timeout)

class SplitSSEClientAsync(SplitSSEClientBase):  # pylint: disable=too-many-instance-attributes
    """Split streaming endpoint SSE client."""

    def __init__(self, sdk_metadata, client_key=None, base_url='https://streaming.split.io'):
        """
        Construct a split sse client.

        :param sdk_metadata: SDK version & machine name & IP.
        :type sdk_metadata: splitio.client.util.SdkMetadata

        :param client_key: client key.
        :type client_key: str

        :param base_url: scheme + :// + host
        :type base_url: str
        """
        SplitSSEClientBase.__init__(self, base_url)
        self.status = SplitSSEClient._Status.IDLE
        self._metadata = headers_from_metadata(sdk_metadata, client_key)
        self._client = SSEClientAsync(self.KEEPALIVE_TIMEOUT)
        self._event_source = None
        self._event_source_ended = asyncio.Event()

    async def start(self, token):
        """
        Open a connection to start listening for events.

        :param token: (parsed) JWT
        :type token: splitio.models.token.Token

        :returns: yield events received from SSEClientAsync object
        :rtype: SSEEvent
        """
        _LOGGER.debug(self.status)
        if self.status != SplitSSEClient._Status.IDLE:
            raise Exception('SseClient already started.')

        self.status = SplitSSEClient._Status.CONNECTING
        url = self._build_url(token)
        try:
            self._event_source_ended.clear()
            self._event_source = self._client.start(url, extra_headers=self._metadata)
            first_event = await anext(self._event_source)
            if first_event.event == SSE_EVENT_ERROR:
                return

            yield first_event
            self.status = SplitSSEClient._Status.CONNECTED
            _LOGGER.debug("Split SSE client started")
            async for event in self._event_source:
                if event.data is not None:
                    yield event
        except Exception:  # pylint:disable=broad-except
            _LOGGER.error('SplitSSE Client Exception')
            _LOGGER.debug('stack trace: ', exc_info=True)
        finally:
            self.status = SplitSSEClient._Status.IDLE
            _LOGGER.debug('Split sse connection ended.')
            self._event_source_ended.set()

    async def stop(self):
        """Abort the ongoing connection."""
        _LOGGER.debug("stopping SplitSSE Client")
        if self.status == SplitSSEClient._Status.IDLE:
            _LOGGER.warning('sse already closed. ignoring')
            return

        await self._client.shutdown()
# catching exception to avoid task hanging
        try:
            await self._event_source_ended.wait()
        except asyncio.CancelledError as e:
            _LOGGER.error("Exception waiting for event source ended")
            _LOGGER.debug('stack trace: ', exc_info=True)
            pass

    async def close_sse_http_client(self):
        await self._client.close_session()
