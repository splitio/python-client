"""An SSE client wrapper to be used with split endpoint."""
import logging
import threading
from enum import Enum
import abc
from splitio.push.sse import SSEClient, SSEClientAsync, SSE_EVENT_ERROR
from splitio.util.threadutil import EventGroup
from splitio.api import headers_from_metadata
from splitio.optional.loaders import asyncio

_LOGGER = logging.getLogger(__name__)

KEEPALIVE_TIMEOUT = 70

class SplitSSEClientBase(object, metaclass=abc.ABCMeta):  # pylint: disable=too-many-instance-attributes

    @abc.abstractmethod
    def start(self, token):
        """Open a connection to start listening for events."""

    @abc.abstractmethod
    def stop(self, token):
        """Open a connection to start listening for events."""

    class _Status(Enum):
        IDLE = 0
        CONNECTING = 1
        ERRORED = 2
        CONNECTED = 3

    def _build_url(self, token, base_url):
        """
        Build the url to connect to and return it as a string.

        :param token: (parsed) JWT
        :type token: splitio.models.token.Token

        :returns: true if the connection was successful. False otherwise.
        :rtype: bool
        """
        return '{base}/event-stream?v=1.1&accessToken={token}&channels={channels}'.format(
            base=base_url,
            token=token.token,
            channels=','.join(self._format_channels(token.channels)))

    def _format_channels(self, channels):
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
        self._client = SSEClient(self._raw_event_handler)
        self._callback = event_callback
        self._on_connected = first_event_callback
        self._on_disconnected = connection_closed_callback
        self._base_url = base_url
        self._status = self._Status.IDLE
        self._sse_first_event = None
        self._sse_connection_closed = None
        self._metadata = headers_from_metadata(sdk_metadata, client_key)

    def _raw_event_handler(self, event):
        """
        Handle incoming raw sse event.

        :param event: Incoming raw sse event.
        :type event: splitio.push.sse.SSEEvent
        """
        if self._status == self._Status.CONNECTING:
            self._status = self._Status.CONNECTED if event.event != SSE_EVENT_ERROR \
                else self._Status.ERRORED
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
        if self._status != self._Status.IDLE:
            raise Exception('SseClient already started.')

        self._status = self._Status.CONNECTING

        event_group = EventGroup()
        self._sse_first_event = event_group.make_event()
        self._sse_connection_closed = event_group.make_event()

        def connect(url):
            """Connect to sse in a blocking manner."""
            try:
                self._client.start(url, timeout=KEEPALIVE_TIMEOUT,
                                   extra_headers=self._metadata)
            finally:
                self._status = self._Status.IDLE
                self._sse_connection_closed.set()
                self._on_disconnected()

        url = self._build_url(token, self._base_url)
        task = threading.Thread(target=connect, name='SSEConnection', args=(url,), daemon=True)
        task.start()
        event_group.wait()
        return self._status == self._Status.CONNECTED

    def stop(self, blocking=False, timeout=None):
        """Abort the ongoing connection."""
        if self._status == self._Status.IDLE:
            _LOGGER.warning('sse already closed. ignoring')
            return

        self._client.shutdown()
        if blocking:
            self._sse_connection_closed.wait(timeout)

class SplitSSEClientAsync(SplitSSEClientBase):  # pylint: disable=too-many-instance-attributes
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
        self._client = SSEClientAsync(self._raw_event_handler)
        self._callback = event_callback
        self._on_connected = first_event_callback
        self._on_disconnected = connection_closed_callback
        self._base_url = base_url
        self._status = self._Status.IDLE
        self._sse_first_event = None
        self._sse_connection_closed = None
        self._metadata = headers_from_metadata(sdk_metadata, client_key)
        self._raw_event_first_call = False

    async def _raw_event_handler(self, event):
        """
        Handle incoming raw sse event.

        :param event: Incoming raw sse event.
        :type event: splitio.push.sse.SSEEvent
        """
        self._raw_event_first_call = True
        if self._status == self._Status.CONNECTING:
            self._status = self._Status.CONNECTED if event.event != SSE_EVENT_ERROR \
                else self._Status.ERRORED
            self._sse_first_event.set()
            if self._on_connected is not None:
                await self._on_connected()

        if event.data is not None:
            await self._callback(event)

    async def start(self, token):
        """
        Open a connection to start listening for events.

        :param token: (parsed) JWT
        :type token: splitio.models.token.Token

        :returns: true if the connection was successful. False otherwise.
        :rtype: bool
        """
        if self._status != self._Status.IDLE:
            raise Exception('SseClient already started.')

        self._status = self._Status.CONNECTING

        event_group = EventGroup()
        self._sse_first_event = event_group.make_event()
        self._sse_connection_closed = event_group.make_event()

        async def connect_split_sse_client(url):
            """Connect to sse in a blocking manner."""
            try:
                await self._client.start(url, timeout=KEEPALIVE_TIMEOUT,
                                   extra_headers=self._metadata)
            finally:
                self._status = self._Status.IDLE
                self._sse_connection_closed.set()
                self._raw_event_first_call = True
                await self._on_disconnected()

        url = self._build_url(token, self._base_url)
        self._sse_client_task = asyncio.get_running_loop().create_task(connect_split_sse_client(url))

        while not self._raw_event_first_call:
            await asyncio.sleep(.1)
        return self._status == self._Status.CONNECTED

    async def stop(self, blocking=False, timeout=None):
        """Abort the ongoing connection."""
        if self._status == self._Status.IDLE:
            _LOGGER.warning('sse already closed. ignoring')
            return

        await self._client.shutdown()
        if not self._sse_client_task.done():
            self._sse_client_task.cancel()

        if  self._status != self._Status.IDLE:
            self._status = self._Status.IDLE
            self._sse_connection_closed.set()
            await self._on_disconnected()
