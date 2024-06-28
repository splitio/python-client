"""Push subsystem manager class and helpers."""
import logging
from threading import Timer
import abc
import sys

from splitio.optional.loaders import asyncio
from splitio.api import APIException
from splitio.util.time import get_current_epoch_time_ms
from splitio.push import AuthException
from splitio.push.splitsse import SplitSSEClient, SplitSSEClientAsync
from splitio.push.sse import SSE_EVENT_ERROR
from splitio.push.parser import parse_incoming_event, EventParsingException, EventType, \
    MessageType
from splitio.push.processor import MessageProcessor, MessageProcessorAsync
from splitio.push.status_tracker import PushStatusTracker, Status, PushStatusTrackerAsync
from splitio.models.telemetry import StreamingEventTypes

if sys.version_info.major == 3 and sys.version_info.minor < 10:
  from splitio.optional.loaders import _anext as anext

_TOKEN_REFRESH_GRACE_PERIOD = 10 * 60  # 10 minutes

_LOGGER = logging.getLogger(__name__)

class PushManagerBase(object, metaclass=abc.ABCMeta):
    """Worker template."""

    @abc.abstractmethod
    def update_workers_status(self, enabled):
        """Enable/Disable push update workers."""

    @abc.abstractmethod
    def start(self):
        """Start a new connection if not already running."""

    @abc.abstractmethod
    def stop(self, blocking=False):
        """Stop the current ongoing connection."""

    def _get_time_period(self, token):
        return (token.exp - token.iat) - _TOKEN_REFRESH_GRACE_PERIOD


class PushManager(PushManagerBase):  # pylint:disable=too-many-instance-attributes
    """Push notifications susbsytem manager."""

    def __init__(self, auth_api, synchronizer, feedback_loop, sdk_metadata, telemetry_runtime_producer, sse_url=None, client_key=None):
        """
        Class constructor.

        :param auth_api: sdk-auth-service api client
        :type auth_api: splitio.api.auth.AuthAPI

        :param synchronizer: split data synchronizer facade
        :type synchronizer: splitio.sync.synchronizer.Synchronizer

        :param feedback_loop: queue where push status updates are published.
        :type feedback_loop: queue.Queue

        :param sdk_metadata: SDK version & machine name & IP.
        :type sdk_metadata: splitio.client.util.SdkMetadata

        :param telemetry_runtime_producer: Telemetry object to record runtime events
        :type sdk_metadata: splitio.engine.telemetry.TelemetryRunTimeProducer

        :param sse_url: streaming base url.
        :type sse_url: str

        :param client_key: client key.
        :type client_key: str
        """
        self._auth_api = auth_api
        self._feedback_loop = feedback_loop
        self._processor = MessageProcessor(synchronizer, telemetry_runtime_producer)
        self._status_tracker = PushStatusTracker(telemetry_runtime_producer)
        self._event_handlers = {
            EventType.MESSAGE: self._handle_message,
            EventType.ERROR: self._handle_error
        }

        self._message_handlers = {
            MessageType.UPDATE: self._handle_update,
            MessageType.CONTROL: self._handle_control,
            MessageType.OCCUPANCY: self._handle_occupancy
        }

        kwargs = {} if sse_url is None else {'base_url': sse_url}
        self._sse_client = SplitSSEClient(self._event_handler, sdk_metadata, self._handle_connection_ready,
                                          self._handle_connection_end, client_key, **kwargs)
        self._running = False
        self._next_refresh = Timer(0, lambda: 0)
        self._telemetry_runtime_producer = telemetry_runtime_producer


    def update_workers_status(self, enabled):
        """
        Enable/Disable push update workers.

        :param enabled: if True, enable workers. If False, disable them.
        :type enabled: bool
        """
        self._processor.update_workers_status(enabled)

    def start(self):
        """Start a new connection if not already running."""
        if self._running:
            _LOGGER.warning('Push manager already has a connection running. Ignoring')
            return

        self._trigger_connection_flow()

    def stop(self, blocking=False):
        """
        Stop the current ongoing connection.

        :param blocking: whether to wait for the connection to be successfully closed or not
        :type blocking: bool
        """
        if not self._running:
            _LOGGER.warning('Push manager does not have an open SSE connection. Ignoring')
            return

        self._running = False
        self._processor.update_workers_status(False)
        self._status_tracker.notify_sse_shutdown_expected()
        self._next_refresh.cancel()
        self._sse_client.stop(blocking)

    def _event_handler(self, event):
        """
        Process an incoming event.

        :param event: Incoming event
        :type event: splitio.push.sse.SSEEvent
        """
        try:
            parsed = parse_incoming_event(event)
        except EventParsingException:
            _LOGGER.error('error parsing event of type %s', event.event_type)
            _LOGGER.debug(str(event), exc_info=True)
            return

        try:
            handle = self._event_handlers[parsed.event_type]
        except KeyError:
            _LOGGER.error('no handler for message of type %s', parsed.event_type)
            _LOGGER.debug(str(event), exc_info=True)
            return

        try:
            handle(parsed)
        except Exception:  # pylint:disable=broad-except
            _LOGGER.error('something went wrong when processing message of type %s',
                          parsed.event_type)
            _LOGGER.debug(str(parsed), exc_info=True)

    def _token_refresh(self):
        """Refresh auth token."""
        _LOGGER.info("retriggering authentication flow.")
        self.stop(True)
        self._trigger_connection_flow()

    def _trigger_connection_flow(self):
        """Authenticate and start a connection."""
        try:
            token = self._auth_api.authenticate()
        except APIException:
            _LOGGER.error('error performing sse auth request.')
            _LOGGER.debug('stack trace: ', exc_info=True)
            self._feedback_loop.put(Status.PUSH_RETRYABLE_ERROR)
            return

        if token is None or not token.push_enabled:
            self._feedback_loop.put(Status.PUSH_NONRETRYABLE_ERROR)
            return
        self._telemetry_runtime_producer.record_token_refreshes()
        _LOGGER.debug("auth token fetched. connecting to streaming.")
        self._status_tracker.reset()
        if self._sse_client.start(token):
            _LOGGER.debug("connected to streaming, scheduling next refresh")
            self._setup_next_token_refresh(token)
            self._running = True
            self._telemetry_runtime_producer.record_streaming_event((StreamingEventTypes.CONNECTION_ESTABLISHED, 0,  get_current_epoch_time_ms()))

    def _setup_next_token_refresh(self, token):
        """
        Schedule next token refresh.

        :param token: Last fetched token.
        :type token: splitio.models.token.Token
        """
        if self._next_refresh is not None:
            self._next_refresh.cancel()
        self._next_refresh = Timer(self._get_time_period(token), self._token_refresh)
        self._next_refresh.setName('TokenRefresh')
        self._next_refresh.start()
        self._telemetry_runtime_producer.record_streaming_event((StreamingEventTypes.TOKEN_REFRESH, 1000 * token.exp,  get_current_epoch_time_ms()))

    def _handle_message(self, event):
        """
        Handle incoming update message.

        :param event: Incoming Update message
        :type event: splitio.push.sse.parser.Update
        """
        try:
            handle = self._message_handlers[event.message_type]
        except KeyError:
            _LOGGER.error('no handler for message of type %s', event.message_type)
            _LOGGER.debug(str(event), exc_info=True)
            return

        handle(event)

    def _handle_update(self, event):
        """
        Handle incoming update message.

        :param event: Incoming Update message
        :type event: splitio.push.sse.parser.Update
        """
        _LOGGER.debug('handling update event: %s', str(event))
        self._processor.handle(event)

    def _handle_control(self, event):
        """
        Handle incoming control message.

        :param event: Incoming control message.
        :type event: splitio.push.sse.parser.ControlMessage
        """
        _LOGGER.debug('handling control event: %s', str(event))
        feedback = self._status_tracker.handle_control_message(event)
        if feedback is not None:
            self._feedback_loop.put(feedback)

    def _handle_occupancy(self, event):
        """
        Handle incoming notification message.

        :param event: Incoming occupancy message.
        :type event: splitio.push.sse.parser.Occupancy
        """
        _LOGGER.debug('handling occupancy event: %s', str(event))
        feedback = self._status_tracker.handle_occupancy(event)
        if feedback is not None:
            self._feedback_loop.put(feedback)

    def _handle_error(self, event):
        """
        Handle incoming error message.

        :param event: Incoming ably error
        :type event: splitio.push.sse.parser.AblyError
        """
        _LOGGER.debug('handling ably error event: %s', str(event))
        feedback = self._status_tracker.handle_ably_error(event)
        if feedback is not None:
            self._feedback_loop.put(feedback)

    def _handle_connection_ready(self):
        """Handle a successful connection to SSE."""
        self._feedback_loop.put(Status.PUSH_SUBSYSTEM_UP)
        _LOGGER.info('sse initial event received. enabling')

    def _handle_connection_end(self):
        """
        Handle a connection ending.

        If the connection shutdown was not requested, trigger a restart.
        """
        feedback = self._status_tracker.handle_disconnect()
        if feedback is not None:
            self._feedback_loop.put(feedback)


class PushManagerAsync(PushManagerBase):  # pylint:disable=too-many-instance-attributes
    """Push notifications susbsytem manager."""

    def __init__(self, auth_api, synchronizer, feedback_loop, sdk_metadata, telemetry_runtime_producer, sse_url=None, client_key=None):
        """
        Class constructor.

        :param auth_api: sdk-auth-service api client
        :type auth_api: splitio.api.auth.AuthAPI

        :param synchronizer: split data synchronizer facade
        :type synchronizer: splitio.sync.synchronizer.Synchronizer

        :param feedback_loop: queue where push status updates are published.
        :type feedback_loop: queue.Queue

        :param sdk_metadata: SDK version & machine name & IP.
        :type sdk_metadata: splitio.client.util.SdkMetadata

        :param telemetry_runtime_producer: Telemetry object to record runtime events
        :type sdk_metadata: splitio.engine.telemetry.TelemetryRunTimeProducer

        :param sse_url: streaming base url.
        :type sse_url: str

        :param client_key: client key.
        :type client_key: str
        """
        self._auth_api = auth_api
        self._feedback_loop = feedback_loop
        self._processor = MessageProcessorAsync(synchronizer, telemetry_runtime_producer)
        self._status_tracker = PushStatusTrackerAsync(telemetry_runtime_producer)
        self._event_handlers = {
            EventType.MESSAGE: self._handle_message,
            EventType.ERROR: self._handle_error
        }

        self._message_handlers = {
            MessageType.UPDATE: self._handle_update,
            MessageType.CONTROL: self._handle_control,
            MessageType.OCCUPANCY: self._handle_occupancy
        }

        kwargs = {} if sse_url is None else {'base_url': sse_url}
        self._sse_client = SplitSSEClientAsync(sdk_metadata, client_key, **kwargs)
        self._running = False
        self._telemetry_runtime_producer = telemetry_runtime_producer
        self._token_task = None

    async def update_workers_status(self, enabled):
        """
        Enable/Disable push update workers.

        :param enabled: if True, enable workers. If False, disable them.
        :type enabled: bool
        """
        await self._processor.update_workers_status(enabled)

    def start(self):
        """Start a new connection if not already running."""
        if self._running:
            _LOGGER.warning('Push manager already has a connection running. Ignoring')
            return

        self._running_task = asyncio.get_running_loop().create_task(self._trigger_connection_flow())

    async def stop(self, blocking=False):
        """
        Stop the current ongoing connection.

        :param blocking: whether to wait for the connection to be successfully closed or not
        :type blocking: bool
        """
        if not self._running:
            _LOGGER.warning('Push manager does not have an open SSE connection. Ignoring')
            return

        if self._token_task:
            self._token_task.cancel()
            self._token_task = None

        if blocking:
            await self._stop_current_conn()
        else:
            asyncio.get_running_loop().create_task(self._stop_current_conn())

    async def close_sse_http_client(self):
        await self._sse_client.close_sse_http_client()

    async def _event_handler(self, event):
        """
        Process an incoming event.

        :param event: Incoming event
        :type event: splitio.push.sse.SSEEvent
        """
        parsed = None
        try:
            parsed = parse_incoming_event(event)
            handle = self._event_handlers[parsed.event_type]
        except Exception:
            _LOGGER.error('Parsing exception or no handler for message of type %s', parsed.event_type if parsed else 'unknown')
            _LOGGER.debug(str(event), exc_info=True)
            return

        try:
            await handle(parsed)
        except Exception:  # pylint:disable=broad-except
            event_type = "unknown" if parsed is None else parsed.event_type
            _LOGGER.error('something went wrong when processing message of type %s', event_type)
            _LOGGER.debug(str(parsed), exc_info=True)

    async def _token_refresh(self, current_token):
        """Refresh auth token.

        :param current_token: token (parsed) JWT
        :type current_token: splitio.models.token.Token
        """
        _LOGGER.debug("Next token refresh in " + str(self._get_time_period(current_token)) + " seconds")
        await asyncio.sleep(self._get_time_period(current_token))
        await self._stop_current_conn()
        self._running_task = asyncio.get_running_loop().create_task(self._trigger_connection_flow())

    async def _get_auth_token(self):
        """Get new auth token"""
        try:
            token = await self._auth_api.authenticate()
        except APIException as e:
            _LOGGER.error('error performing sse auth request.')
            _LOGGER.debug('stack trace: ', exc_info=True)
            await self._feedback_loop.put(Status.PUSH_RETRYABLE_ERROR)
            raise AuthException(e)

        if token is not None and not token.push_enabled:
            await self._feedback_loop.put(Status.PUSH_NONRETRYABLE_ERROR)
            raise AuthException("Push is not enabled")

        await self._telemetry_runtime_producer.record_token_refreshes()
        await self._telemetry_runtime_producer.record_streaming_event((StreamingEventTypes.TOKEN_REFRESH, 1000 * token.exp,  get_current_epoch_time_ms()))
        _LOGGER.debug("auth token fetched. connecting to streaming.")
        return token

    async def _trigger_connection_flow(self):
        """Authenticate and start a connection."""
        self._status_tracker.reset()

        try:
            token = await self._get_auth_token()
            events_source = self._sse_client.start(token)
            self._running = True

            first_event = await anext(events_source)
            if first_event.data is not None:
                await self._event_handler(first_event)

            _LOGGER.debug("connected to streaming, scheduling next refresh")
            self._token_task = asyncio.get_running_loop().create_task(self._token_refresh(token))
            await self._handle_connection_ready()
            await self._telemetry_runtime_producer.record_streaming_event((StreamingEventTypes.CONNECTION_ESTABLISHED, 0,  get_current_epoch_time_ms()))

            async for event in events_source:
                await self._event_handler(event)
            await self._handle_connection_end()  # TODO(mredolatti): this is not tested
        except AuthException as e:
            _LOGGER.error("error getting auth token: " + str(e))
            _LOGGER.debug("trace: ", exc_info=True)
        except StopAsyncIteration: # will enter here if there was an error
            await self._feedback_loop.put(Status.PUSH_RETRYABLE_ERROR)
        finally:
            if self._token_task is not None:
                self._token_task.cancel()
                self._token_task = None
            self._running = False
            await self._processor.update_workers_status(False)

    async def _handle_message(self, event):
        """
        Handle incoming update message.

        :param event: Incoming Update message
        :type event: splitio.push.sse.parser.Update
        """
        try:
            handle = self._message_handlers[event.message_type]
        except KeyError:
            _LOGGER.error('no handler for message of type %s', event.message_type)
            _LOGGER.debug(str(event), exc_info=True)
            return

        await handle(event)

    async def _handle_update(self, event):
        """
        Handle incoming update message.

        :param event: Incoming Update message
        :type event: splitio.push.sse.parser.Update
        """
        _LOGGER.debug('handling update event: %s', str(event))
        await self._processor.handle(event)

    async def _handle_control(self, event):
        """
        Handle incoming control message.

        :param event: Incoming control message.
        :type event: splitio.push.sse.parser.ControlMessage
        """
        _LOGGER.debug('handling control event: %s', str(event))
        feedback = await self._status_tracker.handle_control_message(event)
        if feedback is not None:
            await self._feedback_loop.put(feedback)

    async def _handle_occupancy(self, event):
        """
        Handle incoming notification message.

        :param event: Incoming occupancy message.
        :type event: splitio.push.sse.parser.Occupancy
        """
        _LOGGER.debug('handling occupancy event: %s', str(event))
        feedback = await self._status_tracker.handle_occupancy(event)
        if feedback is not None:
            await self._feedback_loop.put(feedback)

    async def _handle_error(self, event):
        """
        Handle incoming error message.

        :param event: Incoming ably error
        :type event: splitio.push.sse.parser.AblyError
        """
        _LOGGER.debug('handling ably error event: %s', str(event))
        feedback = await self._status_tracker.handle_ably_error(event)
        if feedback is not None:
            await self._feedback_loop.put(feedback)

    async def _handle_connection_ready(self):
        """Handle a successful connection to SSE."""
        await self._feedback_loop.put(Status.PUSH_SUBSYSTEM_UP)
        _LOGGER.info('sse initial event received. enabling')

    async def _handle_connection_end(self):
        """
        Handle a connection ending.

        If the connection shutdown was not requested, trigger a restart.
        """
        feedback = await self._status_tracker.handle_disconnect()
        if feedback is not None:
            await self._feedback_loop.put(feedback)

    async def _stop_current_conn(self):
        """Abort current streaming connection and stop it's associated workers."""
        _LOGGER.debug("Aborting SplitSSE tasks.")
        await self._processor.update_workers_status(False)
        self._status_tracker.notify_sse_shutdown_expected()
        await self._sse_client.stop()
        self._running_task.cancel()
        await self._running_task
        self._running_task = None
        _LOGGER.debug("SplitSSE tasks are stopped")
