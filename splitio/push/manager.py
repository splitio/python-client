"""Push subsystem manager class and helpers."""

import logging
from enum import Enum
from threading import Timer

from splitio.api import APIException
from splitio.push.splitsse import SplitSSEClient
from splitio.push.parser import parse_incoming_event, EventParsingException, EventType
from splitio.push.processor import MessageProcessor
from splitio.push.status_tracker import PushStatusTracker


_TOKEN_REFRESH_GRACE_PERIOD = 10 * 60  # 10 minutes


_LOGGER = logging.getLogger(__name__)


class _PushInitializationResult(Enum):
    """Streming connection initialization result."""

    SUCCESS = 0
    RETRYABLE_ERROR = 1
    NONRETRYABLE_ERROR = 2


class PushManager(object):
    """Push notifications susbsytem manager."""

    def __init__(self, auth_api, sse_url=None):
        """
        Class constructor.

        :param auth_api: sdk-auth-service api client
        :type auth_api: splitio.api.auth.AuthAPI
        """
        self._auth_api = auth_api
        self._processor = MessageProcessor(object())
        self._status_tracker = PushStatusTracker()
        self._handlers = {
            EventType.MESSAGE: self._handle_update,
            EventType.OCCUPANCY: self._handle_occupancy,
            EventType.ERROR: self._handle_error
        }

        self._sse_client = SplitSSEClient(self._event_handler) if sse_url is None \
            else SplitSSEClient(self._event_handler, sse_url)
        self._running = False
        self._next_refresh = Timer(0, lambda: 0)

    def _handle_update(self, event):
        """
        Handle incoming data update message.

        :param event: Incoming Update message
        :type event: splitio.push.sse.parser.Update
        """
        _LOGGER.debug('handling update event: %s', str(event))
        self._processor.handle(event)

    def _handle_occupancy(self, event):
        """
        Handle incoming notification message.

        :param event: Incoming occupancy message.
        :type event: splitio.push.sse.parser.Occupancy
        """
        _LOGGER.debug('handling occupancy event: %s', str(event))
        feedback = self._status_tracker.handle_occupancy(event)
        if feedback is not None:
            # Send this event back to sync manager
            pass

    def _handle_error(self, event):
        """
        Handle incoming error message.

        :param event: Incoming ably error
        :type event: splitio.push.sse.parser.AblyError
        """
        _LOGGER.debug('handling ably error event: %s', str(event))
        feedback = self._status_tracker.handle_ably_error(event)
        if feedback is not None:
            # Send this event back to sync manager
            pass

    def _event_handler(self, event):
        """
        Process an incoming event.

        :param event: Incoming event
        :type event: splitio.push.sse.SSEEvent
        """
        try:
            parsed = parse_incoming_event(event)
        except EventParsingException:
            _LOGGER.error('error parsing event of type %s', event.event)
            _LOGGER.debug(str(event), exc_info=True)
            return

        try:
            handle = self._handlers[parsed.event_type]
        except KeyError:
            _LOGGER.error('no handler for message of type %s', parsed.event_type)
            _LOGGER.debug(str(event), exc_info=True)
            return

        try:
            handle(parsed)
        except Exception:  #pylint:disable=broad-except
            _LOGGER.error('something went wrong when processing message of type %s',
                          parsed.event_type)
            _LOGGER.debug(str(parsed), exc_info=True)

    def _token_refresh(self):
        """Refresh auth token."""
        self.stop(True)
        self._trigger_connection_flow()

    def _trigger_connection_flow(self):
        """
        Authenticate and start a connection.

        :returns: Result of initialization procedure
        :rtype: _PushInitializationResult
        """
        try:
            token = self._auth_api.authenticate()
        except APIException:
            _LOGGER.error('error performing sse auth request.')
            _LOGGER.debug('stack trace: ', exc_info=True)
            return _PushInitializationResult.RETRYABLE_ERROR

        if not token.push_enabled:
            return _PushInitializationResult.NONRETRYABLE_ERROR

        self._status_tracker.reset()
        if  self._sse_client.start(token):
            # TODO: Reset backoff
            self._setup_next_token_refresh(token)
            return _PushInitializationResult.SUCCESS

        return _PushInitializationResult.RETRYABLE_ERROR

    def _setup_next_token_refresh(self, token):
        """
        Schedule next token refresh.

        :param token: Last fetched token.
        :type token: splitio.models.token.Token
        """
        if self._next_refresh is not None:
            self._next_refresh.cancel()
        self._next_refresh = Timer((token.exp - token.iat)/1000 - _TOKEN_REFRESH_GRACE_PERIOD,
                                   self._token_refresh)

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

        self._status_tracker.notify_sse_shutdown_expected()
        self._next_refresh.cancel()
        self._sse_client.stop(blocking)
