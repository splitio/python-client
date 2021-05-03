"""NotificationManagerKeeper implementation."""
from enum import Enum
import logging
from splitio.push.parser import ControlType


_LOGGER = logging.getLogger(__name__)


class Status(Enum):
    """Push subsystem statuses."""

    PUSH_SUBSYSTEM_UP = 0
    PUSH_SUBSYSTEM_DOWN = 1
    PUSH_RETRYABLE_ERROR = 2
    PUSH_NONRETRYABLE_ERROR = 3


class LastEventTimestamps(object):  # pylint:disable=too-few-public-methods
    """Simple class to keep track of the last time multiple events occurred."""

    def __init__(self):
        """Class constructor."""
        self.control = -1
        self.occupancy = -1

    def reset(self):
        """Restore original values."""
        self.control = -1
        self.occupancy = -1


class PushStatusTracker(object):
    """Tracks status of notification manager/publishers."""

    def __init__(self):
        """Class constructor."""
        self._publishers = {}
        self._last_control_message = None
        self._last_status_propagated = None
        self._timestamps = LastEventTimestamps()
        self._shutdown_expected = None
        self.reset()  # Set proper initial values

    def reset(self):
        """
        Reset the status to initial conditions.

        This asssumes a healthy connection until proven wrong.
        """
        self._publishers.update({'control_pri': 2, 'control_sec': 2})
        self._last_control_message = ControlType.STREAMING_ENABLED
        self._last_status_propagated = Status.PUSH_SUBSYSTEM_UP
        self._timestamps.reset()
        self._shutdown_expected = False

    def handle_occupancy(self, event):
        """
        Handle an incoming occupancy event.

        :param event: incoming occupancy event.
        :type event: splitio.push.sse.parser.Occupancy

        :returns: A new status if required. None otherwise
        :rtype: Optional[Status]
        """
        if self._shutdown_expected:  # we don't care about occupancy if a disconnection is expected
            return None

        if event.channel not in self._publishers:
            _LOGGER.info("received occupancy message from an unknown channel `%s`. Ignoring",
                         event.channel)
            return None

        if self._timestamps.occupancy > event.timestamp:
            _LOGGER.info('receved an old occupancy message. ignoring.')
            return None
        self._timestamps.occupancy = event.timestamp

        self._publishers[event.channel] = event.publishers
        return self._update_status()

    def handle_control_message(self, event):
        """
        Handle an incoming Control event.

        :param event: Incoming control event
        :type event: splitio.push.parser.ControlMessage
        """
        # we don't care about control messages if a disconnection is expected
        if self._shutdown_expected:
            return None

        if self._timestamps.control > event.timestamp:
            _LOGGER.info('receved an old control message. ignoring.')
            return None
        self._timestamps.control = event.timestamp

        self._last_control_message = event.control_type
        return self._update_status()

    def handle_ably_error(self, event):
        """
        Handle an ably-specific error.

        :param event: parsed ably error
        :type event: splitio.push.parser.AblyError

        :returns: A new status if required. None otherwise
        :rtype: Optional[Status]
        """
        if self._shutdown_expected:  # we don't care about an incoming error if a shutdown is expected
            return None

        _LOGGER.debug('handling ably error event: %s', str(event))
        if event.should_be_ignored():
            _LOGGER.debug('ignoring sse error message: %s', event)
            return None

        # Indicate that the connection will eventually end. 2 possibilities:
        # 1. The server closes the connection after sending the error
        # 2. RETRYABLE_ERROR is propagated and the connection is closed on the clint side.
        # By doing this we guarantee that only one error will be propagated
        self.notify_sse_shutdown_expected()

        if event.is_retryable():
            _LOGGER.info('received retryable error message. '
                         'Restarting the whole flow with backoff.')
            return self._propagate_status(Status.PUSH_RETRYABLE_ERROR)

        _LOGGER.info('received non-retryable sse error message. Disabling streaming.')
        return self._propagate_status(Status.PUSH_NONRETRYABLE_ERROR)

    def notify_sse_shutdown_expected(self):
        """Let the status tracker know that an sse shutdown has been requested."""
        self._shutdown_expected = True

    def _update_status(self):
        """
        Evaluate the current/previous status and emit a new status message if appropriate.

        :returns: A new status if required. None otherwise
        :rtype: Optional[Status]
        """
        if self._last_status_propagated == Status.PUSH_SUBSYSTEM_UP:
            if not self._occupancy_ok() \
                    or self._last_control_message == ControlType.STREAMING_PAUSED:
                return self._propagate_status(Status.PUSH_SUBSYSTEM_DOWN)

            if self._last_control_message == ControlType.STREAMING_DISABLED:
                return self._propagate_status(Status.PUSH_NONRETRYABLE_ERROR)

        if self._last_status_propagated == Status.PUSH_SUBSYSTEM_DOWN:
            if self._occupancy_ok() and self._last_control_message == ControlType.STREAMING_ENABLED:
                return self._propagate_status(Status.PUSH_SUBSYSTEM_UP)

            if self._last_control_message == ControlType.STREAMING_DISABLED:
                return self._propagate_status(Status.PUSH_NONRETRYABLE_ERROR)

        return None

    def handle_disconnect(self):
        """
        Handle non-requested SSE disconnection.

        It should properly handle:
        - connection reset/timeout
        - disconnection after an ably error

        :returns: A new status if required. None otherwise
        :rtype: Optional[Status]
        """
        if not self._shutdown_expected:
            return self._propagate_status(Status.PUSH_RETRYABLE_ERROR)
        return None

    def _propagate_status(self, status):
        """
        Store and propagates a new status.

        :param status: Status to propagate.
        :type status: Status

        :returns: Status to propagate
        :rtype: status
        """
        self._last_status_propagated = status
        return status

    def _occupancy_ok(self):
        """
        Return whether we have enough publishers.

        :returns: True if publisher count is enough. False otherwise
        :rtype: bool
        """
        return any(count > 0 for (chan, count) in self._publishers.items())
