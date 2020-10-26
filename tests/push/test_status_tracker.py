"""SSE Status tracker unit tests."""
#pylint:disable=protected-access,no-self-use,line-too-long
from splitio.push.status_tracker import PushStatusTracker, Status
from splitio.push.parser import ControlType, AblyError, OccupancyMessage, ControlMessage


class StatusTrackerTests(object):
    """Parser tests."""

    def test_initial_status_and_reset(self):
        """Test the initial status is ok and reset() works as expected."""
        tracker = PushStatusTracker()
        assert tracker._occupancy_ok()
        assert tracker._last_control_message == ControlType.STREAMING_ENABLED
        assert tracker._last_status_propagated == Status.PUSH_SUBSYSTEM_UP
        assert not tracker._shutdown_expected

        tracker._last_control_message = ControlType.STREAMING_PAUSED
        tracker._publishers['control_pri'] = 0
        tracker._publishers['control_sec'] = 1
        tracker._last_status_propagated = Status.PUSH_NONRETRYABLE_ERROR
        tracker.reset()
        assert tracker._occupancy_ok()
        assert tracker._last_control_message == ControlType.STREAMING_ENABLED
        assert tracker._last_status_propagated == Status.PUSH_SUBSYSTEM_UP
        assert not tracker._shutdown_expected

    def test_handling_occupancy(self):
        """Test handling occupancy works properly."""
        tracker = PushStatusTracker()
        assert tracker._occupancy_ok()

        message = OccupancyMessage('[?occupancy=metrics.publishers]control_sec', 123, 0)
        assert tracker.handle_occupancy(message) is None

        # old message
        message = OccupancyMessage('[?occupancy=metrics.publishers]control_pri', 122, 0)
        assert tracker.handle_occupancy(message) is None

        message = OccupancyMessage('[?occupancy=metrics.publishers]control_pri', 124, 0)
        assert tracker.handle_occupancy(message) is Status.PUSH_SUBSYSTEM_DOWN

        message = OccupancyMessage('[?occupancy=metrics.publishers]control_pri', 125, 1)
        assert tracker.handle_occupancy(message) is Status.PUSH_SUBSYSTEM_UP

        message = OccupancyMessage('[?occupancy=metrics.publishers]control_sec', 125, 2)
        assert tracker.handle_occupancy(message) is None

    def test_handling_control(self):
        """Test handling incoming control messages."""
        tracker = PushStatusTracker()
        assert tracker._last_control_message == ControlType.STREAMING_ENABLED
        assert tracker._last_status_propagated == Status.PUSH_SUBSYSTEM_UP

        message = ControlMessage('control_pri', 123, ControlType.STREAMING_ENABLED)
        assert tracker.handle_control_message(message) is None

        # old message
        message = ControlMessage('control_pri', 122, ControlType.STREAMING_PAUSED)
        assert tracker.handle_control_message(message) is None

        message = ControlMessage('control_pri', 124, ControlType.STREAMING_PAUSED)
        assert tracker.handle_control_message(message) is Status.PUSH_SUBSYSTEM_DOWN

        message = ControlMessage('control_pri', 125, ControlType.STREAMING_ENABLED)
        assert tracker.handle_control_message(message) is Status.PUSH_SUBSYSTEM_UP

        message = ControlMessage('control_pri', 126, ControlType.STREAMING_DISABLED)
        assert tracker.handle_control_message(message) is Status.PUSH_NONRETRYABLE_ERROR

        # test that disabling works as well with streaming paused
        tracker = PushStatusTracker()
        assert tracker._last_control_message == ControlType.STREAMING_ENABLED
        assert tracker._last_status_propagated == Status.PUSH_SUBSYSTEM_UP

        message = ControlMessage('control_pri', 124, ControlType.STREAMING_PAUSED)
        assert tracker.handle_control_message(message) is Status.PUSH_SUBSYSTEM_DOWN

        message = ControlMessage('control_pri', 126, ControlType.STREAMING_DISABLED)
        assert tracker.handle_control_message(message) is Status.PUSH_NONRETRYABLE_ERROR

    def test_control_occupancy_overlap(self):
        """Test control and occupancy messages together."""
        tracker = PushStatusTracker()
        assert tracker._last_control_message == ControlType.STREAMING_ENABLED
        assert tracker._last_status_propagated == Status.PUSH_SUBSYSTEM_UP

        message = ControlMessage('control_pri', 122, ControlType.STREAMING_PAUSED)
        assert tracker.handle_control_message(message) is Status.PUSH_SUBSYSTEM_DOWN

        message = OccupancyMessage('[?occupancy=metrics.publishers]control_sec', 123, 0)
        assert tracker.handle_occupancy(message) is None

        message = OccupancyMessage('[?occupancy=metrics.publishers]control_pri', 124, 0)
        assert tracker.handle_occupancy(message) is None

        message = ControlMessage('control_pri', 125, ControlType.STREAMING_ENABLED)
        assert tracker.handle_control_message(message) is None

        message = OccupancyMessage('[?occupancy=metrics.publishers]control_pri', 126, 1)
        assert tracker.handle_occupancy(message) is Status.PUSH_SUBSYSTEM_UP

    def test_ably_error(self):
        """Test the status tracker reacts appropriately to an ably error."""
        tracker = PushStatusTracker()
        assert tracker._last_control_message == ControlType.STREAMING_ENABLED
        assert tracker._last_status_propagated == Status.PUSH_SUBSYSTEM_UP

        message = AblyError(39999, 100, 'some message', 'http://somewhere')
        assert tracker.handle_ably_error(message) is None

        message = AblyError(50000, 100, 'some message', 'http://somewhere')
        assert tracker.handle_ably_error(message) is None

        tracker.reset()
        message = AblyError(40140, 100, 'some message', 'http://somewhere')
        assert tracker.handle_ably_error(message) is Status.PUSH_RETRYABLE_ERROR

        tracker.reset()
        message = AblyError(40149, 100, 'some message', 'http://somewhere')
        assert tracker.handle_ably_error(message) is Status.PUSH_RETRYABLE_ERROR

        tracker.reset()
        message = AblyError(40150, 100, 'some message', 'http://somewhere')
        assert tracker.handle_ably_error(message) is Status.PUSH_NONRETRYABLE_ERROR

        tracker.reset()
        message = AblyError(40139, 100, 'some message', 'http://somewhere')
        assert tracker.handle_ably_error(message) is Status.PUSH_NONRETRYABLE_ERROR

    def test_disconnect_expected(self):
        """Test that no error is propagated when a disconnect is expected."""
        tracker = PushStatusTracker()
        assert tracker._last_control_message == ControlType.STREAMING_ENABLED
        assert tracker._last_status_propagated == Status.PUSH_SUBSYSTEM_UP
        tracker.notify_sse_shutdown_expected()

        assert tracker.handle_ably_error(AblyError(40139, 100, 'some message', 'http://somewhere')) is None
        assert tracker.handle_ably_error(AblyError(40149, 100, 'some message', 'http://somewhere')) is None
        assert tracker.handle_ably_error(AblyError(39999, 100, 'some message', 'http://somewhere')) is None

        assert tracker.handle_control_message(ControlMessage('control_pri', 123, ControlType.STREAMING_ENABLED)) is None
        assert tracker.handle_control_message(ControlMessage('control_pri', 124, ControlType.STREAMING_PAUSED)) is None
        assert tracker.handle_control_message(ControlMessage('control_pri', 125, ControlType.STREAMING_DISABLED)) is None

        assert tracker.handle_occupancy(OccupancyMessage('[?occupancy=metrics.publishers]control_sec', 123, 0)) is None
        assert tracker.handle_occupancy(OccupancyMessage('[?occupancy=metrics.publishers]control_sec', 124, 1)) is None
