"""SSE Status tracker unit tests."""
#pylint:disable=protected-access,no-self-use,line-too-long
import pytest

from splitio.push.status_tracker import PushStatusTracker, Status, PushStatusTrackerAsync
from splitio.push.parser import ControlType, AblyError, OccupancyMessage, ControlMessage
from splitio.engine.telemetry import TelemetryStorageProducer, TelemetryStorageProducerAsync
from splitio.storage.inmemmory import InMemoryTelemetryStorage, InMemoryTelemetryStorageAsync
from splitio.models.telemetry import StreamingEventTypes, SSEStreamingStatus, SSEConnectionError


class StatusTrackerTests(object):
    """Parser tests."""

    def test_initial_status_and_reset(self, mocker):
        """Test the initial status is ok and reset() works as expected."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        tracker = PushStatusTracker(telemetry_runtime_producer)
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

    def test_handling_occupancy(self, mocker):
        """Test handling occupancy works properly."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        tracker = PushStatusTracker(telemetry_runtime_producer)
        assert tracker._occupancy_ok()

        message = OccupancyMessage('[?occupancy=metrics.publishers]control_sec', 123, 0)
        assert tracker.handle_occupancy(message) is None
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._type == StreamingEventTypes.OCCUPANCY_SEC.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._data == len(tracker._publishers))

        # old message
        message = OccupancyMessage('[?occupancy=metrics.publishers]control_pri', 122, 0)
        assert tracker.handle_occupancy(message) is None

        message = OccupancyMessage('[?occupancy=metrics.publishers]control_pri', 124, 0)
        assert tracker.handle_occupancy(message) is Status.PUSH_SUBSYSTEM_DOWN
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._type == StreamingEventTypes.STREAMING_STATUS.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._data == SSEStreamingStatus.PAUSED.value)

        message = OccupancyMessage('[?occupancy=metrics.publishers]control_pri', 125, 1)
        assert tracker.handle_occupancy(message) is Status.PUSH_SUBSYSTEM_UP
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._type == StreamingEventTypes.STREAMING_STATUS.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._data == SSEStreamingStatus.ENABLED.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-2]._type == StreamingEventTypes.OCCUPANCY_PRI.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-2]._data == len(tracker._publishers))

        message = OccupancyMessage('[?occupancy=metrics.publishers]control_sec', 125, 2)
        assert tracker.handle_occupancy(message) is None

    def test_handling_control(self, mocker):
        """Test handling incoming control messages."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        tracker = PushStatusTracker(telemetry_runtime_producer)
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
        tracker = PushStatusTracker(mocker.Mock())
        assert tracker._last_control_message == ControlType.STREAMING_ENABLED
        assert tracker._last_status_propagated == Status.PUSH_SUBSYSTEM_UP

        message = ControlMessage('control_pri', 124, ControlType.STREAMING_PAUSED)
        assert tracker.handle_control_message(message) is Status.PUSH_SUBSYSTEM_DOWN

        message = ControlMessage('control_pri', 126, ControlType.STREAMING_DISABLED)
        assert tracker.handle_control_message(message) is Status.PUSH_NONRETRYABLE_ERROR
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._type == StreamingEventTypes.STREAMING_STATUS.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._data == SSEStreamingStatus.DISABLED.value)


    def test_control_occupancy_overlap(self, mocker):
        """Test control and occupancy messages together."""
        tracker = PushStatusTracker(mocker.Mock())
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

    def test_ably_error(self, mocker):
        """Test the status tracker reacts appropriately to an ably error."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        tracker = PushStatusTracker(telemetry_runtime_producer)
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
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._type == StreamingEventTypes.ABLY_ERROR.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._data == 40139)


    def test_disconnect_expected(self, mocker):
        """Test that no error is propagated when a disconnect is expected."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        tracker = PushStatusTracker(telemetry_runtime_producer)
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

    def test_telemetry_non_requested_disconnect(self, mocker):
        """Test the initial status is ok and reset() works as expected."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        tracker = PushStatusTracker(telemetry_runtime_producer)
        tracker._shutdown_expected = False
        tracker.handle_disconnect()
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._type == StreamingEventTypes.SSE_CONNECTION_ERROR.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._data == SSEConnectionError.NON_REQUESTED.value)

        tracker._shutdown_expected = True
        tracker.handle_disconnect()
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._type == StreamingEventTypes.SSE_CONNECTION_ERROR.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._data == SSEConnectionError.REQUESTED.value)


class StatusTrackerAsyncTests(object):
    """Parser tests."""

    @pytest.mark.asyncio
    async def test_initial_status_and_reset(self, mocker):
        """Test the initial status is ok and reset() works as expected."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        tracker = PushStatusTrackerAsync(telemetry_runtime_producer)
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

    @pytest.mark.asyncio
    async def test_handling_occupancy(self, mocker):
        """Test handling occupancy works properly."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        tracker = PushStatusTrackerAsync(telemetry_runtime_producer)
        assert tracker._occupancy_ok()

        message = OccupancyMessage('[?occupancy=metrics.publishers]control_sec', 123, 0)
        assert await tracker.handle_occupancy(message) is None
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._type == StreamingEventTypes.OCCUPANCY_SEC.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._data == len(tracker._publishers))

        # old message
        message = OccupancyMessage('[?occupancy=metrics.publishers]control_pri', 122, 0)
        assert await tracker.handle_occupancy(message) is None

        message = OccupancyMessage('[?occupancy=metrics.publishers]control_pri', 124, 0)
        assert await tracker.handle_occupancy(message) is Status.PUSH_SUBSYSTEM_DOWN
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._type == StreamingEventTypes.STREAMING_STATUS.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._data == SSEStreamingStatus.PAUSED.value)

        message = OccupancyMessage('[?occupancy=metrics.publishers]control_pri', 125, 1)
        assert await tracker.handle_occupancy(message) is Status.PUSH_SUBSYSTEM_UP
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._type == StreamingEventTypes.STREAMING_STATUS.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._data == SSEStreamingStatus.ENABLED.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-2]._type == StreamingEventTypes.OCCUPANCY_PRI.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-2]._data == len(tracker._publishers))

        message = OccupancyMessage('[?occupancy=metrics.publishers]control_sec', 125, 2)
        assert await tracker.handle_occupancy(message) is None

    @pytest.mark.asyncio
    async def test_handling_control(self, mocker):
        """Test handling incoming control messages."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        tracker = PushStatusTrackerAsync(telemetry_runtime_producer)
        assert tracker._last_control_message == ControlType.STREAMING_ENABLED
        assert tracker._last_status_propagated == Status.PUSH_SUBSYSTEM_UP

        message = ControlMessage('control_pri', 123, ControlType.STREAMING_ENABLED)
        assert await tracker.handle_control_message(message) is None

        # old message
        message = ControlMessage('control_pri', 122, ControlType.STREAMING_PAUSED)
        assert await tracker.handle_control_message(message) is None

        message = ControlMessage('control_pri', 124, ControlType.STREAMING_PAUSED)
        assert await tracker.handle_control_message(message) is Status.PUSH_SUBSYSTEM_DOWN

        message = ControlMessage('control_pri', 125, ControlType.STREAMING_ENABLED)
        assert await tracker.handle_control_message(message) is Status.PUSH_SUBSYSTEM_UP

        message = ControlMessage('control_pri', 126, ControlType.STREAMING_DISABLED)
        assert await tracker.handle_control_message(message) is Status.PUSH_NONRETRYABLE_ERROR

        # test that disabling works as well with streaming paused
        tracker = PushStatusTrackerAsync(telemetry_runtime_producer)
        assert tracker._last_control_message == ControlType.STREAMING_ENABLED
        assert tracker._last_status_propagated == Status.PUSH_SUBSYSTEM_UP

        message = ControlMessage('control_pri', 124, ControlType.STREAMING_PAUSED)
        assert await tracker.handle_control_message(message) is Status.PUSH_SUBSYSTEM_DOWN

        message = ControlMessage('control_pri', 126, ControlType.STREAMING_DISABLED)
        assert await tracker.handle_control_message(message) is Status.PUSH_NONRETRYABLE_ERROR
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._type == StreamingEventTypes.STREAMING_STATUS.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._data == SSEStreamingStatus.DISABLED.value)


    @pytest.mark.asyncio
    async def test_control_occupancy_overlap(self, mocker):
        """Test control and occupancy messages together."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        tracker = PushStatusTrackerAsync(telemetry_runtime_producer)
        assert tracker._last_control_message == ControlType.STREAMING_ENABLED
        assert tracker._last_status_propagated == Status.PUSH_SUBSYSTEM_UP

        message = ControlMessage('control_pri', 122, ControlType.STREAMING_PAUSED)
        assert await tracker.handle_control_message(message) is Status.PUSH_SUBSYSTEM_DOWN

        message = OccupancyMessage('[?occupancy=metrics.publishers]control_sec', 123, 0)
        assert await tracker.handle_occupancy(message) is None

        message = OccupancyMessage('[?occupancy=metrics.publishers]control_pri', 124, 0)
        assert await tracker.handle_occupancy(message) is None

        message = ControlMessage('control_pri', 125, ControlType.STREAMING_ENABLED)
        assert await tracker.handle_control_message(message) is None

        message = OccupancyMessage('[?occupancy=metrics.publishers]control_pri', 126, 1)
        assert await tracker.handle_occupancy(message) is Status.PUSH_SUBSYSTEM_UP

    @pytest.mark.asyncio
    async def test_ably_error(self, mocker):
        """Test the status tracker reacts appropriately to an ably error."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        tracker = PushStatusTrackerAsync(telemetry_runtime_producer)
        assert tracker._last_control_message == ControlType.STREAMING_ENABLED
        assert tracker._last_status_propagated == Status.PUSH_SUBSYSTEM_UP

        message = AblyError(39999, 100, 'some message', 'http://somewhere')
        assert await tracker.handle_ably_error(message) is None

        message = AblyError(50000, 100, 'some message', 'http://somewhere')
        assert await tracker.handle_ably_error(message) is None

        tracker.reset()
        message = AblyError(40140, 100, 'some message', 'http://somewhere')
        assert await tracker.handle_ably_error(message) is Status.PUSH_RETRYABLE_ERROR

        tracker.reset()
        message = AblyError(40149, 100, 'some message', 'http://somewhere')
        assert await tracker.handle_ably_error(message) is Status.PUSH_RETRYABLE_ERROR

        tracker.reset()
        message = AblyError(40150, 100, 'some message', 'http://somewhere')
        assert await tracker.handle_ably_error(message) is Status.PUSH_NONRETRYABLE_ERROR

        tracker.reset()
        message = AblyError(40139, 100, 'some message', 'http://somewhere')
        assert await tracker.handle_ably_error(message) is Status.PUSH_NONRETRYABLE_ERROR
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._type == StreamingEventTypes.ABLY_ERROR.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._data == 40139)


    @pytest.mark.asyncio
    async def test_disconnect_expected(self, mocker):
        """Test that no error is propagated when a disconnect is expected."""
        telemetry_storage = InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        tracker = PushStatusTrackerAsync(telemetry_runtime_producer)
        assert tracker._last_control_message == ControlType.STREAMING_ENABLED
        assert tracker._last_status_propagated == Status.PUSH_SUBSYSTEM_UP
        tracker.notify_sse_shutdown_expected()

        assert await tracker.handle_ably_error(AblyError(40139, 100, 'some message', 'http://somewhere')) is None
        assert await tracker.handle_ably_error(AblyError(40149, 100, 'some message', 'http://somewhere')) is None
        assert await tracker.handle_ably_error(AblyError(39999, 100, 'some message', 'http://somewhere')) is None

        assert await tracker.handle_control_message(ControlMessage('control_pri', 123, ControlType.STREAMING_ENABLED)) is None
        assert await tracker.handle_control_message(ControlMessage('control_pri', 124, ControlType.STREAMING_PAUSED)) is None
        assert await tracker.handle_control_message(ControlMessage('control_pri', 125, ControlType.STREAMING_DISABLED)) is None

        assert await tracker.handle_occupancy(OccupancyMessage('[?occupancy=metrics.publishers]control_sec', 123, 0)) is None
        assert await tracker.handle_occupancy(OccupancyMessage('[?occupancy=metrics.publishers]control_sec', 124, 1)) is None

    @pytest.mark.asyncio
    async def test_telemetry_non_requested_disconnect(self, mocker):
        """Test the initial status is ok and reset() works as expected."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        tracker = PushStatusTrackerAsync(telemetry_runtime_producer)
        tracker._shutdown_expected = False
        await tracker.handle_disconnect()
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._type == StreamingEventTypes.SSE_CONNECTION_ERROR.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._data == SSEConnectionError.NON_REQUESTED.value)

        tracker._shutdown_expected = True
        await tracker.handle_disconnect()
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._type == StreamingEventTypes.SSE_CONNECTION_ERROR.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._data == SSEConnectionError.REQUESTED.value)
