"""Push notification manager tests."""
#pylint:disable=no-self-use,protected-access
from threading import Thread
from queue import Queue
import pytest

from splitio.api import APIException
from splitio.models.token import Token
from splitio.push.sse import SSEEvent
from splitio.push.parser import parse_incoming_event, EventType, ControlType, ControlMessage, \
    OccupancyMessage, SplitChangeUpdate, SplitKillUpdate, SegmentChangeUpdate
from splitio.push.processor import MessageProcessor, MessageProcessorAsync
from splitio.push.status_tracker import PushStatusTracker
from splitio.push.manager import PushManager, PushManagerAsync, _TOKEN_REFRESH_GRACE_PERIOD
from splitio.push.splitsse import SplitSSEClient, SplitSSEClientAsync
from splitio.push.status_tracker import Status
from splitio.engine.telemetry import TelemetryStorageProducer, TelemetryStorageProducerAsync
from splitio.storage.inmemmory import InMemoryTelemetryStorage, InMemoryTelemetryStorageAsync
from splitio.models.telemetry import StreamingEventTypes
from splitio.optional.loaders import asyncio

from tests.helpers import Any


class PushManagerTests(object):
    """Parser tests."""

    def test_connection_success(self, mocker):
        """Test the initial status is ok and reset() works as expected."""
        api_mock = mocker.Mock()
        api_mock.authenticate.return_value = Token(True, 'abc', {}, 2000000, 1000000)

        sse_mock = mocker.Mock(spec=SplitSSEClient)
        sse_constructor_mock = mocker.Mock()
        sse_constructor_mock.return_value = sse_mock
        timer_mock = mocker.Mock()
        mocker.patch('splitio.push.manager.Timer', new=timer_mock)
        mocker.patch('splitio.push.manager.SplitSSEClient', new=sse_constructor_mock)
        feedback_loop = Queue()
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        manager = PushManager(api_mock, mocker.Mock(), feedback_loop, mocker.Mock(), telemetry_runtime_producer)

        def new_start(*args, **kwargs):  # pylint: disable=unused-argument
            """splitsse.start mock."""
            thread = Thread(target=manager._handle_connection_ready, daemon=True)
            thread.start()
            return True

        sse_mock.start.side_effect = new_start

        manager.start()
        assert feedback_loop.get() == Status.PUSH_SUBSYSTEM_UP
        assert timer_mock.mock_calls == [
            mocker.call(0, Any()),
            mocker.call().cancel(),
            mocker.call(1000000 - _TOKEN_REFRESH_GRACE_PERIOD, manager._token_refresh),
            mocker.call().setName('TokenRefresh'),
            mocker.call().start()
        ]
        assert(telemetry_storage._streaming_events._streaming_events[0]._type == StreamingEventTypes.TOKEN_REFRESH.value)
        assert(telemetry_storage._streaming_events._streaming_events[1]._type == StreamingEventTypes.CONNECTION_ESTABLISHED.value)

    def test_connection_failure(self, mocker):
        """Test the connection fails to be established."""
        api_mock = mocker.Mock()
        api_mock.authenticate.return_value = Token(True, 'abc', {}, 2000000, 1000000)

        sse_mock = mocker.Mock(spec=SplitSSEClient)
        sse_constructor_mock = mocker.Mock()
        sse_constructor_mock.return_value = sse_mock
        timer_mock = mocker.Mock()
        mocker.patch('splitio.push.manager.Timer', new=timer_mock)
        mocker.patch('splitio.push.manager.SplitSSEClient', new=sse_constructor_mock)
        feedback_loop = Queue()
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        manager = PushManager(api_mock, mocker.Mock(), feedback_loop, mocker.Mock(), telemetry_runtime_producer)

        def new_start(*args, **kwargs):  # pylint: disable=unused-argument
            """splitsse.start mock."""
            thread = Thread(target=manager._handle_connection_end, daemon=True)
            thread.start()
            return False

        sse_mock.start.side_effect = new_start

        manager.start()
        assert feedback_loop.get() == Status.PUSH_RETRYABLE_ERROR
        assert timer_mock.mock_calls == [mocker.call(0, Any())]

    def test_push_disabled(self, mocker):
        """Test the initial status is ok and reset() works as expected."""
        api_mock = mocker.Mock()
        api_mock.authenticate.return_value = Token(False, 'abc', {}, 1, 2)

        sse_mock = mocker.Mock(spec=SplitSSEClient)
        sse_constructor_mock = mocker.Mock()
        sse_constructor_mock.return_value = sse_mock
        timer_mock = mocker.Mock()
        mocker.patch('splitio.push.manager.Timer', new=timer_mock)
        mocker.patch('splitio.push.manager.SplitSSEClient', new=sse_constructor_mock)
        feedback_loop = Queue()
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        manager = PushManager(api_mock, mocker.Mock(), feedback_loop, mocker.Mock(), telemetry_runtime_producer)
        manager.start()
        assert feedback_loop.get() == Status.PUSH_NONRETRYABLE_ERROR
        assert timer_mock.mock_calls == [mocker.call(0, Any())]
        assert sse_mock.mock_calls == []


    def test_auth_apiexception(self, mocker):
        """Test the initial status is ok and reset() works as expected."""
        api_mock = mocker.Mock()
        api_mock.authenticate.side_effect = APIException('something')

        sse_mock = mocker.Mock(spec=SplitSSEClient)
        sse_constructor_mock = mocker.Mock()
        sse_constructor_mock.return_value = sse_mock
        timer_mock = mocker.Mock()
        mocker.patch('splitio.push.manager.Timer', new=timer_mock)
        mocker.patch('splitio.push.manager.SplitSSEClient', new=sse_constructor_mock)

        feedback_loop = Queue()
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        manager = PushManager(api_mock, mocker.Mock(), feedback_loop, mocker.Mock(), telemetry_runtime_producer)
        manager.start()
        assert feedback_loop.get() == Status.PUSH_RETRYABLE_ERROR
        assert timer_mock.mock_calls == [mocker.call(0, Any())]
        assert sse_mock.mock_calls == []

    def test_split_change(self, mocker):
        """Test update-type messages are properly forwarded to the processor."""
        sse_event = SSEEvent('1', EventType.MESSAGE, '', '{}')
        update_message = SplitChangeUpdate('chan', 123, 456)
        parse_event_mock = mocker.Mock(spec=parse_incoming_event)
        parse_event_mock.return_value = update_message
        mocker.patch('splitio.push.manager.parse_incoming_event', new=parse_event_mock)

        processor_mock = mocker.Mock(spec=MessageProcessor)
        mocker.patch('splitio.push.manager.MessageProcessor', new=processor_mock)

        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        manager = PushManager(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock(), telemetry_runtime_producer)
        manager._event_handler(sse_event)
        assert parse_event_mock.mock_calls == [mocker.call(sse_event)]
        assert processor_mock.mock_calls == [
            mocker.call(Any()),
            mocker.call().handle(update_message)
        ]

    def test_split_kill(self, mocker):
        """Test update-type messages are properly forwarded to the processor."""
        sse_event = SSEEvent('1', EventType.MESSAGE, '', '{}')
        update_message = SplitKillUpdate('chan', 123, 456, 'some_split', 'off')
        parse_event_mock = mocker.Mock(spec=parse_incoming_event)
        parse_event_mock.return_value = update_message
        mocker.patch('splitio.push.manager.parse_incoming_event', new=parse_event_mock)

        processor_mock = mocker.Mock(spec=MessageProcessor)
        mocker.patch('splitio.push.manager.MessageProcessor', new=processor_mock)

        manager = PushManager(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())
        manager._event_handler(sse_event)
        assert parse_event_mock.mock_calls == [mocker.call(sse_event)]
        assert processor_mock.mock_calls == [
            mocker.call(Any()),
            mocker.call().handle(update_message)
        ]

    def test_segment_change(self, mocker):
        """Test update-type messages are properly forwarded to the processor."""
        sse_event = SSEEvent('1', EventType.MESSAGE, '', '{}')
        update_message = SegmentChangeUpdate('chan', 123, 456, 'some_segment')
        parse_event_mock = mocker.Mock(spec=parse_incoming_event)
        parse_event_mock.return_value = update_message
        mocker.patch('splitio.push.manager.parse_incoming_event', new=parse_event_mock)

        processor_mock = mocker.Mock(spec=MessageProcessor)
        mocker.patch('splitio.push.manager.MessageProcessor', new=processor_mock)

        manager = PushManager(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())
        manager._event_handler(sse_event)
        assert parse_event_mock.mock_calls == [mocker.call(sse_event)]
        assert processor_mock.mock_calls == [
            mocker.call(Any()),
            mocker.call().handle(update_message)
        ]

    def test_control_message(self, mocker):
        """Test control mesage is forwarded to status tracker."""
        sse_event = SSEEvent('1', EventType.MESSAGE, '', '{}')
        control_message = ControlMessage('chan', 123, ControlType.STREAMING_ENABLED)
        parse_event_mock = mocker.Mock(spec=parse_incoming_event)
        parse_event_mock.return_value = control_message
        mocker.patch('splitio.push.manager.parse_incoming_event', new=parse_event_mock)

        status_tracker_mock = mocker.Mock(spec=PushStatusTracker)
        mocker.patch('splitio.push.manager.PushStatusTracker', new=status_tracker_mock)

        manager = PushManager(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())
        manager._event_handler(sse_event)
        assert parse_event_mock.mock_calls == [mocker.call(sse_event)]
        assert status_tracker_mock.mock_calls[1] == mocker.call().handle_control_message(control_message)

    def test_occupancy_message(self, mocker):
        """Test control mesage is forwarded to status tracker."""
        sse_event = SSEEvent('1', EventType.MESSAGE, '', '{}')
        occupancy_message = OccupancyMessage('[?occupancy=metrics.publishers]control_pri', 123, 2)
        parse_event_mock = mocker.Mock(spec=parse_incoming_event)
        parse_event_mock.return_value = occupancy_message
        mocker.patch('splitio.push.manager.parse_incoming_event', new=parse_event_mock)

        status_tracker_mock = mocker.Mock(spec=PushStatusTracker)
        mocker.patch('splitio.push.manager.PushStatusTracker', new=status_tracker_mock)

        manager = PushManager(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())
        manager._event_handler(sse_event)
        assert parse_event_mock.mock_calls == [mocker.call(sse_event)]
        assert status_tracker_mock.mock_calls[1] == mocker.call().handle_occupancy(occupancy_message)

class PushManagerAsyncTests(object):
    """Parser tests."""

    @pytest.mark.asyncio
    async def test_connection_success(self, mocker):
        """Test the initial status is ok and reset() works as expected."""
        api_mock = mocker.Mock()

        async def authenticate():
            return Token(True, 'abc', {}, 2000000, 1000000)
        api_mock.authenticate.side_effect = authenticate

        self.token = None
        def timer_mock(se, token):
            self.token = token
            return (token.exp - token.iat) - _TOKEN_REFRESH_GRACE_PERIOD
        mocker.patch('splitio.push.manager.PushManagerAsync._get_time_period', new=timer_mock)

        async def coro():
            yield SSEEvent('1', EventType.MESSAGE, '', '{}')
            yield SSEEvent('1', EventType.MESSAGE, '', '{}')

        sse_mock = mocker.Mock(spec=SplitSSEClientAsync)
        sse_mock.start.return_value = coro()

        feedback_loop = asyncio.Queue()
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        manager = PushManagerAsync(api_mock, mocker.Mock(), feedback_loop, mocker.Mock(), telemetry_runtime_producer)
        manager._sse_client = sse_mock

        async def deferred_shutdown():
            await asyncio.sleep(1)
            await manager.stop(True)

        manager.start()
        shutdown_task = asyncio.get_running_loop().create_task(deferred_shutdown())

        assert await feedback_loop.get() == Status.PUSH_SUBSYSTEM_UP
        assert self.token.push_enabled
        assert self.token.token == 'abc'
        assert self.token.channels == {}
        assert self.token.exp == 2000000
        assert self.token.iat == 1000000

        await shutdown_task
        assert not manager._running
        assert(telemetry_storage._streaming_events._streaming_events[0]._type == StreamingEventTypes.TOKEN_REFRESH.value)
        assert(telemetry_storage._streaming_events._streaming_events[1]._type == StreamingEventTypes.CONNECTION_ESTABLISHED.value)

    @pytest.mark.asyncio
    async def test_connection_failure(self, mocker):
        """Test the connection fails to be established."""
        api_mock = mocker.Mock()
        async def authenticate():
            return Token(True, 'abc', {}, 2000000, 1000000)
        api_mock.authenticate.side_effect = authenticate

        sse_mock = mocker.Mock(spec=SplitSSEClientAsync)
        feedback_loop = asyncio.Queue()
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        manager = PushManagerAsync(api_mock, mocker.Mock(), feedback_loop, mocker.Mock(), telemetry_runtime_producer)
        manager._sse_client = sse_mock

        async def coro():
            if False: yield '' # fit a never-called yield directive to force the func to be an async generator
            return
        sse_mock.start.return_value = coro()

        manager.start()
        assert await feedback_loop.get() == Status.PUSH_RETRYABLE_ERROR

        await manager.stop(True)
        assert not manager._running

    @pytest.mark.asyncio
    async def test_push_disabled(self, mocker):
        """Test the initial status is ok and reset() works as expected."""
        api_mock = mocker.Mock()
        async def authenticate():
            return Token(False, 'abc', {}, 1, 2)
        api_mock.authenticate.side_effect = authenticate

        sse_mock = mocker.Mock(spec=SplitSSEClientAsync)
        feedback_loop = asyncio.Queue()

        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        manager = PushManagerAsync(api_mock, mocker.Mock(), feedback_loop, mocker.Mock(), telemetry_runtime_producer)
        manager._sse_client = sse_mock

        manager.start()
        assert await feedback_loop.get() == Status.PUSH_NONRETRYABLE_ERROR
        assert sse_mock.mock_calls == []

        await manager.stop(True)
        assert not manager._running

    @pytest.mark.asyncio
    async def test_auth_apiexception(self, mocker):
        """Test the initial status is ok and reset() works as expected."""
        api_mock = mocker.Mock()
        api_mock.authenticate.side_effect = APIException('something')

        sse_mock = mocker.Mock(spec=SplitSSEClientAsync)

        feedback_loop = asyncio.Queue()
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        manager = PushManagerAsync(api_mock, mocker.Mock(), feedback_loop, mocker.Mock(), telemetry_runtime_producer)
        manager._sse_client = sse_mock
        manager.start()
        assert await feedback_loop.get() == Status.PUSH_RETRYABLE_ERROR
        assert sse_mock.mock_calls == []

        await manager.stop(True)
        assert not manager._running

    @pytest.mark.asyncio
    async def test_split_change(self, mocker):
        """Test update-type messages are properly forwarded to the processor."""
        sse_event = SSEEvent('1', EventType.MESSAGE, '', '{}')
        update_message = SplitChangeUpdate('chan', 123, 456)
        parse_event_mock = mocker.Mock(spec=parse_incoming_event)
        parse_event_mock.return_value = update_message
        mocker.patch('splitio.push.manager.parse_incoming_event', new=parse_event_mock)

        processor_mock = mocker.Mock(spec=MessageProcessorAsync)
        mocker.patch('splitio.push.manager.MessageProcessorAsync', new=processor_mock)

        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        manager = PushManagerAsync(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock(), telemetry_runtime_producer)
        await manager._event_handler(sse_event)
        assert parse_event_mock.mock_calls == [mocker.call(sse_event)]
        assert processor_mock.mock_calls == [
            mocker.call(Any()),
            mocker.call().handle(update_message)
        ]

    @pytest.mark.asyncio
    async def test_split_kill(self, mocker):
        """Test update-type messages are properly forwarded to the processor."""
        sse_event = SSEEvent('1', EventType.MESSAGE, '', '{}')
        update_message = SplitKillUpdate('chan', 123, 456, 'some_split', 'off')
        parse_event_mock = mocker.Mock(spec=parse_incoming_event)
        parse_event_mock.return_value = update_message
        mocker.patch('splitio.push.manager.parse_incoming_event', new=parse_event_mock)

        processor_mock = mocker.Mock(spec=MessageProcessorAsync)
        mocker.patch('splitio.push.manager.MessageProcessorAsync', new=processor_mock)

        manager = PushManagerAsync(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())
        await manager._event_handler(sse_event)
        assert parse_event_mock.mock_calls == [mocker.call(sse_event)]
        assert processor_mock.mock_calls == [
            mocker.call(Any()),
            mocker.call().handle(update_message)
        ]

        await manager.stop(True)
        assert not manager._running

    @pytest.mark.asyncio
    async def test_segment_change(self, mocker):
        """Test update-type messages are properly forwarded to the processor."""
        sse_event = SSEEvent('1', EventType.MESSAGE, '', '{}')
        update_message = SegmentChangeUpdate('chan', 123, 456, 'some_segment')
        parse_event_mock = mocker.Mock(spec=parse_incoming_event)
        parse_event_mock.return_value = update_message
        mocker.patch('splitio.push.manager.parse_incoming_event', new=parse_event_mock)

        processor_mock = mocker.Mock(spec=MessageProcessorAsync)
        mocker.patch('splitio.push.manager.MessageProcessorAsync', new=processor_mock)

        manager = PushManagerAsync(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())
        await manager._event_handler(sse_event)
        assert parse_event_mock.mock_calls == [mocker.call(sse_event)]
        assert processor_mock.mock_calls == [
            mocker.call(Any()),
            mocker.call().handle(update_message)
        ]

    @pytest.mark.asyncio
    async def test_control_message(self, mocker):
        """Test control mesage is forwarded to status tracker."""
        sse_event = SSEEvent('1', EventType.MESSAGE, '', '{}')
        control_message = ControlMessage('chan', 123, ControlType.STREAMING_ENABLED)
        parse_event_mock = mocker.Mock(spec=parse_incoming_event)
        parse_event_mock.return_value = control_message
        mocker.patch('splitio.push.manager.parse_incoming_event', new=parse_event_mock)

        status_tracker_mock = mocker.Mock(spec=PushStatusTracker)
        mocker.patch('splitio.push.manager.PushStatusTrackerAsync', new=status_tracker_mock)

        manager = PushManagerAsync(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())
        await manager._event_handler(sse_event)
        assert parse_event_mock.mock_calls == [mocker.call(sse_event)]
        assert status_tracker_mock.mock_calls[1] == mocker.call().handle_control_message(control_message)

    @pytest.mark.asyncio
    async def test_occupancy_message(self, mocker):
        """Test control mesage is forwarded to status tracker."""
        sse_event = SSEEvent('1', EventType.MESSAGE, '', '{}')
        occupancy_message = OccupancyMessage('[?occupancy=metrics.publishers]control_pri', 123, 2)
        parse_event_mock = mocker.Mock(spec=parse_incoming_event)
        parse_event_mock.return_value = occupancy_message
        mocker.patch('splitio.push.manager.parse_incoming_event', new=parse_event_mock)

        status_tracker_mock = mocker.Mock(spec=PushStatusTracker)
        mocker.patch('splitio.push.manager.PushStatusTrackerAsync', new=status_tracker_mock)

        manager = PushManagerAsync(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())
        await manager._event_handler(sse_event)
        assert parse_event_mock.mock_calls == [mocker.call(sse_event)]
        assert status_tracker_mock.mock_calls[1] == mocker.call().handle_occupancy(occupancy_message)
