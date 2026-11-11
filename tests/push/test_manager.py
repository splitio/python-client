"""Push notification manager tests."""
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

        def new_start(*args, **kwargs):
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
        assert (telemetry_storage._streaming_events._streaming_events[0]._type == StreamingEventTypes.TOKEN_REFRESH.value)
        assert (telemetry_storage._streaming_events._streaming_events[1]._type == StreamingEventTypes.CONNECTION_ESTABLISHED.value)

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

        def new_start(*args, **kwargs):
            """splitsse.start mock."""
            thread = Thread(target=manager._handle_connection_end, daemon=True)
            thread.start()
            return False

        sse_mock.start.side_effect = new_start

        manager.start()
        assert feedback_loop.get() == Status.PUSH_RETRYABLE_ERROR
        assert timer_mock.mock_calls == [mocker.call(0, Any())]

    def test_empty_auth_respnse(self, mocker):
        """Test the initial status is ok and reset() works as expected."""
        api_mock = mocker.Mock()
        api_mock.authenticate.return_value = Token(False, None, None, None, None)

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

    def _build_manager(self, mocker, api_mock, sse_mock, timer_mock, feedback_loop):
        sse_constructor_mock = mocker.Mock()
        sse_constructor_mock.return_value = sse_mock
        mocker.patch('splitio.push.manager.Timer', new=timer_mock)
        mocker.patch('splitio.push.manager.SplitSSEClient', new=sse_constructor_mock)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        return PushManager(api_mock, mocker.Mock(), feedback_loop, mocker.Mock(), telemetry_runtime_producer)

    def test_auth_unexpected_exception(self, mocker):
        """Test a non-API error during auth (e.g. non-JSON 200 response) is reported as retryable."""
        api_mock = mocker.Mock()
        api_mock.authenticate.side_effect = ValueError('Expecting value: line 1 column 1 (char 0)')
        sse_mock = mocker.Mock(spec=SplitSSEClient)
        timer_mock = mocker.Mock()
        feedback_loop = Queue()
        manager = self._build_manager(mocker, api_mock, sse_mock, timer_mock, feedback_loop)

        manager.start()
        assert feedback_loop.get(timeout=1) == Status.PUSH_RETRYABLE_ERROR
        assert sse_mock.mock_calls == []
        assert not manager._running

    def test_stop_while_authenticating(self, mocker):
        """Test the SSE connection is not opened if the manager is stopped during auth."""
        feedback_loop = Queue()
        sse_mock = mocker.Mock(spec=SplitSSEClient)
        api_mock = mocker.Mock()
        manager = self._build_manager(mocker, api_mock, sse_mock, mocker.Mock(), feedback_loop)

        def authenticate():
            manager.stop()  # destroyed while waiting for the auth response
            return Token(True, 'abc', {}, 2000000, 1000000)
        api_mock.authenticate.side_effect = authenticate

        manager.start()
        assert sse_mock.start.mock_calls == []
        assert feedback_loop.empty()
        assert not manager._running

    def test_stop_while_connecting(self, mocker):
        """Test the SSE connection is closed if the manager is stopped while connecting."""
        feedback_loop = Queue()
        sse_mock = mocker.Mock(spec=SplitSSEClient)
        api_mock = mocker.Mock()
        api_mock.authenticate.return_value = Token(True, 'abc', {}, 2000000, 1000000)
        timer_mock = mocker.Mock()
        manager = self._build_manager(mocker, api_mock, sse_mock, timer_mock, feedback_loop)

        def start(token):
            manager.stop()  # destroyed while opening the SSE connection
            return True
        sse_mock.start.side_effect = start

        manager.start()
        assert sse_mock.stop.mock_calls == [mocker.call()]
        assert timer_mock.mock_calls == [mocker.call(0, Any())]  # no token refresh scheduled
        assert feedback_loop.empty()
        assert not manager._running

        # It can be started again
        sse_mock.start.side_effect = None
        sse_mock.start.return_value = True
        manager.start()
        assert manager._running

    def test_sse_start_unexpected_exception(self, mocker):
        """Test an error raised by the sse client when connecting is reported as retryable."""
        api_mock = mocker.Mock()
        api_mock.authenticate.return_value = Token(True, 'abc', {}, 2000000, 1000000)
        sse_mock = mocker.Mock(spec=SplitSSEClient)
        sse_mock.start.side_effect = Exception('SseClient already started.')
        timer_mock = mocker.Mock()
        feedback_loop = Queue()
        manager = self._build_manager(mocker, api_mock, sse_mock, timer_mock, feedback_loop)

        manager.start()
        assert feedback_loop.get(timeout=1) == Status.PUSH_RETRYABLE_ERROR
        assert timer_mock.mock_calls == [mocker.call(0, Any())]  # no token refresh scheduled
        assert not manager._running

    def test_token_refresh_unexpected_exception(self, mocker):
        """Test a failed token refresh reports a retryable error instead of killing the thread."""
        api_mock = mocker.Mock()
        api_mock.authenticate.side_effect = [
            Token(True, 'abc', {}, 2000000, 1000000),
            KeyError('x-ably-capability'),
        ]
        sse_mock = mocker.Mock(spec=SplitSSEClient)
        sse_mock.start.return_value = True
        timer_mock = mocker.Mock()
        feedback_loop = Queue()
        manager = self._build_manager(mocker, api_mock, sse_mock, timer_mock, feedback_loop)

        manager.start()
        assert manager._running

        # Run the refresh in its own thread, as Timer would.
        refresh = Thread(target=manager._token_refresh)
        refresh.start()
        refresh.join(1)
        assert not refresh.is_alive()
        assert feedback_loop.get(timeout=1) == Status.PUSH_RETRYABLE_ERROR
        assert not manager._running

    def test_token_refresh_stop_exception(self, mocker):
        """Test an error while closing the connection during a token refresh is reported."""
        api_mock = mocker.Mock()
        api_mock.authenticate.return_value = Token(True, 'abc', {}, 2000000, 1000000)
        sse_mock = mocker.Mock(spec=SplitSSEClient)
        sse_mock.start.return_value = True
        sse_mock.stop.side_effect = OSError('Transport endpoint is not connected')
        timer_mock = mocker.Mock()
        feedback_loop = Queue()
        manager = self._build_manager(mocker, api_mock, sse_mock, timer_mock, feedback_loop)

        manager.start()
        manager._token_refresh()
        assert feedback_loop.get(timeout=1) == Status.PUSH_RETRYABLE_ERROR

    def test_split_change(self, mocker):
        """Test update-type messages are properly forwarded to the processor."""
        sse_event = SSEEvent('1', EventType.MESSAGE, '', '{}')
        update_message = SplitChangeUpdate('chan', 123, 456, None, None, None)
        parse_event_mock = mocker.Mock(spec=parse_incoming_event)
        parse_event_mock.return_value = update_message
        mocker.patch('splitio.push.manager.parse_incoming_event', new=parse_event_mock)

        processor_mock = mocker.Mock(spec=MessageProcessor)
        mocker.patch('splitio.push.manager.MessageProcessor', new=processor_mock)

        telemetry_runtime_producer = mocker.Mock()
        synchronizer = mocker.Mock()
        manager = PushManager(mocker.Mock(), synchronizer, mocker.Mock(), mocker.Mock(), telemetry_runtime_producer)
        manager._event_handler(sse_event)
        assert parse_event_mock.mock_calls == [mocker.call(sse_event)]
        assert processor_mock.mock_calls == [
            mocker.call(synchronizer, telemetry_runtime_producer),
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

        telemetry_runtime_producer = mocker.Mock()
        synchronizer = mocker.Mock()
        manager = PushManager(mocker.Mock(), synchronizer, mocker.Mock(), mocker.Mock(), telemetry_runtime_producer)
        manager._event_handler(sse_event)
        assert parse_event_mock.mock_calls == [mocker.call(sse_event)]
        assert processor_mock.mock_calls == [
            mocker.call(synchronizer, telemetry_runtime_producer),
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

        telemetry_runtime_producer = mocker.Mock()
        synchronizer = mocker.Mock()
        manager = PushManager(mocker.Mock(), synchronizer, mocker.Mock(), mocker.Mock(), telemetry_runtime_producer)
        manager._event_handler(sse_event)
        assert parse_event_mock.mock_calls == [mocker.call(sse_event)]
        assert processor_mock.mock_calls == [
            mocker.call(synchronizer, telemetry_runtime_producer),
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

        def timer_mock(token):
            self.token = token
            return (token.exp - token.iat) - _TOKEN_REFRESH_GRACE_PERIOD

        async def coro():
            t = 0
            try:
                while t < 3:
                    await asyncio.sleep(1)
                    yield SSEEvent('1', EventType.MESSAGE, '', '{}')
                    t += 1
            except Exception:
                pass

        sse_mock = mocker.Mock(spec=SplitSSEClientAsync)
        sse_mock.start.return_value = coro()

        async def stop():
            pass
        sse_mock.stop = stop

        feedback_loop = asyncio.Queue()
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        manager = PushManagerAsync(api_mock, mocker.Mock(), feedback_loop, mocker.Mock(), telemetry_runtime_producer)
        manager._get_time_period = timer_mock
        manager._sse_client = sse_mock

        async def deferred_shutdown():
            await asyncio.sleep(2)
            await manager.stop(True)

        manager.start()
        sse_mock.status = SplitSSEClient._Status.IDLE
        shutdown_task = asyncio.get_running_loop().create_task(deferred_shutdown())

        assert await feedback_loop.get() == Status.PUSH_SUBSYSTEM_UP
        assert self.token.push_enabled
        assert self.token.token == 'abc'
        assert self.token.channels == {}
        assert self.token.exp == 2000000
        assert self.token.iat == 1000000

        try:
            await shutdown_task
        except Exception:
            pass
        assert not manager._running
        assert (telemetry_storage._streaming_events._streaming_events[0]._type == StreamingEventTypes.TOKEN_REFRESH.value)
        assert (telemetry_storage._streaming_events._streaming_events[1]._type == StreamingEventTypes.CONNECTION_ESTABLISHED.value)

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
            if False:
                yield ''  # fit a never-called yield directive to force the func to be an async generator
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
    async def test_auth_unexpected_exception(self, mocker):
        """Test a non-API error during auth (e.g. non-JSON 200 response) is reported as retryable."""
        api_mock = mocker.Mock()

        async def authenticate():
            raise ValueError('Expecting value: line 1 column 1 (char 0)')
        api_mock.authenticate.side_effect = authenticate

        sse_mock = mocker.Mock(spec=SplitSSEClientAsync)
        feedback_loop = asyncio.Queue()
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        manager = PushManagerAsync(api_mock, mocker.Mock(), feedback_loop, mocker.Mock(), telemetry_runtime_producer)
        manager._sse_client = sse_mock

        manager.start()
        assert await asyncio.wait_for(feedback_loop.get(), 1) == Status.PUSH_RETRYABLE_ERROR
        assert sse_mock.mock_calls == []
        assert not manager._running

    @pytest.mark.asyncio
    async def test_sse_start_unexpected_exception(self, mocker):
        """Test an error raised by the sse client is reported as retryable."""
        api_mock = mocker.Mock()

        async def authenticate():
            return Token(True, 'abc', {}, 2000000, 1000000)
        api_mock.authenticate.side_effect = authenticate

        sse_mock = mocker.Mock(spec=SplitSSEClientAsync)
        sse_mock.start.side_effect = Exception('sse client error')
        feedback_loop = asyncio.Queue()
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        manager = PushManagerAsync(api_mock, mocker.Mock(), feedback_loop, mocker.Mock(), telemetry_runtime_producer)
        manager._sse_client = sse_mock

        manager.start()
        assert await asyncio.wait_for(feedback_loop.get(), 1) == Status.PUSH_RETRYABLE_ERROR
        assert not manager._running

    @pytest.mark.asyncio
    async def test_token_refresh_reconnects(self, mocker):
        """Test a token refresh is not cancelled by the cleanup of the connection it closes."""
        api_mock = mocker.Mock()
        auth_calls = []

        async def authenticate():
            auth_calls.append(1)
            return Token(True, 'abc', {}, 2000000, 1000000)
        api_mock.authenticate.side_effect = authenticate

        class FakeSSEClient(object):
            """Ends the event stream on stop() and waits for it, without masking cancellation."""

            def __init__(self):
                self.starts = 0

            def start(self, token):
                self.starts += 1
                self._shutdown = asyncio.Event()
                self._ended = asyncio.Event()

                async def events():
                    try:
                        yield SSEEvent('1', EventType.MESSAGE, None, None)
                        await self._shutdown.wait()
                    finally:
                        self._ended.set()
                return events()

            async def stop(self):
                self._shutdown.set()
                await self._ended.wait()

        feedback_loop = asyncio.Queue()
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        manager = PushManagerAsync(api_mock, mocker.Mock(), feedback_loop, mocker.Mock(), telemetry_runtime_producer)
        processor = mocker.Mock()

        async def update_workers_status(enabled):
            pass
        processor.update_workers_status = update_workers_status
        manager._processor = processor
        manager._get_time_period = lambda token: 0.1
        manager._sse_client = FakeSSEClient()

        manager.start()
        await asyncio.sleep(0.5)
        assert len(auth_calls) >= 3  # initial connection + refreshes
        assert manager._sse_client.starts == len(auth_calls)
        assert manager._running

        await manager.stop(True)
        assert not manager._running

    async def _build_refresh_manager(self, mocker, feedback_loop):
        async def noop(*_args, **_kwargs):
            pass
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        manager = PushManagerAsync(mocker.Mock(), mocker.Mock(), feedback_loop, mocker.Mock(),
                                   telemetry_producer.get_telemetry_runtime_producer())
        manager._processor = mocker.Mock()
        manager._processor.update_workers_status = noop
        manager._sse_client = mocker.Mock()
        manager._sse_client.stop = noop
        manager._get_time_period = lambda token: 0
        return manager

    @pytest.mark.asyncio
    async def test_token_refresh_connection_task_pending(self, mocker):
        """Test the refresh reconnects when the connection task is still ending when cancelled."""
        feedback_loop = asyncio.Queue()
        manager = await self._build_refresh_manager(mocker, feedback_loop)

        async def ending_connection():  # e.g. still running its cleanup
            await asyncio.sleep(1)
        manager._running_task = asyncio.get_running_loop().create_task(ending_connection())
        await asyncio.sleep(0)
        flows = []

        async def connection_flow():
            flows.append(1)
        manager._trigger_connection_flow = connection_flow

        refresh = asyncio.get_running_loop().create_task(
            manager._token_refresh(Token(True, 'abc', {}, 2000000, 1000000)))
        await asyncio.wait_for(refresh, 1)
        assert not refresh.cancelled()
        await asyncio.sleep(0)
        assert flows == [1]  # new connection flow started
        assert feedback_loop.empty()

    @pytest.mark.asyncio
    async def test_token_refresh_stop_exception(self, mocker):
        """Test an error while closing the connection during a token refresh is reported."""
        feedback_loop = asyncio.Queue()
        manager = await self._build_refresh_manager(mocker, feedback_loop)

        async def stop():
            raise OSError('Transport endpoint is not connected')
        manager._sse_client.stop = stop

        await manager._token_refresh(Token(True, 'abc', {}, 2000000, 1000000))
        assert await asyncio.wait_for(feedback_loop.get(), 1) == Status.PUSH_RETRYABLE_ERROR

    @pytest.mark.asyncio
    async def test_stop_during_token_refresh(self, mocker):
        """Test a stop while the refresh closes the connection doesn't reconnect."""
        feedback_loop = asyncio.Queue()
        manager = await self._build_refresh_manager(mocker, feedback_loop)
        manager._running = True
        stop_tasks = []

        async def sse_stop():  # stop() arrives while the refresh is closing the connection
            if not stop_tasks:
                stop_tasks.append(asyncio.get_running_loop().create_task(manager.stop(True)))
            await asyncio.sleep(0)
        manager._sse_client.stop = sse_stop
        flows = []

        async def connection_flow():
            flows.append(1)
        manager._trigger_connection_flow = connection_flow

        await asyncio.wait_for(manager._token_refresh(Token(True, 'abc', {}, 2000000, 1000000)), 1)
        await asyncio.gather(*stop_tasks)
        await asyncio.sleep(0)
        assert flows == []  # no new connection flow after stop
        assert manager._running_task is None
        assert feedback_loop.empty()

    @pytest.mark.asyncio
    async def test_stop_while_authenticating(self, mocker):
        """Test a stop during the auth request doesn't open the SSE connection."""
        feedback_loop = asyncio.Queue()
        manager = await self._build_refresh_manager(mocker, feedback_loop)
        auth_started = asyncio.Event()
        release_auth = asyncio.Event()

        async def get_token():
            auth_started.set()
            await release_auth.wait()
            return Token(True, 'abc', {}, 2000000, 1000000)
        manager._get_auth_token = get_token

        manager.start()
        await asyncio.wait_for(auth_started.wait(), 1)
        await manager.stop(True)  # not running yet: only flags the stop
        release_auth.set()
        await asyncio.sleep(0.1)
        assert manager._sse_client.start.mock_calls == []
        assert not manager._running
        assert feedback_loop.empty()

    @pytest.mark.asyncio
    async def test_split_change(self, mocker):
        """Test update-type messages are properly forwarded to the processor."""
        sse_event = SSEEvent('1', EventType.MESSAGE, '', '{}')
        update_message = SplitChangeUpdate('chan', 123, 456, None, None, None)
        parse_event_mock = mocker.Mock(spec=parse_incoming_event)
        parse_event_mock.return_value = update_message
        mocker.patch('splitio.push.manager.parse_incoming_event', new=parse_event_mock)

        processor_mock = mocker.Mock(spec=MessageProcessorAsync)
        mocker.patch('splitio.push.manager.MessageProcessorAsync', new=processor_mock)

        telemetry_runtime_producer = mocker.Mock()
        synchronizer = mocker.Mock()
        manager = PushManagerAsync(mocker.Mock(), synchronizer, mocker.Mock(), mocker.Mock(), telemetry_runtime_producer)
        await manager._event_handler(sse_event)
        assert parse_event_mock.mock_calls == [mocker.call(sse_event)]
        assert processor_mock.mock_calls == [
            mocker.call(synchronizer, telemetry_runtime_producer),
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

        telemetry_runtime_producer = mocker.Mock()
        synchronizer = mocker.Mock()
        manager = PushManagerAsync(mocker.Mock(), synchronizer, mocker.Mock(), mocker.Mock(), telemetry_runtime_producer)
        await manager._event_handler(sse_event)
        assert parse_event_mock.mock_calls == [mocker.call(sse_event)]
        assert processor_mock.mock_calls == [
            mocker.call(synchronizer, telemetry_runtime_producer),
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

        telemetry_runtime_producer = mocker.Mock()
        synchronizer = mocker.Mock()
        manager = PushManagerAsync(mocker.Mock(), synchronizer, mocker.Mock(), mocker.Mock(), telemetry_runtime_producer)
        await manager._event_handler(sse_event)
        assert parse_event_mock.mock_calls == [mocker.call(sse_event)]
        assert processor_mock.mock_calls == [
            mocker.call(synchronizer, telemetry_runtime_producer),
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
