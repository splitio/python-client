"""Push notification manager tests."""
#pylint:disable=no-self-use,protected-access
from threading import Thread
from queue import Queue

from splitio.api.auth import APIException

from splitio.models.token import Token

from splitio.push.sse import SSEEvent
from splitio.push.parser import parse_incoming_event, EventType, ControlType, ControlMessage, \
    OccupancyMessage, SplitChangeUpdate, SplitKillUpdate, SegmentChangeUpdate
from splitio.push.processor import MessageProcessor
from splitio.push.status_tracker import PushStatusTracker
from splitio.push.manager import PushManager, _TOKEN_REFRESH_GRACE_PERIOD
from splitio.push.splitsse import SplitSSEClient
from splitio.push.status_tracker import Status

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
        manager = PushManager(api_mock, mocker.Mock(), feedback_loop, mocker.Mock())

        def new_start(*args, **kwargs):  # pylint: disable=unused-argument
            """splitsse.start mock."""
            thread = Thread(target=manager._handle_connection_ready)
            thread.setDaemon(True)
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
        manager = PushManager(api_mock, mocker.Mock(), feedback_loop, mocker.Mock())

        def new_start(*args, **kwargs):  # pylint: disable=unused-argument
            """splitsse.start mock."""
            thread = Thread(target=manager._handle_connection_end)
            thread.setDaemon(True)
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
        manager = PushManager(api_mock, mocker.Mock(), feedback_loop, mocker.Mock())
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
        manager = PushManager(api_mock, mocker.Mock(), feedback_loop, mocker.Mock())
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

        manager = PushManager(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())
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

        manager = PushManager(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())
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

        manager = PushManager(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())
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

        manager = PushManager(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())
        manager._event_handler(sse_event)
        assert parse_event_mock.mock_calls == [mocker.call(sse_event)]
        assert status_tracker_mock.mock_calls == [
            mocker.call(),
            mocker.call().handle_control_message(control_message)
        ]

    def test_occupancy_message(self, mocker):
        """Test control mesage is forwarded to status tracker."""
        sse_event = SSEEvent('1', EventType.MESSAGE, '', '{}')
        occupancy_message = OccupancyMessage('[?occupancy=metrics.publishers]control_pri', 123, 2)
        parse_event_mock = mocker.Mock(spec=parse_incoming_event)
        parse_event_mock.return_value = occupancy_message
        mocker.patch('splitio.push.manager.parse_incoming_event', new=parse_event_mock)

        status_tracker_mock = mocker.Mock(spec=PushStatusTracker)
        mocker.patch('splitio.push.manager.PushStatusTracker', new=status_tracker_mock)

        manager = PushManager(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())
        manager._event_handler(sse_event)
        assert parse_event_mock.mock_calls == [mocker.call(sse_event)]
        assert status_tracker_mock.mock_calls == [
            mocker.call(),
            mocker.call().handle_occupancy(occupancy_message)
        ]
