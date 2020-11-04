"""Message processor tests."""
from queue import Queue
from splitio.push.processor import MessageProcessor
from splitio.sync.synchronizer import Synchronizer
from splitio.push.parser import SplitChangeUpdate, SegmentChangeUpdate, SplitKillUpdate


class ProcessorTests(object):
    """Message processor test cases."""

    def test_split_change(self, mocker):
        """Test split change is properly handled."""
        sync_mock = mocker.Mock(spec=Synchronizer)
        queue_mock = mocker.Mock(spec=Queue)
        mocker.patch('splitio.push.processor.Queue', new=queue_mock)
        processor = MessageProcessor(sync_mock)
        update = SplitChangeUpdate('sarasa', 123, 123)
        processor.handle(update)
        assert queue_mock.mock_calls == [
            mocker.call(),  # construction of split queue
            mocker.call(),  # construction of split queue
            mocker.call().put(update)
        ]

    def test_split_kill(self, mocker):
        """Test split kill is properly handled."""
        sync_mock = mocker.Mock(spec=Synchronizer)
        queue_mock = mocker.Mock(spec=Queue)
        mocker.patch('splitio.push.processor.Queue', new=queue_mock)
        processor = MessageProcessor(sync_mock)
        update = SplitKillUpdate('sarasa', 123, 456, 'some_split', 'off')
        processor.handle(update)
        assert queue_mock.mock_calls == [
            mocker.call(),  # construction of split queue
            mocker.call(),  # construction of split queue
            mocker.call().put(update)
        ]
        assert sync_mock.kill_split.mock_calls == [
            mocker.call('some_split', 'off', 456)
        ]

    def test_segment_change(self, mocker):
        """Test segment change is properly handled."""
        sync_mock = mocker.Mock(spec=Synchronizer)
        queue_mock = mocker.Mock(spec=Queue)
        mocker.patch('splitio.push.processor.Queue', new=queue_mock)
        processor = MessageProcessor(sync_mock)
        update = SegmentChangeUpdate('sarasa', 123, 123, 'some_segment')
        processor.handle(update)
        assert queue_mock.mock_calls == [
            mocker.call(),  # construction of split queue
            mocker.call(),  # construction of split queue
            mocker.call().put(update)
        ]

    def test_todo(self):
        """Fix previous tests so that we validate WHICH queue the update is pushed into."""
        assert NotImplementedError("DO THAT")
