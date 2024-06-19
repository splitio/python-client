"""Message processor tests."""
from queue import Queue
import pytest

from splitio.push.processor import MessageProcessor, MessageProcessorAsync
from splitio.sync.synchronizer import Synchronizer, SynchronizerAsync
from splitio.push.parser import SplitChangeUpdate, SegmentChangeUpdate, SplitKillUpdate
from splitio.optional.loaders import asyncio


class ProcessorTests(object):
    """Message processor test cases."""

    def test_split_change(self, mocker):
        """Test split change is properly handled."""
        sync_mock = mocker.Mock(spec=Synchronizer)
        queue_mock = mocker.Mock(spec=Queue)
        mocker.patch('splitio.push.processor.Queue', new=queue_mock)
        processor = MessageProcessor(sync_mock, mocker.Mock())
        update = SplitChangeUpdate('sarasa', 123, 123, None, None, None)
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
        processor = MessageProcessor(sync_mock, mocker.Mock())
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
        processor = MessageProcessor(sync_mock, mocker.Mock())
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

class ProcessorAsyncTests(object):
    """Message processor test cases."""

    @pytest.mark.asyncio
    async def test_split_change(self, mocker):
        """Test split change is properly handled."""
        sync_mock = mocker.Mock(spec=Synchronizer)
        self._update = None
        async def put_mock(first, event):
            self._update = event

        mocker.patch('splitio.push.processor.asyncio.Queue.put', new=put_mock)
        processor = MessageProcessorAsync(sync_mock, mocker.Mock())
        update = SplitChangeUpdate('sarasa', 123, 123, None, None, None)
        await processor.handle(update)
        assert update == self._update

    @pytest.mark.asyncio
    async def test_split_kill(self, mocker):
        """Test split kill is properly handled."""

        self._killed_split = None
        async def kill_mock(split_name, default_treatment, change_number):
            self._killed_split = (split_name, default_treatment, change_number)

        sync_mock = mocker.Mock(spec=SynchronizerAsync)
        sync_mock.kill_split = kill_mock

        self._update = None
        async def put_mock(first, event):
            self._update = event

        mocker.patch('splitio.push.processor.asyncio.Queue.put', new=put_mock)
        processor = MessageProcessorAsync(sync_mock, mocker.Mock())
        update = SplitKillUpdate('sarasa', 123, 456, 'some_split', 'off')
        await processor.handle(update)
        assert update == self._update
        assert ('some_split', 'off', 456) == self._killed_split

    @pytest.mark.asyncio
    async def test_segment_change(self, mocker):
        """Test segment change is properly handled."""

        sync_mock = mocker.Mock(spec=SynchronizerAsync)
        queue_mock = mocker.Mock(spec=asyncio.Queue)

        self._update = None
        async def put_mock(first, event):
            self._update = event

        mocker.patch('splitio.push.processor.asyncio.Queue.put', new=put_mock)
        processor = MessageProcessorAsync(sync_mock, mocker.Mock())
        update = SegmentChangeUpdate('sarasa', 123, 123, 'some_segment')
        await processor.handle(update)
        assert update == self._update
