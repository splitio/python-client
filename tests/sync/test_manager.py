"""Manager tests."""

import threading
import unittest.mock as mock
import time
import pytest

from splitio.api.auth import AuthAPI
from splitio.api import auth, client, APIException
from splitio.client.util import get_metadata
from splitio.client.config import DEFAULT_CONFIG
from splitio.tasks.split_sync import SplitSynchronizationTask
from splitio.tasks.segment_sync import SegmentSynchronizationTask
from splitio.tasks.impressions_sync import ImpressionsSyncTask, ImpressionsCountSyncTask
from splitio.tasks.events_sync import EventsSyncTask
from splitio.engine.telemetry import TelemetryStorageProducer
from splitio.storage.inmemmory import InMemoryTelemetryStorage
from splitio.models.telemetry import SSESyncMode, StreamingEventTypes
from splitio.push.manager import Status

from splitio.sync.split import SplitSynchronizer
from splitio.sync.segment import SegmentSynchronizer
from splitio.sync.impression import ImpressionSynchronizer, ImpressionsCountSynchronizer
from splitio.sync.event import EventSynchronizer
from splitio.sync.synchronizer import Synchronizer, SplitTasks, SplitSynchronizers, RedisSynchronizer, RedisSynchronizerAsync
from splitio.sync.manager import Manager, RedisManager, RedisManagerAsync

from splitio.storage import SplitStorage

from splitio.api import APIException

from splitio.client.util import SdkMetadata


class SyncManagerTests(object):
    """Synchronizer Manager tests."""

    def test_error(self, mocker):
        split_task = mocker.Mock(spec=SplitSynchronizationTask)
        split_tasks = SplitTasks(split_task, mocker.Mock(), mocker.Mock(), mocker.Mock(),
                                 mocker.Mock(), mocker.Mock())

        storage = mocker.Mock(spec=SplitStorage)
        api = mocker.Mock()

        def run(x):
            raise APIException("something broke")

        api.fetch_splits.side_effect = run
        storage.get_change_number.return_value = -1

        split_sync = SplitSynchronizer(api, storage)
        synchronizers = SplitSynchronizers(split_sync, mocker.Mock(), mocker.Mock(),
                                           mocker.Mock(), mocker.Mock(), mocker.Mock())

        synchronizer = Synchronizer(synchronizers, split_tasks)
        manager = Manager(threading.Event(), synchronizer,  mocker.Mock(), False, SdkMetadata('1.0', 'some', '1.2.3.4'), mocker.Mock())

        manager._SYNC_ALL_ATTEMPTS = 1
        manager.start(2)  # should not throw!

    def test_start_streaming_false(self, mocker):
        splits_ready_event = threading.Event()
        synchronizer = mocker.Mock(spec=Synchronizer)
        manager = Manager(splits_ready_event, synchronizer, mocker.Mock(), False, SdkMetadata('1.0', 'some', '1.2.3.4'), mocker.Mock())
        try:
            manager.start()
        except:
            pass
        splits_ready_event.wait(2)
        assert splits_ready_event.is_set()
        assert len(synchronizer.sync_all.mock_calls) == 1
        assert len(synchronizer.start_periodic_fetching.mock_calls) == 1
        assert len(synchronizer.start_periodic_data_recording.mock_calls) == 1

    def test_telemetry(self, mocker):
        splits_ready_event = threading.Event()
        synchronizer = mocker.Mock(spec=Synchronizer)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        manager = Manager(splits_ready_event, synchronizer, mocker.Mock(), True, SdkMetadata('1.0', 'some', '1.2.3.4'), telemetry_runtime_producer)
        try:
            manager.start()
        except:
            pass
        splits_ready_event.wait(2)

        manager._queue.put(Status.PUSH_SUBSYSTEM_UP)
        manager._queue.put(Status.PUSH_NONRETRYABLE_ERROR)
        time.sleep(1)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-2]._type == StreamingEventTypes.SYNC_MODE_UPDATE.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-2]._data == SSESyncMode.STREAMING.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._type == StreamingEventTypes.SYNC_MODE_UPDATE.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._data == SSESyncMode.POLLING.value)

class RedisSyncManagerTests(object):
    """Synchronizer Redis Manager tests."""

    synchronizers = SplitSynchronizers(None, None, None, None, None, None, None, None)
    tasks = SplitTasks(None, None, None, None, None, None, None, None)
    synchronizer = RedisSynchronizer(synchronizers, tasks)
    manager = RedisManager(synchronizer)

    @mock.patch('splitio.sync.synchronizer.RedisSynchronizer.start_periodic_data_recording')
    def test_recreate_and_start(self, mocker):

        assert(isinstance(self.manager._synchronizer, RedisSynchronizer))

        self.manager.recreate()
        assert(not mocker.called)

        self.manager.start()
        assert(mocker.called)

    @mock.patch('splitio.sync.synchronizer.RedisSynchronizer.shutdown')
    def test_recreate_and_stop(self, mocker):

        self.manager.recreate()
        assert(not mocker.called)

        self.manager.stop(True)
        assert(mocker.called)


class RedisSyncManagerAsyncTests(object):
    """Synchronizer Redis Manager async tests."""

    synchronizers = SplitSynchronizers(None, None, None, None, None, None, None, None)
    tasks = SplitTasks(None, None, None, None, None, None, None, None)
    synchronizer = RedisSynchronizerAsync(synchronizers, tasks)
    manager = RedisManagerAsync(synchronizer)

    @mock.patch('splitio.sync.synchronizer.RedisSynchronizerAsync.start_periodic_data_recording')
    def test_recreate_and_start(self, mocker):
        assert(isinstance(self.manager._synchronizer, RedisSynchronizerAsync))

        self.manager.recreate()
        assert(not mocker.called)

        self.manager.start()
        assert(mocker.called)

    @pytest.mark.asyncio
    async def test_recreate_and_stop(self, mocker):
        self.called = False
        async def shutdown(block):
            self.called = True
        self.manager._synchronizer.shutdown = shutdown
        self.manager.recreate()
        assert(not self.called)

        await self.manager.stop(True)
        assert(self.called)
