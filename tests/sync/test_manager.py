"""Manager tests."""

import threading
import unittest.mock as mock
import time
import pytest

from splitio.optional.loaders import asyncio
from splitio.api.auth import AuthAPI
from splitio.api import auth, client, APIException
from splitio.client.util import get_metadata
from splitio.client.config import DEFAULT_CONFIG
from splitio.tasks.split_sync import SplitSynchronizationTask, SplitSynchronizationTaskAsync
from splitio.tasks.segment_sync import SegmentSynchronizationTask
from splitio.tasks.impressions_sync import ImpressionsSyncTask, ImpressionsCountSyncTask
from splitio.tasks.events_sync import EventsSyncTask
from splitio.engine.telemetry import TelemetryStorageProducer, TelemetryStorageProducerAsync
from splitio.storage.inmemmory import InMemoryTelemetryStorage, InMemoryTelemetryStorageAsync
from splitio.models.telemetry import SSESyncMode, StreamingEventTypes
from splitio.push.manager import Status
from splitio.sync.split import SplitSynchronizer, SplitSynchronizerAsync
from splitio.sync.segment import SegmentSynchronizer
from splitio.sync.impression import ImpressionSynchronizer, ImpressionsCountSynchronizer
from splitio.sync.event import EventSynchronizer
from splitio.sync.synchronizer import Synchronizer, SynchronizerAsync, SplitTasks, SplitSynchronizers, RedisSynchronizer, RedisSynchronizerAsync
from splitio.sync.manager import Manager, ManagerAsync, RedisManager, RedisManagerAsync
from splitio.storage import SplitStorage, RuleBasedSegmentsStorage
from splitio.api import APIException
from splitio.client.util import SdkMetadata


@pytest.fixture(autouse=True)
def _no_backoff_wait(mocker):
    """Retry loops back off 1, 2, 4... seconds; don't actually wait in tests."""
    mocker.patch('splitio.util.backoff.Backoff.get', return_value=0)


class SyncManagerTests(object):
    """Synchronizer Manager tests."""

    def test_error(self, mocker):
        split_task = mocker.Mock(spec=SplitSynchronizationTask)
        split_tasks = SplitTasks(split_task, mocker.Mock(), mocker.Mock(), mocker.Mock(),
                                 mocker.Mock(), mocker.Mock())

        storage = mocker.Mock(spec=SplitStorage)
        rb_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)
        api = mocker.Mock()

        def run(x):
            raise APIException("something broke")

        api.fetch_splits.side_effect = run
        storage.get_change_number.return_value = -1

        split_sync = SplitSynchronizer(api, storage, rb_storage)
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
        manager._push = mocker.Mock()  # don't open a real streaming connection
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

    def _wait_for(self, condition, timeout=2):
        deadline = time.time() + timeout
        while not condition() and time.time() < deadline:
            time.sleep(0.01)
        return condition()

    def _build_streaming_manager(self, mocker):
        synchronizer = mocker.Mock(spec=Synchronizer)
        manager = Manager(threading.Event(), synchronizer, mocker.Mock(), True,
                          SdkMetadata('1.0', 'some', '1.2.3.4'), mocker.Mock())
        manager._backoff = mocker.Mock()
        manager._backoff.get.return_value = 0
        manager._push = mocker.Mock()
        return manager, synchronizer, manager._push

    def _stop_handler(self, manager):
        manager._push_status_handler_active = False
        manager._queue.put(manager._CENTINEL_EVENT)
        manager._push_status_handler.join(1)
        assert not manager._push_status_handler.is_alive()

    def test_streaming_handler_survives_unexpected_exception(self, mocker):
        """Test the status handler falls back to polling, retries streaming and keeps running."""
        manager, synchronizer, push_mock = self._build_streaming_manager(mocker)
        worker_calls = []
        def update_workers_status(enabled):
            worker_calls.append(enabled)
            if len(worker_calls) == 1:
                raise RuntimeError("can't start new thread")
        push_mock.update_workers_status.side_effect = update_workers_status
        manager._push_status_handler.start()

        # PUSH_SUBSYSTEM_UP stops polling and then fails enabling workers: polling must restart
        # and the streaming flow must be retried (as a retryable error).
        manager._queue.put(Status.PUSH_SUBSYSTEM_UP)
        assert self._wait_for(lambda: len(push_mock.start.mock_calls) == 1)
        assert len(synchronizer.stop_periodic_fetching.mock_calls) == 1
        assert len(synchronizer.start_periodic_fetching.mock_calls) == 2  # fallback + retry flow
        assert push_mock.stop.mock_calls == [mocker.call(True)]
        assert manager._push_status_handler.is_alive()

        # The handler still processes later status changes.
        manager._queue.put(Status.PUSH_SUBSYSTEM_DOWN)
        assert self._wait_for(lambda: len(synchronizer.start_periodic_fetching.mock_calls) == 3)
        assert manager._push_status_handler.is_alive()
        self._stop_handler(manager)

    def test_streaming_handler_retryable_error_restart_fails(self, mocker):
        """Test streaming is retried when restarting it raises after a retryable error."""
        manager, synchronizer, push_mock = self._build_streaming_manager(mocker)
        push_mock.start.side_effect = [Exception('SseClient already started.'), None]
        manager._push_status_handler.start()

        manager._queue.put(Status.PUSH_RETRYABLE_ERROR)
        assert self._wait_for(lambda: len(push_mock.start.mock_calls) == 2)
        assert len(synchronizer.start_periodic_fetching.mock_calls) == 3  # flow, fallback, flow
        assert manager._push_status_handler.is_alive()
        time.sleep(0.1)
        assert len(push_mock.start.mock_calls) == 2  # no more retries once it succeeds
        self._stop_handler(manager)

    def test_streaming_handler_nonretryable_error_fails(self, mocker):
        """Test an error while disabling streaming leaves polling on and streaming off."""
        manager, synchronizer, push_mock = self._build_streaming_manager(mocker)
        push_mock.stop.side_effect = OSError('Transport endpoint is not connected')
        manager._push_status_handler.start()

        manager._queue.put(Status.PUSH_NONRETRYABLE_ERROR)
        manager._push_status_handler.join(1)
        assert not manager._push_status_handler.is_alive()
        assert len(synchronizer.start_periodic_fetching.mock_calls) == 1
        assert push_mock.start.mock_calls == []

    def test_stop_while_backing_off(self, mocker):
        """Test streaming is not restarted when the manager is stopped during the retry backoff."""
        manager, synchronizer, push_mock = self._build_streaming_manager(mocker)
        manager._backoff.get.return_value = 30
        manager._push_status_handler.start()

        manager._queue.put(Status.PUSH_RETRYABLE_ERROR)
        assert self._wait_for(lambda: len(synchronizer.start_periodic_fetching.mock_calls) == 1)
        start = time.time()
        manager.stop(True)
        manager._push_status_handler.join(1)
        assert not manager._push_status_handler.is_alive()
        assert time.time() - start < 1  # the backoff wait was interrupted
        assert push_mock.start.mock_calls == []


class SyncManagerAsyncTests(object):
    """Synchronizer Manager tests."""

    @pytest.mark.asyncio
    async def test_error(self, mocker):
        split_task = mocker.Mock(spec=SplitSynchronizationTask)
        split_tasks = SplitTasks(split_task, mocker.Mock(), mocker.Mock(), mocker.Mock(),
                                 mocker.Mock(), mocker.Mock())

        storage = mocker.Mock(spec=SplitStorage)
        rb_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)
        api = mocker.Mock()

        async def run(x):
            raise APIException("something broke")
        api.fetch_splits = run

        async def get_change_number():
            return -1
        storage.get_change_number = get_change_number

        split_sync = SplitSynchronizerAsync(api, storage, rb_storage)
        synchronizers = SplitSynchronizers(split_sync, mocker.Mock(), mocker.Mock(),
                                           mocker.Mock(), mocker.Mock(), mocker.Mock())

        synchronizer = SynchronizerAsync(synchronizers, split_tasks)
        manager = ManagerAsync(synchronizer,  mocker.Mock(), False, SdkMetadata('1.0', 'some', '1.2.3.4'), mocker.Mock())

        manager._SYNC_ALL_ATTEMPTS = 1
        await manager.start(2)  # should not throw!

    @pytest.mark.asyncio
    async def test_start_streaming_false(self, mocker):
        synchronizer = mocker.Mock(spec=SynchronizerAsync)
        self.sync_all_called = 0
        async def sync_all(retry):
            self.sync_all_called += 1
        synchronizer.sync_all = sync_all

        self.fetching_called = 0
        def start_periodic_fetching():
            self.fetching_called += 1
        synchronizer.start_periodic_fetching = start_periodic_fetching

        self.rcording_called = 0
        def start_periodic_data_recording():
            self.rcording_called += 1
        synchronizer.start_periodic_data_recording = start_periodic_data_recording

        manager = ManagerAsync(synchronizer, mocker.Mock(), False, SdkMetadata('1.0', 'some', '1.2.3.4'), mocker.Mock())
        try:
            await manager.start()
        except:
            pass
        assert self.sync_all_called == 1
        assert self.fetching_called == 1
        assert self.rcording_called == 1

    @pytest.mark.asyncio
    async def test_telemetry(self, mocker):
        synchronizer = mocker.Mock(spec=SynchronizerAsync)
        async def sync_all(retry=1):
            pass
        synchronizer.sync_all = sync_all

        async def stop_periodic_fetching():
            pass
        synchronizer.stop_periodic_fetching = stop_periodic_fetching

        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        manager = ManagerAsync(synchronizer, mocker.Mock(), True, SdkMetadata('1.0', 'some', '1.2.3.4'), telemetry_runtime_producer)
        try:
            await manager.start()
        except:
            pass

        await manager._queue.put(Status.PUSH_SUBSYSTEM_UP)
        await manager._queue.put(Status.PUSH_NONRETRYABLE_ERROR)
        await asyncio.sleep(1)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-2]._type == StreamingEventTypes.SYNC_MODE_UPDATE.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-2]._data == SSESyncMode.STREAMING.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._type == StreamingEventTypes.SYNC_MODE_UPDATE.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._data == SSESyncMode.POLLING.value)

    @pytest.mark.asyncio
    async def test_streaming_handler_survives_unexpected_exception(self, mocker):
        """Test the status handler falls back to polling, retries streaming and keeps running."""
        synchronizer = mocker.Mock(spec=SynchronizerAsync)
        calls = {'sync_all': 0, 'stop_periodic_fetching': 0}
        async def sync_all(retry=1):
            calls['sync_all'] += 1
        synchronizer.sync_all = sync_all
        async def stop_periodic_fetching():
            calls['stop_periodic_fetching'] += 1
        synchronizer.stop_periodic_fetching = stop_periodic_fetching

        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        manager = ManagerAsync(synchronizer, mocker.Mock(), True,
                               SdkMetadata('1.0', 'some', '1.2.3.4'),
                               telemetry_producer.get_telemetry_runtime_producer())
        manager._backoff = mocker.Mock()
        manager._backoff.get.return_value = 0
        push_mock = mocker.Mock()
        worker_calls = []
        async def update_workers_status(enabled):
            worker_calls.append(enabled)
            if len(worker_calls) == 1:
                raise RuntimeError('worker failure')
        push_mock.update_workers_status = update_workers_status
        stop_calls = []
        async def stop(blocking=False):
            stop_calls.append(blocking)
        push_mock.stop = stop
        manager._push = push_mock
        handler = asyncio.get_running_loop().create_task(manager._streaming_feedback_handler())

        # PUSH_SUBSYSTEM_UP stops polling and then fails enabling workers: polling must restart
        # and the streaming flow must be retried (as a retryable error).
        await manager._queue.put(Status.PUSH_SUBSYSTEM_UP)
        await asyncio.sleep(0.1)
        assert calls['stop_periodic_fetching'] == 1
        assert len(synchronizer.start_periodic_fetching.mock_calls) == 2  # fallback + retry flow
        assert stop_calls == [True]
        assert len(push_mock.start.mock_calls) == 1
        assert not handler.done()

        # The handler still processes later status changes.
        await manager._queue.put(Status.PUSH_SUBSYSTEM_DOWN)
        await asyncio.sleep(0.1)
        assert len(synchronizer.start_periodic_fetching.mock_calls) == 3
        assert not handler.done()

        manager._push_status_handler_active = False
        await manager._queue.put(manager._CENTINEL_EVENT)
        await asyncio.wait_for(handler, 1)

    @pytest.mark.asyncio
    async def test_stop_while_backing_off(self, mocker):
        """Test streaming is not restarted when the manager is stopped during the retry backoff."""
        synchronizer = mocker.Mock(spec=SynchronizerAsync)
        async def noop(*_args, **_kwargs):
            pass
        synchronizer.sync_all = noop
        manager = ManagerAsync(synchronizer, mocker.Mock(), True,
                               SdkMetadata('1.0', 'some', '1.2.3.4'), mocker.Mock())
        manager._backoff = mocker.Mock()
        manager._backoff.get.return_value = 0.2
        push_mock = mocker.Mock()
        push_mock.update_workers_status = noop
        push_mock.stop = noop
        manager._push = push_mock
        handler = asyncio.get_running_loop().create_task(manager._streaming_feedback_handler())

        await manager._queue.put(Status.PUSH_RETRYABLE_ERROR)
        await asyncio.sleep(0.05)
        manager._push_status_handler_active = False  # as done by stop()
        await asyncio.wait_for(handler, 1)
        assert push_mock.start.mock_calls == []


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
