"""Manager tests."""

import threading
import unittest.mock as mock

from splitio.tasks.split_sync import SplitSynchronizationTask
from splitio.tasks.segment_sync import SegmentSynchronizationTask
from splitio.tasks.impressions_sync import ImpressionsSyncTask, ImpressionsCountSyncTask
from splitio.tasks.events_sync import EventsSyncTask

from splitio.sync.split import SplitSynchronizer
from splitio.sync.segment import SegmentSynchronizer
from splitio.sync.impression import ImpressionSynchronizer, ImpressionsCountSynchronizer
from splitio.sync.event import EventSynchronizer
from splitio.sync.synchronizer import Synchronizer, SplitTasks, SplitSynchronizers, RedisSynchronizer
from splitio.sync.manager import Manager, RedisManager

from splitio.storage import SplitStorage

from splitio.api import APIException

from splitio.client.util import SdkMetadata


class ManagerTests(object):
    """Synchronizer Manager tests."""

    def test_error(self, mocker):
        split_task = mocker.Mock(spec=SplitSynchronizationTask)
        split_tasks = SplitTasks(split_task, mocker.Mock(), mocker.Mock(), mocker.Mock(),
                                 mocker.Mock())

        storage = mocker.Mock(spec=SplitStorage)
        api = mocker.Mock()

        def run(x):
            raise APIException("something broke")

        api.fetch_splits.side_effect = run
        storage.get_change_number.return_value = -1

        split_sync = SplitSynchronizer(api, storage)
        synchronizers = SplitSynchronizers(split_sync, mocker.Mock(), mocker.Mock(),
                                           mocker.Mock(), mocker.Mock())

        synchronizer = Synchronizer(synchronizers, split_tasks)
        manager = Manager(threading.Event(), synchronizer,  mocker.Mock(), False, SdkMetadata('1.0', 'some', '1.2.3.4'))

        manager.start()  # should not throw!

    def test_start_streaming_false(self, mocker):
        splits_ready_event = threading.Event()
        synchronizer = mocker.Mock(spec=Synchronizer)
        manager = Manager(splits_ready_event, synchronizer, mocker.Mock(), False, SdkMetadata('1.0', 'some', '1.2.3.4'))
        manager.start()

        splits_ready_event.wait(2)
        assert splits_ready_event.is_set()
        assert len(synchronizer.sync_all.mock_calls) == 1
        assert len(synchronizer.start_periodic_fetching.mock_calls) == 1
        assert len(synchronizer.start_periodic_data_recording.mock_calls) == 1

class RedisManagerTests(object):
    """Synchronizer Redis Manager tests."""

    synchronizers = SplitSynchronizers(None, None, None, None, None, None, None)
    tasks = SplitTasks(None, None, None, None, None, None, None)
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
