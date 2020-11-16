"""Manager tests."""

import pytest
import threading

from splitio.tasks.split_sync import SplitSynchronizationTask
from splitio.tasks.segment_sync import SegmentSynchronizationTask
from splitio.tasks.impressions_sync import ImpressionsSyncTask, ImpressionsCountSyncTask
from splitio.tasks.events_sync import EventsSyncTask
from splitio.tasks.telemetry_sync import TelemetrySynchronizationTask

from splitio.sync.split import SplitSynchronizer
from splitio.sync.segment import SegmentSynchronizer
from splitio.sync.impression import ImpressionSynchronizer, ImpressionsCountSynchronizer
from splitio.sync.event import EventSynchronizer
from splitio.sync.telemetry import TelemetrySynchronizer
from splitio.sync.synchronizer import Synchronizer, SplitTasks, SplitSynchronizers
from splitio.sync.manager import Manager

from splitio.storage import SplitStorage
from splitio.api import APIException


class ManagerTests(object):
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
        manager = Manager(threading.Event(), synchronizer,  mocker.Mock(), False)

        manager.start()  # should not throw!

    def test_start_streaming_false(self, mocker):
        splits_ready_event = threading.Event()
        synchronizer = mocker.Mock(spec=Synchronizer)
        manager = Manager(splits_ready_event, synchronizer, mocker.Mock(), False)
        manager.start()

        splits_ready_event.wait(2)
        assert splits_ready_event.is_set()
        assert len(synchronizer.sync_all.mock_calls) == 1
        assert len(synchronizer.start_periodic_fetching.mock_calls) == 1
        assert len(synchronizer.start_periodic_data_recording.mock_calls) == 1
