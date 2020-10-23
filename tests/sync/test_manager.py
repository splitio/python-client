"""Manager tests."""

import pytest
import threading

from splitio.tasks.split_sync import SplitSynchronizationTask
from splitio.tasks.segment_sync import SegmentSynchronizationTask
from splitio.tasks.impressions_sync import ImpressionsSyncTask, ImpressionsCountSyncTask
from splitio.tasks.events_sync import EventsSyncTask
from splitio.tasks.telemetry_sync import TelemetrySynchronizationTask

from splitio.synchronizers.split import SplitSynchronizer
from splitio.synchronizers.segment import SegmentSynchronizer
from splitio.synchronizers.impression import ImpressionSynchronizer, ImpressionsCountSynchronizer
from splitio.synchronizers.event import EventSynchronizer
from splitio.synchronizers.telemetry import TelemetrySynchronizer

from splitio.push.synchronizer import Synchronizer, SplitTasks, SplitSynchronizers

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
        manager = Manager(threading.Event(), synchronizer)

        with pytest.raises(APIException):
            manager.start()

    def test_start(self, mocker):
        splits_ready_event = threading.Event()
        synchronizer = mocker.Mock(spec=Synchronizer)
        manager = Manager(splits_ready_event, synchronizer)
        manager.start()

        splits_ready_event.wait(2)
        assert splits_ready_event.is_set()
        assert len(synchronizer.sync_all.mock_calls) == 1
        assert len(synchronizer.start_periodic_fetching.mock_calls) == 1
        assert len(synchronizer.start_periodic_data_recording.mock_calls) == 1
