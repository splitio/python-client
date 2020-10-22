"""Manager tests."""

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
from splitio.push.manager import Manager

from splitio.storage import SplitStorage
from splitio.api import APIException


class ManagerTests(object):
    """Synchronizer Manager tests."""

    def test_error(self, mocker):
        split_task = mocker.Mock(spec=SplitSynchronizationTask)
        segment_task = mocker.Mock(spec=SegmentSynchronizationTask)
        impression_task = mocker.Mock(spec=ImpressionsSyncTask)
        impression_count_task = mocker.Mock(spec=ImpressionsCountSyncTask)
        event_task = mocker.Mock(spec=EventsSyncTask)
        telemetry_task = mocker.Mock(spec=TelemetrySynchronizationTask)
        split_tasks = SplitTasks(split_task, segment_task, impression_task, event_task,
                                 telemetry_task, impression_count_task)

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
        manager = Manager(synchronizer)
