"""Synchronizer tests."""

import pytest

from splitio.sync.synchronizer import Synchronizer, SplitTasks, SplitSynchronizers
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
from splitio.storage import SegmentStorage, SplitStorage
from splitio.api import APIException
from splitio.models.splits import Split
from splitio.models.segments import Segment


class SynchronizerTests(object):
    def test_sync_all_failed_splits(self, mocker):
        api = mocker.Mock()
        storage = mocker.Mock()

        def run(x):
            raise APIException("something broke")
        api.fetch_splits.side_effect = run

        split_sync = SplitSynchronizer(api, storage)
        split_synchronizers = SplitSynchronizers(split_sync, mocker.Mock(), mocker.Mock(),
                                                 mocker.Mock(), mocker.Mock(), mocker.Mock())
        sychronizer = Synchronizer(split_synchronizers, mocker.Mock(spec=SplitTasks))

        with pytest.raises(APIException):
            sychronizer.synchronize_splits(None)
        with pytest.raises(APIException):
            sychronizer.sync_all()

    def test_sync_all_failed_segments(self, mocker):
        api = mocker.Mock()
        storage = mocker.Mock()
        split_storage = mocker.Mock(spec=SplitStorage)
        split_storage.get_segment_names.return_value = ['segmentA']
        split_sync = mocker.Mock(spec=SplitSynchronizer)
        split_sync.synchronize_splits.return_value = None

        def run(x, y):
            raise APIException("something broke")
        api.fetch_segment.side_effect = run

        segment_sync = SegmentSynchronizer(api, split_storage, storage)
        split_synchronizers = SplitSynchronizers(split_sync, segment_sync, mocker.Mock(),
                                                 mocker.Mock(), mocker.Mock(), mocker.Mock())
        sychronizer = Synchronizer(split_synchronizers, mocker.Mock(spec=SplitTasks))

        with pytest.raises(RuntimeError):
            sychronizer.sync_all()
        assert not sychronizer._synchronize_segments()

    splits = [{
        'changeNumber': 123,
        'trafficTypeName': 'user',
        'name': 'some_name',
        'trafficAllocation': 100,
        'trafficAllocationSeed': 123456,
        'seed': 321654,
        'status': 'ACTIVE',
        'killed': False,
        'defaultTreatment': 'off',
        'algo': 2,
        'conditions': []
    }]

    def test_sync_all(self, mocker):
        split_storage = mocker.Mock(spec=SplitStorage)
        split_storage.get_change_number.return_value = 123
        split_storage.get_segment_names.return_value = ['segmentA']
        split_api = mocker.Mock()
        split_api.fetch_splits.return_value = {'splits': self.splits, 'since': 123,
                                               'till': 123}
        split_sync = SplitSynchronizer(split_api, split_storage)

        segment_storage = mocker.Mock(spec=SegmentStorage)
        segment_storage.get_change_number.return_value = 123
        segment_api = mocker.Mock()
        segment_api.fetch_segment.return_value = {'name': 'segmentA', 'added': ['key1', 'key2',
                                                  'key3'], 'removed': [], 'since': 123, 'till': 123}
        segment_sync = SegmentSynchronizer(segment_api, split_storage, segment_storage)

        split_synchronizers = SplitSynchronizers(split_sync, segment_sync, mocker.Mock(),
                                                 mocker.Mock(), mocker.Mock(), mocker.Mock())

        synchronizer = Synchronizer(split_synchronizers, mocker.Mock(spec=SplitTasks))
        synchronizer.sync_all()

        inserted_split = split_storage.put.mock_calls[0][1][0]
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == 'some_name'

        inserted_segment = segment_storage.update.mock_calls[0][1]
        assert inserted_segment[0] == 'segmentA'
        assert inserted_segment[1] == ['key1', 'key2', 'key3']
        assert inserted_segment[2] == []

    def test_start_periodic_fetching(self, mocker):
        split_task = mocker.Mock(spec=SplitSynchronizationTask)
        segment_task = mocker.Mock(spec=SegmentSynchronizationTask)
        split_tasks = SplitTasks(split_task, segment_task, mocker.Mock(), mocker.Mock(),
                                 mocker.Mock(), mocker.Mock())
        synchronizer = Synchronizer(mocker.Mock(spec=SplitSynchronizers), split_tasks)
        synchronizer.start_periodic_fetching()

        assert len(split_task.start.mock_calls) == 1
        assert len(segment_task.start.mock_calls) == 1

    def test_stop_periodic_fetching(self, mocker):
        split_task = mocker.Mock(spec=SplitSynchronizationTask)
        segment_task = mocker.Mock(spec=SegmentSynchronizationTask)
        split_tasks = SplitTasks(split_task, segment_task, mocker.Mock(), mocker.Mock(),
                                 mocker.Mock(), mocker.Mock())
        synchronizer = Synchronizer(mocker.Mock(spec=SplitSynchronizers), split_tasks)
        synchronizer.stop_periodic_fetching(True)

        assert len(split_task.stop.mock_calls) == 1
        assert len(segment_task.stop.mock_calls) == 1
        assert len(segment_task.pause.mock_calls) == 0

        synchronizer.stop_periodic_fetching(False)

        assert len(split_task.stop.mock_calls) == 2
        assert len(segment_task.stop.mock_calls) == 1
        assert len(segment_task.pause.mock_calls) == 1

    def test_start_periodic_data_recording(self, mocker):
        impression_task = mocker.Mock(spec=ImpressionsSyncTask)
        impression_count_task = mocker.Mock(spec=ImpressionsCountSyncTask)
        event_task = mocker.Mock(spec=EventsSyncTask)
        telemetry_task = mocker.Mock(spec=TelemetrySynchronizationTask)
        split_tasks = SplitTasks(mocker.Mock(), mocker.Mock(), impression_task, event_task,
                                 telemetry_task, impression_count_task)
        synchronizer = Synchronizer(mocker.Mock(spec=SplitSynchronizers), split_tasks)
        synchronizer.start_periodic_data_recording()

        assert len(impression_task.start.mock_calls) == 1
        assert len(impression_count_task.start.mock_calls) == 1
        assert len(event_task.start.mock_calls) == 1
        assert len(telemetry_task.start.mock_calls) == 1

    def test_stop_periodic_data_recording(self, mocker):

        def stop_mock(event):
            event.set()
            return

        def stop_mock_2():
            return

        impression_task = mocker.Mock(spec=ImpressionsSyncTask)
        impression_task.stop.side_effect = stop_mock
        impression_count_task = mocker.Mock(spec=ImpressionsCountSyncTask)
        impression_count_task.stop.side_effect = stop_mock
        event_task = mocker.Mock(spec=EventsSyncTask)
        event_task.stop.side_effect = stop_mock
        telemetry_task = mocker.Mock(spec=TelemetrySynchronizationTask)
        telemetry_task.stop.side_effect = stop_mock_2
        split_tasks = SplitTasks(mocker.Mock(), mocker.Mock(), impression_task, event_task,
                                 telemetry_task, impression_count_task)
        synchronizer = Synchronizer(mocker.Mock(spec=SplitSynchronizers), split_tasks)
        synchronizer.stop_periodic_data_recording()

        assert len(impression_task.stop.mock_calls) == 1
        assert len(impression_count_task.stop.mock_calls) == 1
        assert len(event_task.stop.mock_calls) == 1
        assert len(telemetry_task.stop.mock_calls) == 1
