"""Synchronizer tests."""

from turtle import clear

from splitio.sync.synchronizer import Synchronizer, SplitTasks, SplitSynchronizers
from splitio.tasks.split_sync import SplitSynchronizationTask
from splitio.tasks.unique_keys_sync import UniqueKeysSyncTask, ClearFilterSyncTask
from splitio.tasks.segment_sync import SegmentSynchronizationTask
from splitio.tasks.impressions_sync import ImpressionsSyncTask, ImpressionsCountSyncTask
from splitio.tasks.events_sync import EventsSyncTask
from splitio.sync.split import SplitSynchronizer
from splitio.sync.segment import SegmentSynchronizer
from splitio.sync.impression import ImpressionSynchronizer, ImpressionsCountSynchronizer
from splitio.sync.event import EventSynchronizer
from splitio.storage import SegmentStorage, SplitStorage
from splitio.api import APIException
from splitio.models.splits import Split
from splitio.models.segments import Segment
from splitio.storage.inmemmory import InMemorySegmentStorage, InMemorySplitStorage

class SynchronizerTests(object):
    def test_sync_all_failed_splits(self, mocker):
        api = mocker.Mock()
        storage = mocker.Mock()

        def run(x, c):
            raise APIException("something broke")
        api.fetch_splits.side_effect = run

        split_sync = SplitSynchronizer(api, storage)
        split_synchronizers = SplitSynchronizers(split_sync, mocker.Mock(), mocker.Mock(),
                                                 mocker.Mock(), mocker.Mock())
        sychronizer = Synchronizer(split_synchronizers, mocker.Mock(spec=SplitTasks))

        sychronizer.synchronize_splits(None)  # APIExceptions are handled locally and should not be propagated!

        sychronizer.sync_all()  # sync_all should not throw!

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
                                                 mocker.Mock(), mocker.Mock())
        sychronizer = Synchronizer(split_synchronizers, mocker.Mock(spec=SplitTasks))

        sychronizer.sync_all()  # SyncAll should not throw!
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
        'conditions': [{
            'conditionType': 'WHITELIST',
            'matcherGroup':{
                'combiner': 'AND',
                'matchers':[{
                    'matcherType': 'IN_SEGMENT',
                    'negate': False,
                    'userDefinedSegmentMatcherData': {
                        'segmentName': 'segmentA'
                    }
                }]
            },
            'partitions': [{
                'size': 100,
                'treatment': 'on'
            }]
        }]
    }]

    def test_synchronize_splits(self, mocker):
        split_storage = InMemorySplitStorage()
        split_api = mocker.Mock()
        split_api.fetch_splits.return_value = {'splits': self.splits, 'since': 123,
                                               'till': 123}
        split_sync = SplitSynchronizer(split_api, split_storage)
        segment_storage = InMemorySegmentStorage()
        segment_api = mocker.Mock()
        segment_api.fetch_segment.return_value = {'name': 'segmentA', 'added': ['key1', 'key2',
                                                  'key3'], 'removed': [], 'since': 123, 'till': 123}
        segment_sync = SegmentSynchronizer(segment_api, split_storage, segment_storage)
        split_synchronizers = SplitSynchronizers(split_sync, segment_sync, mocker.Mock(),
                                                 mocker.Mock(), mocker.Mock())
        synchronizer = Synchronizer(split_synchronizers, mocker.Mock(spec=SplitTasks))

        synchronizer.synchronize_splits(123)

        inserted_split = split_storage.get('some_name')
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == 'some_name'

        if not segment_sync._worker_pool.wait_for_completion():
            inserted_segment = segment_storage.get('segmentA')
            assert inserted_segment.name == 'segmentA'
            assert inserted_segment.keys == {'key1', 'key2', 'key3'}

    def test_synchronize_splits_calling_segment_sync_once(self, mocker):
        split_storage = InMemorySplitStorage()
        split_api = mocker.Mock()
        split_api.fetch_splits.return_value = {'splits': self.splits, 'since': 123,
                                               'till': 123}
        split_sync = SplitSynchronizer(split_api, split_storage)
        counts = {'segments': 0}

        def sync_segments(*_):
            """Sync Segments."""
            counts['segments'] += 1
            return True

        segment_sync = mocker.Mock()
        segment_sync.synchronize_segments.side_effect = sync_segments
        segment_sync.segment_exist_in_storage.return_value = False
        split_synchronizers = SplitSynchronizers(split_sync, segment_sync, mocker.Mock(),
                                                 mocker.Mock(), mocker.Mock())
        synchronizer = Synchronizer(split_synchronizers, mocker.Mock(spec=SplitTasks))
        synchronizer.synchronize_splits(123, True)

        assert counts['segments'] == 1

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
                                                 mocker.Mock(), mocker.Mock())

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
                                 mocker.Mock())
        synchronizer = Synchronizer(mocker.Mock(spec=SplitSynchronizers), split_tasks)
        synchronizer.start_periodic_fetching()

        assert len(split_task.start.mock_calls) == 1
        assert len(segment_task.start.mock_calls) == 1

    def test_stop_periodic_fetching(self, mocker):
        split_task = mocker.Mock(spec=SplitSynchronizationTask)
        segment_task = mocker.Mock(spec=SegmentSynchronizationTask)
        segment_sync = mocker.Mock(spec=SegmentSynchronizer)
        split_synchronizers = SplitSynchronizers(mocker.Mock(), segment_sync, mocker.Mock(),
                                                 mocker.Mock(), mocker.Mock())
        split_tasks = SplitTasks(split_task, segment_task, mocker.Mock(), mocker.Mock(),
                                 mocker.Mock())
        synchronizer = Synchronizer(split_synchronizers, split_tasks)
        synchronizer.stop_periodic_fetching()

        assert len(split_task.stop.mock_calls) == 1
        assert len(segment_task.stop.mock_calls) == 1
        assert len(segment_sync.shutdown.mock_calls) == 0

    def test_start_periodic_data_recording(self, mocker):
        impression_task = mocker.Mock(spec=ImpressionsSyncTask)
        impression_count_task = mocker.Mock(spec=ImpressionsCountSyncTask)
        event_task = mocker.Mock(spec=EventsSyncTask)
        unique_keys_task = mocker.Mock(spec=UniqueKeysSyncTask)
        clear_filter_task = mocker.Mock(spec=ClearFilterSyncTask)
        split_tasks = SplitTasks(mocker.Mock(), mocker.Mock(), impression_task, event_task,
                                 impression_count_task, unique_keys_task, clear_filter_task)
        synchronizer = Synchronizer(mocker.Mock(spec=SplitSynchronizers), split_tasks)
        synchronizer.start_periodic_data_recording()

        assert len(impression_task.start.mock_calls) == 1
        assert len(impression_count_task.start.mock_calls) == 1
        assert len(event_task.start.mock_calls) == 1
        assert len(unique_keys_task.start.mock_calls) == 1
        assert len(clear_filter_task.start.mock_calls) == 1

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
        unique_keys_task = mocker.Mock(spec=UniqueKeysSyncTask)
        unique_keys_task.stop.side_effect = stop_mock
        clear_filter_task = mocker.Mock(spec=ClearFilterSyncTask)
        clear_filter_task.stop.side_effect = stop_mock
        split_tasks = SplitTasks(mocker.Mock(), mocker.Mock(), impression_task, event_task,
                                 impression_count_task, unique_keys_task, clear_filter_task)
        synchronizer = Synchronizer(mocker.Mock(spec=SplitSynchronizers), split_tasks)
        synchronizer.stop_periodic_data_recording(True)

        assert len(impression_task.stop.mock_calls) == 1
        assert len(impression_count_task.stop.mock_calls) == 1
        assert len(event_task.stop.mock_calls) == 1
        assert len(unique_keys_task.stop.mock_calls) == 1
        assert len(clear_filter_task.stop.mock_calls) == 1


    def test_shutdown(self, mocker):

        def stop_mock(event):
            event.set()
            return

        def stop_mock_2():
            return

        split_task = mocker.Mock(spec=SplitSynchronizationTask)
        split_task.stop.side_effect = stop_mock_2
        segment_task = mocker.Mock(spec=SegmentSynchronizationTask)
        segment_task.stop.side_effect = stop_mock_2
        impression_task = mocker.Mock(spec=ImpressionsSyncTask)
        impression_task.stop.side_effect = stop_mock
        impression_count_task = mocker.Mock(spec=ImpressionsCountSyncTask)
        impression_count_task.stop.side_effect = stop_mock
        event_task = mocker.Mock(spec=EventsSyncTask)
        event_task.stop.side_effect = stop_mock
        unique_keys_task = mocker.Mock(spec=UniqueKeysSyncTask)
        unique_keys_task.stop.side_effect = stop_mock
        clear_filter_task = mocker.Mock(spec=ClearFilterSyncTask)
        clear_filter_task.stop.side_effect = stop_mock

        segment_sync = mocker.Mock(spec=SegmentSynchronizer)

        split_synchronizers = SplitSynchronizers(mocker.Mock(), segment_sync, mocker.Mock(),
                                                 mocker.Mock(), mocker.Mock(), mocker.Mock())
        split_tasks = SplitTasks(split_task, segment_task, impression_task, event_task,
                                 impression_count_task, unique_keys_task, clear_filter_task)
        synchronizer = Synchronizer(split_synchronizers, split_tasks)
        synchronizer.shutdown(True)

        assert len(split_task.stop.mock_calls) == 1
        assert len(segment_task.stop.mock_calls) == 1
        assert len(segment_sync.shutdown.mock_calls) == 1
        assert len(impression_task.stop.mock_calls) == 1
        assert len(impression_count_task.stop.mock_calls) == 1
        assert len(event_task.stop.mock_calls) == 1
        assert len(unique_keys_task.stop.mock_calls) == 1
        assert len(clear_filter_task.stop.mock_calls) == 1

    def test_sync_all_ok(self, mocker):
        """Test that 3 attempts are done before failing."""
        split_synchronizers = mocker.Mock(spec=SplitSynchronizers)
        counts = {'splits': 0, 'segments': 0}

        def sync_splits(*_):
            """Sync Splits."""
            counts['splits'] += 1
            return []

        def sync_segments(*_):
            """Sync Segments."""
            counts['segments'] += 1
            return True

        split_synchronizers.split_sync.synchronize_splits.side_effect = sync_splits
        split_synchronizers.segment_sync.synchronize_segments.side_effect = sync_segments
        split_tasks = mocker.Mock(spec=SplitTasks)
        synchronizer = Synchronizer(split_synchronizers, split_tasks)

        synchronizer.sync_all()
        assert counts['splits'] == 1
        assert counts['segments'] == 1

    def test_sync_all_split_attempts(self, mocker):
        """Test that 3 attempts are done before failing."""
        split_synchronizers = mocker.Mock(spec=SplitSynchronizers)
        counts = {'splits': 0, 'segments': 0}
        def sync_splits(*_):
            """Sync Splits."""
            counts['splits'] += 1
            raise Exception('sarasa')

        split_synchronizers.split_sync.synchronize_splits.side_effect = sync_splits
        split_tasks = mocker.Mock(spec=SplitTasks)
        synchronizer = Synchronizer(split_synchronizers, split_tasks)

        synchronizer.sync_all()
        assert counts['splits'] == 3

    def test_sync_all_segment_attempts(self, mocker):
        """Test that segments don't trigger retries."""
        split_synchronizers = mocker.Mock(spec=SplitSynchronizers)
        counts = {'splits': 0, 'segments': 0}

        def sync_segments(*_):
            """Sync Splits."""

            counts['segments'] += 1
            return False

        split_synchronizers.segment_sync.synchronize_segments.side_effect = sync_segments
        split_tasks = mocker.Mock(spec=SplitTasks)
        synchronizer = Synchronizer(split_synchronizers, split_tasks)

        synchronizer._synchronize_segments()
        assert counts['segments'] == 1
