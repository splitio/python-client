"""Synchronizer tests."""
import unittest.mock as mock
import pytest

from splitio.sync.synchronizer import Synchronizer, SynchronizerAsync, SplitTasks, SplitSynchronizers, LocalhostSynchronizer, LocalhostSynchronizerAsync, RedisSynchronizer, RedisSynchronizerAsync
from splitio.tasks.split_sync import SplitSynchronizationTask, SplitSynchronizationTaskAsync
from splitio.tasks.unique_keys_sync import UniqueKeysSyncTask, ClearFilterSyncTask, UniqueKeysSyncTaskAsync, ClearFilterSyncTaskAsync
from splitio.tasks.segment_sync import SegmentSynchronizationTask, SegmentSynchronizationTaskAsync
from splitio.tasks.impressions_sync import ImpressionsSyncTask, ImpressionsCountSyncTask, ImpressionsCountSyncTaskAsync, ImpressionsSyncTaskAsync
from splitio.tasks.events_sync import EventsSyncTask, EventsSyncTaskAsync
from splitio.sync.split import SplitSynchronizer, SplitSynchronizerAsync, LocalSplitSynchronizer, LocalhostMode, LocalSplitSynchronizerAsync
from splitio.sync.segment import SegmentSynchronizer, SegmentSynchronizerAsync, LocalSegmentSynchronizer, LocalSegmentSynchronizerAsync
from splitio.sync.impression import ImpressionSynchronizer, ImpressionSynchronizerAsync, ImpressionsCountSynchronizer, ImpressionsCountSynchronizerAsync
from splitio.sync.event import EventSynchronizer, EventSynchronizerAsync
from splitio.storage import SegmentStorage, SplitStorage
from splitio.api import APIException, APIUriException
from splitio.models.splits import Split
from splitio.models.segments import Segment
from splitio.storage.inmemmory import InMemorySegmentStorage, InMemorySplitStorage, InMemorySegmentStorageAsync, InMemorySplitStorageAsync

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

class SynchronizerTests(object):
    def test_sync_all_failed_splits(self, mocker):
        api = mocker.Mock()
        storage = mocker.Mock()
        class flag_set_filter():
            def should_filter():
                return False
            def intersect(sets):
                return True
        storage.flag_set_filter = flag_set_filter
        storage.flag_set_filter.flag_sets = {}
        storage.flag_set_filter.sorted_flag_sets = []

        def run(x, c):
            raise APIException("something broke")
        api.fetch_splits.side_effect = run

        split_sync = SplitSynchronizer(api, storage)
        split_synchronizers = SplitSynchronizers(split_sync, mocker.Mock(), mocker.Mock(),
                                                 mocker.Mock(), mocker.Mock())
        sychronizer = Synchronizer(split_synchronizers, mocker.Mock(spec=SplitTasks))

        sychronizer.synchronize_splits(None)  # APIExceptions are handled locally and should not be propagated!

        # test forcing to have only one retry attempt and then exit
        sychronizer.sync_all(1)  # sync_all should not throw!

    def test_sync_all_failed_splits_with_flagsets(self, mocker):
        api = mocker.Mock()
        storage = mocker.Mock()
        class flag_set_filter():
            def should_filter():
                return False
            def intersect(sets):
                return True
        storage.flag_set_filter = flag_set_filter
        storage.flag_set_filter.flag_sets = {}
        storage.flag_set_filter.sorted_flag_sets = []

        def run(x, c):
            raise APIException("something broke", 414)
        api.fetch_splits.side_effect = run

        split_sync = SplitSynchronizer(api, storage)
        split_synchronizers = SplitSynchronizers(split_sync, mocker.Mock(), mocker.Mock(),
                                                 mocker.Mock(), mocker.Mock())
        synchronizer = Synchronizer(split_synchronizers, mocker.Mock(spec=SplitTasks))

        synchronizer.synchronize_splits(None)
        synchronizer.sync_all(3)
        assert synchronizer._backoff._attempt == 0

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

        sychronizer.sync_all(1)  # SyncAll should not throw!
        assert not sychronizer._synchronize_segments()

    def test_synchronize_splits(self, mocker):
        split_storage = InMemorySplitStorage()
        split_api = mocker.Mock()
        split_api.fetch_splits.return_value = {'splits': splits, 'since': 123,
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
        split_api.fetch_splits.return_value = {'splits': splits, 'since': 123,
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
        class flag_set_filter():
            def should_filter():
                return False
            def intersect(sets):
                return True
        split_storage.flag_set_filter = flag_set_filter
        split_storage.flag_set_filter.flag_sets = {}
        split_storage.flag_set_filter.sorted_flag_sets = []

        split_api = mocker.Mock()
        split_api.fetch_splits.return_value = {'splits': splits, 'since': 123,
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

        inserted_split = split_storage.update.mock_calls[0][1][0][0]
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

        synchronizer.sync_all(2)
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


class SynchronizerAsyncTests(object):

    @pytest.mark.asyncio
    async def test_sync_all_failed_splits(self, mocker):
        api = mocker.Mock()
        storage = mocker.Mock()
        class flag_set_filter():
            def should_filter():
                return False
            def intersect(sets):
                return True
        storage.flag_set_filter = flag_set_filter
        storage.flag_set_filter.flag_sets = {}
        storage.flag_set_filter.sorted_flag_sets = []

        async def run(x, c):
            raise APIException("something broke")
        api.fetch_splits = run

        async def get_change_number():
            return 1234
        storage.get_change_number = get_change_number

        split_sync = SplitSynchronizerAsync(api, storage)
        split_synchronizers = SplitSynchronizers(split_sync, mocker.Mock(), mocker.Mock(),
                                                 mocker.Mock(), mocker.Mock())
        sychronizer = SynchronizerAsync(split_synchronizers, mocker.Mock(spec=SplitTasks))

        await sychronizer.synchronize_splits(None)  # APIExceptions are handled locally and should not be propagated!

        # test forcing to have only one retry attempt and then exit
        await sychronizer.sync_all(1)  # sync_all should not throw!

    @pytest.mark.asyncio
    async def test_sync_all_failed_splits_with_flagsets(self, mocker):
        api = mocker.Mock()
        storage = mocker.Mock()
        class flag_set_filter():
            def should_filter():
                return False
            def intersect(sets):
                return True
        storage.flag_set_filter = flag_set_filter
        storage.flag_set_filter.flag_sets = {}
        storage.flag_set_filter.sorted_flag_sets = []

        async def get_change_number():
            pass
        storage.get_change_number = get_change_number

        async def run(x, c):
            raise APIException("something broke", 414)
        api.fetch_splits = run

        split_sync = SplitSynchronizerAsync(api, storage)
        split_synchronizers = SplitSynchronizers(split_sync, mocker.Mock(), mocker.Mock(),
                                                 mocker.Mock(), mocker.Mock())
        synchronizer = SynchronizerAsync(split_synchronizers, mocker.Mock(spec=SplitTasks))

        await synchronizer.synchronize_splits(None)  # APIExceptions are handled locally and should not be propagated!

        # test forcing to have only one retry attempt and then exit
        await synchronizer.sync_all(3)  # sync_all should not throw!
        assert synchronizer._backoff._attempt == 0

    @pytest.mark.asyncio
    async def test_sync_all_failed_segments(self, mocker):
        api = mocker.Mock()
        storage = mocker.Mock()
        split_storage = mocker.Mock(spec=SplitStorage)
        split_storage.get_segment_names.return_value = ['segmentA']
        split_sync = mocker.Mock(spec=SplitSynchronizer)
        split_sync.synchronize_splits.return_value = None

        async def run(x, y):
            raise APIException("something broke")
        api.fetch_segment = run

        async def get_segment_names():
            return ['seg']
        split_storage.get_segment_names = get_segment_names

        segment_sync = SegmentSynchronizerAsync(api, split_storage, storage)
        split_synchronizers = SplitSynchronizers(split_sync, segment_sync, mocker.Mock(),
                                                 mocker.Mock(), mocker.Mock())
        sychronizer = SynchronizerAsync(split_synchronizers, mocker.Mock(spec=SplitTasks))

        await sychronizer.sync_all(1)  # SyncAll should not throw!
        assert not await sychronizer._synchronize_segments()
        await segment_sync.shutdown()

    @pytest.mark.asyncio
    async def test_synchronize_splits(self, mocker):
        split_storage = InMemorySplitStorageAsync()
        split_api = mocker.Mock()

        async def fetch_splits(change, options):
            return {'splits': splits, 'since': 123,
                                               'till': 123}
        split_api.fetch_splits = fetch_splits

        split_sync = SplitSynchronizerAsync(split_api, split_storage)
        segment_storage = InMemorySegmentStorageAsync()
        segment_api = mocker.Mock()

        async def get_change_number():
            return 123
        split_storage.get_change_number = get_change_number

        async def fetch_segment(segment_name, change, options):
            return {'name': 'segmentA', 'added': ['key1', 'key2',
                                                  'key3'], 'removed': [], 'since': 123, 'till': 123}
        segment_api.fetch_segment = fetch_segment

        segment_sync = SegmentSynchronizerAsync(segment_api, split_storage, segment_storage)
        split_synchronizers = SplitSynchronizers(split_sync, segment_sync, mocker.Mock(),
                                                 mocker.Mock(), mocker.Mock())
        synchronizer = SynchronizerAsync(split_synchronizers, mocker.Mock(spec=SplitTasks))

        await synchronizer.synchronize_splits(123)

        inserted_split = await split_storage.get('some_name')
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == 'some_name'

        await segment_sync._jobs.await_completion()
        inserted_segment = await segment_storage.get('segmentA')
        assert inserted_segment.name == 'segmentA'
        assert inserted_segment.keys == {'key1', 'key2', 'key3'}

        await segment_sync.shutdown()

    @pytest.mark.asyncio
    async def test_synchronize_splits_calling_segment_sync_once(self, mocker):
        split_storage = InMemorySplitStorageAsync()
        async def get_change_number():
            return 123
        split_storage.get_change_number = get_change_number

        split_api = mocker.Mock()
        async def fetch_splits(change, options):
            return {'splits': splits, 'since': 123,
                                               'till': 123}
        split_api.fetch_splits = fetch_splits

        split_sync = SplitSynchronizerAsync(split_api, split_storage)
        counts = {'segments': 0}

        segment_sync = mocker.Mock()
        async def sync_segments(*_):
            """Sync Segments."""
            counts['segments'] += 1
            return True
        segment_sync.synchronize_segments = sync_segments

        async def segment_exist_in_storage(segment):
            return False
        segment_sync.segment_exist_in_storage = segment_exist_in_storage

        split_synchronizers = SplitSynchronizers(split_sync, segment_sync, mocker.Mock(),
                                                 mocker.Mock(), mocker.Mock())
        synchronizer = SynchronizerAsync(split_synchronizers, mocker.Mock(spec=SplitTasks))
        await synchronizer.synchronize_splits(123, True)

        assert counts['segments'] == 1

    @pytest.mark.asyncio
    async def test_sync_all(self, mocker):
        split_storage = InMemorySplitStorageAsync()
        async def get_change_number():
            return 123
        split_storage.get_change_number = get_change_number

        self.added_split = None
        async def update(split, deleted, change_number):
            if len(split) > 0:
                self.added_split = split
        split_storage.update = update

        async def get_segment_names():
            return ['segmentA']
        split_storage.get_segment_names = get_segment_names

        class flag_set_filter():
            def should_filter():
                return False
            def intersect(sets):
                return True
        split_storage.flag_set_filter = flag_set_filter
        split_storage.flag_set_filter.flag_sets = {}
        split_storage.flag_set_filter.sorted_flag_sets = []

        split_api = mocker.Mock()
        async def fetch_splits(change, options):
            return {'splits': splits, 'since': 123, 'till': 123}
        split_api.fetch_splits = fetch_splits

        split_sync = SplitSynchronizerAsync(split_api, split_storage)
        segment_storage = InMemorySegmentStorageAsync()
        async def get_change_number(segment):
            return 123
        segment_storage.get_change_number = get_change_number

        self.inserted_segment = []
        async def update(segment, added, removed, till):
            self.inserted_segment.append(segment)
            self.inserted_segment.append(added)
            self.inserted_segment.append(removed)
        segment_storage.update = update

        segment_api = mocker.Mock()
        async def fetch_segment(segment_name, change, options):
            return {'name': 'segmentA', 'added': ['key1', 'key2', 'key3'],
                    'removed': [], 'since': 123, 'till': 123}
        segment_api.fetch_segment = fetch_segment

        segment_sync = SegmentSynchronizerAsync(segment_api, split_storage, segment_storage)
        split_synchronizers = SplitSynchronizers(split_sync, segment_sync, mocker.Mock(),
                                                 mocker.Mock(), mocker.Mock())
        synchronizer = SynchronizerAsync(split_synchronizers, mocker.Mock(spec=SplitTasks))
        await synchronizer.sync_all()
        await segment_sync._jobs.await_completion()

        assert isinstance(self.added_split[0], Split)
        assert self.added_split[0].name == 'some_name'

        assert self.inserted_segment[0] == 'segmentA'
        assert self.inserted_segment[1] == ['key1', 'key2', 'key3']
        assert self.inserted_segment[2] == []

    @pytest.mark.asyncio
    async def test_start_periodic_fetching(self, mocker):
        split_task = mocker.Mock(spec=SplitSynchronizationTask)
        segment_task = mocker.Mock(spec=SegmentSynchronizationTask)
        split_tasks = SplitTasks(split_task, segment_task, mocker.Mock(), mocker.Mock(),
                                 mocker.Mock())
        synchronizer = SynchronizerAsync(mocker.Mock(spec=SplitSynchronizers), split_tasks)
        synchronizer.start_periodic_fetching()

        assert len(split_task.start.mock_calls) == 1
        assert len(segment_task.start.mock_calls) == 1

    @pytest.mark.asyncio
    async def test_stop_periodic_fetching(self, mocker):
        split_task = mocker.Mock(spec=SplitSynchronizationTaskAsync)
        segment_task = mocker.Mock(spec=SegmentSynchronizationTaskAsync)
        segment_sync = mocker.Mock(spec=SegmentSynchronizerAsync)
        split_synchronizers = SplitSynchronizers(mocker.Mock(), segment_sync, mocker.Mock(),
                                                 mocker.Mock(), mocker.Mock())
        split_tasks = SplitTasks(split_task, segment_task, mocker.Mock(), mocker.Mock(),
                                 mocker.Mock())
        synchronizer = SynchronizerAsync(split_synchronizers, split_tasks)
        self.split_task_stopped = 0
        async def stop_split():
            self.split_task_stopped += 1
        split_task.stop = stop_split

        self.segment_task_stopped = 0
        async def stop_segment():
            self.segment_task_stopped += 1
        segment_task.stop = stop_segment

        self.segment_sync_stopped = 0
        async def shutdown():
            self.segment_sync_stopped += 1
        segment_sync.shutdown = shutdown

        await synchronizer.stop_periodic_fetching()

        assert self.split_task_stopped == 1
        assert self.segment_task_stopped == 1
        assert self.segment_sync_stopped == 0

    def test_start_periodic_data_recording(self, mocker):
        impression_task = mocker.Mock(spec=ImpressionsSyncTaskAsync)
        impression_count_task = mocker.Mock(spec=ImpressionsCountSyncTaskAsync)
        event_task = mocker.Mock(spec=EventsSyncTaskAsync)
        unique_keys_task = mocker.Mock(spec=UniqueKeysSyncTaskAsync)
        clear_filter_task = mocker.Mock(spec=ClearFilterSyncTaskAsync)
        split_tasks = SplitTasks(None, None, impression_task, event_task,
                                 impression_count_task, unique_keys_task, clear_filter_task)
        synchronizer = SynchronizerAsync(mocker.Mock(spec=SplitSynchronizers), split_tasks)
        synchronizer.start_periodic_data_recording()

        assert len(impression_task.start.mock_calls) == 1
        assert len(impression_count_task.start.mock_calls) == 1
        assert len(event_task.start.mock_calls) == 1

class RedisSynchronizerTests(object):
    def test_start_periodic_data_recording(self, mocker):
        impression_count_task = mocker.Mock(spec=ImpressionsCountSyncTask)
        unique_keys_task = mocker.Mock(spec=UniqueKeysSyncTask)
        clear_filter_task = mocker.Mock(spec=ClearFilterSyncTask)
        split_tasks = SplitTasks(None, None, None, None,
            impression_count_task,
            None,
            unique_keys_task,
            clear_filter_task
        )
        synchronizer = RedisSynchronizer(mocker.Mock(spec=SplitSynchronizers), split_tasks)
        synchronizer.start_periodic_data_recording()

        assert len(impression_count_task.start.mock_calls) == 1
        assert len(unique_keys_task.start.mock_calls) == 1
        assert len(clear_filter_task.start.mock_calls) == 1

    def test_stop_periodic_data_recording(self, mocker):

        def stop_mock(event):
            event.set()
            return

        impression_count_task = mocker.Mock(spec=ImpressionsCountSyncTask)
        impression_count_task.stop.side_effect = stop_mock
        unique_keys_task = mocker.Mock(spec=UniqueKeysSyncTask)
        unique_keys_task.stop.side_effect = stop_mock
        clear_filter_task = mocker.Mock(spec=ClearFilterSyncTask)
        clear_filter_task.stop.side_effect = stop_mock

        split_tasks = SplitTasks(None, None, None, None,
            impression_count_task,
            None,
            unique_keys_task,
            clear_filter_task
        )
        synchronizer = RedisSynchronizer(mocker.Mock(spec=SplitSynchronizers), split_tasks)
        synchronizer.stop_periodic_data_recording(True)

        assert len(impression_count_task.stop.mock_calls) == 1
        assert len(unique_keys_task.stop.mock_calls) == 1
        assert len(clear_filter_task.stop.mock_calls) == 1

    def test_shutdown(self, mocker):

        def stop_mock(event):
            event.set()
            return

        impression_count_task = mocker.Mock(spec=ImpressionsCountSyncTask)
        impression_count_task.stop.side_effect = stop_mock
        unique_keys_task = mocker.Mock(spec=UniqueKeysSyncTask)
        unique_keys_task.stop.side_effect = stop_mock
        clear_filter_task = mocker.Mock(spec=ClearFilterSyncTask)
        clear_filter_task.stop.side_effect = stop_mock

        segment_sync = mocker.Mock(spec=SegmentSynchronizer)

        split_tasks = SplitTasks(None, None, None, None,
            impression_count_task,
            None,
            unique_keys_task,
            clear_filter_task
        )
        synchronizer = RedisSynchronizer(mocker.Mock(spec=SplitSynchronizers), split_tasks)
        synchronizer.shutdown(True)

        assert len(impression_count_task.stop.mock_calls) == 1
        assert len(unique_keys_task.stop.mock_calls) == 1
        assert len(clear_filter_task.stop.mock_calls) == 1

class RedisSynchronizerAsyncTests(object):
    @pytest.mark.asyncio
    async def test_start_periodic_data_recording(self, mocker):
        impression_count_task = mocker.Mock(spec=ImpressionsCountSyncTaskAsync)
        unique_keys_task = mocker.Mock(spec=UniqueKeysSyncTaskAsync)
        clear_filter_task = mocker.Mock(spec=ClearFilterSyncTaskAsync)
        split_tasks = SplitTasks(None, None, None, None,
            impression_count_task,
            None,
            unique_keys_task,
            clear_filter_task
        )
        synchronizer = RedisSynchronizerAsync(mocker.Mock(spec=SplitSynchronizers), split_tasks)
        synchronizer.start_periodic_data_recording()

        assert len(impression_count_task.start.mock_calls) == 1
        assert len(unique_keys_task.start.mock_calls) == 1
        assert len(clear_filter_task.start.mock_calls) == 1

    @pytest.mark.asyncio
    async def test_stop_periodic_data_recording(self, mocker):
        impression_task = mocker.Mock(spec=ImpressionsSyncTaskAsync)
        self.stop_imp_calls = 0
        async def stop_imp(arg=None):
            self.stop_imp_calls += 1
            return
        impression_task.stop = stop_imp

        impression_count_task = mocker.Mock(spec=ImpressionsCountSyncTaskAsync)
        self.stop_imp_count_calls = 0
        async def stop_imp_count(arg=None):
            self.stop_imp_count_calls += 1
            return
        impression_count_task.stop = stop_imp_count

        event_task = mocker.Mock(spec=EventsSyncTaskAsync)
        self.stop_event_calls = 0
        async def stop_event(arg=None):
            self.stop_event_calls += 1
            return
        event_task.stop = stop_event

        unique_keys_task = mocker.Mock(spec=UniqueKeysSyncTaskAsync)
        self.stop_unique_keys_calls = 0
        async def stop_unique_keys(arg=None):
            self.stop_unique_keys_calls += 1
            return
        unique_keys_task.stop = stop_unique_keys

        clear_filter_task = mocker.Mock(spec=ClearFilterSyncTaskAsync)
        self.stop_clear_filter_calls = 0
        async def stop_clear_filter(arg=None):
            self.stop_clear_filter_calls += 1
            return
        clear_filter_task.stop = stop_clear_filter

        split_tasks = SplitTasks(mocker.Mock(), mocker.Mock(), impression_task, event_task,
                                 impression_count_task, unique_keys_task, clear_filter_task)
        synchronizer = SynchronizerAsync(mocker.Mock(spec=SplitSynchronizers), split_tasks)
        await synchronizer.stop_periodic_data_recording(True)

        assert self.stop_imp_count_calls == 1
        assert self.stop_imp_calls == 1
        assert self.stop_event_calls == 1
        assert self.stop_unique_keys_calls == 1
        assert self.stop_clear_filter_calls == 1

    @pytest.mark.asyncio
    async def test_shutdown(self, mocker):
        split_task = mocker.Mock(spec=SplitSynchronizationTask)
        self.split_task_stopped = 0
        async def stop_split():
            self.split_task_stopped += 1
        split_task.stop = stop_split

        segment_task = mocker.Mock(spec=SegmentSynchronizationTask)
        self.segment_task_stopped = 0
        async def stop_segment():
            self.segment_task_stopped += 1
        segment_task.stop = stop_segment

        impression_task = mocker.Mock(spec=ImpressionsSyncTaskAsync)
        self.stop_imp_calls = 0
        async def stop_imp(arg=None):
            self.stop_imp_calls += 1
            return
        impression_task.stop = stop_imp

        impression_count_task = mocker.Mock(spec=ImpressionsCountSyncTaskAsync)
        self.stop_imp_count_calls = 0
        async def stop_imp_count(arg=None):
            self.stop_imp_count_calls += 1
            return
        impression_count_task.stop = stop_imp_count

        event_task = mocker.Mock(spec=EventsSyncTaskAsync)
        self.stop_event_calls = 0
        async def stop_event(arg=None):
            self.stop_event_calls += 1
            return
        event_task.stop = stop_event

        unique_keys_task = mocker.Mock(spec=UniqueKeysSyncTaskAsync)
        self.stop_unique_keys_calls = 0
        async def stop_unique_keys(arg=None):
            self.stop_unique_keys_calls += 1
            return
        unique_keys_task.stop = stop_unique_keys

        clear_filter_task = mocker.Mock(spec=ClearFilterSyncTaskAsync)
        self.stop_clear_filter_calls = 0
        async def stop_clear_filter(arg=None):
            self.stop_clear_filter_calls += 1
            return
        clear_filter_task.stop = stop_clear_filter

        segment_sync = mocker.Mock(spec=SegmentSynchronizerAsync)
        self.segment_sync_stopped = 0
        async def shutdown():
            self.segment_sync_stopped += 1
        segment_sync.shutdown = shutdown

        split_synchronizers = SplitSynchronizers(mocker.Mock(), segment_sync, mocker.Mock(),
                                                 mocker.Mock(), mocker.Mock(), mocker.Mock())
        split_tasks = SplitTasks(split_task, segment_task, impression_task, event_task,
                                 impression_count_task, unique_keys_task, clear_filter_task)
        synchronizer = SynchronizerAsync(split_synchronizers, split_tasks)
        await synchronizer.shutdown(True)

        assert self.split_task_stopped == 1
        assert self.segment_task_stopped == 1
        assert self.segment_sync_stopped == 1
        assert self.stop_imp_count_calls == 1
        assert self.stop_imp_calls == 1
        assert self.stop_event_calls == 1
        assert self.stop_unique_keys_calls == 1
        assert self.stop_clear_filter_calls == 1

    @pytest.mark.asyncio
    async def test_sync_all_ok(self, mocker):
        """Test that 3 attempts are done before failing."""
        split_synchronizers = mocker.Mock(spec=SplitSynchronizers)
        counts = {'splits': 0, 'segments': 0}

        async def sync_splits(*_):
            """Sync Splits."""
            counts['splits'] += 1
            return []

        async def sync_segments(*_):
            """Sync Segments."""
            counts['segments'] += 1
            return True

        split_synchronizers.split_sync.synchronize_splits = sync_splits
        split_synchronizers.segment_sync.synchronize_segments = sync_segments
        split_tasks = mocker.Mock(spec=SplitTasks)
        synchronizer = SynchronizerAsync(split_synchronizers, split_tasks)

        await synchronizer.sync_all()
        assert counts['splits'] == 1
        assert counts['segments'] == 1

    @pytest.mark.asyncio
    async def test_sync_all_split_attempts(self, mocker):
        """Test that 3 attempts are done before failing."""
        split_synchronizers = mocker.Mock(spec=SplitSynchronizers)
        counts = {'splits': 0, 'segments': 0}
        async def sync_splits(*_):
            """Sync Splits."""
            counts['splits'] += 1
            raise Exception('sarasa')

        split_synchronizers.split_sync.synchronize_splits = sync_splits
        split_tasks = mocker.Mock(spec=SplitTasks)
        synchronizer = SynchronizerAsync(split_synchronizers, split_tasks)

        await synchronizer.sync_all(2)
        assert counts['splits'] == 3

    @pytest.mark.asyncio
    async def test_sync_all_segment_attempts(self, mocker):
        """Test that segments don't trigger retries."""
        split_synchronizers = mocker.Mock(spec=SplitSynchronizers)
        counts = {'splits': 0, 'segments': 0}

        async def sync_segments(*_):
            """Sync Segments."""
            counts['segments'] += 1
            return False
        split_synchronizers.segment_sync.synchronize_segments = sync_segments

        split_tasks = mocker.Mock(spec=SplitTasks)
        synchronizer = SynchronizerAsync(split_synchronizers, split_tasks)

        await synchronizer._synchronize_segments()
        assert counts['segments'] == 1

        impression_count_task = mocker.Mock(spec=ImpressionsCountSyncTaskAsync)
        self.imp_count_calls = 0
        async def imp_count_stop_mock():
            self.imp_count_calls += 1
        impression_count_task.stop = imp_count_stop_mock

        unique_keys_task = mocker.Mock(spec=UniqueKeysSyncTaskAsync)
        self.unique_keys_calls = 0
        async def unique_keys_stop_mock():
            self.unique_keys_calls += 1
        unique_keys_task.stop = unique_keys_stop_mock

        clear_filter_task = mocker.Mock(spec=ClearFilterSyncTaskAsync)
        self.clear_filter_calls = 0
        async def clear_filter_stop_mock():
            self.clear_filter_calls += 1
        clear_filter_task.stop = clear_filter_stop_mock

        split_tasks = SplitTasks(None, None, None, None,
            impression_count_task,
            None,
            unique_keys_task,
            clear_filter_task
        )
        synchronizer = RedisSynchronizerAsync(mocker.Mock(spec=SplitSynchronizers), split_tasks)
        await synchronizer.stop_periodic_data_recording(True)

        assert self.imp_count_calls == 1
        assert self.unique_keys_calls == 1
        assert self.clear_filter_calls == 1

    def test_shutdown(self, mocker):

        def stop_mock(event):
            event.set()
            return

        impression_count_task = mocker.Mock(spec=ImpressionsCountSyncTask)
        impression_count_task.stop.side_effect = stop_mock
        unique_keys_task = mocker.Mock(spec=UniqueKeysSyncTask)
        unique_keys_task.stop.side_effect = stop_mock
        clear_filter_task = mocker.Mock(spec=ClearFilterSyncTask)
        clear_filter_task.stop.side_effect = stop_mock

        segment_sync = mocker.Mock(spec=SegmentSynchronizer)

        split_tasks = SplitTasks(None, None, None, None,
            impression_count_task,
            None,
            unique_keys_task,
            clear_filter_task
        )
        synchronizer = RedisSynchronizer(mocker.Mock(spec=SplitSynchronizers), split_tasks)
        synchronizer.shutdown(True)

        assert len(impression_count_task.stop.mock_calls) == 1
        assert len(unique_keys_task.stop.mock_calls) == 1
        assert len(clear_filter_task.stop.mock_calls) == 1


class LocalhostSynchronizerTests(object):

    @mock.patch('splitio.sync.segment.LocalSegmentSynchronizer.synchronize_segments')
    def test_synchronize_splits(self, mocker):
        split_sync = LocalSplitSynchronizer(mocker.Mock(), mocker.Mock(), mocker.Mock())
        segment_sync = LocalSegmentSynchronizer(mocker.Mock(), mocker.Mock(), mocker.Mock())
        synchronizers = SplitSynchronizers(split_sync, segment_sync, None, None, None)
        local_synchronizer = LocalhostSynchronizer(synchronizers, mocker.Mock(), mocker.Mock())

        def synchronize_splits(*args, **kwargs):
            return ["segmentA", "segmentB"]
        split_sync.synchronize_splits = synchronize_splits

        def segment_exist_in_storage(*args, **kwargs):
            return False
        segment_sync.segment_exist_in_storage = segment_exist_in_storage

        assert(local_synchronizer.synchronize_splits())
        assert(mocker.called)

    def test_start_and_stop_tasks(self, mocker):
        synchronizers = SplitSynchronizers(
            LocalSplitSynchronizer(mocker.Mock(), mocker.Mock(), mocker.Mock()),
            LocalSegmentSynchronizer(mocker.Mock(), mocker.Mock(), mocker.Mock()), None, None, None)
        split_task = SplitSynchronizationTask(synchronizers.split_sync.synchronize_splits, 30)
        segment_task = SegmentSynchronizationTask(synchronizers.segment_sync.synchronize_segments, 30)
        tasks = SplitTasks(split_task, segment_task, None, None, None,)

        self.split_task_start_called = False
        def split_task_start(*args, **kwargs):
            self.split_task_start_called = True
        split_task.start = split_task_start

        self.segment_task_start_called = False
        def segment_task_start(*args, **kwargs):
            self.segment_task_start_called = True
        segment_task.start = segment_task_start

        self.split_task_stop_called = False
        def split_task_stop(*args, **kwargs):
            self.split_task_stop_called = True
        split_task.stop = split_task_stop

        self.segment_task_stop_called = False
        def segment_task_stop(*args, **kwargs):
            self.segment_task_stop_called = True
        segment_task.stop = segment_task_stop

        local_synchronizer = LocalhostSynchronizer(synchronizers, tasks, LocalhostMode.JSON)
        local_synchronizer.start_periodic_fetching()
        assert(self.split_task_start_called)
        assert(self.segment_task_start_called)

        local_synchronizer.stop_periodic_fetching()
        assert(self.split_task_stop_called)
        assert(self.segment_task_stop_called)


class LocalhostSynchronizerAsyncTests(object):

    @pytest.mark.asyncio
    async def test_synchronize_splits(self, mocker):
        split_sync = LocalSplitSynchronizerAsync(mocker.Mock(), mocker.Mock(), mocker.Mock())
        segment_sync = LocalSegmentSynchronizerAsync(mocker.Mock(), mocker.Mock(), mocker.Mock())
        synchronizers = SplitSynchronizers(split_sync, segment_sync, None, None, None)
        local_synchronizer = LocalhostSynchronizerAsync(synchronizers, mocker.Mock(), mocker.Mock())

        self.called = False
        async def synchronize_segments(*args):
            self.called = True
        segment_sync.synchronize_segments = synchronize_segments

        async def synchronize_splits(*args, **kwargs):
            return ["segmentA", "segmentB"]
        split_sync.synchronize_splits = synchronize_splits

        async def segment_exist_in_storage(*args, **kwargs):
            return False
        segment_sync.segment_exist_in_storage = segment_exist_in_storage

        assert(await local_synchronizer.synchronize_splits())
        assert(self.called)

    @pytest.mark.asyncio
    async def test_start_and_stop_tasks(self, mocker):
        synchronizers = SplitSynchronizers(
            LocalSplitSynchronizerAsync(mocker.Mock(), mocker.Mock(), mocker.Mock()),
            LocalSegmentSynchronizerAsync(mocker.Mock(), mocker.Mock(), mocker.Mock()), None, None, None)
        split_task = SplitSynchronizationTaskAsync(synchronizers.split_sync.synchronize_splits, 30)
        segment_task = SegmentSynchronizationTaskAsync(synchronizers.segment_sync.synchronize_segments, 30)
        tasks = SplitTasks(split_task, segment_task, None, None, None,)

        self.split_task_start_called = False
        def split_task_start(*args, **kwargs):
            self.split_task_start_called = True
        split_task.start = split_task_start

        self.segment_task_start_called = False
        def segment_task_start(*args, **kwargs):
            self.segment_task_start_called = True
        segment_task.start = segment_task_start

        self.split_task_stop_called = False
        async def split_task_stop(*args, **kwargs):
            self.split_task_stop_called = True
        split_task.stop = split_task_stop

        self.segment_task_stop_called = False
        async def segment_task_stop(*args, **kwargs):
            self.segment_task_stop_called = True
        segment_task.stop = segment_task_stop

        local_synchronizer = LocalhostSynchronizerAsync(synchronizers, tasks, LocalhostMode.JSON)
        local_synchronizer.start_periodic_fetching()
        assert(self.split_task_start_called)
        assert(self.segment_task_start_called)

        await local_synchronizer.stop_periodic_fetching()
        assert(self.split_task_stop_called)
        assert(self.segment_task_stop_called)
