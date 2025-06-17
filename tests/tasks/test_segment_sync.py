"""Split syncrhonization task test module."""

import threading
import time
import pytest

from splitio.api.commons import FetchOptions
from splitio.tasks import segment_sync
from splitio.storage import SegmentStorage, SplitStorage, RuleBasedSegmentsStorage
from splitio.models.splits import Split
from splitio.models.segments import Segment
from splitio.models.grammar.condition import Condition
from splitio.models.grammar.matchers import UserDefinedSegmentMatcher
from splitio.sync.segment import SegmentSynchronizer, SegmentSynchronizerAsync
from splitio.optional.loaders import asyncio

class SegmentSynchronizationTests(object):
    """Split synchronization task test cases."""

    def test_normal_operation(self, mocker):
        """Test the normal operation flow."""
        split_storage = mocker.Mock(spec=SplitStorage)
        split_storage.get_segment_names.return_value = ['segmentA', 'segmentB', 'segmentC']
        rbs_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)
        rbs_storage.get_segment_names.return_value = []

        # Setup a mocked segment storage whose changenumber returns -1 on first fetch and
        # 123 afterwards.
        storage = mocker.Mock(spec=SegmentStorage)

        def change_number_mock(segment_name):
            if segment_name == 'segmentA' and change_number_mock._count_a == 0:
                change_number_mock._count_a = 1
                return -1
            if segment_name == 'segmentB' and change_number_mock._count_b == 0:
                change_number_mock._count_b = 1
                return -1
            if segment_name == 'segmentC' and change_number_mock._count_c == 0:
                change_number_mock._count_c = 1
                return -1
            return 123
        change_number_mock._count_a = 0
        change_number_mock._count_b = 0
        change_number_mock._count_c = 0
        storage.get_change_number.side_effect = change_number_mock

        # Setup a mocked segment api to return segments mentioned before.
        def fetch_segment_mock(segment_name, change_number, fetch_options):
            if segment_name == 'segmentA' and fetch_segment_mock._count_a == 0:
                fetch_segment_mock._count_a = 1
                return {'name': 'segmentA', 'added': ['key1', 'key2', 'key3'], 'removed': [],
                        'since': -1, 'till': 123}
            if segment_name == 'segmentB' and fetch_segment_mock._count_b == 0:
                fetch_segment_mock._count_b = 1
                return {'name': 'segmentB', 'added': ['key4', 'key5', 'key6'], 'removed': [],
                        'since': -1, 'till': 123}
            if segment_name == 'segmentC' and fetch_segment_mock._count_c == 0:
                fetch_segment_mock._count_c = 1
                return {'name': 'segmentC', 'added': ['key7', 'key8', 'key9'], 'removed': [],
                        'since': -1, 'till': 123}
            return {'added': [], 'removed': [], 'since': 123, 'till': 123}
        fetch_segment_mock._count_a = 0
        fetch_segment_mock._count_b = 0
        fetch_segment_mock._count_c = 0

        api = mocker.Mock()
        fetch_options = FetchOptions(True, None, None, None, None)
        api.fetch_segment.side_effect = fetch_segment_mock

        segments_synchronizer = SegmentSynchronizer(api, split_storage, storage, rbs_storage)
        task = segment_sync.SegmentSynchronizationTask(segments_synchronizer.synchronize_segments,
                                                       0.5)
        task.start()
        time.sleep(0.7)

        assert task.is_running()

        stop_event = threading.Event()
        task.stop(stop_event)
        stop_event.wait()
        assert not task.is_running()

        api_calls = [call for call in api.fetch_segment.mock_calls]
        assert mocker.call('segmentA', -1, fetch_options) in api_calls
        assert mocker.call('segmentB', -1, fetch_options) in api_calls
        assert mocker.call('segmentC', -1, fetch_options) in api_calls
        assert mocker.call('segmentA', 123, fetch_options) in api_calls
        assert mocker.call('segmentB', 123, fetch_options) in api_calls
        assert mocker.call('segmentC', 123, fetch_options) in api_calls

        segment_put_calls = storage.put.mock_calls
        segments_to_validate = set(['segmentA', 'segmentB', 'segmentC'])
        for call in segment_put_calls:
            _, positional_args, _ = call
            segment = positional_args[0]
            assert isinstance(segment, Segment)
            assert segment.name in segments_to_validate
            segments_to_validate.remove(segment.name)

    def test_that_errors_dont_stop_task(self, mocker):
        """Test that if fetching segments fails at some_point, the task will continue running."""
        split_storage = mocker.Mock(spec=SplitStorage)
        split_storage.get_segment_names.return_value = ['segmentA', 'segmentB', 'segmentC']
        rbs_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)
        rbs_storage.get_segment_names.return_value = []

        # Setup a mocked segment storage whose changenumber returns -1 on first fetch and
        # 123 afterwards.
        storage = mocker.Mock(spec=SegmentStorage)

        def change_number_mock(segment_name):
            if segment_name == 'segmentA' and change_number_mock._count_a == 0:
                change_number_mock._count_a = 1
                return -1
            if segment_name == 'segmentB' and change_number_mock._count_b == 0:
                change_number_mock._count_b = 1
                return -1
            if segment_name == 'segmentC' and change_number_mock._count_c == 0:
                change_number_mock._count_c = 1
                return -1
            return 123
        change_number_mock._count_a = 0
        change_number_mock._count_b = 0
        change_number_mock._count_c = 0
        storage.get_change_number.side_effect = change_number_mock

        # Setup a mocked segment api to return segments mentioned before.
        def fetch_segment_mock(segment_name, change_number, fetch_options):
            if segment_name == 'segmentA' and fetch_segment_mock._count_a == 0:
                fetch_segment_mock._count_a = 1
                return {'name': 'segmentA', 'added': ['key1', 'key2', 'key3'], 'removed': [],
                        'since': -1, 'till': 123}
            if segment_name == 'segmentB' and fetch_segment_mock._count_b == 0:
                fetch_segment_mock._count_b = 1
                raise Exception("some exception")
            if segment_name == 'segmentC' and fetch_segment_mock._count_c == 0:
                fetch_segment_mock._count_c = 1
                return {'name': 'segmentC', 'added': ['key7', 'key8', 'key9'], 'removed': [],
                        'since': -1, 'till': 123}
            return {'added': [], 'removed': [], 'since': 123, 'till': 123}
        fetch_segment_mock._count_a = 0
        fetch_segment_mock._count_b = 0
        fetch_segment_mock._count_c = 0

        api = mocker.Mock()
        fetch_options = FetchOptions(True, None, None, None, None)
        api.fetch_segment.side_effect = fetch_segment_mock

        segments_synchronizer = SegmentSynchronizer(api, split_storage, storage, rbs_storage)
        task = segment_sync.SegmentSynchronizationTask(segments_synchronizer.synchronize_segments,
                                                       0.5)
        task.start()
        time.sleep(0.7)

        assert task.is_running()

        stop_event = threading.Event()
        task.stop(stop_event)
        stop_event.wait()
        assert not task.is_running()

        api_calls = [call for call in api.fetch_segment.mock_calls]
        assert mocker.call('segmentA', -1, fetch_options) in api_calls
        assert mocker.call('segmentB', -1, fetch_options) in api_calls
        assert mocker.call('segmentC', -1, fetch_options) in api_calls
        assert mocker.call('segmentA', 123, fetch_options) in api_calls
        assert mocker.call('segmentC', 123, fetch_options) in api_calls

        segment_put_calls = storage.put.mock_calls
        segments_to_validate = set(['segmentA', 'segmentB', 'segmentC'])
        for call in segment_put_calls:
            _, positional_args, _ = call
            segment = positional_args[0]
            assert isinstance(segment, Segment)
            assert segment.name in segments_to_validate
            segments_to_validate.remove(segment.name)


class SegmentSynchronizationAsyncTests(object):
    """Split synchronization async task test cases."""

    @pytest.mark.asyncio
    async def test_normal_operation(self, mocker):
        """Test the normal operation flow."""
        split_storage = mocker.Mock(spec=SplitStorage)
        async def get_segment_names():
            return ['segmentA', 'segmentB', 'segmentC']
        split_storage.get_segment_names = get_segment_names

        rbs_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)
        async def get_segment_names_rbs():
            return []
        rbs_storage.get_segment_names = get_segment_names_rbs

        # Setup a mocked segment storage whose changenumber returns -1 on first fetch and
        # 123 afterwards.
        storage = mocker.Mock(spec=SegmentStorage)

        async def change_number_mock(segment_name):
            if segment_name == 'segmentA' and change_number_mock._count_a == 0:
                change_number_mock._count_a = 1
                return -1
            if segment_name == 'segmentB' and change_number_mock._count_b == 0:
                change_number_mock._count_b = 1
                return -1
            if segment_name == 'segmentC' and change_number_mock._count_c == 0:
                change_number_mock._count_c = 1
                return -1
            return 123
        change_number_mock._count_a = 0
        change_number_mock._count_b = 0
        change_number_mock._count_c = 0
        storage.get_change_number = change_number_mock

        self.segments = []
        async def put(segment):
            self.segments.append(segment)
        storage.put = put

        async def update(*arg):
            pass
        storage.update = update

        # Setup a mocked segment api to return segments mentioned before.
        self.segment_name = []
        self.change_number = []
        self.fetch_options = []
        async def fetch_segment_mock(segment_name, change_number, fetch_options):
            self.segment_name.append(segment_name)
            self.change_number.append(change_number)
            self.fetch_options.append(fetch_options)
            if segment_name == 'segmentA' and fetch_segment_mock._count_a == 0:
                fetch_segment_mock._count_a = 1
                return {'name': 'segmentA', 'added': ['key1', 'key2', 'key3'], 'removed': [],
                        'since': -1, 'till': 123}
            if segment_name == 'segmentB' and fetch_segment_mock._count_b == 0:
                fetch_segment_mock._count_b = 1
                return {'name': 'segmentB', 'added': ['key4', 'key5', 'key6'], 'removed': [],
                        'since': -1, 'till': 123}
            if segment_name == 'segmentC' and fetch_segment_mock._count_c == 0:
                fetch_segment_mock._count_c = 1
                return {'name': 'segmentC', 'added': ['key7', 'key8', 'key9'], 'removed': [],
                        'since': -1, 'till': 123}
            return {'added': [], 'removed': [], 'since': 123, 'till': 123}
        fetch_segment_mock._count_a = 0
        fetch_segment_mock._count_b = 0
        fetch_segment_mock._count_c = 0

        api = mocker.Mock()
        fetch_options = FetchOptions(True, None, None, None, None)
        api.fetch_segment = fetch_segment_mock

        segments_synchronizer = SegmentSynchronizerAsync(api, split_storage, storage, rbs_storage)
        task = segment_sync.SegmentSynchronizationTaskAsync(segments_synchronizer.synchronize_segments,
                                                       0.5)
        task.start()
        await asyncio.sleep(0.7)
        assert task.is_running()

        await task.stop()
        assert not task.is_running()

        api_calls = []
        for i in range(6):
            api_calls.append((self.segment_name[i], self.change_number[i], self.fetch_options[i]))
        
        assert ('segmentA', -1, FetchOptions(True, None, None, None, None)) in api_calls
        assert ('segmentA', 123, FetchOptions(True, None, None, None, None)) in api_calls
        assert ('segmentB', -1, FetchOptions(True, None, None, None, None)) in api_calls
        assert ('segmentB', 123, FetchOptions(True, None, None, None, None)) in api_calls
        assert ('segmentC', -1, FetchOptions(True, None, None, None, None)) in api_calls
        assert ('segmentC', 123, FetchOptions(True, None, None, None, None)) in api_calls

        segments_to_validate = set(['segmentA', 'segmentB', 'segmentC'])
        for segment in self.segments:
            assert isinstance(segment, Segment)
            assert segment.name in segments_to_validate
            segments_to_validate.remove(segment.name)

    @pytest.mark.asyncio
    async def test_that_errors_dont_stop_task(self, mocker):
        """Test that if fetching segments fails at some_point, the task will continue running."""
        split_storage = mocker.Mock(spec=SplitStorage)
        async def get_segment_names():
            return ['segmentA', 'segmentB', 'segmentC']
        split_storage.get_segment_names = get_segment_names

        rbs_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)
        async def get_segment_names_rbs():
            return []
        rbs_storage.get_segment_names = get_segment_names_rbs

        # Setup a mocked segment storage whose changenumber returns -1 on first fetch and
        # 123 afterwards.
        storage = mocker.Mock(spec=SegmentStorage)

        async def change_number_mock(segment_name):
            if segment_name == 'segmentA' and change_number_mock._count_a == 0:
                change_number_mock._count_a = 1
                return -1
            if segment_name == 'segmentB' and change_number_mock._count_b == 0:
                change_number_mock._count_b = 1
                return -1
            if segment_name == 'segmentC' and change_number_mock._count_c == 0:
                change_number_mock._count_c = 1
                return -1
            return 123
        change_number_mock._count_a = 0
        change_number_mock._count_b = 0
        change_number_mock._count_c = 0
        storage.get_change_number = change_number_mock

        self.segments = []
        async def put(segment):
            self.segments.append(segment)
        storage.put = put

        async def update(*arg):
            pass
        storage.update = update

        # Setup a mocked segment api to return segments mentioned before.
        self.segment_name = []
        self.change_number = []
        self.fetch_options = []
        async def fetch_segment_mock(segment_name, change_number, fetch_options):
            self.segment_name.append(segment_name)
            self.change_number.append(change_number)
            self.fetch_options.append(fetch_options)
            if segment_name == 'segmentA' and fetch_segment_mock._count_a == 0:
                fetch_segment_mock._count_a = 1
                return {'name': 'segmentA', 'added': ['key1', 'key2', 'key3'], 'removed': [],
                        'since': -1, 'till': 123}
            if segment_name == 'segmentB' and fetch_segment_mock._count_b == 0:
                fetch_segment_mock._count_b = 1
                raise Exception("some exception")
            if segment_name == 'segmentC' and fetch_segment_mock._count_c == 0:
                fetch_segment_mock._count_c = 1
                return {'name': 'segmentC', 'added': ['key7', 'key8', 'key9'], 'removed': [],
                        'since': -1, 'till': 123}
            return {'added': [], 'removed': [], 'since': 123, 'till': 123}
        fetch_segment_mock._count_a = 0
        fetch_segment_mock._count_b = 0
        fetch_segment_mock._count_c = 0

        api = mocker.Mock()
        fetch_options = FetchOptions(True, None, None, None, None)
        api.fetch_segment = fetch_segment_mock

        segments_synchronizer = SegmentSynchronizerAsync(api, split_storage, storage, rbs_storage)
        task = segment_sync.SegmentSynchronizationTaskAsync(segments_synchronizer.synchronize_segments,
                                                       0.5)
        task.start()
        await asyncio.sleep(0.7)
        assert task.is_running()

        await task.stop()
        assert not task.is_running()
        
        api_calls = []
        for i in range(5):
            api_calls.append((self.segment_name[i], self.change_number[i], self.fetch_options[i]))
        
        assert ('segmentA', -1, FetchOptions(True, None, None, None, None)) in api_calls
        assert ('segmentA', 123, FetchOptions(True, None, None, None, None)) in api_calls
        assert ('segmentB', -1, FetchOptions(True, None, None, None, None)) in api_calls
        assert ('segmentC', -1, FetchOptions(True, None, None, None, None)) in api_calls
        assert ('segmentC', 123, FetchOptions(True, None, None, None, None)) in api_calls

        segments_to_validate = set(['segmentA', 'segmentB', 'segmentC'])
        for segment in self.segments:
            assert isinstance(segment, Segment)
            assert segment.name in segments_to_validate
            segments_to_validate.remove(segment.name)
