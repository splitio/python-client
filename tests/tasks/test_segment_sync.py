"""Split syncrhonization task test module."""

import threading
import time
from splitio.api import APIException
from splitio.tasks import segment_sync
from splitio.storage import SegmentStorage, SplitStorage
from splitio.models.splits import Split
from splitio.models.segments import Segment
from splitio.models.grammar.condition import Condition
from splitio.models.grammar.matchers import UserDefinedSegmentMatcher


class SegmentSynchronizationTests(object):
    """Split synchronization task test cases."""

    def test_normal_operation(self, mocker):
        """Test the normal operation flow."""
        split_storage = mocker.Mock(spec=SplitStorage)
        split_storage.get_segment_names.return_value = ['segmentA', 'segmentB', 'segmentC']

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
        def fetch_segment_mock(segment_name, change_number):
            if segment_name == 'segmentA' and fetch_segment_mock._count_a == 0:
                fetch_segment_mock._count_a = 1
                return {'name': 'segmentA', 'added': ['key1', 'key2', 'key3'], 'removed': [], 'since': -1, 'till': 123}
            if segment_name == 'segmentB' and fetch_segment_mock._count_b == 0:
                fetch_segment_mock._count_b = 1
                return {'name': 'segmentB', 'added': ['key4', 'key5', 'key6'], 'removed': [], 'since': -1, 'till': 123}
            if segment_name == 'segmentC' and fetch_segment_mock._count_c == 0:
                fetch_segment_mock._count_c = 1
                return {'name': 'segmentC', 'added': ['key7', 'key8', 'key9'], 'removed': [], 'since': -1, 'till': 123}
            return {'added': [], 'removed': [], 'since': 123, 'till': 123}
        fetch_segment_mock._count_a = 0
        fetch_segment_mock._count_b = 0
        fetch_segment_mock._count_c = 0

        api = mocker.Mock()
        api.fetch_segment.side_effect = fetch_segment_mock

        task = segment_sync.SegmentSynchronizationTask(api, storage, split_storage, 1)
        task.start()
        time.sleep(0.5)

        assert task.is_running()

        stop_event = threading.Event()
        task.stop(stop_event)
        stop_event.wait()
        assert not task.is_running()

        api_calls = [call for call in api.fetch_segment.mock_calls]
        assert mocker.call('segmentA', -1) in api_calls
        assert mocker.call('segmentB', -1) in api_calls
        assert mocker.call('segmentC', -1) in api_calls
        assert mocker.call('segmentA', 123) in api_calls
        assert mocker.call('segmentB', 123) in api_calls
        assert mocker.call('segmentC', 123) in api_calls

        segment_put_calls = storage.put.mock_calls
        segments_to_validate = set(['segmentA', 'segmentB', 'segmentC'])
        for call in segment_put_calls:
            func_name, positional_args, keyword_args = call
            segment = positional_args[0]
            assert isinstance(segment, Segment)
            assert segment.name in segments_to_validate
            segments_to_validate.remove(segment.name)

    def test_that_errors_dont_stop_task(self, mocker):
        """Test that if fetching segments fails at some_point, the task will continue running."""
        # TODO!
