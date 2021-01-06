"""Split Worker tests."""

import threading
import time
import pytest

from splitio.api import APIException
from splitio.storage import SplitStorage, SegmentStorage
from splitio.models.splits import Split
from splitio.sync.segment import SegmentSynchronizer
from splitio.models.segments import Segment


class SegmentsSynchronizerTests(object):
    """Segments synchronizer test cases."""

    def test_synchronize_segments_error(self, mocker):
        """On error."""
        split_storage = mocker.Mock(spec=SplitStorage)
        split_storage.get_segment_names.return_value = ['segmentA', 'segmentB', 'segmentC']

        storage = mocker.Mock(spec=SegmentStorage)
        storage.get_change_number.return_value = -1

        api = mocker.Mock()

        def run(x):
            raise APIException("something broke")

        api.fetch_segment.side_effect = run
        segments_synchronizer = SegmentSynchronizer(api, split_storage, storage)
        assert not segments_synchronizer.synchronize_segments()

    def test_synchronize_segments(self, mocker):
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
        api.fetch_segment.side_effect = fetch_segment_mock

        segments_synchronizer = SegmentSynchronizer(api, split_storage, storage)
        assert segments_synchronizer.synchronize_segments()

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
            _, positional_args, _ = call
            segment = positional_args[0]
            assert isinstance(segment, Segment)
            assert segment.name in segments_to_validate
            segments_to_validate.remove(segment.name)

    def test_synchronize_segment(self, mocker):
        """Test particular segment update."""
        split_storage = mocker.Mock(spec=SplitStorage)
        storage = mocker.Mock(spec=SegmentStorage)

        def change_number_mock(segment_name):
            if change_number_mock._count_a == 0:
                change_number_mock._count_a = 1
                return -1
            return 123
        change_number_mock._count_a = 0
        storage.get_change_number.side_effect = change_number_mock

        def fetch_segment_mock(segment_name, change_number):
            if fetch_segment_mock._count_a == 0:
                fetch_segment_mock._count_a = 1
                return {'name': 'segmentA', 'added': ['key1', 'key2', 'key3'], 'removed': [],
                        'since': -1, 'till': 123}
            return {'added': [], 'removed': [], 'since': 123, 'till': 123}
        fetch_segment_mock._count_a = 0

        api = mocker.Mock()
        api.fetch_segment.side_effect = fetch_segment_mock

        segments_synchronizer = SegmentSynchronizer(api, split_storage, storage)
        segments_synchronizer.synchronize_segment('segmentA')

        api_calls = [call for call in api.fetch_segment.mock_calls]
        assert mocker.call('segmentA', -1) in api_calls
        assert mocker.call('segmentA', 123) in api_calls

    def test_recreate(self, mocker):
        """Test recreate logic."""
        segments_synchronizer = SegmentSynchronizer(mocker.Mock(), mocker.Mock(), mocker.Mock())
        current_pool = segments_synchronizer._worker_pool
        segments_synchronizer.recreate()
        assert segments_synchronizer._worker_pool != current_pool
