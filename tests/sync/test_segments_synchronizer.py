"""Split Worker tests."""

import os

from splitio.util.backoff import Backoff
from splitio.api import APIException
from splitio.api.commons import FetchOptions
from splitio.storage import SplitStorage, SegmentStorage, RuleBasedSegmentsStorage
from splitio.storage.inmemmory import InMemorySegmentStorage, InMemorySegmentStorageAsync, InMemorySplitStorage, InMemorySplitStorageAsync
from splitio.sync.segment import SegmentSynchronizer, SegmentSynchronizerAsync, LocalSegmentSynchronizer, LocalSegmentSynchronizerAsync
from splitio.models.segments import Segment
from splitio.models import rule_based_segments
from splitio.optional.loaders import aiofiles, asyncio

import pytest

class SegmentsSynchronizerTests(object):
    """Segments synchronizer test cases."""

    def test_synchronize_segments_error(self, mocker):
        """On error."""
        split_storage = mocker.Mock(spec=SplitStorage)
        split_storage.get_segment_names.return_value = ['segmentA', 'segmentB', 'segmentC']

        storage = mocker.Mock(spec=SegmentStorage)
        storage.get_change_number.return_value = -1
        rbs_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)
        rbs_storage.get_segment_names.return_value = []

        api = mocker.Mock()

        def run(x):
            raise APIException("something broke")

        api.fetch_segment.side_effect = run
        segments_synchronizer = SegmentSynchronizer(api, split_storage, storage, rbs_storage)
        assert not segments_synchronizer.synchronize_segments()

    def test_synchronize_segments(self, mocker):
        """Test the normal operation flow."""
        split_storage = mocker.Mock(spec=SplitStorage)
        split_storage.get_segment_names.return_value = ['segmentA', 'segmentB', 'segmentC']

        rbs_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)
        rbs_storage.get_segment_names.return_value = ['rbs']
        rbs_storage.get.return_value = rule_based_segments.from_raw({'name': 'rbs', 'conditions': [], 'trafficTypeName': 'user', 'changeNumber': 123, 'status': 'ACTIVE', 'excluded': {'keys': [], 'segments': [{'type': 'standard', 'name': 'segmentD'}]}})

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
            if segment_name == 'segmentD' and change_number_mock._count_d == 0:
                change_number_mock._count_d = 1
                return -1
            return 123
        change_number_mock._count_a = 0
        change_number_mock._count_b = 0
        change_number_mock._count_c = 0
        change_number_mock._count_d = 0
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
            if segment_name == 'segmentD' and fetch_segment_mock._count_d == 0:
                fetch_segment_mock._count_d = 1
                return {'name': 'segmentD', 'added': ['key10'], 'removed': [],
                        'since': -1, 'till': 123}
            return {'added': [], 'removed': [], 'since': 123, 'till': 123}
        fetch_segment_mock._count_a = 0
        fetch_segment_mock._count_b = 0
        fetch_segment_mock._count_c = 0
        fetch_segment_mock._count_d = 0

        api = mocker.Mock()
        api.fetch_segment.side_effect = fetch_segment_mock

        segments_synchronizer = SegmentSynchronizer(api, split_storage, storage, rbs_storage)
        assert segments_synchronizer.synchronize_segments()

        api_calls = [call for call in api.fetch_segment.mock_calls]

        assert mocker.call('segmentA', -1, FetchOptions(True, None, None, None, None)) in api_calls
        assert mocker.call('segmentB', -1, FetchOptions(True, None, None, None, None)) in api_calls
        assert mocker.call('segmentC', -1, FetchOptions(True, None, None, None, None)) in api_calls
        assert mocker.call('segmentD', -1, FetchOptions(True, None, None, None, None)) in api_calls
        assert mocker.call('segmentA', 123, FetchOptions(True, None, None, None, None)) in api_calls
        assert mocker.call('segmentB', 123, FetchOptions(True, None, None, None, None)) in api_calls
        assert mocker.call('segmentC', 123, FetchOptions(True, None, None, None, None)) in api_calls
        assert mocker.call('segmentD', 123, FetchOptions(True, None, None, None, None)) in api_calls

        segment_put_calls = storage.put.mock_calls
        segments_to_validate = set(['segmentA', 'segmentB', 'segmentC', 'segmentD'])
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
        rbs_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)
        rbs_storage.get_segment_names.return_value = []

        def change_number_mock(segment_name):
            if change_number_mock._count_a == 0:
                change_number_mock._count_a = 1
                return -1
            return 123
        change_number_mock._count_a = 0
        storage.get_change_number.side_effect = change_number_mock

        def fetch_segment_mock(segment_name, change_number, fetch_options):
            if fetch_segment_mock._count_a == 0:
                fetch_segment_mock._count_a = 1
                return {'name': 'segmentA', 'added': ['key1', 'key2', 'key3'], 'removed': [],
                        'since': -1, 'till': 123}
            return {'added': [], 'removed': [], 'since': 123, 'till': 123}
        fetch_segment_mock._count_a = 0

        api = mocker.Mock()
        api.fetch_segment.side_effect = fetch_segment_mock

        segments_synchronizer = SegmentSynchronizer(api, split_storage, storage, rbs_storage)
        segments_synchronizer.synchronize_segment('segmentA')

        api_calls = [call for call in api.fetch_segment.mock_calls]
        assert mocker.call('segmentA', -1, FetchOptions(True, None, None, None, None)) in api_calls
        assert mocker.call('segmentA', 123, FetchOptions(True, None, None, None, None)) in api_calls

    def test_synchronize_segment_cdn(self, mocker):
        """Test particular segment update cdn bypass."""
        mocker.patch('splitio.sync.segment._ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES', new=3)

        split_storage = mocker.Mock(spec=SplitStorage)
        storage = mocker.Mock(spec=SegmentStorage)
        rbs_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)
        rbs_storage.get_segment_names.return_value = []

        def change_number_mock(segment_name):
            change_number_mock._count_a += 1
            if change_number_mock._count_a == 1:
                return -1
            elif change_number_mock._count_a >= 2 and change_number_mock._count_a <= 3:
                return 123
            elif change_number_mock._count_a <= 7:
                return 1234
            return 12345 # Return proper cn for CDN Bypass

        change_number_mock._count_a = 0
        storage.get_change_number.side_effect = change_number_mock

        def fetch_segment_mock(segment_name, change_number, fetch_options):
            fetch_segment_mock._count_a += 1
            if fetch_segment_mock._count_a == 1:
                return {'name': 'segmentA', 'added': ['key1', 'key2', 'key3'], 'removed': [],
                        'since': -1, 'till': 123}
            elif fetch_segment_mock._count_a == 2:
                return {'added': [], 'removed': [], 'since': 123, 'till': 123}
            elif fetch_segment_mock._count_a == 3:
                return {'added': [], 'removed': [], 'since': 123, 'till': 1234}
            elif fetch_segment_mock._count_a >= 4 and fetch_segment_mock._count_a <= 6:
                return {'added': [], 'removed': [], 'since': 1234, 'till': 1234}
            elif fetch_segment_mock._count_a == 7:
                return {'added': [], 'removed': [], 'since': 1234, 'till': 12345}
            return {'added': [], 'removed': [], 'since': 12345, 'till': 12345}
        fetch_segment_mock._count_a = 0

        api = mocker.Mock()
        api.fetch_segment.side_effect = fetch_segment_mock

        segments_synchronizer = SegmentSynchronizer(api, split_storage, storage, rbs_storage)
        segments_synchronizer.synchronize_segment('segmentA')

        assert mocker.call('segmentA', -1, FetchOptions(True, None, None, None, None)) in api.fetch_segment.mock_calls
        assert mocker.call('segmentA', 123, FetchOptions(True, None, None, None, None)) in api.fetch_segment.mock_calls

        segments_synchronizer._backoff = Backoff(1, 0.1)
        segments_synchronizer.synchronize_segment('segmentA', 12345)
        assert mocker.call('segmentA', 12345, FetchOptions(True, 1234, None, None, None)) in api.fetch_segment.mock_calls
        assert len(api.fetch_segment.mock_calls) == 8 # 2 ok + BACKOFF(2 since==till + 2 re-attempts) + CDN(2 since==till)

    def test_recreate(self, mocker):
        """Test recreate logic."""
        segments_synchronizer = SegmentSynchronizer(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())
        current_pool = segments_synchronizer._worker_pool
        segments_synchronizer.recreate()
        assert segments_synchronizer._worker_pool != current_pool


class SegmentsSynchronizerAsyncTests(object):
    """Segments synchronizer async test cases."""

    @pytest.mark.asyncio
    async def test_synchronize_segments_error(self, mocker):
        """On error."""
        split_storage = mocker.Mock(spec=SplitStorage)
        rbs_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)

        async def get_segment_names_rbs():
            return []
        rbs_storage.get_segment_names = get_segment_names_rbs

        async def get_segment_names():
            return ['segmentA', 'segmentB', 'segmentC']
        split_storage.get_segment_names = get_segment_names

        storage = mocker.Mock(spec=SegmentStorage)
        async def get_change_number(*args):
            return -1
        storage.get_change_number = get_change_number

        async def put(*args):
            pass
        storage.put = put

        api = mocker.Mock()
        async def run(*args):
            raise APIException("something broke")
        api.fetch_segment = run

        segments_synchronizer = SegmentSynchronizerAsync(api, split_storage, storage, rbs_storage)
        assert not await segments_synchronizer.synchronize_segments()
        await segments_synchronizer.shutdown()

    @pytest.mark.asyncio
    async def test_synchronize_segments(self, mocker):
        """Test the normal operation flow."""
        split_storage = mocker.Mock(spec=SplitStorage)
        async def get_segment_names():
            return ['segmentA', 'segmentB', 'segmentC']
        split_storage.get_segment_names = get_segment_names

        rbs_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)
        async def get_segment_names_rbs():
            return ['rbs']
        rbs_storage.get_segment_names = get_segment_names_rbs
        
        async def get_rbs(segment_name):
            return rule_based_segments.from_raw({'name': 'rbs', 'conditions': [], 'trafficTypeName': 'user', 'changeNumber': 123, 'status': 'ACTIVE', 'excluded': {'keys': [], 'segments': [{'type': 'standard', 'name': 'segmentD'}]}})
        rbs_storage.get = get_rbs

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
            if segment_name == 'segmentD' and change_number_mock._count_d == 0:
                change_number_mock._count_d = 1
                return -1
            return 123
        change_number_mock._count_a = 0
        change_number_mock._count_b = 0
        change_number_mock._count_c = 0
        change_number_mock._count_d = 0
        storage.get_change_number = change_number_mock

        self.segment_put = []
        async def put(segment):
            self.segment_put.append(segment)
        storage.put = put

        async def update(*args):
            pass
        storage.update = update

        # Setup a mocked segment api to return segments mentioned before.
        self.options = []
        self.segment = []
        self.change = []
        async def fetch_segment_mock(segment_name, change_number, fetch_options):
            self.segment.append(segment_name)
            self.options.append(fetch_options)
            self.change.append(change_number)
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
            if segment_name == 'segmentD' and fetch_segment_mock._count_d == 0:
                fetch_segment_mock._count_d = 1
                return {'name': 'segmentD', 'added': ['key10'], 'removed': [],
                        'since': -1, 'till': 123}
            return {'added': [], 'removed': [], 'since': 123, 'till': 123}
        fetch_segment_mock._count_a = 0
        fetch_segment_mock._count_b = 0
        fetch_segment_mock._count_c = 0
        fetch_segment_mock._count_d = 0

        api = mocker.Mock()
        api.fetch_segment = fetch_segment_mock

        segments_synchronizer = SegmentSynchronizerAsync(api, split_storage, storage, rbs_storage)
        assert await segments_synchronizer.synchronize_segments()

        api_calls = []
        for i in range(8):
            api_calls.append((self.segment[i], self.change[i], self.options[i]))
        
        assert ('segmentD', -1, FetchOptions(True, None, None, None, None)) in api_calls
        assert ('segmentD', 123, FetchOptions(True, None, None, None, None)) in api_calls
        assert ('segmentA', -1, FetchOptions(True, None, None, None, None)) in api_calls
        assert ('segmentA', 123, FetchOptions(True, None, None, None, None)) in api_calls
        assert ('segmentB', -1, FetchOptions(True, None, None, None, None)) in api_calls
        assert ('segmentB', 123, FetchOptions(True, None, None, None, None)) in api_calls
        assert ('segmentC', -1, FetchOptions(True, None, None, None, None)) in api_calls
        assert ('segmentC', 123, FetchOptions(True, None, None, None, None)) in api_calls

        segments_to_validate = set(['segmentA', 'segmentB', 'segmentC', 'segmentD'])
        for segment in self.segment_put:
            assert isinstance(segment, Segment)
            assert segment.name in segments_to_validate
            segments_to_validate.remove(segment.name)

        await segments_synchronizer.shutdown()

    @pytest.mark.asyncio
    async def test_synchronize_segment(self, mocker):
        """Test particular segment update."""
        split_storage = mocker.Mock(spec=SplitStorage)
        storage = mocker.Mock(spec=SegmentStorage)
        rbs_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)

        async def get_segment_names_rbs():
            return []
        rbs_storage.get_segment_names = get_segment_names_rbs

        async def change_number_mock(segment_name):
            if change_number_mock._count_a == 0:
                change_number_mock._count_a = 1
                return -1
            return 123
        change_number_mock._count_a = 0
        storage.get_change_number = change_number_mock
        async def put(segment):
            pass
        storage.put = put

        async def update(*args):
            pass
        storage.update = update

        self.options = []
        self.segment = []
        self.change = []
        async def fetch_segment_mock(segment_name, change_number, fetch_options):
            self.segment.append(segment_name)
            self.options.append(fetch_options)
            self.change.append(change_number)
            if fetch_segment_mock._count_a == 0:
                fetch_segment_mock._count_a = 1
                return {'name': 'segmentA', 'added': ['key1', 'key2', 'key3'], 'removed': [],
                        'since': -1, 'till': 123}
            return {'added': [], 'removed': [], 'since': 123, 'till': 123}
        fetch_segment_mock._count_a = 0

        api = mocker.Mock()
        api.fetch_segment = fetch_segment_mock

        segments_synchronizer = SegmentSynchronizerAsync(api, split_storage, storage, rbs_storage)
        await segments_synchronizer.synchronize_segment('segmentA')

        assert (self.segment[0], self.change[0], self.options[0]) == ('segmentA', -1, FetchOptions(True, None, None, None, None))
        assert (self.segment[1], self.change[1], self.options[1]) == ('segmentA', 123, FetchOptions(True, None, None, None, None))

        await segments_synchronizer.shutdown()

    @pytest.mark.asyncio
    async def test_synchronize_segment_cdn(self, mocker):
        """Test particular segment update cdn bypass."""
        mocker.patch('splitio.sync.segment._ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES', new=3)

        split_storage = mocker.Mock(spec=SplitStorage)
        storage = mocker.Mock(spec=SegmentStorage)
        rbs_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)

        async def get_segment_names_rbs():
            return []
        rbs_storage.get_segment_names = get_segment_names_rbs

        async def change_number_mock(segment_name):
            change_number_mock._count_a += 1
            if change_number_mock._count_a == 1:
                return -1
            elif change_number_mock._count_a >= 2 and change_number_mock._count_a <= 3:
                return 123
            elif change_number_mock._count_a <= 7:
                return 1234
            return 12345 # Return proper cn for CDN Bypass
        change_number_mock._count_a = 0
        storage.get_change_number = change_number_mock
        async def put(segment):
            pass
        storage.put = put

        async def update(*args):
            pass
        storage.update = update

        self.options = []
        self.segment = []
        self.change = []
        async def fetch_segment_mock(segment_name, change_number, fetch_options):
            self.segment.append(segment_name)
            self.options.append(fetch_options)
            self.change.append(change_number)
            fetch_segment_mock._count_a += 1
            if fetch_segment_mock._count_a == 1:
                return {'name': 'segmentA', 'added': ['key1', 'key2', 'key3'], 'removed': [],
                        'since': -1, 'till': 123}
            elif fetch_segment_mock._count_a == 2:
                return {'added': [], 'removed': [], 'since': 123, 'till': 123}
            elif fetch_segment_mock._count_a == 3:
                return {'added': [], 'removed': [], 'since': 123, 'till': 1234}
            elif fetch_segment_mock._count_a >= 4 and fetch_segment_mock._count_a <= 6:
                return {'added': [], 'removed': [], 'since': 1234, 'till': 1234}
            elif fetch_segment_mock._count_a == 7:
                return {'added': [], 'removed': [], 'since': 1234, 'till': 12345}
            return {'added': [], 'removed': [], 'since': 12345, 'till': 12345}
        fetch_segment_mock._count_a = 0

        api = mocker.Mock()
        api.fetch_segment = fetch_segment_mock

        segments_synchronizer = SegmentSynchronizerAsync(api, split_storage, storage, rbs_storage)
        await segments_synchronizer.synchronize_segment('segmentA')

        assert (self.segment[0], self.change[0], self.options[0]) == ('segmentA', -1, FetchOptions(True, None, None, None, None))
        assert (self.segment[1], self.change[1], self.options[1]) == ('segmentA', 123, FetchOptions(True, None, None, None, None))

        segments_synchronizer._backoff = Backoff(1, 0.1)
        await segments_synchronizer.synchronize_segment('segmentA', 12345)
        assert (self.segment[7], self.change[7], self.options[7]) == ('segmentA', 12345, FetchOptions(True, 1234, None, None, None))
        assert len(self.segment) == 8 # 2 ok + BACKOFF(2 since==till + 2 re-attempts) + CDN(2 since==till)
        await segments_synchronizer.shutdown()

    @pytest.mark.asyncio
    async def test_recreate(self, mocker):
        """Test recreate logic."""
        segments_synchronizer = SegmentSynchronizerAsync(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())
        current_pool = segments_synchronizer._worker_pool
        await segments_synchronizer.shutdown()
        segments_synchronizer.recreate()

        assert segments_synchronizer._worker_pool != current_pool
        await segments_synchronizer.shutdown()


class LocalSegmentsSynchronizerTests(object):
    """Segments synchronizer test cases."""

    def test_synchronize_segments_error(self, mocker):
        """On error."""
        split_storage = mocker.Mock(spec=SplitStorage)
        split_storage.get_segment_names.return_value = ['segmentA', 'segmentB', 'segmentC']

        storage = mocker.Mock(spec=SegmentStorage)
        storage.get_change_number.return_value = -1

        segments_synchronizer = LocalSegmentSynchronizer('/,/,/invalid folder name/,/,/', split_storage, storage)
        assert not segments_synchronizer.synchronize_segments()

    def test_synchronize_segments(self, mocker):
        """Test the normal operation flow."""
        split_storage = mocker.Mock(spec=InMemorySplitStorage)
        split_storage.get_segment_names.return_value = ['segmentA', 'segmentB', 'segmentC']
        storage = InMemorySegmentStorage()

        segment_a = {'name': 'segmentA', 'added': ['key1', 'key2', 'key3'], 'removed': [],
                        'since': -1, 'till': 123}
        segment_b = {'name': 'segmentB', 'added': ['key4', 'key5', 'key6'], 'removed': [],
                        'since': -1, 'till': 123}
        segment_c = {'name': 'segmentC', 'added': ['key7', 'key8', 'key9'], 'removed': [],
                        'since': -1, 'till': 123}
        blank = {'added': [], 'removed': [], 'since': 123, 'till': 123}

        def read_segment_from_json_file(*args, **kwargs):
            if args[0] == 'segmentA':
                return segment_a
            if args[0] == 'segmentB':
                return segment_b
            if args[0] == 'segmentC':
                return segment_c
            return blank

        segments_synchronizer = LocalSegmentSynchronizer('segment_path', split_storage, storage)
        segments_synchronizer._read_segment_from_json_file = read_segment_from_json_file
        assert segments_synchronizer.synchronize_segments()

        segment = storage.get('segmentA')
        assert segment.name == 'segmentA'
        assert segment.contains('key1')
        assert segment.contains('key2')
        assert segment.contains('key3')

        segment = storage.get('segmentB')
        assert segment.name == 'segmentB'
        assert segment.contains('key4')
        assert segment.contains('key5')
        assert segment.contains('key6')

        segment = storage.get('segmentC')
        assert segment.name == 'segmentC'
        assert segment.contains('key7')
        assert segment.contains('key8')
        assert segment.contains('key9')

        # Should sync when changenumber is not changed
        segment_a['added'] = ['key111']
        segments_synchronizer.synchronize_segments(['segmentA'])
        segment = storage.get('segmentA')
        assert segment.contains('key111')

        # Should not sync when changenumber below till
        segment_a['till'] = 122
        segment_a['added'] = ['key222']
        segments_synchronizer.synchronize_segments(['segmentA'])
        segment = storage.get('segmentA')
        assert not segment.contains('key222')

        # Should sync when changenumber above till
        segment_a['till'] = 124
        segments_synchronizer.synchronize_segments(['segmentA'])
        segment = storage.get('segmentA')
        assert segment.contains('key222')

        # Should sync when till is default (-1)
        segment_a['till'] = -1
        segment_a['added'] = ['key33']
        segments_synchronizer.synchronize_segments(['segmentA'])
        segment = storage.get('segmentA')
        assert segment.contains('key33')

        # verify remove keys
        segment_a['added'] = []
        segment_a['removed'] = ['key111']
        segment_a['till'] = 125
        segments_synchronizer.synchronize_segments(['segmentA'])
        segment = storage.get('segmentA')
        assert not segment.contains('key111')

    def test_reading_json(self, mocker):
        """Test reading json file."""
        f = open("./segmentA.json", "w")
        f.write('{"name": "segmentA", "added": ["key1", "key2", "key3"], "removed": [],"since": -1, "till": 123}')
        f.close()
        split_storage = mocker.Mock(spec=InMemorySplitStorage)
        storage = InMemorySegmentStorage()
        segments_synchronizer = LocalSegmentSynchronizer('.', split_storage, storage)
        assert segments_synchronizer.synchronize_segments(['segmentA'])

        segment = storage.get('segmentA')
        assert segment.name == 'segmentA'
        assert segment.contains('key1')
        assert segment.contains('key2')
        assert segment.contains('key3')

        os.remove("./segmentA.json")

    def test_json_elements_sanitization(self, mocker):
        """Test sanitization."""
        segment_synchronizer = LocalSegmentSynchronizer(mocker.Mock(), mocker.Mock(), mocker.Mock())
        segment1 = {"name": 'seg', "added": [], "removed": [], "since": -1, "till": 12}

        # should reject segment if 'name' is null
        segment2 = {"name": None, "added": [], "removed": [], "since": -1, "till": 12}
        exception_called = False
        try:
            segment_synchronizer._sanitize_segment(segment2)
        except:
            exception_called = True
        assert(exception_called)

        # should reject segment if 'name' does not exist
        segment2 = {"added": [], "removed": [], "since": -1, "till": 12}
        exception_called = False
        try:
            segment_synchronizer._sanitize_segment(segment2)
        except:
            exception_called = True
        assert(exception_called)

        # should add missing 'added' element
        segment2 = {"name": 'seg', "removed": [], "since": -1, "till": 12}
        assert(segment_synchronizer._sanitize_segment(segment2) == segment1)

        # should add missing 'removed' element
        segment2 = {"name": 'seg', "added": [], "since": -1, "till": 12}
        assert(segment_synchronizer._sanitize_segment(segment2) == segment1)

        # should reset added and remved to array if values are None
        segment2 = {"name": 'seg', "added": None, "removed": None, "since": -1, "till": 12}
        assert(segment_synchronizer._sanitize_segment(segment2) == segment1)

        # should reset since and till to -1 if values are None
        segment3 = segment1.copy()
        segment3["till"] = -1
        segment2 = {"name": 'seg', "added": [], "removed": [], "since": None, "till": None}
        assert(segment_synchronizer._sanitize_segment(segment2) == segment3)

        # should add since and till with -1 if they are missing
        segment2 = {"name": 'seg', "added": [], "removed": []}
        assert(segment_synchronizer._sanitize_segment(segment2) == segment3)

        # should reset since and till to -1 if values are 0
        segment2 = {"name": 'seg', "added": [], "removed": [], "since": 0, "till": 0}
        assert(segment_synchronizer._sanitize_segment(segment2) == segment3)

        # should reset till and since to -1 if values below -1
        segment2 = {"name": 'seg', "added": [], "removed": [], "since": -2, "till": -2}
        assert(segment_synchronizer._sanitize_segment(segment2) == segment3)

        # should reset since to till if value above till
        segment3["since"] = 12
        segment3["till"] = 12
        segment2 = {"name": 'seg', "added": [], "removed": [], "since": 20, "till": 12}
        assert(segment_synchronizer._sanitize_segment(segment2) == segment3)


class LocalSegmentsSynchronizerTests(object):
    """Segments synchronizer test cases."""

    @pytest.mark.asyncio
    async def test_synchronize_segments_error(self, mocker):
        """On error."""
        split_storage = mocker.Mock(spec=SplitStorage)
        async def get_segment_names():
            return ['segmentA', 'segmentB', 'segmentC']
        split_storage.get_segment_names = get_segment_names

        storage = mocker.Mock(spec=SegmentStorage)
        async def get_change_number():
            return -1
        storage.get_change_number = get_change_number

        segments_synchronizer = LocalSegmentSynchronizerAsync('/,/,/invalid folder name/,/,/', split_storage, storage)
        assert not await segments_synchronizer.synchronize_segments()

    @pytest.mark.asyncio
    async def test_synchronize_segments(self, mocker):
        """Test the normal operation flow."""
        split_storage = mocker.Mock(spec=InMemorySplitStorage)
        async def get_segment_names():
            return ['segmentA', 'segmentB', 'segmentC']
        split_storage.get_segment_names = get_segment_names

        storage = InMemorySegmentStorageAsync()

        segment_a = {'name': 'segmentA', 'added': ['key1', 'key2', 'key3'], 'removed': [],
                        'since': -1, 'till': 123}
        segment_b = {'name': 'segmentB', 'added': ['key4', 'key5', 'key6'], 'removed': [],
                        'since': -1, 'till': 123}
        segment_c = {'name': 'segmentC', 'added': ['key7', 'key8', 'key9'], 'removed': [],
                        'since': -1, 'till': 123}
        blank = {'added': [], 'removed': [], 'since': 123, 'till': 123}

        async def read_segment_from_json_file(*args, **kwargs):
            if args[0] == 'segmentA':
                return segment_a
            if args[0] == 'segmentB':
                return segment_b
            if args[0] == 'segmentC':
                return segment_c
            return blank

        segments_synchronizer = LocalSegmentSynchronizerAsync('segment_path', split_storage, storage)
        segments_synchronizer._read_segment_from_json_file = read_segment_from_json_file
        assert await segments_synchronizer.synchronize_segments()

        segment = await storage.get('segmentA')
        assert segment.name == 'segmentA'
        assert segment.contains('key1')
        assert segment.contains('key2')
        assert segment.contains('key3')

        segment = await storage.get('segmentB')
        assert segment.name == 'segmentB'
        assert segment.contains('key4')
        assert segment.contains('key5')
        assert segment.contains('key6')

        segment = await storage.get('segmentC')
        assert segment.name == 'segmentC'
        assert segment.contains('key7')
        assert segment.contains('key8')
        assert segment.contains('key9')

        # Should sync when changenumber is not changed
        segment_a['added'] = ['key111']
        await segments_synchronizer.synchronize_segments(['segmentA'])
        segment = await storage.get('segmentA')
        assert segment.contains('key111')

        # Should not sync when changenumber below till
        segment_a['till'] = 122
        segment_a['added'] = ['key222']
        await segments_synchronizer.synchronize_segments(['segmentA'])
        segment = await storage.get('segmentA')
        assert not segment.contains('key222')

        # Should sync when changenumber above till
        segment_a['till'] = 124
        await segments_synchronizer.synchronize_segments(['segmentA'])
        segment = await storage.get('segmentA')
        assert segment.contains('key222')

        # Should sync when till is default (-1)
        segment_a['till'] = -1
        segment_a['added'] = ['key33']
        await segments_synchronizer.synchronize_segments(['segmentA'])
        segment = await storage.get('segmentA')
        assert segment.contains('key33')

        # verify remove keys
        segment_a['added'] = []
        segment_a['removed'] = ['key111']
        segment_a['till'] = 125
        await segments_synchronizer.synchronize_segments(['segmentA'])
        segment = await storage.get('segmentA')
        assert not segment.contains('key111')

    @pytest.mark.asyncio
    async def test_reading_json(self, mocker):
        """Test reading json file."""
        async with aiofiles.open("./segmentA.json", "w") as f:
            await f.write('{"name": "segmentA", "added": ["key1", "key2", "key3"], "removed": [],"since": -1, "till": 123}')
        split_storage = mocker.Mock(spec=InMemorySplitStorageAsync)
        storage = InMemorySegmentStorageAsync()
        segments_synchronizer = LocalSegmentSynchronizerAsync('.', split_storage, storage)
        assert await segments_synchronizer.synchronize_segments(['segmentA'])

        segment = await storage.get('segmentA')
        assert segment.name == 'segmentA'
        assert segment.contains('key1')
        assert segment.contains('key2')
        assert segment.contains('key3')

        os.remove("./segmentA.json")