"""UWSGI Storage unit tests."""
#pylint: disable=no-self-usage
import json

from splitio.storage.uwsgi import UWSGIEventStorage, UWSGIImpressionStorage,  \
    UWSGISegmentStorage, UWSGISplitStorage,  UWSGITelemetryStorage

from splitio.models.splits import Split
from splitio.models.segments import Segment
from splitio.models.impressions import Impression
from splitio.models.events import Event, EventWrapper

from splitio.storage.adapters.uwsgi_cache import get_uwsgi


class UWSGISplitStorageTests(object):
    """UWSGI Split Storage test cases."""

    @staticmethod
    def _get_from_raw_mock(mocker):
        def _do(raw):
            mock_split = mocker.Mock()
            mock_split = mocker.Mock(spec=Split)
            mock_split.to_json.return_value = raw
            split_name = mocker.PropertyMock()
            split_name.return_value = raw['name']
            type(mock_split).name = split_name
            traffic_type_name = mocker.PropertyMock()
            traffic_type_name.return_value = raw['trafficTypeName']
            type(mock_split).traffic_type_name = traffic_type_name
            return mock_split

        from_raw_mock = mocker.Mock()
        from_raw_mock.side_effect = lambda x: _do(x)
        return from_raw_mock

    def test_store_retrieve_split(self, mocker):
        """Test storing and retrieving splits."""
        uwsgi = get_uwsgi(True)
        storage = UWSGISplitStorage(uwsgi)
        from_raw_mock = self._get_from_raw_mock(mocker)
        mocker.patch('splitio.models.splits.from_raw', new=from_raw_mock)

        raw_split = {'name': 'some_split', 'trafficTypeName': 'user'}
        split = from_raw_mock(raw_split)

        from_raw_mock.reset_mock()  # clear mock calls so they don't interfere with the testing itself.
        storage.put(split)

        retrieved = storage.get('some_split')

        assert retrieved.name == split.name and retrieved.traffic_type_name == split.traffic_type_name
        assert from_raw_mock.mock_calls == [mocker.call(raw_split)]
        assert split.to_json.mock_calls == [mocker.call()]

        assert storage.get('nonexistant_split') is None

        storage.remove('some_split')
        assert storage.get('some_split') == None

    def test_get_splits(self, mocker):
        """Test retrieving a list of passed splits."""
        uwsgi = get_uwsgi(True)
        storage = UWSGISplitStorage(uwsgi)
        from_raw_mock = self._get_from_raw_mock(mocker)
        mocker.patch('splitio.models.splits.from_raw', new=from_raw_mock)

        split_1 = from_raw_mock({'name': 'some_split_1', 'trafficTypeName': 'user'})
        split_2 = from_raw_mock({'name': 'some_split_2', 'trafficTypeName': 'user'})
        storage.put(split_1)
        storage.put(split_2)

        splits = storage.fetch_many(['some_split_1', 'some_split_2', 'some_split_3'])
        assert len(splits) == 3
        assert splits['some_split_1'].name == 'some_split_1'
        assert splits['some_split_2'].name == 'some_split_2'
        assert 'some_split_3' in splits

    def test_set_get_changenumber(self, mocker):
        """Test setting and retrieving changenumber."""
        uwsgi = get_uwsgi(True)
        storage = UWSGISplitStorage(uwsgi)

        assert storage.get_change_number() == None
        storage.set_change_number(123)
        assert storage.get_change_number() == 123

    def test_get_split_names(self, mocker):
        """Test getting all split names."""
        uwsgi = get_uwsgi(True)
        storage = UWSGISplitStorage(uwsgi)
        from_raw_mock = self._get_from_raw_mock(mocker)
        mocker.patch('splitio.models.splits.from_raw', new=from_raw_mock)

        split_1 = from_raw_mock({'name': 'some_split_1', 'trafficTypeName': 'user'})
        split_2 = from_raw_mock({'name': 'some_split_2', 'trafficTypeName': 'user'})
        storage.put(split_1)
        storage.put(split_2)

        assert set(storage.get_split_names()) == set(['some_split_1', 'some_split_2'])
        storage.remove('some_split_1')
        assert storage.get_split_names() == ['some_split_2']

    def test_get_all_splits(self, mocker):
        """Test fetching all splits."""
        uwsgi = get_uwsgi(True)
        storage = UWSGISplitStorage(uwsgi)
        from_raw_mock = self._get_from_raw_mock(mocker)
        mocker.patch('splitio.models.splits.from_raw', new=from_raw_mock)

        split_1 = from_raw_mock({'name': 'some_split_1', 'trafficTypeName': 'user'})
        split_2 = from_raw_mock({'name': 'some_split_2', 'trafficTypeName': 'user'})
        storage.put(split_1)
        storage.put(split_2)

        splits = storage.get_all_splits()
        s1 = next(split for split in splits if split.name == 'some_split_1')
        s2 = next(split for split in splits if split.name == 'some_split_2')

        assert s1.traffic_type_name == 'user'
        assert s2.traffic_type_name == 'user'

    def test_is_valid_traffic_type(self, mocker):
        """Test that traffic type validation works properly."""
        uwsgi = get_uwsgi(True)
        storage = UWSGISplitStorage(uwsgi)
        from_raw_mock = self._get_from_raw_mock(mocker)
        mocker.patch('splitio.models.splits.from_raw', new=from_raw_mock)

        split_1 = from_raw_mock({'name': 'some_split_1', 'trafficTypeName': 'user'})
        storage.put(split_1)
        assert storage.is_valid_traffic_type('user') is True
        assert storage.is_valid_traffic_type('account') is False

        split_2 = from_raw_mock({'name': 'some_split_2', 'trafficTypeName': 'account'})
        storage.put(split_2)
        assert storage.is_valid_traffic_type('user') is True
        assert storage.is_valid_traffic_type('account') is True

        split_3 = from_raw_mock({'name': 'some_split_3', 'trafficTypeName': 'user'})
        storage.put(split_3)
        assert storage.is_valid_traffic_type('user') is True
        assert storage.is_valid_traffic_type('account') is True

        storage.remove('some_split_1')
        assert storage.is_valid_traffic_type('user') is True
        assert storage.is_valid_traffic_type('account') is True

        storage.remove('some_split_2')
        assert storage.is_valid_traffic_type('user') is True
        assert storage.is_valid_traffic_type('account') is False

        storage.remove('some_split_3')
        assert storage.is_valid_traffic_type('user') is False
        assert storage.is_valid_traffic_type('account') is False

class UWSGISegmentStorageTests(object):
    """UWSGI Segment storage test cases."""

    def test_store_retrieve_segment(self, mocker):
        """Test storing and fetching segments."""
        uwsgi = get_uwsgi(True)
        storage = UWSGISegmentStorage(uwsgi)
        segment = mocker.Mock(spec=Segment)
        segment_keys = mocker.PropertyMock()
        segment_keys.return_value = ['abc']
        type(segment).keys = segment_keys
        segment.to_json = {}
        segment_name = mocker.PropertyMock()
        segment_name.return_value = 'some_segment'
        segment_change_number = mocker.PropertyMock()
        segment_change_number.return_value = 123
        type(segment).name = segment_name
        type(segment).change_number = segment_change_number
        from_raw_mock = mocker.Mock()
        from_raw_mock.return_value = 'ok'
        mocker.patch('splitio.models.segments.from_raw', new=from_raw_mock)

        storage.put(segment)
        assert storage.get('some_segment') == 'ok'
        assert from_raw_mock.mock_calls == [mocker.call({'till': 123, 'removed': [], 'added': [u'abc'], 'name': 'some_segment'})]
        assert storage.get('nonexistant-segment') is None

    def test_get_set_change_number(self, mocker):
        """Test setting and getting change number."""
        uwsgi = get_uwsgi(True)
        storage = UWSGISegmentStorage(uwsgi)
        assert storage.get_change_number('some_segment') is None
        storage.set_change_number('some_segment', 123)
        assert storage.get_change_number('some_segment') == 123

    def test_segment_contains(self, mocker):
        """Test that segment contains works properly."""
        uwsgi = get_uwsgi(True)
        storage = UWSGISegmentStorage(uwsgi)

        from_raw_mock = mocker.Mock()
        from_raw_mock.return_value = Segment('some_segment', ['abc'], 123)
        mocker.patch('splitio.models.segments.from_raw', new=from_raw_mock)
        segment = mocker.Mock(spec=Segment)
        segment_keys = mocker.PropertyMock()
        segment_keys.return_value = ['abc']
        type(segment).keys = segment_keys
        segment.to_json = {}
        segment_name = mocker.PropertyMock()
        segment_name.return_value = 'some_segment'
        segment_change_number = mocker.PropertyMock()
        segment_change_number.return_value = 123
        type(segment).name = segment_name
        type(segment).change_number = segment_change_number
        storage.put(segment)

        assert storage.segment_contains('some_segment', 'abc')
        assert not storage.segment_contains('some_segment', 'qwe')



class UWSGIImpressionsStorageTests(object):
    """UWSGI Impressions storage test cases."""

    def test_put_pop_impressions(self, mocker):
        """Test storing and fetching impressions."""
        uwsgi = get_uwsgi(True)
        storage = UWSGIImpressionStorage(uwsgi)
        impressions = [
            Impression('key1', 'feature1', 'on', 'some_label', 123456, 'buck1', 321654),
            Impression('key2', 'feature2', 'on', 'some_label', 123456, 'buck1', 321654),
            Impression('key3', 'feature2', 'on', 'some_label', 123456, 'buck1', 321654),
            Impression('key4', 'feature1', 'on', 'some_label', 123456, 'buck1', 321654)
        ]
        storage.put(impressions)
        res = storage.pop_many(10)
        assert res == impressions

    def test_flush(self):
        """Test requesting, querying and acknowledging a flush."""
        uwsgi = get_uwsgi(True)
        storage = UWSGIImpressionStorage(uwsgi)
        assert storage.should_flush() is False
        storage.request_flush()
        assert storage.should_flush() is True
        storage.acknowledge_flush()
        assert storage.should_flush() is False




class UWSGIEventsStorageTests(object):
    """UWSGI Events storage test cases."""

    def test_put_pop_events(self, mocker):
        """Test storing and fetching events."""
        uwsgi = get_uwsgi(True)
        storage = UWSGIEventStorage(uwsgi)
        events = [
            EventWrapper(event=Event('key1', 'user', 'purchase', 10, 123456, None),  size=32768),
            EventWrapper(event=Event('key2', 'user', 'purchase', 10, 123456, None),  size=32768),
            EventWrapper(event=Event('key3', 'user', 'purchase', 10, 123456, None),  size=32768),
            EventWrapper(event=Event('key4', 'user', 'purchase', 10, 123456, None),  size=32768),
        ]

        storage.put(events)
        res = storage.pop_many(10)
        assert res == [
            Event('key1', 'user', 'purchase', 10, 123456, None),
            Event('key2', 'user', 'purchase', 10, 123456, None),
            Event('key3', 'user', 'purchase', 10, 123456, None),
            Event('key4', 'user', 'purchase', 10, 123456, None)
        ]

class UWSGITelemetryStorageTests(object):
    """UWSGI-based telemetry storage test cases."""

    def test_latencies(self):
        """Test storing and popping latencies."""
        storage = UWSGITelemetryStorage(get_uwsgi(True))
        storage.inc_latency('some_latency', 2)
        storage.inc_latency('some_latency', 2)
        storage.inc_latency('some_latency', 2)
        assert storage.pop_latencies() == {
            'some_latency': [0, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        }
        assert storage.pop_latencies() == {}

    def test_counters(self):
        """Test storing and popping counters."""
        storage = UWSGITelemetryStorage(get_uwsgi(True))
        storage.inc_counter('some_counter')
        storage.inc_counter('some_counter')
        storage.inc_counter('some_counter')
        assert storage.pop_counters() == {'some_counter': 3}
        assert storage.pop_counters() == {}

    def test_gauges(self):
        """Test storing and popping gauges."""
        storage = UWSGITelemetryStorage(get_uwsgi(True))
        storage.put_gauge('some_gauge1', 123)
        storage.put_gauge('some_gauge2', 456)
        assert storage.pop_gauges() == {'some_gauge1': 123, 'some_gauge2': 456}
        assert storage.pop_gauges() == {}

