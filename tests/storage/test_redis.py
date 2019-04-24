"""Redis storage test module."""
#pylint: disable=no-self-use

import json

from splitio.client.util import get_metadata
from splitio.storage.redis import RedisEventsStorage, RedisImpressionsStorage, \
    RedisSegmentStorage, RedisSplitStorage, RedisTelemetryStorage
from splitio.models.segments import Segment
from splitio.models.impressions import Impression
from splitio.models.events import Event
from splitio.storage.adapters.redis import RedisAdapter, RedisAdapterException


class RedisSplitStorageTests(object):
    """Redis split storage test cases."""

    def test_get_split(self, mocker):
        """Test retrieving a split works."""
        adapter = mocker.Mock(spec=RedisAdapter)
        adapter.get.return_value = '{"name": "some_split"}'
        from_raw = mocker.Mock()
        mocker.patch('splitio.models.splits.from_raw', new=from_raw)

        storage = RedisSplitStorage(adapter)
        storage.get('some_split')

        assert adapter.get.mock_calls == [mocker.call('SPLITIO.split.some_split')]
        assert from_raw.mock_calls == [mocker.call({"name": "some_split"})]

        # Test that a missing split returns None and doesn't call from_raw
        adapter.reset_mock()
        from_raw.reset_mock()
        adapter.get.return_value = None

        result = storage.get('some_split')
        assert result is None
        assert adapter.get.mock_calls == [mocker.call('SPLITIO.split.some_split')]
        assert not from_raw.mock_calls


    def test_get_changenumber(self, mocker):
        """Test fetching changenumber."""
        adapter = mocker.Mock(spec=RedisAdapter)
        storage = RedisSplitStorage(adapter)
        adapter.get.return_value = '-1'
        assert storage.get_change_number() == -1
        assert adapter.get.mock_calls == [mocker.call('SPLITIO.splits.till')]

    def test_get_all_splits(self, mocker):
        """Test fetching all splits."""
        adapter = mocker.Mock(spec=RedisAdapter)
        storage = RedisSplitStorage(adapter)
        from_raw = mocker.Mock()
        mocker.patch('splitio.models.splits.from_raw', new=from_raw)

        adapter.keys.return_value = [
            'SPLITIO.split.split1',
            'SPLITIO.split.split2',
            'SPLITIO.split.split3'
        ]
        def _mget_mock(*_):
            return ['{"name": "split1"}', '{"name": "split2"}', '{"name": "split3"}']
        adapter.mget.side_effect = _mget_mock

        storage.get_all_splits()

        assert adapter.keys.mock_calls == [mocker.call('SPLITIO.split.*')]
        assert adapter.mget.mock_calls == [
            mocker.call(['SPLITIO.split.split1', 'SPLITIO.split.split2', 'SPLITIO.split.split3'])
        ]

        assert len(from_raw.mock_calls) == 3
        assert mocker.call({'name': 'split1'}) in from_raw.mock_calls
        assert mocker.call({'name': 'split2'}) in from_raw.mock_calls
        assert mocker.call({'name': 'split3'}) in from_raw.mock_calls

    def test_get_split_names(self, mocker):
        """Test getching split names."""
        adapter = mocker.Mock(spec=RedisAdapter)
        storage = RedisSplitStorage(adapter)
        adapter.keys.return_value = [
            'SPLITIO.split.split1',
            'SPLITIO.split.split2',
            'SPLITIO.split.split3'
        ]
        assert storage.get_split_names() == ['split1', 'split2', 'split3']


class RedisSegmentStorageTests(object):
    """Redis segment storage test cases."""

    def test_fetch_segment(self, mocker):
        """Test fetching a whole segment."""
        adapter = mocker.Mock(spec=RedisAdapter)
        adapter.smembers.return_value = set(["key1", "key2", "key3"])
        adapter.get.return_value = '100'
        from_raw = mocker.Mock()
        mocker.patch('splitio.models.segments.from_raw', new=from_raw)

        storage = RedisSegmentStorage(adapter)
        result = storage.get('some_segment')
        assert isinstance(result, Segment)
        assert result.name == 'some_segment'
        assert result.contains('key1')
        assert result.contains('key2')
        assert result.contains('key3')
        assert result.change_number == 100
        assert adapter.smembers.mock_calls == [mocker.call('SPLITIO.segment.some_segment')]
        assert adapter.get.mock_calls == [mocker.call('SPLITIO.segment.some_segment.till')]

        # Assert that if segment doesn't exist, None is returned
        adapter.reset_mock()
        from_raw.reset_mock()
        adapter.smembers.return_value = set()
        assert storage.get('some_segment') is None
        assert adapter.smembers.mock_calls == [mocker.call('SPLITIO.segment.some_segment')]
        assert adapter.get.mock_calls == [mocker.call('SPLITIO.segment.some_segment.till')]

    def test_fetch_change_number(self, mocker):
        """Test fetching change number."""
        adapter = mocker.Mock(spec=RedisAdapter)
        adapter.get.return_value = '100'

        storage = RedisSegmentStorage(adapter)
        result = storage.get_change_number('some_segment')
        assert result == 100
        assert adapter.get.mock_calls == [mocker.call('SPLITIO.segment.some_segment.till')]

    def test_segment_contains(self, mocker):
        """Test segment contains functionality."""
        adapter = mocker.Mock(spec=RedisAdapter)
        storage = RedisSegmentStorage(adapter)
        adapter.sismember.return_value = True
        assert storage.segment_contains('some_segment', 'some_key') is True
        assert adapter.sismember.mock_calls == [
            mocker.call('SPLITIO.segment.some_segment', 'some_key')
        ]


class RedisImpressionsStorageTests(object):  #pylint: disable=too-few-public-methods
    """Redis Events storage test cases."""

    def test_add_impressions(self, mocker):
        """Test that adding impressions to storage works."""
        adapter = mocker.Mock(spec=RedisAdapter)
        metadata = get_metadata({})
        storage = RedisImpressionsStorage(adapter, metadata)

        impressions = [
            Impression('key1', 'feature1', 'on', 'some_label', 123456, 'buck1', 321654),
            Impression('key2', 'feature2', 'on', 'some_label', 123456, 'buck1', 321654),
            Impression('key3', 'feature2', 'on', 'some_label', 123456, 'buck1', 321654),
            Impression('key4', 'feature1', 'on', 'some_label', 123456, 'buck1', 321654)
        ]

        assert storage.put(impressions) is True

        to_validate = [json.dumps({
            'm': {  # METADATA PORTION
                's': metadata.sdk_version,
                'n': metadata.instance_name,
                'i': metadata.instance_ip,
            },
            'i': {  # IMPRESSION PORTION
                'k': impression.matching_key,
                'b': impression.bucketing_key,
                'f': impression.feature_name,
                't': impression.treatment,
                'r': impression.label,
                'c': impression.change_number,
                'm': impression.time,
            }
        }) for impression in impressions]

        assert adapter.rpush.mock_calls == [mocker.call('SPLITIO.impressions', *to_validate)]

        # Assert that if an exception is thrown it's caught and False is returned
        adapter.reset_mock()
        def _raise_exc(*_):
            raise RedisAdapterException('something')
        adapter.rpush.side_effect = _raise_exc
        assert storage.put(impressions) is False


class RedisEventsStorageTests(object):  #pylint: disable=too-few-public-methods
    """Redis Impression storage test cases."""

    def test_add_events(self, mocker):
        """Test that adding impressions to storage works."""
        adapter = mocker.Mock(spec=RedisAdapter)
        metadata = get_metadata({})

        storage = RedisEventsStorage(adapter, metadata)

        events = [
            Event('key1', 'user', 'purchase', 10, 123456),
            Event('key2', 'user', 'purchase', 10, 123456),
            Event('key3', 'user', 'purchase', 10, 123456),
            Event('key4', 'user', 'purchase', 10, 123456),
        ]
        assert storage.put(events) is True

        list_of_raw_events = [json.dumps({
            'm': {  # METADATA PORTION
                's': metadata.sdk_version,
                'n': metadata.instance_name,
                'i': metadata.instance_ip,
            },
            'e': {  # EVENT PORTION
                'key': event.key,
                'trafficTypeName': event.traffic_type_name,
                'eventTypeId': event.event_type_id,
                'value': event.value,
                'timestamp': event.timestamp,
            }
        }) for event in events]

        # To deal with python2 & 3 differences in hashing/order when dumping json.
        list_of_raw_json_strings_called = adapter.rpush.mock_calls[0][1][1:]
        list_of_events_called = [json.loads(event) for event in list_of_raw_json_strings_called]
        list_of_events_sent = [json.loads(event) for event in list_of_raw_events]
        for item in list_of_events_sent:
            assert item in list_of_events_called

#        assert adapter.rpush.mock_calls == [mocker.call('SPLITIO.events', to_validate)]
        # Assert that if an exception is thrown it's caught and False is returned
        adapter.reset_mock()
        def _raise_exc(*_):
            raise RedisAdapterException('something')
        adapter.rpush.side_effect = _raise_exc
        assert storage.put(events) is False


class RedisTelemetryStorageTests(object):
    """Redis-based telemetry storage test cases."""

    def test_inc_latency(self, mocker):
        """Test incrementing latency."""
        adapter = mocker.Mock(spec=RedisAdapter)
        metadata = get_metadata({})

        storage = RedisTelemetryStorage(adapter, metadata)
        storage.inc_latency('some_latency', 0)
        storage.inc_latency('some_latency', 1)
        storage.inc_latency('some_latency', 5)
        storage.inc_latency('some_latency', 5)
        storage.inc_latency('some_latency', 22)
        assert adapter.incr.mock_calls == [
            mocker.call('SPLITIO/' + metadata.sdk_version + '/' + metadata.instance_name + '/latency.some_latency.bucket.0'),
            mocker.call('SPLITIO/' + metadata.sdk_version + '/' + metadata.instance_name + '/latency.some_latency.bucket.1'),
            mocker.call('SPLITIO/' + metadata.sdk_version + '/' + metadata.instance_name + '/latency.some_latency.bucket.5'),
            mocker.call('SPLITIO/' + metadata.sdk_version + '/' + metadata.instance_name + '/latency.some_latency.bucket.5')
        ]

    def test_inc_counter(self, mocker):
        """Test incrementing latency."""
        adapter = mocker.Mock(spec=RedisAdapter)
        metadata = get_metadata({})

        storage = RedisTelemetryStorage(adapter, metadata)
        storage.inc_counter('some_counter_1')
        storage.inc_counter('some_counter_1')
        storage.inc_counter('some_counter_1')
        storage.inc_counter('some_counter_2')
        storage.inc_counter('some_counter_2')
        assert adapter.incr.mock_calls == [
            mocker.call('SPLITIO/' + metadata.sdk_version + '/' + metadata.instance_name + '/count.some_counter_1'),
            mocker.call('SPLITIO/' + metadata.sdk_version + '/' + metadata.instance_name + '/count.some_counter_1'),
            mocker.call('SPLITIO/' + metadata.sdk_version + '/' + metadata.instance_name + '/count.some_counter_1'),
            mocker.call('SPLITIO/' + metadata.sdk_version + '/' + metadata.instance_name + '/count.some_counter_2'),
            mocker.call('SPLITIO/' + metadata.sdk_version + '/' + metadata.instance_name + '/count.some_counter_2')
        ]

    def test_inc_gauge(self, mocker):
        """Test incrementing latency."""
        adapter = mocker.Mock(spec=RedisAdapter)
        metadata = get_metadata({})

        storage = RedisTelemetryStorage(adapter, metadata)
        storage.put_gauge('gauge1', 123)
        storage.put_gauge('gauge2', 456)
        assert adapter.set.mock_calls == [
            mocker.call('SPLITIO/' + metadata.sdk_version + '/' + metadata.instance_name + '/gauge.gauge1', 123),
            mocker.call('SPLITIO/' + metadata.sdk_version + '/' + metadata.instance_name + '/gauge.gauge2', 456)
        ]
