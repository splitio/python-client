"""Redis storage test module."""
# pylint: disable=no-self-use

import json
import time
import unittest.mock as mock
import pytest

from splitio.client.util import get_metadata, SdkMetadata
from splitio.storage.redis import RedisEventsStorage, RedisImpressionsStorage, \
    RedisSegmentStorage, RedisSplitStorage, RedisTelemetryStorage
from splitio.storage.adapters.redis import RedisAdapter, RedisAdapterException, build
from splitio.models.segments import Segment
from splitio.models.impressions import Impression
from splitio.models.events import Event, EventWrapper
from splitio.models.telemetry import MethodExceptions, MethodLatencies, TelemetryConfig, MethodExceptionsAndLatencies
from splitio.storage import FlagSetsFilter
from tests.integration import splits_json


class RedisSplitStorageTests(object):
    """Redis split storage test cases."""

    def test_get_split(self, mocker):
        """Test retrieving a split works."""
        adapter = mocker.Mock(spec=RedisAdapter)
        adapter.get.return_value = '{"name": "some_split"}'
        from_raw = mocker.Mock()
        mocker.patch('splitio.storage.redis.splits.from_raw', new=from_raw)

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

    def test_get_split_with_cache(self, mocker):
        """Test retrieving a split works."""
        adapter = mocker.Mock(spec=RedisAdapter)
        adapter.get.return_value = '{"name": "some_split"}'
        from_raw = mocker.Mock()
        mocker.patch('splitio.storage.redis.splits.from_raw', new=from_raw)

        storage = RedisSplitStorage(adapter, True, 1)
        storage.get('some_split')
        assert adapter.get.mock_calls == [mocker.call('SPLITIO.split.some_split')]
        assert from_raw.mock_calls == [mocker.call({"name": "some_split"})]

        # hit the cache:
        storage.get('some_split')
        storage.get('some_split')
        storage.get('some_split')
        assert adapter.get.mock_calls == [mocker.call('SPLITIO.split.some_split')]
        assert from_raw.mock_calls == [mocker.call({"name": "some_split"})]

        # Test that a missing split returns None and doesn't call from_raw
        adapter.reset_mock()
        from_raw.reset_mock()
        adapter.get.return_value = None

        # Still cached
        result = storage.get('some_split')
        assert result is not None
        time.sleep(1)  # wait for expiration
        result = storage.get('some_split')
        assert result is None
        assert adapter.get.mock_calls == [mocker.call('SPLITIO.split.some_split')]
        assert not from_raw.mock_calls

    def test_get_splits_with_cache(self, mocker):
        """Test retrieving a list of passed splits."""
        adapter = mocker.Mock(spec=RedisAdapter)
        storage = RedisSplitStorage(adapter)
        from_raw = mocker.Mock()
        mocker.patch('splitio.storage.redis.splits.from_raw', new=from_raw)

        adapter.mget.return_value = ['{"name": "split1"}', '{"name": "split2"}', None]

        result = storage.fetch_many(['split1', 'split2', 'split3'])
        assert len(result) == 3

        assert mocker.call({'name': 'split1'}) in from_raw.mock_calls
        assert mocker.call({'name': 'split2'}) in from_raw.mock_calls

        assert result['split1'] is not None
        assert result['split2'] is not None
        assert 'split3' in result

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
        mocker.patch('splitio.storage.redis.splits.from_raw', new=from_raw)

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

    def test_is_valid_traffic_type(self, mocker):
        """Test that traffic type validation works."""
        adapter = mocker.Mock(spec=RedisAdapter)
        storage = RedisSplitStorage(adapter)

        adapter.get.return_value = '1'
        assert storage.is_valid_traffic_type('any') is True

        adapter.get.return_value = '0'
        assert storage.is_valid_traffic_type('any') is False

        adapter.get.return_value = None
        assert storage.is_valid_traffic_type('any') is False

    def test_is_valid_traffic_type_with_cache(self, mocker):
        """Test that traffic type validation works."""
        adapter = mocker.Mock(spec=RedisAdapter)
        storage = RedisSplitStorage(adapter, True, 1)

        adapter.get.return_value = '1'
        assert storage.is_valid_traffic_type('any') is True

        adapter.get.return_value = '0'
        assert storage.is_valid_traffic_type('any') is True
        time.sleep(1)
        assert storage.is_valid_traffic_type('any') is False

        adapter.get.return_value = None
        time.sleep(1)
        assert storage.is_valid_traffic_type('any') is False

    @mock.patch('splitio.storage.adapters.redis.RedisPipelineAdapter.execute', return_value = [{'split1', 'split2'}])
    def test_flag_sets(self, mocker):
        """Test Flag sets scenarios."""
        adapter = build({})
        storage = RedisSplitStorage(adapter, True, 1)
        assert storage.flag_set_filter.flag_sets == set({})
        assert sorted(storage.get_feature_flags_by_sets(['set1', 'set2'])) == ['split1', 'split2']

        storage.flag_set_filter = FlagSetsFilter(['set2', 'set3'])
        assert storage.get_feature_flags_by_sets(['set1']) == []
        assert sorted(storage.get_feature_flags_by_sets(['set2'])) == ['split1', 'split2']

        storage2 = RedisSplitStorage(adapter, True, 1, ['set2', 'set3'])
        assert storage2.flag_set_filter.flag_sets == set({'set2', 'set3'})

    def test_fetching_split_with_flag_set(self, mocker):
        """Test retrieving a split works."""
        adapter = mocker.Mock(spec=RedisAdapter)
        adapter.get.return_value = json.dumps(splits_json["splitChange1_1"]["splits"][0])
        adapter.keys.return_value = ['SPLIT_1', 'SPLIT_2']

        def mget(keys):
            if keys == ['SPLIT_2']:
                return [json.dumps(splits_json["splitChange1_1"]["splits"][0])]
            if keys == ['SPLIT_2', 'SPLIT_1']:
                return [json.dumps(splits_json["splitChange1_1"]["splits"][0]), json.dumps(splits_json["splitChange1_1"]["splits"][1])]
        adapter.mget = mget

        storage = RedisSplitStorage(adapter, config_flag_sets=['set_1'])

        def get_feature_flags_by_sets(flag_sets):
            if flag_sets=={'set_1'}:
                return []
            if flag_sets=={'set2'}:
                return ['SPLIT_2']
            if flag_sets=={'set2', 'set1'}:
                return ['SPLIT_2', 'SPLIT_1']
        storage.get_feature_flags_by_sets = get_feature_flags_by_sets

        assert storage.get('SPLIT_2') == None
        assert storage.get_split_names() == []
        assert storage.get_all_splits() == []

        storage = RedisSplitStorage(adapter, config_flag_sets=['set2'])
        storage.get_feature_flags_by_sets = get_feature_flags_by_sets
        assert storage.get('SPLIT_2').name == 'SPLIT_2'
        assert storage.get_split_names() == ['SPLIT_2']
        splits = storage.get_all_splits()
        assert splits[0].name == 'SPLIT_2'
        assert len(splits) == 1

        storage = RedisSplitStorage(adapter, config_flag_sets=['set2', 'set1'])
        storage.get_feature_flags_by_sets = get_feature_flags_by_sets
        assert storage.get('SPLIT_2').name == 'SPLIT_2'
        assert storage.get_split_names() == ['SPLIT_2', 'SPLIT_1']
        splits = storage.get_all_splits()
        assert splits[0].name == 'SPLIT_2'
        assert splits[1].name == 'SPLIT_1'
        assert len(splits) == 2


class RedisSegmentStorageTests(object):
    """Redis segment storage test cases."""

    def test_fetch_segment(self, mocker):
        """Test fetching a whole segment."""
        adapter = mocker.Mock(spec=RedisAdapter)
        adapter.smembers.return_value = set(["key1", "key2", "key3"])
        adapter.get.return_value = '100'
        from_raw = mocker.Mock()
        mocker.patch('splitio.storage.redis.segments.from_raw', new=from_raw)

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


class RedisImpressionsStorageTests(object):  # pylint: disable=too-few-public-methods
    """Redis Impressions storage test cases."""

    def test_wrap_impressions(self, mocker):
        """Test wrap impressions."""
        adapter = mocker.Mock(spec=RedisAdapter)
        metadata = get_metadata({})
        storage = RedisImpressionsStorage(adapter, metadata)

        impressions = [
            Impression('key1', 'feature1', 'on', 'some_label', 123456, 'buck1', 321654),
            Impression('key2', 'feature2', 'on', 'some_label', 123456, 'buck1', 321654),
            Impression('key3', 'feature2', 'on', 'some_label', 123456, 'buck1', 321654),
            Impression('key4', 'feature1', 'on', 'some_label', 123456, 'buck1', 321654)
        ]

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

        assert storage._wrap_impressions(impressions) == to_validate

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

    def test_add_impressions_to_pipe(self, mocker):
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

        storage.add_impressions_to_pipe(impressions, adapter)
        assert adapter.rpush.mock_calls == [mocker.call('SPLITIO.impressions', *to_validate)]


class RedisEventsStorageTests(object):  # pylint: disable=too-few-public-methods
    """Redis Impression storage test cases."""

    def test_add_events(self, mocker):
        """Test that adding impressions to storage works."""
        adapter = mocker.Mock(spec=RedisAdapter)
        metadata = get_metadata({})

        storage = RedisEventsStorage(adapter, metadata)

        events = [
            EventWrapper(event=Event('key1', 'user', 'purchase', 10, 123456, None),  size=32768),
            EventWrapper(event=Event('key2', 'user', 'purchase', 10, 123456, None),  size=32768),
            EventWrapper(event=Event('key3', 'user', 'purchase', 10, 123456, None),  size=32768),
            EventWrapper(event=Event('key4', 'user', 'purchase', 10, 123456, None),  size=32768),
        ]
        assert storage.put(events) is True

        list_of_raw_events = [json.dumps({
            'm': {  # METADATA PORTION
                's': metadata.sdk_version,
                'n': metadata.instance_name,
                'i': metadata.instance_ip,
            },
            'e': {  # EVENT PORTION
                'key': e.event.key,
                'trafficTypeName': e.event.traffic_type_name,
                'eventTypeId': e.event.event_type_id,
                'value': e.event.value,
                'timestamp': e.event.timestamp,
                'properties': e.event.properties,
            }
        }) for e in events]

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
    """Redis Telemetry storage test cases."""

    def test_init(self, mocker):
        redis_telemetry = RedisTelemetryStorage(mocker.Mock(), mocker.Mock())
        assert(redis_telemetry._redis_client is not None)
        assert(redis_telemetry._sdk_metadata is not None)
        assert(isinstance(redis_telemetry._method_latencies, MethodLatencies))
        assert(isinstance(redis_telemetry._method_exceptions, MethodExceptions))
        assert(isinstance(redis_telemetry._tel_config, TelemetryConfig))
        assert(redis_telemetry._make_pipe is not None)

    @mock.patch('splitio.models.telemetry.TelemetryConfig.record_config')
    def test_record_config(self, mocker):
        redis_telemetry = RedisTelemetryStorage(mocker.Mock(), mocker.Mock())
        redis_telemetry.record_config(mocker.Mock(), mocker.Mock())
        assert(mocker.called)

    @mock.patch('splitio.storage.adapters.redis.RedisAdapter.hset')
    def test_push_config_stats(self, mocker):
        adapter = build({})
        redis_telemetry = RedisTelemetryStorage(adapter, mocker.Mock())
        redis_telemetry.push_config_stats()
        assert(mocker.called)

    def test_format_config_stats(self, mocker):
        redis_telemetry = RedisTelemetryStorage(mocker.Mock(), mocker.Mock())
        json_value = redis_telemetry._format_config_stats()
        stats = redis_telemetry._tel_config.get_stats()
        assert(json_value == json.dumps({
            'aF': stats['aF'],
            'rF': stats['rF'],
            'sT': stats['sT'],
            'oM': stats['oM'],
            'fsT': redis_telemetry._tel_config.get_flag_sets(),
            'fsI': redis_telemetry._tel_config.get_invalid_flag_sets(),
            't': redis_telemetry.pop_config_tags(),
        }))

    def test_record_active_and_redundant_factories(self, mocker):
        redis_telemetry = RedisTelemetryStorage(mocker.Mock(), mocker.Mock())
        active_factory_count = 1
        redundant_factory_count = 2
        redis_telemetry.record_active_and_redundant_factories(1, 2)
        assert (redis_telemetry._tel_config._active_factory_count == active_factory_count)
        assert (redis_telemetry._tel_config._redundant_factory_count == redundant_factory_count)

    def test_add_latency_to_pipe(self, mocker):
        adapter = build({})
        metadata = SdkMetadata('python-1.1.1', 'hostname', 'ip')
        redis_telemetry = RedisTelemetryStorage(adapter, metadata)
        pipe = adapter._decorated.pipeline()

        def _mocked_hincrby(*args, **kwargs):
            assert(args[1] == RedisTelemetryStorage._TELEMETRY_LATENCIES_KEY)
            assert(args[2][-11:] == 'treatment/0')
            assert(args[3] == 1)
        # should increment bucket 0
        with mock.patch('redis.client.Pipeline.hincrby', _mocked_hincrby):
            redis_telemetry.add_latency_to_pipe(MethodExceptionsAndLatencies.TREATMENT, 0, pipe)

        def _mocked_hincrby2(*args, **kwargs):
            assert(args[1] == RedisTelemetryStorage._TELEMETRY_LATENCIES_KEY)
            assert(args[2][-11:] == 'treatment/3')
            assert(args[3] == 1)
        # should increment bucket 3
        with mock.patch('redis.client.Pipeline.hincrby', _mocked_hincrby2):
            redis_telemetry.add_latency_to_pipe(MethodExceptionsAndLatencies.TREATMENT, 3, pipe)

    def test_record_exception(self, mocker):
        def _mocked_hincrby(*args, **kwargs):
            assert(args[1] == RedisTelemetryStorage._TELEMETRY_EXCEPTIONS_KEY)
            assert(args[2] == 'python-1.1.1/hostname/ip/treatment')
            assert(args[3] == 1)

        adapter = build({})
        metadata = SdkMetadata('python-1.1.1', 'hostname', 'ip')
        redis_telemetry = RedisTelemetryStorage(adapter, metadata)
        with mock.patch('redis.client.Pipeline.hincrby', _mocked_hincrby):
            with mock.patch('redis.client.Pipeline.execute') as mock_method:
                mock_method.return_value = [1]
                redis_telemetry.record_exception(MethodExceptionsAndLatencies.TREATMENT)

    def test_expire_latency_keys(self, mocker):
        redis_telemetry = RedisTelemetryStorage(mocker.Mock(), mocker.Mock())
        def _mocked_method(*args, **kwargs):
            assert(args[1] == RedisTelemetryStorage._TELEMETRY_LATENCIES_KEY)
            assert(args[2] == RedisTelemetryStorage._TELEMETRY_KEY_DEFAULT_TTL)
            assert(args[3] == 1)
            assert(args[4] == 2)

        with mock.patch('splitio.storage.redis.RedisTelemetryStorage.expire_keys', _mocked_method):
            redis_telemetry.expire_latency_keys(1, 2)

    @mock.patch('redis.client.Redis.expire')
    def test_expire_keys(self, mocker):
        adapter = build({})
        metadata = SdkMetadata('python-1.1.1', 'hostname', 'ip')
        redis_telemetry = RedisTelemetryStorage(adapter, metadata)
        redis_telemetry.expire_keys('key', 12, 1, 2)
        assert(not mocker.called)
        redis_telemetry.expire_keys('key', 12, 2, 2)
        assert(mocker.called)
