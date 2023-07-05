"""Redis storage test module."""
# pylint: disable=no-self-use

import json
import time
import unittest.mock as mock
import pytest

from splitio.client.util import get_metadata, SdkMetadata
from splitio.optional.loaders import asyncio
from splitio.storage.redis import RedisEventsStorage, RedisImpressionsStorage, \
    RedisSegmentStorage, RedisSegmentStorageAsync, RedisSplitStorage, RedisSplitStorageAsync, RedisTelemetryStorage
from splitio.storage.adapters.redis import RedisAdapter, RedisAdapterException, build
from redis.asyncio.client import Redis as aioredis
from splitio.storage.adapters import redis
from splitio.models.segments import Segment
from splitio.models.impressions import Impression
from splitio.models.events import Event, EventWrapper
from splitio.models.telemetry import MethodExceptions, MethodLatencies, TelemetryConfig, MethodExceptionsAndLatencies


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

class RedisSplitStorageAsyncTests(object):
    """Redis split storage test cases."""

    @pytest.mark.asyncio
    async def test_get_split(self, mocker):
        """Test retrieving a split works."""
        redis_mock = await aioredis.from_url("redis://localhost")
        adapter = redis.RedisAdapterAsync(redis_mock, 'some_prefix')

        self.redis_ret = None
        self.name = None
        async def get(sel, name):
            self.name = name
            self.redis_ret = '{"name": "some_split"}'
            return self.redis_ret
        mocker.patch('splitio.storage.adapters.redis.RedisAdapterAsync.get', new=get)

        from_raw = mocker.Mock()
        mocker.patch('splitio.storage.redis.splits.from_raw', new=from_raw)

        storage = RedisSplitStorageAsync(adapter)
        await storage.get('some_split')

        assert self.name == 'SPLITIO.split.some_split'
        assert self.redis_ret == '{"name": "some_split"}'

        # Test that a missing split returns None and doesn't call from_raw
        from_raw.reset_mock()
        self.name = None
        async def get2(sel, name):
            self.name = name
            return None
        mocker.patch('splitio.storage.adapters.redis.RedisAdapterAsync.get', new=get2)

        result = await storage.get('some_split')
        assert result is None
        assert self.name == 'SPLITIO.split.some_split'
        assert not from_raw.mock_calls

    @pytest.mark.asyncio
    async def test_get_split_with_cache(self, mocker):
        """Test retrieving a split works."""
        redis_mock = await aioredis.from_url("redis://localhost")
        adapter = redis.RedisAdapterAsync(redis_mock, 'some_prefix')

        self.redis_ret = None
        self.name = None
        async def get(sel, name):
            self.name = name
            self.redis_ret = '{"name": "some_split"}'
            return self.redis_ret
        mocker.patch('splitio.storage.adapters.redis.RedisAdapterAsync.get', new=get)

        from_raw = mocker.Mock()
        mocker.patch('splitio.storage.redis.splits.from_raw', new=from_raw)

        storage = RedisSplitStorageAsync(adapter, True, 1)
        await storage.get('some_split')
        assert self.name == 'SPLITIO.split.some_split'
        assert self.redis_ret == '{"name": "some_split"}'

        # hit the cache:
        self.name = None
        await storage.get('some_split')
        self.name = None
        await storage.get('some_split')
        self.name = None
        await storage.get('some_split')
        assert self.name == None

        # Test that a missing split returns None and doesn't call from_raw
        from_raw.reset_mock()
        self.name = None
        async def get2(sel, name):
            self.name = name
            return None
        mocker.patch('splitio.storage.adapters.redis.RedisAdapterAsync.get', new=get2)

        # Still cached
        result = await storage.get('some_split')
        assert result is not None
        assert self.name == None
        await asyncio.sleep(1)  # wait for expiration
        result = await storage.get('some_split')
        assert self.name == 'SPLITIO.split.some_split'
        assert result is None

    @pytest.mark.asyncio
    async def test_get_splits_with_cache(self, mocker):
        """Test retrieving a list of passed splits."""
        redis_mock = await aioredis.from_url("redis://localhost")
        adapter = redis.RedisAdapterAsync(redis_mock, 'some_prefix')
        storage = RedisSplitStorageAsync(adapter, True, 1)

        self.redis_ret = None
        self.name = None
        async def mget(sel, name):
            self.name = name
            self.redis_ret = ['{"name": "split1"}', '{"name": "split2"}', None]
            return self.redis_ret
        mocker.patch('splitio.storage.adapters.redis.RedisAdapterAsync.mget', new=mget)

        from_raw = mocker.Mock()
        mocker.patch('splitio.storage.redis.splits.from_raw', new=from_raw)

        result = await storage.fetch_many(['split1', 'split2', 'split3'])
        assert len(result) == 3

        assert '{"name": "split1"}' in self.redis_ret
        assert '{"name": "split2"}' in self.redis_ret

        assert result['split1'] is not None
        assert result['split2'] is not None
        assert 'split3' in result

        # fetch again
        self.name = None
        result = await storage.fetch_many(['split1', 'split2', 'split3'])
        assert result['split1'] is not None
        assert result['split2'] is not None
        assert 'split3' in result
        assert self.name == None

        # wait for expire
        await asyncio.sleep(1)
        self.name = None
        result = await storage.fetch_many(['split1', 'split2', 'split3'])
        assert self.name == ['SPLITIO.split.split1', 'SPLITIO.split.split2', 'SPLITIO.split.split3']

    @pytest.mark.asyncio
    async def test_get_changenumber(self, mocker):
        """Test fetching changenumber."""
        redis_mock = await aioredis.from_url("redis://localhost")
        adapter = redis.RedisAdapterAsync(redis_mock, 'some_prefix')
        storage = RedisSplitStorageAsync(adapter)

        self.redis_ret = None
        self.name = None
        async def get(sel, name):
            self.name = name
            self.redis_ret = '-1'
            return self.redis_ret
        mocker.patch('splitio.storage.adapters.redis.RedisAdapterAsync.get', new=get)

        assert await storage.get_change_number() == -1
        assert self.name == 'SPLITIO.splits.till'

    @pytest.mark.asyncio
    async def test_get_all_splits(self, mocker):
        """Test fetching all splits."""
        from_raw = mocker.Mock()
        mocker.patch('splitio.storage.redis.splits.from_raw', new=from_raw)

        redis_mock = await aioredis.from_url("redis://localhost")
        adapter = redis.RedisAdapterAsync(redis_mock, 'some_prefix')
        storage = RedisSplitStorageAsync(adapter)

        self.redis_ret = None
        self.name = None
        async def mget(sel, name):
            self.name = name
            self.redis_ret = ['{"name": "split1"}', '{"name": "split2"}', '{"name": "split3"}']
            return self.redis_ret
        mocker.patch('splitio.storage.adapters.redis.RedisAdapterAsync.mget', new=mget)

        self.key = None
        self.keys_ret = None
        async def keys(sel, key):
            self.key = key
            self.keys_ret = [
            'SPLITIO.split.split1',
            'SPLITIO.split.split2',
            'SPLITIO.split.split3'
            ]
            return self.keys_ret
        mocker.patch('splitio.storage.adapters.redis.RedisAdapterAsync.keys', new=keys)

        await storage.get_all_splits()

        assert self.key == 'SPLITIO.split.*'
        assert self.keys_ret == ['SPLITIO.split.split1', 'SPLITIO.split.split2', 'SPLITIO.split.split3']
        assert len(from_raw.mock_calls) == 3
        assert mocker.call({'name': 'split1'}) in from_raw.mock_calls
        assert mocker.call({'name': 'split2'}) in from_raw.mock_calls
        assert mocker.call({'name': 'split3'}) in from_raw.mock_calls

    @pytest.mark.asyncio
    async def test_get_split_names(self, mocker):
        """Test getching split names."""
        redis_mock = await aioredis.from_url("redis://localhost")
        adapter = redis.RedisAdapterAsync(redis_mock, 'some_prefix')
        storage = RedisSplitStorageAsync(adapter)

        self.key = None
        self.keys_ret = None
        async def keys(sel, key):
            self.key = key
            self.keys_ret = [
            'SPLITIO.split.split1',
            'SPLITIO.split.split2',
            'SPLITIO.split.split3'
            ]
            return self.keys_ret
        mocker.patch('splitio.storage.adapters.redis.RedisAdapterAsync.keys', new=keys)

        assert await storage.get_split_names() == ['split1', 'split2', 'split3']

    @pytest.mark.asyncio
    async def test_is_valid_traffic_type(self, mocker):
        """Test that traffic type validation works."""
        redis_mock = await aioredis.from_url("redis://localhost")
        adapter = redis.RedisAdapterAsync(redis_mock, 'some_prefix')
        storage = RedisSplitStorageAsync(adapter)

        async def get(sel, name):
            return '1'
        mocker.patch('splitio.storage.adapters.redis.RedisAdapterAsync.get', new=get)
        assert await storage.is_valid_traffic_type('any') is True

        async def get2(sel, name):
            return '0'
        mocker.patch('splitio.storage.adapters.redis.RedisAdapterAsync.get', new=get2)
        assert await storage.is_valid_traffic_type('any') is False

        async def get3(sel, name):
            return None
        mocker.patch('splitio.storage.adapters.redis.RedisAdapterAsync.get', new=get3)
        assert await storage.is_valid_traffic_type('any') is False

    @pytest.mark.asyncio
    async def test_is_valid_traffic_type_with_cache(self, mocker):
        """Test that traffic type validation works."""
        redis_mock = await aioredis.from_url("redis://localhost")
        adapter = redis.RedisAdapterAsync(redis_mock, 'some_prefix')
        storage = RedisSplitStorageAsync(adapter, True, 1)

        async def get(sel, name):
            return '1'
        mocker.patch('splitio.storage.adapters.redis.RedisAdapterAsync.get', new=get)
        assert await storage.is_valid_traffic_type('any') is True

        async def get2(sel, name):
            return '0'
        mocker.patch('splitio.storage.adapters.redis.RedisAdapterAsync.get', new=get2)
        assert await storage.is_valid_traffic_type('any') is True
        await asyncio.sleep(1)
        assert await storage.is_valid_traffic_type('any') is False

        async def get3(sel, name):
            return None
        mocker.patch('splitio.storage.adapters.redis.RedisAdapterAsync.get', new=get3)
        await asyncio.sleep(1)
        assert await storage.is_valid_traffic_type('any') is False

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

class RedisSegmentStorageAsyncTests(object):
    """Redis segment storage test cases."""

    @pytest.mark.asyncio
    async def test_fetch_segment(self, mocker):
        """Test fetching a whole segment."""
        redis_mock = await aioredis.from_url("redis://localhost")
        adapter = redis.RedisAdapterAsync(redis_mock, 'some_prefix')

        self.key = None
        async def smembers(key):
            self.key = key
            return set(["key1", "key2", "key3"])
        adapter.smembers = smembers

        self.key2 = None
        async def get(key):
            self.key2 = key
            return '100'
        adapter.get = get

        from_raw = mocker.Mock()
        mocker.patch('splitio.storage.redis.segments.from_raw', new=from_raw)

        storage = RedisSegmentStorageAsync(adapter)
        result = await storage.get('some_segment')
        assert isinstance(result, Segment)
        assert result.name == 'some_segment'
        assert result.contains('key1')
        assert result.contains('key2')
        assert result.contains('key3')
        assert result.change_number == 100
        assert self.key ==  'SPLITIO.segment.some_segment'
        assert self.key2 == 'SPLITIO.segment.some_segment.till'

        # Assert that if segment doesn't exist, None is returned
        from_raw.reset_mock()
        async def smembers2(key):
            self.key = key
            return set()
        adapter.smembers = smembers2
        assert await storage.get('some_segment') is None

    @pytest.mark.asyncio
    async def test_fetch_change_number(self, mocker):
        """Test fetching change number."""
        redis_mock = await aioredis.from_url("redis://localhost")
        adapter = redis.RedisAdapterAsync(redis_mock, 'some_prefix')

        self.key = None
        async def get(key):
            self.key = key
            return '100'
        adapter.get = get

        storage = RedisSegmentStorageAsync(adapter)
        result = await storage.get_change_number('some_segment')
        assert result == 100
        assert self.key == 'SPLITIO.segment.some_segment.till'

    @pytest.mark.asyncio
    async def test_segment_contains(self, mocker):
        """Test segment contains functionality."""
        redis_mock = await aioredis.from_url("redis://localhost")
        adapter = redis.RedisAdapterAsync(redis_mock, 'some_prefix')
        storage = RedisSegmentStorageAsync(adapter)
        self.key = None
        self.segment = None
        async def sismember(segment, key):
            self.key = key
            self.segment = segment
            return True
        adapter.sismember = sismember

        assert await storage.segment_contains('some_segment', 'some_key') is True
        assert self.segment == 'SPLITIO.segment.some_segment'
        assert self.key == 'some_key'


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
            't': redis_telemetry.pop_config_tags()
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
