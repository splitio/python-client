import unittest.mock as mock
import ast
import json
import pytest

from splitio.engine.impressions.adapters import InMemorySenderAdapter, RedisSenderAdapter, PluggableSenderAdapter
from splitio.engine.impressions import adapters
from splitio.api.telemetry import TelemetryAPI
from splitio.storage.adapters.redis import RedisAdapter
from splitio.engine.impressions.manager import Counter
from tests.storage.test_pluggable import StorageMockAdapter


class InMemorySenderAdapterTests(object):
    """In memory sender adapter test."""

    def test_uniques_formatter(self, mocker):
        """Test formatting dict to json."""

        uniques = {"feature1": set({'key1', 'key2', 'key3'}),
                   "feature2": set({'key6', 'key1', 'key10'}),
                   }
        formatted = [
            {'f': 'feature1', 'ks': ['key1', 'key2', 'key3']},
            {'f': 'feature2', 'ks': ['key1', 'key6', 'key10']},
        ]

        sender_adapter = InMemorySenderAdapter(mocker.Mock())
        for i in range(0,1):
            assert(sorted(sender_adapter._uniques_formatter(uniques)[i]["ks"]) == sorted(formatted[i]["ks"]))


    @mock.patch('splitio.api.telemetry.TelemetryAPI.record_unique_keys')
    def test_record_unique_keys(self, mocker):
        """Test sending unique keys."""

        uniques = {"feature1": set({'key1', 'key2', 'key3'}),
                   "feature2": set({'key1', 'key2', 'key3'}),
                   }
        telemetry_api = TelemetryAPI(mocker.Mock(), 'some_api_key', mocker.Mock(), mocker.Mock())
        sender_adapter = InMemorySenderAdapter(telemetry_api)
        sender_adapter.record_unique_keys(uniques)

        assert(mocker.called)

class RedisSenderAdapterTests(object):
    """Redis sender adapter test."""

    def test_uniques_formatter(self, mocker):
        """Test formatting dict to json."""

        uniques = {"feature1": set({'key1', 'key2', 'key3'}),
                   "feature2": set({'key6', 'key1', 'key10'}),
                   }
        formatted = [
            {'f': 'feature1', 'ks': ['key1', 'key2', 'key3']},
            {'f': 'feature2', 'ks': ['key6', 'key1', 'key10']},
        ]

        for i in range(0,1):
            assert(sorted(ast.literal_eval(adapters._uniques_formatter(uniques)[i])["ks"]) == sorted(formatted[i]["ks"]))

    @mock.patch('splitio.storage.adapters.redis.RedisAdapter.rpush')
    def test_record_unique_keys(self, mocker):
        """Test sending unique keys."""

        uniques = {"feature1": set({'key1', 'key2', 'key3'}),
                   "feature2": set({'key1', 'key2', 'key3'}),
                   }
        redis_client = RedisAdapter(mocker.Mock(), mocker.Mock())
        sender_adapter = RedisSenderAdapter(redis_client)
        sender_adapter.record_unique_keys(uniques)

        assert(mocker.called)

    @mock.patch('splitio.storage.adapters.redis.RedisPipelineAdapter.hincrby')
    def test_flush_counters(self, mocker):
        """Test sending counters."""

        counters = [
            Counter.CountPerFeature('f1', 123, 2),
            Counter.CountPerFeature('f2', 123, 123),
        ]
        redis_client = RedisAdapter(mocker.Mock(), mocker.Mock())
        sender_adapter = RedisSenderAdapter(redis_client)
        sender_adapter.flush_counters(counters)

        assert(mocker.called)

    @mock.patch('splitio.storage.adapters.redis.RedisAdapter.expire')
    def test_expire_keys(self, mocker):
        """Test set expire key."""

        total_keys = 100
        inserted = 10
        redis_client = RedisAdapter(mocker.Mock(), mocker.Mock())
        sender_adapter = RedisSenderAdapter(redis_client)
        sender_adapter._expire_keys(mocker.Mock(), mocker.Mock(), total_keys, inserted)
        assert(not mocker.called)

        total_keys = 100
        inserted = 100
        sender_adapter._expire_keys(mocker.Mock(), mocker.Mock(), total_keys, inserted)
        assert(mocker.called)

class PluggableSenderAdapterTests(object):
    """Pluggable sender adapter test."""

    def test_record_unique_keys(self, mocker):
        """Test sending unique keys."""
        adapter = StorageMockAdapter()
        sender_adapter = PluggableSenderAdapter(adapter)

        uniques = {"feature1": set({"key1", "key2", "key3"}),
                   "feature2": set({"key1", "key6", "key10"}),
                   }
        formatted = [
            '{"f": "feature1", "ks": ["key3", "key2", "key1"]}',
            '{"f": "feature2", "ks": ["key1", "key10", "key6"]}',
        ]

        sender_adapter.record_unique_keys(uniques)
        assert(sorted(json.loads(adapter._keys[adapters._MTK_QUEUE_KEY][0])["ks"]) == sorted(json.loads(formatted[0])["ks"]))
        assert(sorted(json.loads(adapter._keys[adapters._MTK_QUEUE_KEY][1])["ks"]) == sorted(json.loads(formatted[1])["ks"]))
        assert(json.loads(adapter._keys[adapters._MTK_QUEUE_KEY][0])["f"] == "feature1")
        assert(json.loads(adapter._keys[adapters._MTK_QUEUE_KEY][1])["f"] == "feature2")
        assert(adapter._expire[adapters._MTK_QUEUE_KEY] == adapters._MTK_KEY_DEFAULT_TTL)
        sender_adapter.record_unique_keys(uniques)
        assert(adapter._expire[adapters._MTK_QUEUE_KEY] != -1)

    def test_flush_counters(self, mocker):
        """Test sending counters."""
        adapter = StorageMockAdapter()
        sender_adapter = PluggableSenderAdapter(adapter)

        counters = [
            Counter.CountPerFeature('f1', 123, 2),
            Counter.CountPerFeature('f2', 123, 123),
        ]

        sender_adapter.flush_counters(counters)
        assert(adapter._keys[adapters._IMP_COUNT_QUEUE_KEY + "." + 'f1::123'] == 2)
        assert(adapter._keys[adapters._IMP_COUNT_QUEUE_KEY + "." + 'f2::123'] == 123)
        assert(adapter._expire[adapters._IMP_COUNT_QUEUE_KEY + "." + 'f1::123'] == adapters._IMP_COUNT_KEY_DEFAULT_TTL)
        sender_adapter.flush_counters(counters)
        assert(adapter._expire[adapters._IMP_COUNT_QUEUE_KEY + "." + 'f2::123'] == adapters._IMP_COUNT_KEY_DEFAULT_TTL)
