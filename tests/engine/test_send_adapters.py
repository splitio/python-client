import unittest.mock as mock
import ast

from splitio.engine.impressions.adapters import InMemorySenderAdapter, RedisSenderAdapter
from splitio.api.telemetry import TelemetryAPI
from splitio.storage.adapters.redis import RedisAdapter
from splitio.engine.impressions.manager import Counter

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

        sender_adapter = RedisSenderAdapter(mocker.Mock())
        for i in range(0,1):
            assert(sorted(ast.literal_eval(sender_adapter._uniques_formatter(uniques)[i])["ks"]) == sorted(formatted[i]["ks"]))

    def test_build_counters(self, mocker):
        """Test formatting counters dict to json."""

        counters = [
            Counter.CountPerFeature('f1', 123, 2),
            Counter.CountPerFeature('f2', 123, 123),
        ]
        formatted = [
            {'f': 'f1', 'm': 123, 'rc': 2},
            {'f': 'f2', 'm': 123, 'rc': 123},
        ]

        sender_adapter = RedisSenderAdapter(mocker.Mock())
        for i in range(0,1):
            assert(sorted(ast.literal_eval(sender_adapter._build_counters(counters))['pf'][i]) == sorted(formatted[i]))

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

    @mock.patch('splitio.storage.adapters.redis.RedisAdapter.rpush')
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
