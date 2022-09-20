import unittest.mock as mock

from splitio.engine.impressions.adapters import InMemorySenderAdapter
from splitio.api.telemetry import TelemetryAPI

class InMemorySenderAdapterTests(object):
    """In memory sender adapter test."""

    def test_uniques_formatter(self, mocker):
        """Test formatting dict to json."""

        uniques = {"feature1": set({'key1', 'key2', 'key3'}),
                   "feature2": set({'key6', 'key1', 'key10'}),
                   }
        formatted = {'keys':  [
            {'f': 'feature1', 'ks': ['key1', 'key2', 'key3']},
            {'f': 'feature2', 'ks': ['key1', 'key6', 'key10']},
        ]}

        sender_adapter = InMemorySenderAdapter(mocker.Mock())
        for i in range(0,1):
            assert(sorted(sender_adapter._uniques_formatter(uniques)["keys"][i]["ks"]) == sorted(formatted["keys"][i]["ks"]))


    @mock.patch('splitio.api.telemetry.TelemetryAPI.record_unique_keys')
    def test_record_unique_keys(self, mocker):
        """Test sending unique keys."""

        uniques = {"feature1": set({'key1', 'key2', 'key3'}),
                   "feature2": set({'key1', 'key2', 'key3'}),
                   }
        telemetry_api = TelemetryAPI(mocker.Mock(), 'some_api_key', mocker.Mock())
        sender_adapter = InMemorySenderAdapter(telemetry_api)
        sender_adapter.record_unique_keys(uniques)

        assert(mocker.called)
