"""Split API tests module."""

import pytest
import unittest.mock as mock

from splitio.api import splits, client, APIException
from splitio.api.commons import FetchOptions
from splitio.client.util import SdkMetadata
from splitio.engine.telemetry import TelemetryStorageProducer
from splitio.storage.inmemmory import InMemoryTelemetryStorage


class SplitAPITests(object):
    """Split API test cases."""

    def test_fetch_split_changes(self, mocker):
        """Test split changes fetching API call."""
        httpclient = mocker.Mock(spec=client.HttpClient)
        httpclient.get.return_value = client.HttpResponse(200, '{"prop1": "value1"}')
        split_api = splits.SplitsAPI(httpclient, 'some_api_key', SdkMetadata('1.0', 'some', '1.2.3.4'), mocker.Mock())

        response = split_api.fetch_splits(123, FetchOptions(False, None, ['set2', 'set1']))
        assert response['prop1'] == 'value1'
        assert httpclient.get.mock_calls == [mocker.call('sdk', '/splitChanges', 'some_api_key',
                                                         extra_headers={
                                                             'SplitSDKVersion': '1.0',
                                                             'SplitSDKMachineIP': '1.2.3.4',
                                                             'SplitSDKMachineName': 'some'
                                                         },
                                                         query={'since': 123, 'sets': 'set1,set2'})]

        httpclient.reset_mock()
        response = split_api.fetch_splits(123, FetchOptions(True))
        assert response['prop1'] == 'value1'
        assert httpclient.get.mock_calls == [mocker.call('sdk', '/splitChanges', 'some_api_key',
                                                         extra_headers={
                                                             'SplitSDKVersion': '1.0',
                                                             'SplitSDKMachineIP': '1.2.3.4',
                                                             'SplitSDKMachineName': 'some',
                                                             'Cache-Control': 'no-cache'
                                                         },
                                                         query={'since': 123})]

        httpclient.reset_mock()
        response = split_api.fetch_splits(123, FetchOptions(True, 123, ['set3']))
        assert response['prop1'] == 'value1'
        assert httpclient.get.mock_calls == [mocker.call('sdk', '/splitChanges', 'some_api_key',
                                                         extra_headers={
                                                             'SplitSDKVersion': '1.0',
                                                             'SplitSDKMachineIP': '1.2.3.4',
                                                             'SplitSDKMachineName': 'some',
                                                             'Cache-Control': 'no-cache'
                                                         },
                                                         query={'since': 123, 'till': 123, 'sets': 'set3'})]

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message')
        httpclient.get.side_effect = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = split_api.fetch_splits(123, FetchOptions())
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'

    @mock.patch('splitio.engine.telemetry.TelemetryRuntimeProducer.record_sync_latency')
    def test_split_telemetry(self, mocker):
        httpclient = mocker.Mock(spec=client.HttpClient)
        httpclient.get.return_value = client.HttpResponse(200, '{"prop1": "value1"}')
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        split_api = splits.SplitsAPI(httpclient, 'some_api_key', SdkMetadata('1.0', 'some', '1.2.3.4'), telemetry_runtime_producer)

        response = split_api.fetch_splits(123, FetchOptions())
        assert(mocker.called)
