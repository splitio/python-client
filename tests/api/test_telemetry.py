"""Telemetry API tests module."""

import pytest
from splitio.api import telemetry, client, APIException
from splitio.client.util import SdkMetadata


class EventsAPITests(object):
    """Impressions API test cases."""

    def test_post_latencies(self, mocker):
        """Test impressions posting API call."""
        httpclient = mocker.Mock(spec=client.HttpClient)
        httpclient.post.return_value = client.HttpResponse(200, '')
        sdk_metadata = SdkMetadata('python-1.2.3', 'some_machine_name', '123.123.123.123')
        telemetry_api = telemetry.TelemetryAPI(httpclient, 'some_api_key', sdk_metadata)
        response = telemetry_api.flush_latencies({
            'l1': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
        })

        call_made = httpclient.post.mock_calls[0]

        # validate positional arguments
        assert call_made[1] == ('events', '/metrics/times', 'some_api_key')

        # validate key-value args (headers)
        assert call_made[2]['extra_headers'] == {
            'SplitSDKVersion': 'python-1.2.3',
            'SplitSDKMachineIP': '123.123.123.123',
            'SplitSDKMachineName': 'some_machine_name'
        }

        # validate key-value args (body)
        assert call_made[2]['body'] == [{
            'name': 'l1',
            'latencies': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
        }]

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message', Exception('something'))
        httpclient.post.side_effect = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = telemetry_api.flush_latencies({
                'l1': [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22]
            })
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'

    def test_post_counters(self, mocker):
        """Test impressions posting API call."""
        httpclient = mocker.Mock(spec=client.HttpClient)
        httpclient.post.return_value = client.HttpResponse(200, '')
        sdk_metadata = SdkMetadata('python-1.2.3', 'some_machine_name', '123.123.123.123')
        telemetry_api = telemetry.TelemetryAPI(httpclient, 'some_api_key', sdk_metadata)
        response = telemetry_api.flush_counters({'counter1': 1, 'counter2': 2})

        call_made = httpclient.post.mock_calls[0]

        # validate positional arguments
        assert call_made[1] == ('events', '/metrics/counters', 'some_api_key')

        # validate key-value args (headers)
        assert call_made[2]['extra_headers'] == {
            'SplitSDKVersion': 'python-1.2.3',
            'SplitSDKMachineIP': '123.123.123.123',
            'SplitSDKMachineName': 'some_machine_name'
        }

        # validate key-value args (body)
        assert call_made[2]['body'] == [
            {'name': 'counter1', 'delta': 1},
            {'name': 'counter2', 'delta': 2}
        ]

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message', Exception('something'))
        httpclient.post.side_effect = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = telemetry_api.flush_counters({'counter1': 1, 'counter2': 2})
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'

    def test_post_gauge(self, mocker):
        """Test impressions posting API call."""
        httpclient = mocker.Mock(spec=client.HttpClient)
        httpclient.post.return_value = client.HttpResponse(200, '')
        sdk_metadata = SdkMetadata('python-1.2.3', 'some_machine_name', '123.123.123.123')
        telemetry_api = telemetry.TelemetryAPI(httpclient, 'some_api_key', sdk_metadata)
        response = telemetry_api.flush_gauges({'gauge1': 1, 'gauge2': 2})

        call_made = httpclient.post.mock_calls[0]

        # validate positional arguments
        assert call_made[1] == ('events', '/metrics/gauge', 'some_api_key')

        # validate key-value args (headers)
        assert call_made[2]['extra_headers'] == {
            'SplitSDKVersion': 'python-1.2.3',
            'SplitSDKMachineIP': '123.123.123.123',
            'SplitSDKMachineName': 'some_machine_name'
        }

        # validate key-value args (body)
        assert call_made[2]['body'] == [
            {'name': 'gauge1', 'value': 1},
            {'name': 'gauge2', 'value': 2}
        ]

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message', Exception('something'))
        httpclient.post.side_effect = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = telemetry_api.flush_gauges({'gauge1': 1, 'gauge2': 2})
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'
