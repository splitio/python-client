"""Split API tests module."""

import pytest

import unittest.mock as mock

from splitio.api import auth, client, APIException
from splitio.client.util import get_metadata
from splitio.client.config import DEFAULT_CONFIG
from splitio.version import __version__
from splitio.engine.telemetry import TelemetryStorageProducer
from splitio.storage.inmemmory import InMemoryTelemetryStorage

class AuthAPITests(object):
    """Auth API test cases."""

    @mock.patch('splitio.engine.telemetry.TelemetryRuntimeProducer.record_sync_latency')
    def test_auth(self, mocker):
        """Test auth API call."""
        token = "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE2MDIwODgxMjcsImlhdCI6MTYwMjA4NDUyN30.5_MjWonhs6yoFhw44hNJm3H7_YMjXpSW105DwjjppqE"
        httpclient = mocker.Mock(spec=client.HttpClient)
        payload = '{{"pushEnabled": true, "token": "{token}"}}'.format(token=token)
        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': True, 'machineName': 'some_machine_name', 'machineIp': '123.123.123.123'})
        sdk_metadata = get_metadata(cfg)
        httpclient.get.return_value = client.HttpResponse(200, payload)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        auth_api = auth.AuthAPI(httpclient, 'some_api_key', sdk_metadata, telemetry_runtime_producer)
        response = auth_api.authenticate()

        assert(mocker.called)
        assert response.push_enabled == True
        assert response.token == token

        call_made = httpclient.get.mock_calls[0]

        # validate positional arguments
        assert call_made[1] == ('auth', '/v2/auth', 'some_api_key')

        # validate key-value args (headers)
        assert call_made[2]['extra_headers'] == {
            'SplitSDKVersion': 'python-%s' % __version__,
            'SplitSDKMachineIP': '123.123.123.123',
            'SplitSDKMachineName': 'some_machine_name'
        }

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message')
        httpclient.get.side_effect = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = auth_api.authenticate()
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'

    @mock.patch('splitio.engine.telemetry.TelemetryRuntimeProducer.record_auth_rejections')
    def test_telemetry_auth_rejections(self, mocker):
        """Test auth API call."""
        token = "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE2MDIwODgxMjcsImlhdCI6MTYwMjA4NDUyN30.5_MjWonhs6yoFhw44hNJm3H7_YMjXpSW105DwjjppqE"
        httpclient = mocker.Mock(spec=client.HttpClient)
        payload = '{{"pushEnabled": true, "token": "{token}"}}'.format(token=token)
        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': True, 'machineName': 'some_machine_name', 'machineIp': '123.123.123.123'})
        sdk_metadata = get_metadata(cfg)
        httpclient.get.return_value = client.HttpResponse(401, payload)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        auth_api = auth.AuthAPI(httpclient, 'some_api_key', sdk_metadata, telemetry_runtime_producer)
        try:
            auth_api.authenticate()
        except:
            pass
        assert(mocker.called)
