"""Impressions API tests module."""

import pytest
import unittest.mock as mock

from splitio.api import telemetry, client, APIException
#from splitio.models.telemetry import
from splitio.client.util import get_metadata
from splitio.client.config import DEFAULT_CONFIG
from splitio.version import __version__
from splitio.engine.telemetry import TelemetryStorageProducer, TelemetryStorageProducerAsync
from splitio.storage.inmemmory import InMemoryTelemetryStorage, InMemoryTelemetryStorageAsync


class TelemetryAPITests(object):
    """Telemetry API test cases."""

    def test_record_unique_keys(self, mocker):
        """Test telemetry posting unique keys."""
        httpclient = mocker.Mock(spec=client.HttpClient)
        httpclient.post.return_value = client.HttpResponse(200, '', {})
        uniques = {'keys': [1, 2, 3]}
        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': True, 'machineName': 'some_machine_name', 'machineIp': '123.123.123.123'})
        sdk_metadata = get_metadata(cfg)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_api = telemetry.TelemetryAPI(httpclient, 'some_api_key', sdk_metadata, telemetry_runtime_producer)
        response = telemetry_api.record_unique_keys(uniques)

        call_made = httpclient.post.mock_calls[0]

        # validate positional arguments
        assert call_made[1] == ('telemetry', 'v1/keys/ss', 'some_api_key')

        # validate key-value args (headers)
        assert call_made[2]['extra_headers'] == {
            'SplitSDKVersion': 'python-%s' % __version__,
            'SplitSDKMachineIP': '123.123.123.123',
            'SplitSDKMachineName': 'some_machine_name'
        }

        # validate key-value args (body)
        assert call_made[2]['body'] == uniques

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message')
        httpclient.post.side_effect = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = telemetry_api.record_unique_keys(uniques)
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'

    def test_record_init(self, mocker):
        """Test telemetry posting init configs."""
        httpclient = mocker.Mock(spec=client.HttpClient)
        httpclient.post.return_value = client.HttpResponse(200, '', {})
        uniques = {'keys': [1, 2, 3]}
        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': True, 'machineName': 'some_machine_name', 'machineIp': '123.123.123.123'})
        sdk_metadata = get_metadata(cfg)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_api = telemetry.TelemetryAPI(httpclient, 'some_api_key', sdk_metadata, telemetry_runtime_producer)
        response = telemetry_api.record_init(uniques)

        call_made = httpclient.post.mock_calls[0]

        # validate positional arguments
        assert call_made[1] == ('telemetry', '/v1/metrics/config', 'some_api_key')

        # validate key-value args (headers)
        assert call_made[2]['extra_headers'] == {
            'SplitSDKVersion': 'python-%s' % __version__,
            'SplitSDKMachineIP': '123.123.123.123',
            'SplitSDKMachineName': 'some_machine_name'
        }

        # validate key-value args (body)
        assert call_made[2]['body'] == uniques

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message')
        httpclient.post.side_effect = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = telemetry_api.record_init(uniques)
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'

    def test_record_stats(self, mocker):
        """Test telemetry posting stats."""
        httpclient = mocker.Mock(spec=client.HttpClient)
        httpclient.post.return_value = client.HttpResponse(200, '', {})
        uniques = {'keys': [1, 2, 3]}
        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': True, 'machineName': 'some_machine_name', 'machineIp': '123.123.123.123'})
        sdk_metadata = get_metadata(cfg)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_api = telemetry.TelemetryAPI(httpclient, 'some_api_key', sdk_metadata, telemetry_runtime_producer)
        response = telemetry_api.record_stats(uniques)

        call_made = httpclient.post.mock_calls[0]

        # validate positional arguments
        assert call_made[1] == ('telemetry', '/v1/metrics/usage', 'some_api_key')

        # validate key-value args (headers)
        assert call_made[2]['extra_headers'] == {
            'SplitSDKVersion': 'python-%s' % __version__,
            'SplitSDKMachineIP': '123.123.123.123',
            'SplitSDKMachineName': 'some_machine_name'
        }

        # validate key-value args (body)
        assert call_made[2]['body'] == uniques

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message')
        httpclient.post.side_effect = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = telemetry_api.record_stats(uniques)
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'


class TelemetryAPIAsyncTests(object):
    """Telemetry API test cases."""

    @pytest.mark.asyncio
    async def test_record_unique_keys(self, mocker):
        """Test telemetry posting unique keys."""
        httpclient = mocker.Mock(spec=client.HttpClientAsync)
        uniques = {'keys': [1, 2, 3]}
        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': True, 'machineName': 'some_machine_name', 'machineIp': '123.123.123.123'})
        sdk_metadata = get_metadata(cfg)
        telemetry_storage = InMemoryTelemetryStorageAsync()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_api = telemetry.TelemetryAPIAsync(httpclient, 'some_api_key', sdk_metadata, telemetry_runtime_producer)
        self.verb = None
        self.url = None
        self.key = None
        self.headers = None
        self.body = None
        async def post(verb, url, key, body, extra_headers):
            self.url = url
            self.verb = verb
            self.key = key
            self.headers = extra_headers
            self.body = body
            return client.HttpResponse(200, '', {})
        httpclient.post = post

        response = await telemetry_api.record_unique_keys(uniques)
        assert self.verb == 'telemetry'
        assert self.url == 'v1/keys/ss'
        assert self.key == 'some_api_key'

        # validate key-value args (headers)
        assert self.headers == {
            'SplitSDKVersion': 'python-%s' % __version__,
            'SplitSDKMachineIP': '123.123.123.123',
            'SplitSDKMachineName': 'some_machine_name'
        }

        # validate key-value args (body)
        assert self.body == uniques

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message')
        httpclient.post = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = await telemetry_api.record_unique_keys(uniques)
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'

    @pytest.mark.asyncio
    async def test_record_init(self, mocker):
        """Test telemetry posting unique keys."""
        httpclient = mocker.Mock(spec=client.HttpClientAsync)
        uniques = {'keys': [1, 2, 3]}
        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': True, 'machineName': 'some_machine_name', 'machineIp': '123.123.123.123'})
        sdk_metadata = get_metadata(cfg)
        telemetry_storage = InMemoryTelemetryStorageAsync()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_api = telemetry.TelemetryAPIAsync(httpclient, 'some_api_key', sdk_metadata, telemetry_runtime_producer)
        self.verb = None
        self.url = None
        self.key = None
        self.headers = None
        self.body = None
        async def post(verb, url, key, body, extra_headers):
            self.url = url
            self.verb = verb
            self.key = key
            self.headers = extra_headers
            self.body = body
            return client.HttpResponse(200, '', {})
        httpclient.post = post

        response = await telemetry_api.record_init(uniques)
        assert self.verb == 'telemetry'
        assert self.url == '/v1/metrics/config'
        assert self.key == 'some_api_key'

        # validate key-value args (headers)
        assert self.headers == {
            'SplitSDKVersion': 'python-%s' % __version__,
            'SplitSDKMachineIP': '123.123.123.123',
            'SplitSDKMachineName': 'some_machine_name'
        }

        # validate key-value args (body)
        assert self.body == uniques

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message')
        httpclient.post = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = await telemetry_api.record_init(uniques)
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'

    @pytest.mark.asyncio
    async def test_record_stats(self, mocker):
        """Test telemetry posting unique keys."""
        httpclient = mocker.Mock(spec=client.HttpClientAsync)
        uniques = {'keys': [1, 2, 3]}
        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': True, 'machineName': 'some_machine_name', 'machineIp': '123.123.123.123'})
        sdk_metadata = get_metadata(cfg)
        telemetry_storage = InMemoryTelemetryStorageAsync()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_api = telemetry.TelemetryAPIAsync(httpclient, 'some_api_key', sdk_metadata, telemetry_runtime_producer)
        self.verb = None
        self.url = None
        self.key = None
        self.headers = None
        self.body = None
        async def post(verb, url, key, body, extra_headers):
            self.url = url
            self.verb = verb
            self.key = key
            self.headers = extra_headers
            self.body = body
            return client.HttpResponse(200, '', {})
        httpclient.post = post

        response = await telemetry_api.record_stats(uniques)
        assert self.verb == 'telemetry'
        assert self.url == '/v1/metrics/usage'
        assert self.key == 'some_api_key'

        # validate key-value args (headers)
        assert self.headers == {
            'SplitSDKVersion': 'python-%s' % __version__,
            'SplitSDKMachineIP': '123.123.123.123',
            'SplitSDKMachineName': 'some_machine_name'
        }

        # validate key-value args (body)
        assert self.body == uniques

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message')
        httpclient.post = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = await telemetry_api.record_stats(uniques)
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'
