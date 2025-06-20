"""Impressions API tests module."""

import pytest
import unittest.mock as mock

from splitio.api import impressions, client, APIException
from splitio.models.impressions import Impression
from splitio.engine.impressions.impressions import ImpressionsMode
from splitio.engine.impressions.manager import Counter
from splitio.client.util import get_metadata
from splitio.client.config import DEFAULT_CONFIG
from splitio.version import __version__
from splitio.engine.telemetry import TelemetryStorageProducer, TelemetryStorageProducerAsync
from splitio.storage.inmemmory import InMemoryTelemetryStorage, InMemoryTelemetryStorageAsync

impressions_mock = [
    Impression('k1', 'f1', 'on', 'l1', 123456, 'b1', 321654, {}),
    Impression('k2', 'f2', 'off', 'l1', 123456, 'b1', 321654, {}),
    Impression('k3', 'f1', 'on', 'l1', 123456, 'b1', 321654, {})
]
expectedImpressions = [{
    'f': 'f1',
    'i': [
        {'k': 'k1', 'b': 'b1', 't': 'on', 'r': 'l1', 'm': 321654, 'c': 123456, 'pt': None},
        {'k': 'k3', 'b': 'b1', 't': 'on', 'r': 'l1', 'm': 321654, 'c': 123456, 'pt': None},
    ],
}, {
    'f': 'f2',
    'i': [
        {'k': 'k2', 'b': 'b1', 't': 'off', 'r': 'l1', 'm': 321654, 'c': 123456, 'pt': None},
    ]
}]

counters = [
    Counter.CountPerFeature('f1', 123, 2),
    Counter.CountPerFeature('f2', 123, 123),
    Counter.CountPerFeature('f1', 456, 111),
    Counter.CountPerFeature('f2', 456, 222)
]

expected_counters = {
    'pf': [
        {'f': 'f1', 'm': 123, 'rc': 2},
        {'f': 'f2', 'm': 123, 'rc': 123},
        {'f': 'f1', 'm': 456, 'rc': 111},
        {'f': 'f2', 'm': 456, 'rc': 222},
    ]
}

class ImpressionsAPITests(object):
    """Impressions API test cases."""

    def test_post_impressions(self, mocker):
        """Test impressions posting API call."""
        httpclient = mocker.Mock(spec=client.HttpClient)
        httpclient.post.return_value = client.HttpResponse(200, '', {})
        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': True, 'machineName': 'some_machine_name', 'machineIp': '123.123.123.123'})
        sdk_metadata = get_metadata(cfg)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impressions_api = impressions.ImpressionsAPI(httpclient, 'some_api_key', sdk_metadata, telemetry_runtime_producer)
        response = impressions_api.flush_impressions(impressions_mock)

        call_made = httpclient.post.mock_calls[0]

        # validate positional arguments
        assert call_made[1] == ('events', 'testImpressions/bulk', 'some_api_key')

        # validate key-value args (headers)
        assert call_made[2]['extra_headers'] == {
            'SplitSDKVersion': 'python-%s' % __version__,
            'SplitSDKMachineIP': '123.123.123.123',
            'SplitSDKMachineName': 'some_machine_name',
            'SplitSDKImpressionsMode': 'OPTIMIZED'
        }

        # validate key-value args (body)
        assert call_made[2]['body'] == expectedImpressions

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message')
        httpclient.post.side_effect = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = impressions_api.flush_impressions(impressions_mock)
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'

    def test_post_impressions_ip_address_disabled(self, mocker):
        """Test impressions posting API call."""
        httpclient = mocker.Mock(spec=client.HttpClient)
        httpclient.post.return_value = client.HttpResponse(200, '', {})
        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': False})
        sdk_metadata = get_metadata(cfg)
        impressions_api = impressions.ImpressionsAPI(httpclient, 'some_api_key', sdk_metadata, mocker.Mock(), ImpressionsMode.DEBUG)
        response = impressions_api.flush_impressions(impressions_mock)

        call_made = httpclient.post.mock_calls[0]

        # validate positional arguments
        assert call_made[1] == ('events', 'testImpressions/bulk', 'some_api_key')

        # validate key-value args (headers)
        assert call_made[2]['extra_headers'] == {
            'SplitSDKVersion': 'python-%s' % __version__,
            'SplitSDKImpressionsMode': 'DEBUG'
        }

        # validate key-value args (body)
        assert call_made[2]['body'] == expectedImpressions

    def test_post_counters(self, mocker):
        """Test impressions posting API call."""
        httpclient = mocker.Mock(spec=client.HttpClient)
        httpclient.post.return_value = client.HttpResponse(200, '', {})
        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': True, 'machineName': 'some_machine_name', 'machineIp': '123.123.123.123'})
        sdk_metadata = get_metadata(cfg)
        impressions_api = impressions.ImpressionsAPI(httpclient, 'some_api_key', sdk_metadata, mocker.Mock())
        response = impressions_api.flush_counters(counters)

        call_made = httpclient.post.mock_calls[0]

        # validate positional arguments
        assert call_made[1] == ('events', 'testImpressions/count', 'some_api_key')

        # validate key-value args (headers)
        assert call_made[2]['extra_headers'] == {
            'SplitSDKVersion': 'python-%s' % __version__,
            'SplitSDKMachineIP': '123.123.123.123',
            'SplitSDKMachineName': 'some_machine_name',
            'SplitSDKImpressionsMode': 'OPTIMIZED'
        }

        # validate key-value args (body)
        assert call_made[2]['body'] == expected_counters

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message')
        httpclient.post.side_effect = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = impressions_api.flush_counters(counters)
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'


class ImpressionsAPIAsyncTests(object):
    """Impressions API test cases."""

    @pytest.mark.asyncio
    async def test_post_impressions(self, mocker):
        """Test impressions posting API call."""
        httpclient = mocker.Mock(spec=client.HttpClientAsync)
        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': True, 'machineName': 'some_machine_name', 'machineIp': '123.123.123.123'})
        sdk_metadata = get_metadata(cfg)
        telemetry_storage = InMemoryTelemetryStorageAsync()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impressions_api = impressions.ImpressionsAPIAsync(httpclient, 'some_api_key', sdk_metadata, telemetry_runtime_producer)

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

        response = await impressions_api.flush_impressions(impressions_mock)

        # validate positional arguments
        assert self.verb == 'events'
        assert self.url == 'testImpressions/bulk'
        assert self.key == 'some_api_key'

        # validate key-value args (headers)
        assert self.headers == {
            'SplitSDKVersion': 'python-%s' % __version__,
            'SplitSDKMachineIP': '123.123.123.123',
            'SplitSDKMachineName': 'some_machine_name',
            'SplitSDKImpressionsMode': 'OPTIMIZED'
        }

        # validate key-value args (body)
        assert self.body == expectedImpressions

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message')
        httpclient.post = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = await impressions_api.flush_impressions(impressions_mock)
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'

    @pytest.mark.asyncio
    async def test_post_impressions_ip_address_disabled(self, mocker):
        """Test impressions posting API call."""
        httpclient = mocker.Mock(spec=client.HttpClientAsync)
        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': False})
        sdk_metadata = get_metadata(cfg)
        impressions_api = impressions.ImpressionsAPIAsync(httpclient, 'some_api_key', sdk_metadata, mocker.Mock(), ImpressionsMode.DEBUG)

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

        response = await impressions_api.flush_impressions(impressions_mock)

        # validate positional arguments
        assert self.verb == 'events'
        assert self.url == 'testImpressions/bulk'
        assert self.key == 'some_api_key'

        # validate key-value args (headers)
        assert self.headers == {
            'SplitSDKVersion': 'python-%s' % __version__,
            'SplitSDKImpressionsMode': 'DEBUG'
        }

        # validate key-value args (body)
        assert self.body == expectedImpressions

    @pytest.mark.asyncio
    async def test_post_counters(self, mocker):
        """Test impressions posting API call."""
        httpclient = mocker.Mock(spec=client.HttpClientAsync)
        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': True, 'machineName': 'some_machine_name', 'machineIp': '123.123.123.123'})
        sdk_metadata = get_metadata(cfg)
        impressions_api = impressions.ImpressionsAPIAsync(httpclient, 'some_api_key', sdk_metadata, mocker.Mock())

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

        response = await impressions_api.flush_counters(counters)

        # validate positional arguments
        assert self.verb == 'events'
        assert self.url == 'testImpressions/count'
        assert self.key == 'some_api_key'

        # validate key-value args (headers)
        assert self.headers == {
            'SplitSDKVersion': 'python-%s' % __version__,
            'SplitSDKMachineIP': '123.123.123.123',
            'SplitSDKMachineName': 'some_machine_name',
            'SplitSDKImpressionsMode': 'OPTIMIZED'
        }

        # validate key-value args (body)
        assert self.body == expected_counters

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message')
        httpclient.post = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = await impressions_api.flush_counters(counters)
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'
