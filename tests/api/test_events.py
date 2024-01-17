"""Impressions API tests module."""

import pytest
import unittest.mock as mock

from splitio.api import events, client, APIException
from splitio.models.events import Event
from splitio.client.util import get_metadata
from splitio.client.config import DEFAULT_CONFIG
from splitio.version import __version__
from splitio.engine.telemetry import TelemetryStorageProducer, TelemetryStorageProducerAsync
from splitio.storage.inmemmory import InMemoryTelemetryStorage, InMemoryTelemetryStorageAsync


class EventsAPITests(object):
    """Impressions API test cases."""
    events = [
        Event('k1', 'user', 'purchase', 12.50, 123456, None),
        Event('k2', 'user', 'purchase', 12.50, 123456, None),
        Event('k3', 'user', 'purchase', None, 123456, {"test": 1234}),
        Event('k4', 'user', 'purchase', None, 123456, None)
    ]
    eventsExpected = [
        {'key': 'k1', 'trafficTypeName': 'user', 'eventTypeId': 'purchase', 'value': 12.50, 'timestamp': 123456, 'properties': None},
        {'key': 'k2', 'trafficTypeName': 'user', 'eventTypeId': 'purchase', 'value': 12.50, 'timestamp': 123456, 'properties': None},
        {'key': 'k3', 'trafficTypeName': 'user', 'eventTypeId': 'purchase', 'value': None, 'timestamp': 123456, 'properties': {"test": 1234}},
        {'key': 'k4', 'trafficTypeName': 'user', 'eventTypeId': 'purchase', 'value': None, 'timestamp': 123456, 'properties': None},
    ]

    def test_post_events(self, mocker):
        """Test impressions posting API call."""
        httpclient = mocker.Mock(spec=client.HttpClient)
        httpclient.post.return_value = client.HttpResponse(200, '', {})
        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': True, 'machineName': 'some_machine_name', 'machineIp': '123.123.123.123'})
        sdk_metadata = get_metadata(cfg)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        events_api = events.EventsAPI(httpclient, 'some_api_key', sdk_metadata, telemetry_runtime_producer)
        response = events_api.flush_events(self.events)
        assert events_api._LOGGER.name == 'splitio.api.events'

        call_made = httpclient.post.mock_calls[0]

        # validate positional arguments
        assert call_made[1] == ('events', 'events/bulk', 'some_api_key')

        # validate key-value args (headers)
        assert call_made[2]['extra_headers'] == {
            'SplitSDKVersion': 'python-%s' % __version__,
            'SplitSDKMachineIP': '123.123.123.123',
            'SplitSDKMachineName': 'some_machine_name'
        }

        # validate key-value args (body)
        assert call_made[2]['body'] == self.eventsExpected

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message')
        httpclient.post.side_effect = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = events_api.flush_events(self.events)
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'

    def test_post_events_ip_address_disabled(self, mocker):
        """Test impressions posting API call."""
        httpclient = mocker.Mock(spec=client.HttpClient)
        httpclient.post.return_value = client.HttpResponse(200, '', {})
        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': False})
        sdk_metadata = get_metadata(cfg)
        events_api = events.EventsAPI(httpclient, 'some_api_key', sdk_metadata, mocker.Mock())
        response = events_api.flush_events(self.events)

        call_made = httpclient.post.mock_calls[0]

        # validate positional arguments
        assert call_made[1] == ('events', 'events/bulk', 'some_api_key')

        # validate key-value args (headers)
        assert call_made[2]['extra_headers'] == {
            'SplitSDKVersion': 'python-%s' % __version__,
        }

        # validate key-value args (body)
        assert call_made[2]['body'] == self.eventsExpected


class EventsAPIAsyncTests(object):
    """Impressions Async API test cases."""
    events = [
        Event('k1', 'user', 'purchase', 12.50, 123456, None),
        Event('k2', 'user', 'purchase', 12.50, 123456, None),
        Event('k3', 'user', 'purchase', None, 123456, {"test": 1234}),
        Event('k4', 'user', 'purchase', None, 123456, None)
    ]
    eventsExpected = [
        {'key': 'k1', 'trafficTypeName': 'user', 'eventTypeId': 'purchase', 'value': 12.50, 'timestamp': 123456, 'properties': None},
        {'key': 'k2', 'trafficTypeName': 'user', 'eventTypeId': 'purchase', 'value': 12.50, 'timestamp': 123456, 'properties': None},
        {'key': 'k3', 'trafficTypeName': 'user', 'eventTypeId': 'purchase', 'value': None, 'timestamp': 123456, 'properties': {"test": 1234}},
        {'key': 'k4', 'trafficTypeName': 'user', 'eventTypeId': 'purchase', 'value': None, 'timestamp': 123456, 'properties': None},
    ]

    @pytest.mark.asyncio
    async def test_post_events(self, mocker):
        """Test impressions posting API call."""
        httpclient = mocker.Mock(spec=client.HttpClientAsync)
        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': True, 'machineName': 'some_machine_name', 'machineIp': '123.123.123.123'})
        sdk_metadata = get_metadata(cfg)
        telemetry_storage = InMemoryTelemetryStorageAsync()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        events_api = events.EventsAPIAsync(httpclient, 'some_api_key', sdk_metadata, telemetry_runtime_producer)

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

        assert events_api._LOGGER.name == 'asyncio'
        response = await events_api.flush_events(self.events)
        # validate positional arguments
        assert self.verb == 'events'
        assert self.url == 'events/bulk'
        assert self.key == 'some_api_key'

        # validate key-value args (headers)
        assert self.headers == {
            'SplitSDKVersion': 'python-%s' % __version__,
            'SplitSDKMachineIP': '123.123.123.123',
            'SplitSDKMachineName': 'some_machine_name'
        }

        # validate key-value args (body)
        assert self.body == self.eventsExpected

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message')
        httpclient.post = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = await events_api.flush_events(self.events)
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'

    @pytest.mark.asyncio
    async def test_post_events_ip_address_disabled(self, mocker):
        """Test impressions posting API call."""
        httpclient = mocker.Mock(spec=client.HttpClientAsync)
        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': False})
        sdk_metadata = get_metadata(cfg)
        events_api = events.EventsAPIAsync(httpclient, 'some_api_key', sdk_metadata, mocker.Mock())

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

        response = await events_api.flush_events(self.events)

        # validate positional arguments
        assert self.verb == 'events'
        assert self.url == 'events/bulk'
        assert self.key == 'some_api_key'

        # validate key-value args (headers)
        assert self.headers == {
            'SplitSDKVersion': 'python-%s' % __version__,
        }

        # validate key-value args (body)
        assert self.body == self.eventsExpected
