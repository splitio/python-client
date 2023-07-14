"""Impressions API tests module."""

import pytest
import unittest.mock as mock

from splitio.api import events, client, APIException
from splitio.models.events import Event
from splitio.client.util import get_metadata
from splitio.client.config import DEFAULT_CONFIG
from splitio.version import __version__
from splitio.engine.telemetry import TelemetryStorageProducer
from splitio.storage.inmemmory import InMemoryTelemetryStorage


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
