"""Impressions API tests module."""

import pytest
from splitio.api import events, client, APIException
from splitio.models.events import Event
from splitio.client.util import SdkMetadata


class EventsAPITests(object):
    """Impressions API test cases."""

    def test_post_events(self, mocker):
        """Test impressions posting API call."""
        httpclient = mocker.Mock(spec=client.HttpClient)
        httpclient.post.return_value = client.HttpResponse(200, '')
        sdk_metadata = SdkMetadata('python-1.2.3', 'some_machine_name', '123.123.123.123')
        events_api = events.EventsAPI(httpclient, 'some_api_key', sdk_metadata)
        response = events_api.flush_events([
            Event('k1', 'user', 'purchase', 12.50, 123456),
            Event('k2', 'user', 'purchase', 12.50, 123456),
            Event('k3', 'user', 'purchase', None, 123456),
            Event('k4', 'user', 'purchase', None, 123456)
        ])

        call_made = httpclient.post.mock_calls[0]

        # validate positional arguments
        assert call_made[1] == ('events', '/events/bulk', 'some_api_key')

        # validate key-value args (headers)
        assert call_made[2]['extra_headers'] == {
            'SplitSDKVersion': 'python-1.2.3',
            'SplitSDKMachineIP': '123.123.123.123',
            'SplitSDKMachineName': 'some_machine_name'
        }

        # validate key-value args (body)
        assert call_made[2]['body'] == [
            {'key': 'k1', 'trafficTypeName': 'user', 'eventTypeId': 'purchase', 'value': 12.50, 'timestamp': 123456},
            {'key': 'k2', 'trafficTypeName': 'user', 'eventTypeId': 'purchase', 'value': 12.50, 'timestamp': 123456},
            {'key': 'k3', 'trafficTypeName': 'user', 'eventTypeId': 'purchase', 'value': None, 'timestamp': 123456},
            {'key': 'k4', 'trafficTypeName': 'user', 'eventTypeId': 'purchase', 'value': None, 'timestamp': 123456}
        ]

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message')
        httpclient.post.side_effect = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = events_api.flush_events([
                Event('k1', 'user', 'purchase', 12.50, 123456),
                Event('k2', 'user', 'purchase', 12.50, 123456),
                Event('k3', 'user', 'purchase', None, 123456),
                Event('k4', 'user', 'purchase', None, 123456)
            ])
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'
