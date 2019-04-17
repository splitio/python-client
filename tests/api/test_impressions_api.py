"""Impressions API tests module."""

import pytest
from splitio.api import impressions, client, APIException
from splitio.models.impressions import Impression
from splitio.client.util import SdkMetadata

class ImpressionsAPITests(object):
    """Impressions API test cases."""

    def test_post_impressions(self, mocker):
        """Test impressions posting API call."""
        httpclient = mocker.Mock(spec=client.HttpClient)
        httpclient.post.return_value = client.HttpResponse(200, '')
        sdk_metadata = SdkMetadata('python-1.2.3', 'some_machine_name', '123.123.123.123')
        impressions_api = impressions.ImpressionsAPI(httpclient, 'some_api_key', sdk_metadata)
        response = impressions_api.flush_impressions([
            Impression('k1', 'f1', 'on', 'l1', 123456, 'b1', 321654),
            Impression('k2', 'f2', 'off', 'l1', 123456, 'b1', 321654),
            Impression('k3', 'f1', 'on', 'l1', 123456, 'b1', 321654),
        ])

        call_made = httpclient.post.mock_calls[0]

        # validate positional arguments
        assert call_made[1] == ('events', '/testImpressions/bulk', 'some_api_key')

        # validate key-value args (headers)
        assert call_made[2]['extra_headers'] == {
            'SplitSDKVersion': 'python-1.2.3',
            'SplitSDKMachineIP': '123.123.123.123',
            'SplitSDKMachineName': 'some_machine_name'
        }

        # validate key-value args (body)
        assert call_made[2]['body'] == [
            {
                'testName': 'f1',
                'keyImpressions': [
                    {'keyName': 'k1', 'bucketingKey': 'b1', 'treatment': 'on', 'label': 'l1', 'time': 321654, 'changeNumber': 123456},
                    {'keyName': 'k3', 'bucketingKey': 'b1', 'treatment': 'on', 'label': 'l1', 'time': 321654, 'changeNumber': 123456},
                ],
            },
            {
                'testName': 'f2',
                'keyImpressions': [
                    {'keyName': 'k2', 'bucketingKey': 'b1', 'treatment': 'off', 'label': 'l1', 'time': 321654, 'changeNumber': 123456},
                ]
            }
        ]

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message')
        httpclient.post.side_effect = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = impressions_api.flush_impressions([
                Impression('k1', 'f1', 'on', 'l1', 123456, 'b1', 321654),
                Impression('k2', 'f2', 'off', 'l1', 123456, 'b1', 321654),
                Impression('k3', 'f1', 'on', 'l1', 123456, 'b1', 321654),
            ])
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'
