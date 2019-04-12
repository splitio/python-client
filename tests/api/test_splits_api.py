"""Split API tests module."""

import pytest
from splitio.api import splits, client, APIException


class SplitAPITests(object):
    """Split API test cases."""

    def test_fetch_split_changes(self, mocker):
        """Test split changes fetching API call."""
        httpclient = mocker.Mock(spec=client.HttpClient)
        httpclient.get.return_value = client.HttpResponse(200, '{"prop1": "value1"}')
        split_api = splits.SplitsAPI(httpclient, 'some_api_key')
        response = split_api.fetch_splits(123)

        assert response['prop1'] == 'value1'
        assert httpclient.get.mock_calls == [mocker.call('sdk', '/splitChanges', 'some_api_key', {'since': 123})]

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message', Exception('something'))
        httpclient.get.side_effect = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = split_api.fetch_splits(123)
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'
