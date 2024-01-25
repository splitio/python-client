"""Split API tests module."""

import pytest
import unittest.mock as mock

from splitio.api import splits, client, APIException
from splitio.api.commons import FetchOptions
from splitio.client.util import SdkMetadata

class SplitAPITests(object):
    """Split API test cases."""

    def test_fetch_split_changes(self, mocker):
        """Test split changes fetching API call."""
        httpclient = mocker.Mock(spec=client.HttpClient)
        httpclient.get.return_value = client.HttpResponse(200, '{"prop1": "value1"}', {})
        split_api = splits.SplitsAPI(httpclient, 'some_api_key', SdkMetadata('1.0', 'some', '1.2.3.4'), mocker.Mock())

        response = split_api.fetch_splits(123, FetchOptions(False, None, 'set1,set2'))
        assert response['prop1'] == 'value1'
        assert httpclient.get.mock_calls == [mocker.call('sdk', 'splitChanges', 'some_api_key',
                                                         extra_headers={
                                                             'SplitSDKVersion': '1.0',
                                                             'SplitSDKMachineIP': '1.2.3.4',
                                                             'SplitSDKMachineName': 'some'
                                                         },
                                                         query={'since': 123, 'sets': 'set1,set2'})]

        httpclient.reset_mock()
        response = split_api.fetch_splits(123, FetchOptions(True, 123, 'set3'))
        assert response['prop1'] == 'value1'
        assert httpclient.get.mock_calls == [mocker.call('sdk', 'splitChanges', 'some_api_key',
                                                         extra_headers={
                                                             'SplitSDKVersion': '1.0',
                                                             'SplitSDKMachineIP': '1.2.3.4',
                                                             'SplitSDKMachineName': 'some',
                                                             'Cache-Control': 'no-cache'
                                                         },
                                                         query={'since': 123, 'till': 123, 'sets': 'set3'})]

        httpclient.reset_mock()
        response = split_api.fetch_splits(123, FetchOptions(True, 123))
        assert response['prop1'] == 'value1'
        assert httpclient.get.mock_calls == [mocker.call('sdk', 'splitChanges', 'some_api_key',
                                                         extra_headers={
                                                             'SplitSDKVersion': '1.0',
                                                             'SplitSDKMachineIP': '1.2.3.4',
                                                             'SplitSDKMachineName': 'some',
                                                             'Cache-Control': 'no-cache'
                                                         },
                                                         query={'since': 123, 'till': 123})]

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message')
        httpclient.get.side_effect = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = split_api.fetch_splits(123, FetchOptions())
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'


class SplitAPIAsyncTests(object):
    """Split async API test cases."""

    @pytest.mark.asyncio
    async def test_fetch_split_changes(self, mocker):
        """Test split changes fetching API call."""
        httpclient = mocker.Mock(spec=client.HttpClientAsync)
        split_api = splits.SplitsAPIAsync(httpclient, 'some_api_key', SdkMetadata('1.0', 'some', '1.2.3.4'), mocker.Mock())
        self.verb = None
        self.url = None
        self.key = None
        self.headers = None
        self.query = None
        async def get(verb, url, key, query, extra_headers):
            self.url = url
            self.verb = verb
            self.key = key
            self.headers = extra_headers
            self.query = query
            return client.HttpResponse(200, '{"prop1": "value1"}', {})
        httpclient.get = get

        response = await split_api.fetch_splits(123, FetchOptions(False, None, 'set1,set2'))
        assert response['prop1'] == 'value1'
        assert self.verb == 'sdk'
        assert self.url == 'splitChanges'
        assert self.key == 'some_api_key'
        assert self.headers == {
            'SplitSDKVersion': '1.0',
            'SplitSDKMachineIP': '1.2.3.4',
            'SplitSDKMachineName': 'some'
        }
        assert self.query == {'since': 123, 'sets': 'set1,set2'}

        httpclient.reset_mock()
        response = await split_api.fetch_splits(123, FetchOptions(True, 123, 'set3'))
        assert response['prop1'] == 'value1'
        assert self.verb == 'sdk'
        assert self.url == 'splitChanges'
        assert self.key == 'some_api_key'
        assert self.headers == {
            'SplitSDKVersion': '1.0',
            'SplitSDKMachineIP': '1.2.3.4',
            'SplitSDKMachineName': 'some',
            'Cache-Control': 'no-cache'
        }
        assert self.query == {'since': 123, 'till': 123, 'sets': 'set3'}

        httpclient.reset_mock()
        response = await split_api.fetch_splits(123, FetchOptions(True, 123))
        assert response['prop1'] == 'value1'
        assert self.verb == 'sdk'
        assert self.url == 'splitChanges'
        assert self.key == 'some_api_key'
        assert self.headers == {
            'SplitSDKVersion': '1.0',
            'SplitSDKMachineIP': '1.2.3.4',
            'SplitSDKMachineName': 'some',
            'Cache-Control': 'no-cache'
        }
        assert self.query == {'since': 123, 'till': 123}

        httpclient.reset_mock()
        def raise_exception(*args, **kwargs):
            raise client.HttpClientException('some_message')
        httpclient.get = raise_exception
        with pytest.raises(APIException) as exc_info:
            response = await split_api.fetch_splits(123, FetchOptions())
            assert exc_info.type == APIException
            assert exc_info.value.message == 'some_message'
