"""HTTPClient test module."""
import pytest
from splitio.api import client_async

class MockResponse:
    def __init__(self, text, status):
        self._text = text
        self.status = status

    async def text(self):
        return self._text

    async def __aexit__(self, exc_type, exc, tb):
        pass

    async def __aenter__(self):
        return self

class HttpClientAsyncTests(object):
    """Http Client test cases."""

    @pytest.mark.asyncio
    async def test_get(self, mocker):
        """Test HTTP GET verb requests."""
        response_mock = MockResponse('ok', 200)
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.api.client_async.aiohttp.ClientSession.get', new=get_mock)
        httpclient = client_async.HttpClientAsync()
        response = await httpclient.get('sdk', '/test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
        assert response.status_code == 200
        assert response.body == 'ok'
        call = mocker.call(
            client_async.HttpClientAsync.SDK_URL + '/test1',
            headers={'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json'},
            params={'param1': 123},
            timeout=None
        )
        assert get_mock.mock_calls == [call]
        get_mock.reset_mock()

        response = await httpclient.get('events', '/test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
        call = mocker.call(
            client_async.HttpClientAsync.EVENTS_URL + '/test1',
            headers={'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json'},
            params={'param1': 123},
            timeout=None
        )
        assert get_mock.mock_calls == [call]
        assert response.status_code == 200
        assert response.body == 'ok'

    @pytest.mark.asyncio
    async def test_get_custom_urls(self, mocker):
        """Test HTTP GET verb requests."""
        response_mock = MockResponse('ok', 200)
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.api.client_async.aiohttp.ClientSession.get', new=get_mock)
        httpclient = client_async.HttpClientAsync(sdk_url='https://sdk.com', events_url='https://events.com')
        response = await httpclient.get('sdk', '/test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
        call = mocker.call(
            'https://sdk.com/test1',
            headers={'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json'},
            params={'param1': 123},
            timeout=None
        )
        assert get_mock.mock_calls == [call]
        assert response.status_code == 200
        assert response.body == 'ok'
        get_mock.reset_mock()

        response = await httpclient.get('events', '/test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
        call = mocker.call(
            'https://events.com/test1',
            headers={'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json'},
            params={'param1': 123},
            timeout=None
        )
        assert response.status_code == 200
        assert response.body == 'ok'
        assert get_mock.mock_calls == [call]


    async def test_post(self, mocker):
        """Test HTTP POST verb requests."""
        response_mock = MockResponse('ok', 200)
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.api.client_async.aiohttp.ClientSession.post', new=get_mock)
        httpclient = client_async.HttpClientAsync()
        response = await httpclient.post('sdk', '/test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
        call = mocker.call(
            client_async.HttpClientAsync.SDK_URL + '/test1',
            json={'p1': 'a'},
            headers={'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json'},
            params={'param1': 123},
            timeout=None
        )
        assert response.status_code == 200
        assert response.body == 'ok'
        assert get_mock.mock_calls == [call]
        get_mock.reset_mock()

        response = await httpclient.post('events', '/test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
        call = mocker.call(
            client_async.HttpClientAsync.EVENTS_URL + '/test1',
            json={'p1': 'a'},
            headers={'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json'},
            params={'param1': 123},
            timeout=None
        )
        assert response.status_code == 200
        assert response.body == 'ok'
        assert get_mock.mock_calls == [call]

    async def test_post_custom_urls(self, mocker):
        """Test HTTP GET verb requests."""
        response_mock = MockResponse('ok', 200)
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.api.client_async.aiohttp.ClientSession.post', new=get_mock)
        httpclient = client_async.HttpClientAsync(sdk_url='https://sdk.com', events_url='https://events.com')
        response = await httpclient.post('sdk', '/test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
        call = mocker.call(
            'https://sdk.com' + '/test1',
            json={'p1': 'a'},
            headers={'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json'},
            params={'param1': 123},
            timeout=None
        )
        assert response.status_code == 200
        assert response.body == 'ok'
        assert get_mock.mock_calls == [call]
        get_mock.reset_mock()

        response = await httpclient.post('events', '/test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
        call = mocker.call(
            'https://events.com' + '/test1',
            json={'p1': 'a'},
            headers={'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json'},
            params={'param1': 123},
            timeout=None
        )
        assert response.status_code == 200
        assert response.body == 'ok'
        assert get_mock.mock_calls == [call]
