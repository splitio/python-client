"""HTTPClient test module."""
import pytest
from splitio.api import client

class HttpClientTests(object):
    """Http Client test cases."""

    def test_get(self, mocker):
        """Test HTTP GET verb requests."""
        response_mock = mocker.Mock()
        response_mock.status_code = 200
        response_mock.headers = {}
        response_mock.text = 'ok'
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.api.client.requests.get', new=get_mock)
        httpclient = client.HttpClient()
        response = httpclient.get('sdk', 'test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
        call = mocker.call(
            client.SDK_URL + '/test1',
            headers={'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json'},
            params={'param1': 123},
            timeout=None
        )
        assert response.status_code == 200
        assert response.body == 'ok'
        assert get_mock.mock_calls == [call]
        get_mock.reset_mock()

        response = httpclient.get('events', 'test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
        call = mocker.call(
            client.EVENTS_URL + '/test1',
            headers={'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json'},
            params={'param1': 123},
            timeout=None
        )
        assert get_mock.mock_calls == [call]
        assert response.status_code == 200
        assert response.body == 'ok'

    def test_get_custom_urls(self, mocker):
        """Test HTTP GET verb requests."""
        response_mock = mocker.Mock()
        response_mock.status_code = 200
        response_mock.headers = {}
        response_mock.text = 'ok'
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.api.client.requests.get', new=get_mock)
        httpclient = client.HttpClient(sdk_url='https://sdk.com', events_url='https://events.com')
        response = httpclient.get('sdk', 'test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
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

        response = httpclient.get('events', 'test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
        call = mocker.call(
            'https://events.com/test1',
            headers={'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json'},
            params={'param1': 123},
            timeout=None
        )
        assert response.status_code == 200
        assert response.body == 'ok'
        assert get_mock.mock_calls == [call]


    def test_post(self, mocker):
        """Test HTTP GET verb requests."""
        response_mock = mocker.Mock()
        response_mock.status_code = 200
        response_mock.headers = {}
        response_mock.text = 'ok'
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.api.client.requests.post', new=get_mock)
        httpclient = client.HttpClient()
        response = httpclient.post('sdk', 'test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
        call = mocker.call(
            client.SDK_URL + '/test1',
            json={'p1': 'a'},
            headers={'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json'},
            params={'param1': 123},
            timeout=None
        )
        assert response.status_code == 200
        assert response.body == 'ok'
        assert get_mock.mock_calls == [call]
        get_mock.reset_mock()

        response = httpclient.post('events', 'test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
        call = mocker.call(
            client.EVENTS_URL + '/test1',
            json={'p1': 'a'},
            headers={'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json'},
            params={'param1': 123},
            timeout=None
        )
        assert response.status_code == 200
        assert response.body == 'ok'
        assert get_mock.mock_calls == [call]

    def test_post_custom_urls(self, mocker):
        """Test HTTP GET verb requests."""
        response_mock = mocker.Mock()
        response_mock.status_code = 200
        response_mock.headers = {}
        response_mock.text = 'ok'
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.api.client.requests.post', new=get_mock)
        httpclient = client.HttpClient(sdk_url='https://sdk.com', events_url='https://events.com')
        response = httpclient.post('sdk', 'test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
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

        response = httpclient.post('events', 'test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
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

class MockResponse:
    def __init__(self, text, status, headers):
        self._text = text
        self.status = status
        self.headers = headers

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
        response_mock = MockResponse('ok', 200, {})
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.api.client.aiohttp.ClientSession.get', new=get_mock)
        httpclient = client.HttpClientAsync()
        response = await httpclient.get('sdk', 'test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
        assert response.status_code == 200
        assert response.body == 'ok'
        call = mocker.call(
            client.SDK_URL + '/test1',
            headers={'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json'},
            params={'param1': 123},
            timeout=None
        )
        assert get_mock.mock_calls == [call]
        get_mock.reset_mock()

        response = await httpclient.get('events', 'test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
        call = mocker.call(
            client.EVENTS_URL + '/test1',
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
        response_mock = MockResponse('ok', 200, {})
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.api.client.aiohttp.ClientSession.get', new=get_mock)
        httpclient = client.HttpClientAsync(sdk_url='https://sdk.com', events_url='https://events.com')
        response = await httpclient.get('sdk', 'test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
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

        response = await httpclient.get('events', 'test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
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
        response_mock = MockResponse('ok', 200, {})
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.api.client.aiohttp.ClientSession.post', new=get_mock)
        httpclient = client.HttpClientAsync()
        response = await httpclient.post('sdk', 'test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
        call = mocker.call(
            client.SDK_URL + '/test1',
            json={'p1': 'a'},
            headers={'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json'},
            params={'param1': 123},
            timeout=None
        )
        assert response.status_code == 200
        assert response.body == 'ok'
        assert get_mock.mock_calls == [call]
        get_mock.reset_mock()

        response = await httpclient.post('events', 'test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
        call = mocker.call(
            client.EVENTS_URL + '/test1',
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
        response_mock = MockResponse('ok', 200, {})
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.api.client.aiohttp.ClientSession.post', new=get_mock)
        httpclient = client.HttpClientAsync(sdk_url='https://sdk.com', events_url='https://events.com')
        response = await httpclient.post('sdk', 'test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
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

        response = await httpclient.post('events', 'test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
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