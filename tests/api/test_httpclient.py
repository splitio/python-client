"""HTTPClient test module."""
from requests_kerberos import HTTPKerberosAuth
import pytest
import unittest.mock as mock
import requests

from splitio.client.config import AuthenticateScheme
from splitio.api import client
from splitio.engine.telemetry import TelemetryStorageProducer, TelemetryStorageProducerAsync
from splitio.storage.inmemmory import InMemoryTelemetryStorage, InMemoryTelemetryStorageAsync

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
        httpclient.set_telemetry_data("metric", mocker.Mock())
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
        httpclient.set_telemetry_data("metric", mocker.Mock())
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
        httpclient.set_telemetry_data("metric", mocker.Mock())
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
        httpclient.set_telemetry_data("metric", mocker.Mock())
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

    def test_telemetry(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        response_mock = mocker.Mock()
        response_mock.status_code = 200
        response_mock.headers = {}
        response_mock.text = 'ok'
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.api.client.requests.post', new=get_mock)
        httpclient = client.HttpClient(timeout=1500, sdk_url='https://sdk.com', events_url='https://events.com')
        httpclient.set_telemetry_data("metric", telemetry_runtime_producer)

        self.metric1 = None
        self.cur_time = 0
        def record_successful_sync(metric_name, cur_time):
            self.metric1 = metric_name
            self.cur_time = cur_time
        httpclient._telemetry_runtime_producer.record_successful_sync = record_successful_sync

        self.metric2 = None
        self.elapsed = 0
        def record_sync_latency(metric_name, elapsed):
            self.metric2 = metric_name
            self.elapsed = elapsed
        httpclient._telemetry_runtime_producer.record_sync_latency = record_sync_latency

        self.metric3 = None
        self.status = 0
        def record_sync_error(metric_name, elapsed):
            self.metric3 = metric_name
            self.status = elapsed
        httpclient._telemetry_runtime_producer.record_sync_error = record_sync_error

        httpclient.post('sdk', 'test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
        assert (self.metric2 == "metric")
        assert (self.metric1 == "metric")
        assert (self.cur_time > self.elapsed)

        response_mock.status_code = 400
        response_mock.headers = {}
        response_mock.text = 'ok'
        httpclient.post('sdk', 'test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
        assert (self.metric3 == "metric")
        assert (self.status == 400)

        # testing get call
        mocker.patch('splitio.api.client.requests.get', new=get_mock)
        self.metric1 = None
        self.cur_time = 0
        self.metric2 = None
        self.elapsed = 0
        response_mock.status_code = 200
        httpclient.get('sdk', 'test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
        assert (self.metric2 == "metric")
        assert (self.metric1 == "metric")
        assert (self.cur_time > self.elapsed)

        self.metric3 = None
        self.status = 0
        response_mock.status_code = 400
        httpclient.get('sdk', 'test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
        assert (self.metric3 == "metric")
        assert (self.status == 400)

class HttpClientKerberosTests(object):
    """Http Client test cases."""

    def test_authentication_scheme(self, mocker):
        global turl
        global theaders
        global tparams
        global ttimeout
        global tjson

        turl = None
        theaders = None
        tparams = None
        ttimeout = None
        class get_mock(object):
            def __init__(self, url, headers, params, timeout):
                global turl
                global theaders
                global tparams
                global ttimeout
                turl = url
                theaders = headers
                tparams = params
                ttimeout = timeout

            def __enter__(self):
                response_mock = mocker.Mock()
                response_mock.status_code = 200
                response_mock.text = 'ok'
                return response_mock

            def __exit__(self, exc_type, exc_val, exc_tb):
                pass

        mocker.patch('splitio.api.client.requests.Session.get', new=get_mock)
        httpclient = client.HttpClientKerberos(sdk_url='https://sdk.com', authentication_scheme=AuthenticateScheme.KERBEROS_SPNEGO, authentication_params=[None, None])
        httpclient.set_telemetry_data("metric", mocker.Mock())
        response = httpclient.get('sdk', '/test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
        assert turl == 'https://sdk.com/test1'
        assert theaders == {'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json'}
        assert tparams == {'param1': 123}
        assert ttimeout == None
        assert response.status_code == 200
        assert response.body == 'ok'

        turl = None
        theaders = None
        tparams = None
        ttimeout = None
        httpclient = client.HttpClientKerberos(sdk_url='https://sdk.com', authentication_scheme=AuthenticateScheme.KERBEROS_SPNEGO, authentication_params=['bilal', 'split'])
        httpclient.set_telemetry_data("metric", mocker.Mock())
        response = httpclient.get('sdk', '/test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
        assert turl == 'https://sdk.com/test1'
        assert theaders == {'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json'}
        assert tparams == {'param1': 123}
        assert ttimeout == None

        assert response.status_code == 200
        assert response.body == 'ok'

        response_mock = mocker.Mock()
        response_mock.status_code = 200
        response_mock.headers = {}
        response_mock.text = 'ok'

        turl = None
        theaders = None
        tparams = None
        ttimeout = None
        tjson = None
        class post_mock(object):
            def __init__(self, url, params, headers, json, timeout):
                global turl
                global theaders
                global tparams
                global ttimeout
                global tjson
                turl = url
                theaders = headers
                tparams = params
                ttimeout = timeout
                tjson = json

            def __enter__(self):
                response_mock = mocker.Mock()
                response_mock.status_code = 200
                response_mock.text = 'ok'
                return response_mock

            def __exit__(self, exc_type, exc_val, exc_tb):
                pass
        mocker.patch('splitio.api.client.requests.Session.post', new=post_mock)

        httpclient = client.HttpClientKerberos(timeout=1500, sdk_url='https://sdk.com', events_url='https://events.com', authentication_scheme=AuthenticateScheme.KERBEROS_PROXY, authentication_params=[None, None])
        httpclient.set_telemetry_data("metric", mocker.Mock())

        response = httpclient.post('events', 'test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
        assert turl == 'https://events.com/test1'
        assert tjson == {'p1': 'a'}
        assert theaders == {'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json'}
        assert tparams == {'param1': 123}
        assert ttimeout == 1.5

        assert response.status_code == 200
        assert response.body == 'ok'

        turl = None
        theaders = None
        tparams = None
        ttimeout = None
        mocker.patch('splitio.api.client.requests.Session.get', new=get_mock)
        httpclient = client.HttpClientKerberos(timeout=1500, sdk_url='https://sdk.com', authentication_scheme=AuthenticateScheme.KERBEROS_PROXY, authentication_params=['bilal', 'split'])
        httpclient.set_telemetry_data("metric", mocker.Mock())
        response = httpclient.get('sdk', '/test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
        assert turl == 'https://sdk.com/test1'
        assert theaders == {'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json'}
        assert tparams == {'param1': 123}
        assert ttimeout == 1.5

        assert response.status_code == 200
        assert response.body == 'ok'

    # test auth settings
        httpclient = client.HttpClientKerberos(sdk_url='https://sdk.com', authentication_scheme=AuthenticateScheme.KERBEROS_SPNEGO, authentication_params=['bilal', 'split'])
        httpclient._set_authentication('sdk')
        for server in ['sdk', 'events', 'auth', 'telemetry']:
            assert(httpclient._sessions[server].auth.principal == 'bilal')
            assert(httpclient._sessions[server].auth.password == 'split')
            assert(isinstance(httpclient._sessions[server].auth, HTTPKerberosAuth))

        httpclient._sessions['sdk'].close()
        httpclient._sessions['events'].close()
        httpclient._sessions['sdk'] = requests.Session()
        httpclient._sessions['events'] = requests.Session()
        assert(httpclient._sessions['sdk'].auth == None)
        assert(httpclient._sessions['events'].auth == None)

        httpclient._set_authentication('sdk')
        assert(httpclient._sessions['sdk'].auth.principal == 'bilal')
        assert(httpclient._sessions['sdk'].auth.password == 'split')
        assert(isinstance(httpclient._sessions['sdk'].auth, HTTPKerberosAuth))
        assert(httpclient._sessions['events'].auth == None)

        httpclient2 = client.HttpClientKerberos(sdk_url='https://sdk.com', authentication_scheme=AuthenticateScheme.KERBEROS_SPNEGO, authentication_params=[None, None])
        for server in ['sdk', 'events', 'auth', 'telemetry']:
            assert(httpclient2._sessions[server].auth.principal == None)
            assert(httpclient2._sessions[server].auth.password == None)
            assert(isinstance(httpclient2._sessions[server].auth, HTTPKerberosAuth))

        httpclient3 = client.HttpClientKerberos(sdk_url='https://sdk.com', authentication_scheme=AuthenticateScheme.KERBEROS_PROXY, authentication_params=['bilal', 'split'])
        for server in ['sdk', 'events', 'auth', 'telemetry']:
            assert(httpclient3._sessions[server].adapters['https://']._principal == 'bilal')
            assert(httpclient3._sessions[server].adapters['https://']._password == 'split')
            assert(isinstance(httpclient3._sessions[server].adapters['https://'], client.HTTPAdapterWithProxyKerberosAuth))

        httpclient4 = client.HttpClientKerberos(sdk_url='https://sdk.com', authentication_scheme=AuthenticateScheme.KERBEROS_PROXY, authentication_params=[None, None])
        for server in ['sdk', 'events', 'auth', 'telemetry']:
            assert(httpclient4._sessions[server].adapters['https://']._principal == None)
            assert(httpclient4._sessions[server].adapters['https://']._password == None)
            assert(isinstance(httpclient4._sessions[server].adapters['https://'], client.HTTPAdapterWithProxyKerberosAuth))

    def test_proxy_exception(self, mocker):
        global count
        count = 0
        class get_mock(object):
            def __init__(self, url, params, headers, timeout):
                pass

            def __enter__(self):
                global count
                count += 1
                if count == 1:
                    raise requests.exceptions.ProxyError()

                response_mock = mocker.Mock()
                response_mock.status_code = 200
                response_mock.text = 'ok'
                return response_mock

            def __exit__(self, exc_type, exc_val, exc_tb):
                pass

        mocker.patch('splitio.api.client.requests.Session.get', new=get_mock)
        httpclient = client.HttpClientKerberos(sdk_url='https://sdk.com', authentication_scheme=AuthenticateScheme.KERBEROS_SPNEGO, authentication_params=[None, None])
        httpclient.set_telemetry_data("metric", mocker.Mock())
        response = httpclient.get('sdk', '/test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
        assert response.status_code == 200
        assert response.body == 'ok'

        count = 0
        class post_mock(object):
            def __init__(self, url, params, headers, json, timeout):
                pass

            def __enter__(self):
                global count
                count += 1
                if count == 1:
                    raise requests.exceptions.ProxyError()

                response_mock = mocker.Mock()
                response_mock.status_code = 200
                response_mock.text = 'ok'
                return response_mock

            def __exit__(self, exc_type, exc_val, exc_tb):
                pass
        mocker.patch('splitio.api.client.requests.Session.post', new=post_mock)

        httpclient = client.HttpClientKerberos(timeout=1500, sdk_url='https://sdk.com', events_url='https://events.com', authentication_scheme=AuthenticateScheme.KERBEROS_PROXY, authentication_params=[None, None])
        httpclient.set_telemetry_data("metric", mocker.Mock())
        response = httpclient.post('events', 'test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
        assert response.status_code == 200
        assert response.body == 'ok'



    def test_telemetry(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        response_mock = mocker.Mock()
        response_mock.status_code = 200
        response_mock.headers = {}
        response_mock.text = 'ok'
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.api.client.requests.post', new=get_mock)
        httpclient = client.HttpClient(timeout=1500, sdk_url='https://sdk.com', events_url='https://events.com')
        httpclient.set_telemetry_data("metric", telemetry_runtime_producer)

        self.metric1 = None
        self.cur_time = 0
        def record_successful_sync(metric_name, cur_time):
            self.metric1 = metric_name
            self.cur_time = cur_time
        httpclient._telemetry_runtime_producer.record_successful_sync = record_successful_sync

        self.metric2 = None
        self.elapsed = 0
        def record_sync_latency(metric_name, elapsed):
            self.metric2 = metric_name
            self.elapsed = elapsed
        httpclient._telemetry_runtime_producer.record_sync_latency = record_sync_latency

        self.metric3 = None
        self.status = 0
        def record_sync_error(metric_name, elapsed):
            self.metric3 = metric_name
            self.status = elapsed
        httpclient._telemetry_runtime_producer.record_sync_error = record_sync_error

        httpclient.post('sdk', 'test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
        assert (self.metric2 == "metric")
        assert (self.metric1 == "metric")
        assert (self.cur_time > self.elapsed)

        response_mock.status_code = 400
        response_mock.headers = {}
        response_mock.text = 'ok'
        httpclient.post('sdk', 'test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
        assert (self.metric3 == "metric")
        assert (self.status == 400)

        # testing get call
        mocker.patch('splitio.api.client.requests.get', new=get_mock)
        self.metric1 = None
        self.cur_time = 0
        self.metric2 = None
        self.elapsed = 0
        response_mock.status_code = 200
        httpclient.get('sdk', 'test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
        assert (self.metric2 == "metric")
        assert (self.metric1 == "metric")
        assert (self.cur_time > self.elapsed)

        self.metric3 = None
        self.status = 0
        response_mock.status_code = 400
        httpclient.get('sdk', 'test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
        assert (self.metric3 == "metric")
        assert (self.status == 400)

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
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        response_mock = MockResponse('ok', 200, {})
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.optional.loaders.aiohttp.ClientSession.get', new=get_mock)
        httpclient = client.HttpClientAsync()
        httpclient.set_telemetry_data("metric", telemetry_runtime_producer)
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
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        response_mock = MockResponse('ok', 200, {})
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.optional.loaders.aiohttp.ClientSession.get', new=get_mock)
        httpclient = client.HttpClientAsync(sdk_url='https://sdk.com', events_url='https://events.com')
        httpclient.set_telemetry_data("metric", telemetry_runtime_producer)
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

    @pytest.mark.asyncio
    async def test_post(self, mocker):
        """Test HTTP POST verb requests."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        response_mock = MockResponse('ok', 200, {})
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.optional.loaders.aiohttp.ClientSession.post', new=get_mock)
        httpclient = client.HttpClientAsync()
        httpclient.set_telemetry_data("metric", telemetry_runtime_producer)
        response = await httpclient.post('sdk', 'test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
        call = mocker.call(
            client.SDK_URL + '/test1',
            json={"p1": "a"},
            headers={'Content-Type': 'application/json', 'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Accept-Encoding': 'gzip'},
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
            headers={'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json', 'Accept-Encoding': 'gzip'},
            params={'param1': 123},
            timeout=None
        )
        assert response.status_code == 200
        assert response.body == 'ok'
        assert get_mock.mock_calls == [call]

    @pytest.mark.asyncio
    async def test_post_custom_urls(self, mocker):
        """Test HTTP GET verb requests."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        response_mock = MockResponse('ok', 200, {})
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.optional.loaders.aiohttp.ClientSession.post', new=get_mock)
        httpclient = client.HttpClientAsync(sdk_url='https://sdk.com', events_url='https://events.com')
        httpclient.set_telemetry_data("metric", telemetry_runtime_producer)
        response = await httpclient.post('sdk', 'test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
        call = mocker.call(
            'https://sdk.com' + '/test1',
            json={"p1": "a"},
            headers={'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json', 'Accept-Encoding': 'gzip'},
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
            json={"p1": "a"},
            headers={'Authorization': 'Bearer some_api_key', 'h1': 'abc', 'Content-Type': 'application/json', 'Accept-Encoding': 'gzip'},
            params={'param1': 123},
            timeout=None
        )
        assert response.status_code == 200
        assert response.body == 'ok'
        assert get_mock.mock_calls == [call]

    @pytest.mark.asyncio
    async def test_telemetry(self, mocker):
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        response_mock = MockResponse('ok', 200, {})
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.optional.loaders.aiohttp.ClientSession.post', new=get_mock)
        httpclient = client.HttpClientAsync(sdk_url='https://sdk.com', events_url='https://events.com')
        httpclient.set_telemetry_data("metric", telemetry_runtime_producer)

        self.metric1 = None
        self.cur_time = 0
        async def record_successful_sync(metric_name, cur_time):
            self.metric1 = metric_name
            self.cur_time = cur_time
        httpclient._telemetry_runtime_producer.record_successful_sync = record_successful_sync

        self.metric2 = None
        self.elapsed = 0
        async def record_sync_latency(metric_name, elapsed):
            self.metric2 = metric_name
            self.elapsed = elapsed
        httpclient._telemetry_runtime_producer.record_sync_latency = record_sync_latency

        self.metric3 = None
        self.status = 0
        async def record_sync_error(metric_name, elapsed):
            self.metric3 = metric_name
            self.status = elapsed
        httpclient._telemetry_runtime_producer.record_sync_error = record_sync_error

        await httpclient.post('events', 'test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
        assert (self.metric2 == "metric")
        assert (self.metric1 == "metric")
        assert (self.cur_time > self.elapsed)

        response_mock = MockResponse('ok', 400, {})
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.optional.loaders.aiohttp.ClientSession.post', new=get_mock)
        await httpclient.post('sdk', 'test1', 'some_api_key', {'p1': 'a'}, {'param1': 123}, {'h1': 'abc'})
        assert (self.metric3 == "metric")
        assert (self.status == 400)

        # testing get call
        response_mock = MockResponse('ok', 200, {})
        get_mock = mocker.Mock()
        get_mock.return_value = response_mock
        mocker.patch('splitio.optional.loaders.aiohttp.ClientSession.get', new=get_mock)
        self.metric1 = None
        self.cur_time = 0
        self.metric2 = None
        self.elapsed = 0
        await httpclient.get('sdk', 'test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
        assert (self.metric2 == "metric")
        assert (self.metric1 == "metric")
        assert (self.cur_time > self.elapsed)

        self.metric3 = None
        self.status = 0
        response_mock = MockResponse('ok', 400, {})
        get_mock.return_value = response_mock
        await httpclient.get('sdk', 'test1', 'some_api_key', {'param1': 123}, {'h1': 'abc'})
        assert (self.metric3 == "metric")
        assert (self.status == 400)
