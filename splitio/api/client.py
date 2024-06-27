"""Synchronous HTTP Client for split API."""
from collections import namedtuple
import requests
import urllib
import abc
import logging
import json

from splitio.optional.loaders import aiohttp
from splitio.util.time import get_current_epoch_time_ms

SDK_URL = 'https://sdk.split.io/api'
EVENTS_URL = 'https://events.split.io/api'
AUTH_URL = 'https://auth.split.io/api'
TELEMETRY_URL = 'https://telemetry.split.io/api'

_LOGGER = logging.getLogger(__name__)


HttpResponse = namedtuple('HttpResponse', ['status_code', 'body', 'headers'])

def _build_url(server, path, urls):
    """
    Build URL according to server specified.

    :param server: Server for whith the request is being made.
    :type server: str
    :param path: URL path to be appended to base host.
    :type path: str

    :return: A fully qualified URL.
    :rtype: str
    """
    url = urls[server]
    url += '/' if urls[server][:-1] != '/' else ''
    return urllib.parse.urljoin(url, path)

def _construct_urls(sdk_url=None, events_url=None, auth_url=None, telemetry_url=None):
    return {
        'sdk': sdk_url if sdk_url is not None else SDK_URL,
        'events': events_url if events_url is not None else EVENTS_URL,
        'auth': auth_url if auth_url is not None else AUTH_URL,
        'telemetry': telemetry_url if telemetry_url is not None else TELEMETRY_URL,
    }

def _build_basic_headers(sdk_key):
    """
    Build basic headers with auth.

    :param sdk_key: API token used to identify backend calls.
    :type sdk_key: str
    """
    return {
        'Content-Type': 'application/json',
        'Authorization': "Bearer %s" % sdk_key
    }

class HttpClientException(Exception):
    """HTTP Client exception."""

    def __init__(self, message):
        """
        Class constructor.

        :param message: Information on why this exception happened.
        :type message: str
        """
        Exception.__init__(self, message)

class HttpClientBase(object, metaclass=abc.ABCMeta):
    """HttpClient wrapper template."""

    @abc.abstractmethod
    def get(self, server, path, apikey):
        """http get request"""

    @abc.abstractmethod
    def post(self, server, path, apikey):
        """http post request"""

    def set_telemetry_data(self, metric_name, telemetry_runtime_producer):
        """
        Set the data needed for telemetry call

        :param metric_name: metric name for telemetry
        :type metric_name: str

        :param telemetry_runtime_producer: telemetry recording instance
        :type telemetry_runtime_producer: splitio.engine.telemetry.TelemetryRuntimeProducer
        """
        self._telemetry_runtime_producer = telemetry_runtime_producer
        self._metric_name = metric_name


class HttpClient(HttpClientBase):
    """HttpClient wrapper."""

    def __init__(self, timeout=None, sdk_url=None, events_url=None, auth_url=None, telemetry_url=None):
        """
        Class constructor.

        :param timeout: How many milliseconds to wait until the server responds.
        :type timeout: int
        :param sdk_url: Optional alternative sdk URL.
        :type sdk_url: str
        :param events_url: Optional alternative events URL.
        :type events_url: str
        :param auth_url: Optional alternative auth URL.
        :type auth_url: str
        :param telemetry_url: Optional alternative telemetry URL.
        :type telemetry_url: str
        """
        self._timeout = timeout/1000 if timeout else None # Convert ms to seconds.
        self._urls = _construct_urls(sdk_url, events_url, auth_url, telemetry_url)

    def get(self, server, path, sdk_key, query=None, extra_headers=None):  # pylint: disable=too-many-arguments
        """
        Issue a get request.

        :param server: Whether the request is for SDK server, Events server or Auth server.
        :typee server: str
        :param path: path to append to the host url.
        :type path: str
        :param sdk_key: sdk key.
        :type sdk_key: str
        :param query: Query string passed as dictionary.
        :type query: dict
        :param extra_headers: key/value pairs of possible extra headers.
        :type extra_headers: dict

        :return: Tuple of status_code & response text
        :rtype: HttpResponse
        """
        headers = _build_basic_headers(sdk_key)
        if extra_headers is not None:
            headers.update(extra_headers)

        start = get_current_epoch_time_ms()
        try:
            response = requests.get(
                _build_url(server, path, self._urls),
                params=query,
                headers=headers,
                timeout=self._timeout
            )
            self._record_telemetry(response.status_code, get_current_epoch_time_ms() - start)
            return HttpResponse(response.status_code, response.text, response.headers)

        except Exception as exc:  # pylint: disable=broad-except
            raise HttpClientException('requests library is throwing exceptions') from exc

    def post(self, server, path, sdk_key, body, query=None, extra_headers=None):  # pylint: disable=too-many-arguments
        """
        Issue a POST request.

        :param server: Whether the request is for SDK server or Events server.
        :typee server: str
        :param path: path to append to the host url.
        :type path: str
        :param sdk_key: sdk key.
        :type sdk_key: str
        :param body: body sent in the request.
        :type body: str
        :param query: Query string passed as dictionary.
        :type query: dict
        :param extra_headers: key/value pairs of possible extra headers.
        :type extra_headers: dict

        :return: Tuple of status_code & response text
        :rtype: HttpResponse
        """
        headers = _build_basic_headers(sdk_key)

        if extra_headers is not None:
            headers.update(extra_headers)

        start = get_current_epoch_time_ms()
        try:
            response = requests.post(
                _build_url(server, path, self._urls),
                json=body,
                params=query,
                headers=headers,
                timeout=self._timeout
            )
            self._record_telemetry(response.status_code, get_current_epoch_time_ms() - start)
            return HttpResponse(response.status_code, response.text, response.headers)

        except Exception as exc:  # pylint: disable=broad-except
            raise HttpClientException('requests library is throwing exceptions') from exc

    def _record_telemetry(self, status_code, elapsed):
        """
        Record Telemetry info

        :param status_code: http request status code
        :type status_code: int

        :param elapsed: response time elapsed.
        :type status_code: int
        """
        self._telemetry_runtime_producer.record_sync_latency(self._metric_name, elapsed)
        if 200 <= status_code < 300:
            self._telemetry_runtime_producer.record_successful_sync(self._metric_name, get_current_epoch_time_ms())
            return
        self._telemetry_runtime_producer.record_sync_error(self._metric_name, status_code)


class HttpClientAsync(HttpClientBase):
    """HttpClientAsync wrapper."""

    def __init__(self, timeout=None, sdk_url=None, events_url=None, auth_url=None, telemetry_url=None):
        """
        Class constructor.
        :param timeout: How many milliseconds to wait until the server responds.
        :type timeout: int
        :param sdk_url: Optional alternative sdk URL.
        :type sdk_url: str
        :param events_url: Optional alternative events URL.
        :type events_url: str
        :param auth_url: Optional alternative auth URL.
        :type auth_url: str
        :param telemetry_url: Optional alternative telemetry URL.
        :type telemetry_url: str
        """
        self._timeout = timeout/1000 if timeout else None  # Convert ms to seconds.
        self._urls = _construct_urls(sdk_url, events_url, auth_url, telemetry_url)
        self._session = aiohttp.ClientSession()

    async def get(self, server, path, apikey, query=None, extra_headers=None):  # pylint: disable=too-many-arguments
        """
        Issue a get request.
        :param server: Whether the request is for SDK server, Events server or Auth server.
        :typee server: str
        :param path: path to append to the host url.
        :type path: str
        :param apikey: api token.
        :type apikey: str
        :param query: Query string passed as dictionary.
        :type query: dict
        :param extra_headers: key/value pairs of possible extra headers.
        :type extra_headers: dict
        :return: Tuple of status_code & response text
        :rtype: HttpResponse
        """
        headers = _build_basic_headers(apikey)
        if extra_headers is not None:
            headers.update(extra_headers)
        start = get_current_epoch_time_ms()
        try:
            url = _build_url(server, path, self._urls)
            _LOGGER.debug("GET request: %s", url)
            _LOGGER.debug("query params: %s", query)
            _LOGGER.debug("headers: %s", headers)
            async with self._session.get(
                url,
                params=query,
                headers=headers,
                timeout=self._timeout
            ) as response:
                body = await response.text()
                _LOGGER.debug("Response:")
                _LOGGER.debug(response)
                _LOGGER.debug(body)
                await self._record_telemetry(response.status, get_current_epoch_time_ms() - start)
                return HttpResponse(response.status, body, response.headers)

        except aiohttp.ClientError as exc:  # pylint: disable=broad-except
            raise HttpClientException('aiohttp library is throwing exceptions') from exc

    async def post(self, server, path, apikey, body, query=None, extra_headers=None):  # pylint: disable=too-many-arguments
        """
        Issue a POST request.
        :param server: Whether the request is for SDK server or Events server.
        :typee server: str
        :param path: path to append to the host url.
        :type path: str
        :param apikey: api token.
        :type apikey: str
        :param body: body sent in the request.
        :type body: str
        :param query: Query string passed as dictionary.
        :type query: dict
        :param extra_headers: key/value pairs of possible extra headers.
        :type extra_headers: dict
        :return: Tuple of status_code & response text
        :rtype: HttpResponse
        """
        headers = _build_basic_headers(apikey)
        if extra_headers is not None:
            headers.update(extra_headers)
        start = get_current_epoch_time_ms()
        try:
            headers['Accept-Encoding'] = 'gzip'
            _LOGGER.debug("POST request: %s", _build_url(server, path, self._urls))
            _LOGGER.debug("query params: %s", query)
            _LOGGER.debug("headers: %s", headers)
            _LOGGER.debug("payload: ")
            _LOGGER.debug(str(json.dumps(body)).encode('utf-8'))
            async with self._session.post(
                _build_url(server, path, self._urls),
                params=query,
                headers=headers,
                json=body,
                timeout=self._timeout
            ) as response:
                body = await response.text()
                _LOGGER.debug("Response:")
                _LOGGER.debug(response)
                _LOGGER.debug(body)
                await self._record_telemetry(response.status, get_current_epoch_time_ms() - start)
                return HttpResponse(response.status, body, response.headers)

        except aiohttp.ClientError as exc:  # pylint: disable=broad-except
            raise HttpClientException('aiohttp library is throwing exceptions') from exc

    async def _record_telemetry(self, status_code, elapsed):
        """
        Record Telemetry info

        :param status_code: http request status code
        :type status_code: int

        :param elapsed: response time elapsed.
        :type status_code: int
        """
        await self._telemetry_runtime_producer.record_sync_latency(self._metric_name, elapsed)
        if 200 <= status_code < 300:
            await self._telemetry_runtime_producer.record_successful_sync(self._metric_name, get_current_epoch_time_ms())
            return

        await self._telemetry_runtime_producer.record_sync_error(self._metric_name, status_code)

    async def close_session(self):
        if not self._session.closed:
            await self._session.close()