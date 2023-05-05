"""Asyncio HTTP Client for split API."""
from collections import namedtuple
import aiohttp
import logging

from splitio.api import build_basic_headers, HttpClientException

_LOGGER = logging.getLogger(__name__)
HttpResponse = namedtuple('HttpResponse', ['status_code', 'body'])

class HttpClientAsync(object):
    """HttpClient wrapper."""

    SDK_URL = 'https://sdk.split.io/api'
    EVENTS_URL = 'https://events.split.io/api'
    AUTH_URL = 'https://auth.split.io/api'
    TELEMETRY_URL = 'https://telemetry.split.io/api'

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
        self._urls = {
            'sdk': sdk_url if sdk_url is not None else self.SDK_URL,
            'events': events_url if events_url is not None else self.EVENTS_URL,
            'auth': auth_url if auth_url is not None else self.AUTH_URL,
            'telemetry': telemetry_url if telemetry_url is not None else self.TELEMETRY_URL,
        }

    def _build_url(self, server, path):
        """
        Build URL according to server specified.

        :param server: Server for whith the request is being made.
        :type server: str
        :param path: URL path to be appended to base host.
        :type path: str

        :return: A fully qualified URL.
        :rtype: str
        """
        return self._urls[server] + path

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
        headers = build_basic_headers(apikey)
        if extra_headers is not None:
            headers.update(extra_headers)
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self._build_url(server, path),
                    params=query,
                    headers=headers,
                    timeout=self._timeout
                ) as response:
                    body = await response.text()
                    return HttpResponse(response.status, body)
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
        headers = build_basic_headers(apikey)

        if extra_headers is not None:
            headers.update(extra_headers)

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self._build_url(server, path),
                    params=query,
                    headers=headers,
                    json=body,
                    timeout=self._timeout
                ) as response:
                    body = await response.text()
                    return HttpResponse(response.status, body)
        except Exception as exc:  # pylint: disable=broad-except
            raise HttpClientException('aiohttp library is throwing exceptions') from exc
