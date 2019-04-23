"""Synchronous HTTP Client for split API."""
from __future__ import division

from collections import namedtuple

from future.utils import raise_from
import requests

HttpResponse = namedtuple('HttpResponse', ['status_code', 'body'])


class HttpClientException(Exception):
    """HTTP Client exception."""

    def __init__(self, message):
        """
        Class constructor.

        :param message: Information on why this exception happened.
        :type message: str
        """
        Exception.__init__(self, message)


class HttpClient(object):
    """HttpClient wrapper."""

    SDK_URL = 'https://sdk.split.io/api'
    EVENTS_URL = 'https://events.split.io/api'

    def __init__(self, timeout=None, sdk_url=None, events_url=None):
        """
        Class constructor.

        :param timeout: How many milliseconds to wait until the server responds.
        :type timeout: int
        :param sdk_url: Optional alternative sdk URL.
        :type sdk_url: str
        :param events_url: Optional alternative events URL.
        :type events_url: str
        """
        self._timeout = timeout / 1000  if timeout else None  # Convert ms to seconds.
        self._urls = {
            'sdk': sdk_url if sdk_url is not None else self.SDK_URL,
            'events': events_url if events_url is not None else self.EVENTS_URL,
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

    @staticmethod
    def _build_basic_headers(apikey):
        """
        Build basic headers with auth.

        :param apikey: API token used to identify backend calls.
        :type apikey: str
        """
        return {
            'Content-Type': 'application/json',
            'Authorization': "Bearer %s" % apikey
        }

    def get(self, server, path, apikey, query=None, extra_headers=None):  #pylint: disable=too-many-arguments
        """
        Issue a get request.

        :param server: Whether the request is for SDK server or Events server.
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
        headers = self._build_basic_headers(apikey)

        if extra_headers is not None:
            headers.update(extra_headers)

        try:
            response = requests.get(
                self._build_url(server, path),
                params=query,
                headers=headers,
                timeout=self._timeout
            )
            return HttpResponse(response.status_code, response.text)
        except Exception as exc:  #pylint: disable=broad-except
            raise_from(HttpClientException('requests library is throwing exceptions'), exc)

    def post(self, server, path, apikey, body, query=None, extra_headers=None):  #pylint: disable=too-many-arguments
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
        headers = self._build_basic_headers(apikey)

        if extra_headers is not None:
            headers.update(extra_headers)

        try:
            response = requests.post(
                self._build_url(server, path),
                json=body,
                params=query,
                headers=headers,
                timeout=self._timeout
            )
            return HttpResponse(response.status_code, response.text)
        except Exception as exc:  #pylint: disable=broad-except
            raise_from(HttpClientException('requests library is throwing exceptions'), exc)
