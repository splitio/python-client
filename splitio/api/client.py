"""Synchronous HTTP Client for split API."""
from __future__ import division

from collections import namedtuple
import requests

HttpResponse = namedtuple('HttpResponse', ['status_code', 'body'])


class HttpClientException(Exception):
    """HTTP Client exception."""

    def __init__(self, custom_message, original_exception=None):
        """
        Class constructor.

        :param message: Information on why this exception happened.
        :type message: str
        :param original_exception: Original exception being caught if any.
        :type original_exception: Exception.
        """
        Exception.__init__(self, custom_message)
        self._custom_message = custom_message
        self._original_exception = original_exception

    @property
    def custom_message(self):
        """Return custom message."""
        return self._custom_message

    @property
    def original_exception(self):
        """Return original exception."""
        return self._original_exception


class HttpClient(object):
    """HttpClient wrapper."""

    SDK_URL = 'https://split.io/api'
    EVENTS_URL = 'https://split.io/api'

    def __init__(self, timeout=None, sdk_url=None, events_url=None):
        """
        Class constructor.

        :param sdk_url: Optional alternative sdk URL.
        :type sdk_url: str
        :param events_url: Optional alternative events URL.
        :type events_url: str
        :param timeout: How many milliseconds to wait until the server responds.
        :type timeout: int
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

    def get(self, server, path, apikey, query=None, extra_headers=None):  #pylint: disable=too-many-arguments
        """
        Issue a get request.

        :param path: path to append to the host url.
        :type path: str
        :param apikey: api token.
        :type apikey: str
        :param extra_headers: key/value pairs of possible extra headers.
        :type extra_headers: dict

        :return: Tuple of status_code & response text
        :rtype: HttpResponse
        """
        headers = {
            'Content-Type': 'application/json',
            'Authorization': "Bearer %s" % apikey
        }

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
        except Exception as exc:
            raise HttpClientException('requests library is throwing exceptions', exc)

    def post(self, server, path, apikey, body, query=None, extra_headers=None):  #pylint: disable=too-many-arguments
        """
        Issue a POST request.

        :param path: path to append to the host url.
        :type path: str
        :param apikey: api token.
        :type apikey: str
        :param body: body sent in the request.
        :type body: str
        :param extra_headers: key/value pairs of possible extra headers.
        :type extra_headers: dict

        :return: Tuple of status_code & response text
        :rtype: HttpResponse
        """
        headers = {
            'Content-Type': 'application/json',
            'Authorization': "Bearer %s" % apikey
        }

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
        except Exception as exc:
            raise HttpClientException('requests library is throwing exceptions', exc)
