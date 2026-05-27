"""Synchronous HTTP Client for split API."""
from collections import namedtuple
import requests
import urllib
import abc
import logging
import json
import threading
from urllib3.util import parse_url

from harness_commons.api.client import HttpClientBase, build_url, construct_urls, HttpResponse
from splitio.optional.loaders import HTTPKerberosAuth, OPTIONAL
from splitio.client.config import AuthenticateScheme
from splitio.optional.loaders import aiohttp
from harness_commons.util.time import get_current_epoch_time_ms

_LOGGER = logging.getLogger(__name__)
_EXC_MSG = '{source} library is throwing exceptions'

class HttpClientException(Exception):
    """HTTP Client exception."""

    def __init__(self, message):
        """
        Class constructor.

        :param message: Information on why this exception happened.
        :type message: str
        """
        Exception.__init__(self, message)

class HTTPAdapterWithProxyKerberosAuth(requests.adapters.HTTPAdapter):
    """HTTPAdapter override for Kerberos Proxy auth"""

    def __init__(self, principal=None, password=None):
        requests.adapters.HTTPAdapter.__init__(self)
        self._principal = principal
        self._password = password

    def proxy_headers(self, proxy):
        headers = {}
        if self._principal is not None:
            auth = HTTPKerberosAuth(principal=self._principal, password=self._password)
        else:
            auth = HTTPKerberosAuth()
        negotiate_details = auth.generate_request_header(None, parse_url(proxy).host, is_preemptive=True)
        headers['Proxy-Authorization'] = negotiate_details
        return headers

class HttpClientKerberos(HttpClientBase):
    """HttpClient wrapper."""

    def __init__(self, timeout=None, sdk_url=None, events_url=None, auth_url=None, telemetry_url=None, authentication_scheme=None, authentication_params=None):
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
        :param authentication_scheme: Optional authentication scheme to use.
        :type authentication_scheme: splitio.client.config.AuthenticateScheme
        :param authentication_params: Optional authentication username and password to use.
        :type authentication_params: [str, str]
        """
        _LOGGER.debug("Initializing httpclient for Kerberos auth")
        self._timeout = timeout/1000 if timeout else None # Convert ms to seconds.
        self._urls = construct_urls(sdk_url, events_url, auth_url, telemetry_url)
        self._authentication_scheme = authentication_scheme
        self._authentication_params = authentication_params
        self._lock = threading.RLock()
        self._sessions = {'sdk': requests.Session(),
                          'events': requests.Session(),
                          'auth': requests.Session(),
                          'telemetry': requests.Session()}
        self._set_authentication()

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
        with self._lock:
            start = get_current_epoch_time_ms()
            try:
                return self._do_get(server, path, sdk_key, query, extra_headers, start)

            except requests.exceptions.ProxyError as exc:
                _LOGGER.debug("Proxy Exception caught, resetting the http session")
                self._sessions[server].close()
                self._sessions[server] = requests.Session()
                self._set_authentication(server_name=server)
                try:
                    return self._do_get(server, path, sdk_key, query, extra_headers, start)

                except Exception as exc:
                    raise HttpClientException(_EXC_MSG.format(source='request')) from exc

            except Exception as exc:  # pylint: disable=broad-except
                raise HttpClientException(_EXC_MSG.format(source='request')) from exc

    def _do_get(self, server, path, sdk_key, query, extra_headers, start):
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
        with self._sessions[server].get(
            build_url(server, path, self._urls),
            headers=self._get_headers(extra_headers, sdk_key),
            params=query,
            timeout=self._timeout
        ) as response:
            self._record_telemetry(response.status_code, get_current_epoch_time_ms() - start)
            return HttpResponse(response.status_code, response.text, response.headers)

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
        with self._lock:
            start = get_current_epoch_time_ms()
            try:
                return self._do_post(server, path, sdk_key, query, extra_headers, body, start)

            except requests.exceptions.ProxyError as exc:
                _LOGGER.debug("Proxy Exception caught, resetting the http session")
                self._sessions[server].close()
                self._sessions[server] = requests.Session()
                self._set_authentication(server_name=server)
                try:
                    return self._do_post(server, path, sdk_key, query, extra_headers, body, start)

                except Exception as exc:
                    raise HttpClientException(_EXC_MSG.format(source='request')) from exc

            except Exception as exc:  # pylint: disable=broad-except
                raise HttpClientException(_EXC_MSG.format(source='request')) from exc

    def _do_post(self, server, path, sdk_key, query, extra_headers, body, start):
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
        with self._sessions[server].post(
            build_url(server, path, self._urls),
            params=query,
            headers=self._get_headers(extra_headers, sdk_key),
            json=body,
            timeout=self._timeout,
        ) as response:
            self._record_telemetry(response.status_code, get_current_epoch_time_ms() - start)
            return HttpResponse(response.status_code, response.text, response.headers)

    def _set_authentication(self, server_name=None):
        """
        Set the authentication for all self._sessions variables based on authentication scheme.

        :param server: If set, will only add the auth for its session variable, otherwise will set all sessions.
        :typee server: str
        """
        for server in ['sdk', 'events', 'auth', 'telemetry']:
            if server_name is not None and server_name != server:
                continue
            if self._authentication_scheme == AuthenticateScheme.KERBEROS_SPNEGO:
                _LOGGER.debug("Using Kerberos Spnego Authentication")
                if self._authentication_params != [None, None]:
                    self._sessions[server].auth = HTTPKerberosAuth(principal=self._authentication_params[0], password=self._authentication_params[1], mutual_authentication=OPTIONAL)
                else:
                    self._sessions[server].auth = HTTPKerberosAuth(mutual_authentication=OPTIONAL)
            elif self._authentication_scheme == AuthenticateScheme.KERBEROS_PROXY:
                _LOGGER.debug("Using Kerberos Proxy Authentication")
                if self._authentication_params != [None, None]:
                    self._sessions[server].mount('https://', HTTPAdapterWithProxyKerberosAuth(principal=self._authentication_params[0], password=self._authentication_params[1]))
                else:
                    self._sessions[server].mount('https://', HTTPAdapterWithProxyKerberosAuth())
