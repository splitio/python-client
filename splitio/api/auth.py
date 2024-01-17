"""Auth API module."""

import logging
import json

from splitio.api import APIException, headers_from_metadata
from splitio.api.client import HttpClientException
from splitio.models.token import from_raw
from splitio.models.telemetry import HTTPExceptionsAndLatencies


class AuthAPI(object):  # pylint: disable=too-few-public-methods
    """Class that uses an httpClient to communicate with the SDK Auth Service API."""

    _LOGGER = logging.getLogger(__name__)

    def __init__(self, client, sdk_key, sdk_metadata, telemetry_runtime_producer):
        """
        Class constructor.

        :param client: HTTP Client responsble for issuing calls to the backend.
        :type client: HttpClient
        :param sdk_key: User sdk key.
        :type sdk_key: string
        :param sdk_metadata: SDK version & machine name & IP.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        """
        self._client = client
        self._sdk_key = sdk_key
        self._metadata = headers_from_metadata(sdk_metadata)
        self._telemetry_runtime_producer = telemetry_runtime_producer
        self._client.set_telemetry_data(HTTPExceptionsAndLatencies.TOKEN, self._telemetry_runtime_producer)

    def authenticate(self):
        """
        Perform authentication.

        :return: Json representation of an authentication.
        :rtype: splitio.models.token.Token
        """
        try:
            response = self._client.get(
                'auth',
                'v2/auth',
                self._sdk_key,
                extra_headers=self._metadata,
            )
            if 200 <= response.status_code < 300:
                payload = json.loads(response.body)
                return from_raw(payload)
            else:
                if (response.status_code >= 400 and response.status_code < 500):
                    self._telemetry_runtime_producer.record_auth_rejections()
                raise APIException(response.body, response.status_code, response.headers)
        except HttpClientException as exc:
            self._LOGGER.error('Exception raised while authenticating')
            self._LOGGER.debug('Exception information: ', exc_info=True)
            raise APIException('Could not perform authentication.') from exc

class AuthAPIAsync(object):  # pylint: disable=too-few-public-methods
    """Async Class that uses an httpClient to communicate with the SDK Auth Service API."""

    _LOGGER = logging.getLogger('asyncio')

    def __init__(self, client, sdk_key, sdk_metadata, telemetry_runtime_producer):
        """
        Class constructor.

        :param client: HTTP Client responsble for issuing calls to the backend.
        :type client: HttpClient
        :param sdk_key: User sdk key.
        :type sdk_key: string
        :param sdk_metadata: SDK version & machine name & IP.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        """
        self._client = client
        self._sdk_key = sdk_key
        self._metadata = headers_from_metadata(sdk_metadata)
        self._telemetry_runtime_producer = telemetry_runtime_producer
        self._client.set_telemetry_data(HTTPExceptionsAndLatencies.TOKEN, self._telemetry_runtime_producer)

    async def authenticate(self):
        """
        Perform authentication.

        :return: Json representation of an authentication.
        :rtype: splitio.models.token.Token
        """
        try:
            response = await self._client.get(
                'auth',
                'v2/auth',
                self._sdk_key,
                extra_headers=self._metadata,
            )
            if 200 <= response.status_code < 300:
                payload = json.loads(response.body)
                return from_raw(payload)
            else:
                if (response.status_code >= 400 and response.status_code < 500):
                    await self._telemetry_runtime_producer.record_auth_rejections()
                raise APIException(response.body, response.status_code, response.headers)
        except HttpClientException as exc:
            self._LOGGER.error('Exception raised while authenticating')
            self._LOGGER.debug('Exception information: ', exc_info=True)
            raise APIException('Could not perform authentication.') from exc
