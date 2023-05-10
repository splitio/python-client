"""Auth API module."""

import logging
import json

from splitio.api.commons import headers_from_metadata, record_telemetry, APIException
from splitio.util.time import get_current_epoch_time_ms
from splitio.api.client import HttpClientException
from splitio.models.token import from_raw
from splitio.models.telemetry import HTTPExceptionsAndLatencies

_LOGGER = logging.getLogger(__name__)


class AuthAPI(object):  # pylint: disable=too-few-public-methods
    """Class that uses an httpClient to communicate with the SDK Auth Service API."""

    def __init__(self, client, apikey, sdk_metadata, telemetry_runtime_producer):
        """
        Class constructor.

        :param client: HTTP Client responsble for issuing calls to the backend.
        :type client: HttpClient
        :param apikey: User apikey token.
        :type apikey: string
        :param sdk_metadata: SDK version & machine name & IP.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        """
        self._client = client
        self._apikey = apikey
        self._metadata = headers_from_metadata(sdk_metadata)
        self._telemetry_runtime_producer = telemetry_runtime_producer

    def authenticate(self):
        """
        Perform authentication.

        :return: Json representation of an authentication.
        :rtype: splitio.models.token.Token
        """
        start = get_current_epoch_time_ms()
        try:
            response = self._client.get(
                'auth',
                '/v2/auth',
                self._apikey,
                extra_headers=self._metadata,
            )
            record_telemetry(response.status_code, get_current_epoch_time_ms() - start, HTTPExceptionsAndLatencies.TOKEN, self._telemetry_runtime_producer)
            if 200 <= response.status_code < 300:
                payload = json.loads(response.body)
                return from_raw(payload)
            else:
                if (response.status_code >= 400 and response.status_code < 500):
                    self._telemetry_runtime_producer.record_auth_rejections()
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            _LOGGER.error('Exception raised while authenticating')
            _LOGGER.debug('Exception information: ', exc_info=True)
            raise APIException('Could not perform authentication.') from exc
