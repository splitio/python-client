"""Auth API module."""

import logging
import json

from splitio.api import APIException
from splitio.api.commons import headers_from_metadata
from splitio.api.client import HttpClientException
from splitio.models.token import from_raw


_LOGGER = logging.getLogger(__name__)


class AuthAPI(object):  # pylint: disable=too-few-public-methods
    """Class that uses an httpClient to communicate with the SDK Auth Service API."""

    def __init__(self, client, apikey, sdk_metadata):
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

    def authenticate(self):
        """
        Perform authentication.

        :return: Json representation of an authentication.
        :rtype: splitio.models.token.Token
        """
        try:
            response = self._client.get(
                'auth',
                '/v2/auth',
                self._apikey,
                extra_headers=self._metadata
            )
            if 200 <= response.status_code < 300:
                payload = json.loads(response.body)
                return from_raw(payload)
            else:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            _LOGGER.error('Exception raised while authenticating')
            _LOGGER.debug('Exception information: ', exc_info=True)
            raise APIException('Could not perform authentication.') from exc
