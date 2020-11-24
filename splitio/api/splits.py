"""Splits API module."""

import logging
import json

from future.utils import raise_from

from splitio.api import APIException, headers_from_metadata
from splitio.api.client import HttpClientException


_LOGGER = logging.getLogger(__name__)


class SplitsAPI(object):  # pylint: disable=too-few-public-methods
    """Class that uses an httpClient to communicate with the splits API."""

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

    def fetch_splits(self, change_number):
        """
        Fetch splits from backend.

        :param changeNumber: Last known timestamp of a split modification.
        :type changeNumber: int

        :return: Json representation of a splitChanges response.
        :rtype: dict
        """
        try:
            response = self._client.get(
                'sdk',
                '/splitChanges',
                self._apikey,
                extra_headers=self._metadata,
                query={'since': change_number}
            )
            if 200 <= response.status_code < 300:
                return json.loads(response.body)
            else:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            _LOGGER.error('Error fetching splits because an exception was raised by the HTTPClient')
            _LOGGER.debug('Error: ', exc_info=True)
            raise_from(APIException('Splits not fetched correctly.'), exc)
