"""Splits API module."""

import logging
import json

from splitio.api import APIException
from splitio.api.commons import headers_from_metadata, build_fetch
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

    def fetch_splits(self, change_number, fetch_options):
        """
        Fetch splits from backend.

        :param change_number: Last known timestamp of a split modification.
        :type change_number: int

        :param fetch_options: Fetch options for getting split definitions.
        :type fetch_options: splitio.api.commons.FetchOptions

        :return: Json representation of a splitChanges response.
        :rtype: dict
        """
        try:
            query, extra_headers = build_fetch(change_number, fetch_options, self._metadata)
            response = self._client.get(
                'sdk',
                '/splitChanges',
                self._apikey,
                extra_headers=extra_headers,
                query=query,
            )
            if 200 <= response.status_code < 300:
                return json.loads(response.body)
            else:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            _LOGGER.error('Error fetching splits because an exception was raised by the HTTPClient')
            _LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Splits not fetched correctly.') from exc
