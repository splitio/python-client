"""Splits API module."""

import logging
import json

from splitio.api import APIException, headers_from_metadata
from splitio.api.client import HttpClientException


_LOGGER = logging.getLogger(__name__)


_CACHE_CONTROL = 'Cache-Control'
_CACHE_CONTROL_NO_CACHE = 'no-cache'


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

    def _build_fetch(self, change_number, fetch_options):
        """
        Build fetch with new flags if that is the case.

        :param change_number: Last known timestamp of a split modification.
        :type change_number: int

        :param fetch_options: Fetch options for getting split definitions.
        :type fetch_options: splitio.api.FetchOptions

        :return: Objects for fetch
        :rtype: dict, dict
        """
        query = {'since': change_number}
        extra_headers = self._metadata
        if fetch_options is None:
            return query, extra_headers
        if fetch_options.cache_control_headers:
            extra_headers[_CACHE_CONTROL] = _CACHE_CONTROL_NO_CACHE
        if fetch_options.change_number is not None:
            query['till'] = fetch_options.change_number
        return query, extra_headers

    def fetch_splits(self, change_number, fetch_options):
        """
        Fetch splits from backend.

        :param change_number: Last known timestamp of a split modification.
        :type change_number: int

        :param fetch_options: Fetch options for getting split definitions.
        :type fetch_options: splitio.api.FetchOptions

        :return: Json representation of a splitChanges response.
        :rtype: dict
        """
        try:
            query, extra_headers = self._build_fetch(change_number, fetch_options)
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
