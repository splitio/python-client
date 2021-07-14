"""Segments API module."""

import json
import logging

from splitio.api import APIException
from splitio.api.commons import headers_from_metadata, build_fetch
from splitio.api.client import HttpClientException


_LOGGER = logging.getLogger(__name__)


class SegmentsAPI(object):  # pylint: disable=too-few-public-methods
    """Class that uses an httpClient to communicate with the segments API."""

    def __init__(self, http_client, apikey, sdk_metadata):
        """
        Class constructor.

        :param client: HTTP Client responsble for issuing calls to the backend.
        :type client: client.HttpClient
        :param apikey: User apikey token.
        :type apikey: string
        :param sdk_metadata: SDK version & machine name & IP.
        :type sdk_metadata: splitio.client.util.SdkMetadata

        """
        self._client = http_client
        self._apikey = apikey
        self._metadata = headers_from_metadata(sdk_metadata)

    def fetch_segment(self, segment_name, change_number, fetch_options):
        """
        Fetch splits from backend.

        :param segment_name: Name of the segment to fetch changes for.
        :type segment_name: str

        :param change_number: Last known timestamp of a segment modification.
        :type change_number: int

        :param fetch_options: Fetch options for getting segment definitions.
        :type fetch_options: splitio.api.commons.FetchOptions

        :return: Json representation of a segmentChange response.
        :rtype: dict
        """
        try:
            query, extra_headers = build_fetch(change_number, fetch_options, self._metadata)
            response = self._client.get(
                'sdk',
                '/segmentChanges/{segment_name}'.format(segment_name=segment_name),
                self._apikey,
                extra_headers=extra_headers,
                query=query,
            )

            if 200 <= response.status_code < 300:
                return json.loads(response.body)
            else:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            _LOGGER.error(
                'Error fetching %s because an exception was raised by the HTTPClient',
                segment_name
            )
            _LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Segments not fetched properly.') from exc
