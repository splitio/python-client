"""Segments API module."""

import json
import logging
import time

from splitio.api import APIException, headers_from_metadata
from splitio.api.commons import build_fetch
from splitio.api.client import HttpClientException
from splitio.models.telemetry import HTTPExceptionsAndLatencies

class SegmentsAPI(object):  # pylint: disable=too-few-public-methods
    """Class that uses an httpClient to communicate with the segments API."""

    _LOGGER = logging.getLogger(__name__)

    def __init__(self, http_client, sdk_key, sdk_metadata, telemetry_runtime_producer):
        """
        Class constructor.

        :param client: HTTP Client responsble for issuing calls to the backend.
        :type client: client.HttpClient
        :param sdk_key: User sdk_key token.
        :type sdk_key: string
        :param sdk_metadata: SDK version & machine name & IP.
        :type sdk_metadata: splitio.client.util.SdkMetadata

        """
        self._client = http_client
        self._sdk_key = sdk_key
        self._metadata = headers_from_metadata(sdk_metadata)
        self._telemetry_runtime_producer = telemetry_runtime_producer
        self._client.set_telemetry_data(HTTPExceptionsAndLatencies.SEGMENT, self._telemetry_runtime_producer)

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
                'segmentChanges/{segment_name}'.format(segment_name=segment_name),
                self._sdk_key,
                extra_headers=extra_headers,
                query=query,
            )
            if 200 <= response.status_code < 300:
                return json.loads(response.body)
            raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            self._LOGGER.error(
                'Error fetching %s because an exception was raised by the HTTPClient',
                segment_name
            )
            self._LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Segments not fetched properly.') from exc


class SegmentsAPIAsync(object):  # pylint: disable=too-few-public-methods
    """Async Class that uses an httpClient to communicate with the segments API."""

    _LOGGER = logging.getLogger('asyncio')

    def __init__(self, http_client, sdk_key, sdk_metadata, telemetry_runtime_producer):
        """
        Class constructor.

        :param client: HTTP Client responsble for issuing calls to the backend.
        :type client: client.HttpClient
        :param sdk_key: User sdk_key token.
        :type sdk_key: string
        :param sdk_metadata: SDK version & machine name & IP.
        :type sdk_metadata: splitio.client.util.SdkMetadata

        """
        self._client = http_client
        self._sdk_key = sdk_key
        self._metadata = headers_from_metadata(sdk_metadata)
        self._telemetry_runtime_producer = telemetry_runtime_producer
        self._client.set_telemetry_data(HTTPExceptionsAndLatencies.SEGMENT, self._telemetry_runtime_producer)

    async def fetch_segment(self, segment_name, change_number, fetch_options):
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
            response = await self._client.get(
                'sdk',
                'segmentChanges/{segment_name}'.format(segment_name=segment_name),
                self._sdk_key,
                extra_headers=extra_headers,
                query=query,
            )
            if 200 <= response.status_code < 300:
                return json.loads(response.body)
            raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            self._LOGGER.error(
                'Error fetching %s because an exception was raised by the HTTPClient',
                segment_name
            )
            self._LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Segments not fetched properly.') from exc
