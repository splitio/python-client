"""Segments API module."""

import json
import logging

from future.utils import raise_from

from splitio.api import APIException
from splitio.api.client import HttpClientException


class SegmentsAPI(object):  #pylint: disable=too-few-public-methods
    """Class that uses an httpClient to communicate with the segments API."""

    def __init__(self, http_client, apikey):
        """
        Class constructor.

        :param client: HTTP Client responsble for issuing calls to the backend.
        :type client: client.HttpClient
        :param apikey: User apikey token.
        :type apikey: string
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._client = http_client
        self._apikey = apikey

    def fetch_segment(self, segment_name, change_number):
        """
        Fetch splits from backend.

        :param segment_name: Name of the segment to fetch changes for.
        :type segment_name: str
        :param change_number: Last known timestamp of a split modification.
        :type change_number: int

        :return: Json representation of a segmentChange response.
        :rtype: dict
        """
        try:
            response = self._client.get(
                'sdk',
                '/segmentChanges/{segment_name}'.format(segment_name=segment_name),
                self._apikey,
                {'since': change_number}
            )

            if 200 <= response.status_code < 300:
                return json.loads(response.body)
            else:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            self._logger.error('Http client is throwing exceptions')
            self._logger.debug('Error: ', exc_info=True)
            raise_from(APIException('Segments not fetched properly.'), exc)
