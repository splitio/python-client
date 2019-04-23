"""Splits API module."""

import logging
import json

from future.utils import raise_from

from splitio.api import APIException
from splitio.api.client import HttpClientException


class SplitsAPI(object):  #pylint: disable=too-few-public-methods
    """Class that uses an httpClient to communicate with the splits API."""

    def __init__(self, client, apikey):
        """
        Class constructor.

        :param client: HTTP Client responsble for issuing calls to the backend.
        :type client: HttpClient
        :param apikey: User apikey token.
        :type apikey: string
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._client = client
        self._apikey = apikey

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
                {'since': change_number}
            )
            if 200 <= response.status_code < 300:
                return json.loads(response.body)
            else:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            self._logger.error('Http client is throwing exceptions')
            self._logger.debug('Error: ', exc_info=True)
            raise_from(APIException('Splits not fetched correctly.'), exc)
