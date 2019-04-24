"""Impressions API module."""

import logging
from itertools import groupby

from future.utils import raise_from

from splitio.api import APIException, headers_from_metadata
from splitio.api.client import HttpClientException


class ImpressionsAPI(object): # pylint: disable=too-few-public-methods
    """Class that uses an httpClient to communicate with the impressions API."""

    def __init__(self, client, apikey, sdk_metadata):
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
        self._metadata = headers_from_metadata(sdk_metadata)

    @staticmethod
    def _build_bulk(impressions):
        """
        Build an impression bulk formatted as the API expects it.

        :param impressions: List of impressions to bundle.
        :type impressions: list(splitio.models.impressions.Impression)

        :return: Dictionary of lists of impressions.
        :rtype: dict
        """
        return [
            {
                'testName': test_name,
                'keyImpressions': [
                    {
                        'keyName': impression.matching_key,
                        'treatment': impression.treatment,
                        'time': impression.time,
                        'changeNumber': impression.change_number,
                        'label': impression.label,
                        'bucketingKey': impression.bucketing_key
                    }
                    for impression in imps
                ]
            }
            for (test_name, imps) in groupby(
                sorted(impressions, key=lambda i: i.feature_name),
                lambda i: i.feature_name
            )
        ]

    def flush_impressions(self, impressions):
        """
        Send impressions to the backend.

        :param impressions: Impressions bulk
        :type impressions: list
        """
        bulk = self._build_bulk(impressions)
        try:
            response = self._client.post(
                'events',
                '/testImpressions/bulk',
                self._apikey,
                body=bulk,
                extra_headers=self._metadata
            )
            if not 200 <= response.status_code < 300:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            self._logger.error('Http client is throwing exceptions')
            self._logger.debug('Error: ', exc_info=True)
            raise_from(APIException('Impressions not flushed properly.'), exc)
