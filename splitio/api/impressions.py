"""Impressions API module."""

import logging
from itertools import groupby

from splitio.api import APIException
from splitio.api.client import HttpClientException
from splitio.api.commons import headers_from_metadata
from splitio.engine.impressions.impressions import ImpressionsMode


_LOGGER = logging.getLogger(__name__)


class ImpressionsAPI(object):  # pylint: disable=too-few-public-methods
    """Class that uses an httpClient to communicate with the impressions API."""

    def __init__(self, client, apikey, sdk_metadata, mode=ImpressionsMode.OPTIMIZED):
        """
        Class constructor.

        :param client: HTTP Client responsble for issuing calls to the backend.
        :type client: HttpClient
        :param apikey: User apikey token.
        :type apikey: string
        """
        self._client = client
        self._apikey = apikey
        self._metadata = headers_from_metadata(sdk_metadata)
        self._metadata['SplitSDKImpressionsMode'] = mode.name

    @staticmethod
    def _build_bulk(impressions):
        """
        Build an impression bulk formatted as the API expects it.

        :param impressions: List of impressions to bundle.
        :type impressions: list(splitio.models.impressions.Impression)

        :return: Dictionary of lists of impressions.
        :rtype: list
        """
        return [
            {
                'f': test_name,
                'i': [
                    {
                        'k': impression.matching_key,
                        't': impression.treatment,
                        'm': impression.time,
                        'c': impression.change_number,
                        'r': impression.label,
                        'b': impression.bucketing_key,
                        'pt': impression.previous_time
                    }
                    for impression in imps
                ]
            }
            for (test_name, imps) in groupby(
                sorted(impressions, key=lambda i: i.feature_name),
                lambda i: i.feature_name
            )
        ]

    @staticmethod
    def _build_counters(counters):
        """
        Build an impression bulk formatted as the API expects it.

        :param counters: List of impression counters per feature.
        :type counters: list[splitio.engine.impressions.Counter.CountPerFeature]

        :return: dict with list of impression count dtos
        :rtype: dict
        """
        return {
            'pf': [
                {
                    'f': pf_count.feature,
                    'm': pf_count.timeframe,
                    'rc': pf_count.count
                } for pf_count in counters
            ]
        }

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
            _LOGGER.error(
                'Error posting impressions because an exception was raised by the HTTPClient'
            )
            _LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Impressions not flushed properly.') from exc

    def flush_counters(self, counters):
        """
        Send impressions to the backend.

        :param impressions: Impressions bulk
        :type impressions: list
        """
        bulk = self._build_counters(counters)
        try:
            response = self._client.post(
                'events',
                '/testImpressions/count',
                self._apikey,
                body=bulk,
                extra_headers=self._metadata
            )
            if not 200 <= response.status_code < 300:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            _LOGGER.error(
                'Error posting impressions counters because an exception was raised by the '
                'HTTPClient'
            )
            _LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Impressions not flushed properly.') from exc
