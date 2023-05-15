"""Impressions API module."""

import logging
from itertools import groupby

from splitio.api import APIException
from splitio.api.client import HttpClientException
from splitio.api.commons import headers_from_metadata, record_telemetry
from splitio.util.time import get_current_epoch_time_ms
from splitio.engine.impressions import ImpressionsMode
from splitio.models.telemetry import HTTPExceptionsAndLatencies


_LOGGER = logging.getLogger(__name__)


class ImpressionsAPI(object):  # pylint: disable=too-few-public-methods
    """Class that uses an httpClient to communicate with the impressions API."""

    def __init__(self, client, sdk_key, sdk_metadata, telemetry_runtime_producer, mode=ImpressionsMode.OPTIMIZED):
        """
        Class constructor.

        :param client: HTTP Client responsble for issuing calls to the backend.
        :type client: HttpClient
        :param sdk_key: sdk key.
        :type sdk_key: string
        """
        self._client = client
        self._sdk_key = sdk_key
        self._metadata = headers_from_metadata(sdk_metadata)
        self._metadata['SplitSDKImpressionsMode'] = mode.name
        self._telemetry_runtime_producer = telemetry_runtime_producer

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
        start = get_current_epoch_time_ms()
        try:
            response = self._client.post(
                'events',
                '/testImpressions/bulk',
                self._sdk_key,
                body=bulk,
                extra_headers=self._metadata,
            )
            record_telemetry(response.status_code, get_current_epoch_time_ms() - start, HTTPExceptionsAndLatencies.IMPRESSION, self._telemetry_runtime_producer)
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
        start = get_current_epoch_time_ms()
        try:
            response = self._client.post(
                'events',
                '/testImpressions/count',
                self._sdk_key,
                body=bulk,
                extra_headers=self._metadata,
            )
            record_telemetry(response.status_code, get_current_epoch_time_ms() - start, HTTPExceptionsAndLatencies.IMPRESSION_COUNT, self._telemetry_runtime_producer)
            if not 200 <= response.status_code < 300:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            _LOGGER.error(
                'Error posting impressions counters because an exception was raised by the '
                'HTTPClient'
            )
            _LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Impressions not flushed properly.') from exc
