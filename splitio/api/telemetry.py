"""Telemetry API Module."""
import logging

import six
from future.utils import raise_from

from splitio.api import APIException, headers_from_metadata
from splitio.api.client import HttpClientException


class TelemetryAPI(object):
    """Class to handle telemetry submission to the backend."""

    def __init__(self, client, apikey, sdk_metadata):
        """
        Class constructor.

        :param client: HTTP Client responsble for issuing calls to the backend.
        :type client: HttpClient
        :param apikey: User apikey token.
        :type apikey: string
        :param sdk_metadata: SDK Version, IP & Machine name
        :type sdk_metadata: splitio.client.util.SdkMetadata
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._client = client
        self._apikey = apikey
        self._metadata = headers_from_metadata(sdk_metadata)
    @staticmethod
    def _build_latencies(latencies):
        """
        Build a latencies bulk as expected by the BE.

        :param latencies: Latencies to bundle.
        :type latencies: dict
        """
        return [
            {'name': name, 'latencies': latencies_list}
            for name, latencies_list in six.iteritems(latencies)
        ]

    def flush_latencies(self, latencies):
        """
        Submit latencies to the backend.

        :param latencies: List of latency buckets with their respective count.
        :type latencies: list
        """
        bulk = self._build_latencies(latencies)
        try:
            response = self._client.post(
                'events',
                '/metrics/times',
                self._apikey,
                body=bulk,
                extra_headers=self._metadata
            )
            if not 200 <= response.status_code < 300:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            self._logger.error('Http client is throwing exceptions')
            self._logger.debug('Error: ', exc_info=True)
            raise_from(APIException('Latencies not flushed correctly.'), exc)

    @staticmethod
    def _build_gauges(gauges):
        """
        Build a gauges bulk as expected by the BE.

        :param gauges: Gauges to bundle.
        :type gauges: dict
        """
        return [
            {'name': name, 'value': value}
            for name, value in six.iteritems(gauges)
        ]

    def flush_gauges(self, gauges):
        """
        Submit gauges to the backend.

        :param gauges: Gauges measured to be sent to the backend.
        :type gauges: List
        """
        bulk = self._build_gauges(gauges)
        try:
            response = self._client.post(
                'events',
                '/metrics/gauge',
                self._apikey,
                body=bulk,
                extra_headers=self._metadata
            )
            if not 200 <= response.status_code < 300:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            self._logger.error('Http client is throwing exceptions')
            self._logger.debug('Error: ', exc_info=True)
            raise_from(APIException('Gauges not flushed correctly.'), exc)

    @staticmethod
    def _build_counters(counters):
        """
        Build a counters bulk as expected by the BE.

        :param counters: Counters to bundle.
        :type counters: dict
        """
        return [
            {'name': name, 'delta': value}
            for name, value in six.iteritems(counters)
        ]

    def flush_counters(self, counters):
        """
        Submit counters to the backend.

        :param counters: Counters measured to be sent to the backend.
        :type counters: List
        """
        bulk = self._build_counters(counters)
        try:
            response = self._client.post(
                'events',
                '/metrics/counters',
                self._apikey,
                body=bulk,
                extra_headers=self._metadata
            )
            if not 200 <= response.status_code < 300:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            self._logger.error('Http client is throwing exceptions')
            self._logger.debug('Error: ', exc_info=True)
            raise_from(APIException('Counters not flushed correctly.'), exc)
