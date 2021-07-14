"""Telemetry API Module."""
import logging

from splitio.api import APIException
from splitio.api.commons import headers_from_metadata
from splitio.api.client import HttpClientException


_LOGGER = logging.getLogger(__name__)


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
            for name, latencies_list in latencies.items()
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
            _LOGGER.error(
                'Error posting latencies because an exception was raised by the HTTPClient'
            )
            _LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Latencies not flushed correctly.') from exc

    @staticmethod
    def _build_gauges(gauges):
        """
        Build a gauges bulk as expected by the BE.

        :param gauges: Gauges to bundle.
        :type gauges: dict
        """
        return [
            {'name': name, 'value': value}
            for name, value in gauges.items()
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
            _LOGGER.error(
                'Error posting gauges because an exception was raised by the HTTPClient'
            )
            _LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Gauges not flushed correctly.') from exc

    @staticmethod
    def _build_counters(counters):
        """
        Build a counters bulk as expected by the BE.

        :param counters: Counters to bundle.
        :type counters: dict
        """
        return [
            {'name': name, 'delta': value}
            for name, value in counters.items()
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
            _LOGGER.error(
                'Error posting counters because an exception was raised by the HTTPClient'
            )
            _LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Counters not flushed correctly.') from exc
