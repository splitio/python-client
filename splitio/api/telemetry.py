"""Impressions API module."""
import logging
import time

from splitio.api import APIException
from splitio.api.client import HttpClientException
from splitio.api.commons import headers_from_metadata, record_telemetry, get_current_epoch_time
from splitio.models.telemetry import HTTPExceptionsAndLatencies

_LOGGER = logging.getLogger(__name__)

class TelemetryAPI(object):  # pylint: disable=too-few-public-methods
    """Class that uses an httpClient to communicate with the Telemetry API."""

    def __init__(self, client, apikey, sdk_metadata, telemetry_runtime_producer):
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
        self._telemetry_runtime_producer = telemetry_runtime_producer

    def record_unique_keys(self, uniques):
        """
        Send unique keys to the backend.

        :param uniques: Unique Keys
        :type json
        """
        start = get_current_epoch_time()
        try:
            response = self._client.post(
                'telemetry',
                '/v1/keys/ss',
                self._apikey,
                body=uniques,
                extra_headers=self._metadata
            )
            record_telemetry(response.status_code,  get_current_epoch_time() - start, HTTPExceptionsAndLatencies.TELEMETRY, self._telemetry_runtime_producer)
            if not 200 <= response.status_code < 300:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            _LOGGER.debug(
                'Error posting unique keys because an exception was raised by the HTTPClient'
            )
            _LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Unique keys not flushed properly.') from exc

    def record_init(self, configs):
        """
        Send init config data to the backend.

        :param configs: configs
        :type json
        """
        start = get_current_epoch_time()
        try:
            response = self._client.post(
                'telemetry',
                '/v1/metrics/config',
                self._apikey,
                body=configs,
                extra_headers=self._metadata,
            )
            record_telemetry(response.status_code, get_current_epoch_time() - start, HTTPExceptionsAndLatencies.TELEMETRY, self._telemetry_runtime_producer)
            if not 200 <= response.status_code < 300:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            _LOGGER.debug(
                'Error posting init config because an exception was raised by the HTTPClient'
            )
            _LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Init config data not flushed properly.') from exc

    def record_stats(self, stats):
        """
        Send runtime stats to the backend.

        :param stats: stats
        :type json
        """
        start = get_current_epoch_time()
        try:
            response = self._client.post(
                'telemetry',
                '/v1/metrics/usage',
                self._apikey,
                body=stats,
                extra_headers=self._metadata,
            )
            record_telemetry(response.status_code, get_current_epoch_time() - start, HTTPExceptionsAndLatencies.TELEMETRY, self._telemetry_runtime_producer)
            if not 200 <= response.status_code < 300:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            _LOGGER.debug(
                'Error posting runtime stats because an exception was raised by the HTTPClient'
            )
            _LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Runtime stats not flushed properly.') from exc
