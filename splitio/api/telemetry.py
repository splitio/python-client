"""Impressions API module."""
import logging

from splitio.api import APIException
from splitio.api.client import HttpClientException
from splitio.api.commons import headers_from_metadata

_LOGGER = logging.getLogger(__name__)

class TelemetryAPI(object):  # pylint: disable=too-few-public-methods
    """Class that uses an httpClient to communicate with the Telemetry API."""

    def __init__(self, client, apikey, sdk_metadata):
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

    def record_unique_keys(self, uniques):
        """
        Send unique keys to the backend.

        :param uniques: Unique Keys
        :type json
        """
        try:
            response = self._client.post(
                'telemetry',
                '/v1/keys/ss',
                self._apikey,
                body=uniques,
                extra_headers=self._metadata
            )
            if not 200 <= response.status_code < 300:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            _LOGGER.error(
                'Error posting unique keys because an exception was raised by the HTTPClient'
            )
            _LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Unique keys not flushed properly.') from exc
