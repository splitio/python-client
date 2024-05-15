"""Splits API module."""

import logging
import json
import time

from splitio.api import APIException
from splitio.api.commons import headers_from_metadata, build_fetch, record_telemetry
from splitio.util.time import get_current_epoch_time_ms
from splitio.api.client import HttpClientException
from splitio.models.telemetry import HTTPExceptionsAndLatencies

_LOGGER = logging.getLogger(__name__)


class SplitsAPI(object):  # pylint: disable=too-few-public-methods
    """Class that uses an httpClient to communicate with the splits API."""

    def __init__(self, client, sdk_key, sdk_metadata, telemetry_runtime_producer):
        """
        Class constructor.

        :param client: HTTP Client responsble for issuing calls to the backend.
        :type client: HttpClient
        :param sdk_key: User sdk_key token.
        :type sdk_key: string
        :param sdk_metadata: SDK version & machine name & IP.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        """
        self._client = client
        self._sdk_key = sdk_key
        self._metadata = headers_from_metadata(sdk_metadata)
        self._telemetry_runtime_producer = telemetry_runtime_producer

    def fetch_splits(self, change_number, fetch_options):
        """
        Fetch feature flags from backend.

        :param change_number: Last known timestamp of a split modification.
        :type change_number: int

        :param fetch_options: Fetch options for getting feature flag definitions.
        :type fetch_options: splitio.api.commons.FetchOptions

        :return: Json representation of a splitChanges response.
        :rtype: dict
        """
        start = get_current_epoch_time_ms()
        try:
            query, extra_headers = build_fetch(change_number, fetch_options, self._metadata)
            response = self._client.get(
                'sdk',
                '/splitChanges',
                self._sdk_key,
                extra_headers=extra_headers,
                query=query,
            )
            record_telemetry(response.status_code, get_current_epoch_time_ms() - start, HTTPExceptionsAndLatencies.SPLIT, self._telemetry_runtime_producer)
            if 200 <= response.status_code < 300:
                return json.loads(response.body)
            else:
                if response.status_code == 414:
                    _LOGGER.error('Error fetching feature flags; the amount of flag sets provided are too big, causing uri length error.')
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            _LOGGER.error('Error fetching feature flags because an exception was raised by the HTTPClient')
            _LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Feature flags not fetched correctly.') from exc
