"""Splits API module."""

import logging
import json

from splitio.api import APIException, headers_from_metadata
from splitio.api.commons import build_fetch, FetchOptions
from splitio.api.client import HttpClientException
from splitio.models.telemetry import HTTPExceptionsAndLatencies
from splitio.util.time import utctime_ms
from splitio.spec import SPEC_VERSION
from splitio.sync import util

_LOGGER = logging.getLogger(__name__)
_SPEC_1_1 = "1.1"
_PROXY_CHECK_INTERVAL_MILLISECONDS_SS =  24 * 60 * 60 * 1000

class SplitsAPIBase(object):  # pylint: disable=too-few-public-methods
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
        self._client.set_telemetry_data(HTTPExceptionsAndLatencies.SPLIT, self._telemetry_runtime_producer)
        self._spec_version = SPEC_VERSION
        self._last_proxy_check_timestamp = 0
        self.clear_storage = False
        self._old_spec_since = None

    def _check_last_proxy_check_timestamp(self, since):
        if self._spec_version == _SPEC_1_1 and ((utctime_ms() - self._last_proxy_check_timestamp) >= _PROXY_CHECK_INTERVAL_MILLISECONDS_SS):
            _LOGGER.info("Switching to new Feature flag spec (%s) and fetching.", SPEC_VERSION);
            self._spec_version = SPEC_VERSION
            self._old_spec_since = since
    
    def _check_old_spec_since(self, change_number):
        if self._spec_version == _SPEC_1_1 and self._old_spec_since is not None:
            since = self._old_spec_since
            self._old_spec_since = None
            return since
        return change_number
    

class SplitsAPI(SplitsAPIBase):  # pylint: disable=too-few-public-methods
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
        SplitsAPIBase.__init__(self, client, sdk_key, sdk_metadata, telemetry_runtime_producer)

    def fetch_splits(self, change_number, rbs_change_number, fetch_options):
        """
        Fetch feature flags from backend.

        :param change_number: Last known timestamp of a split modification.
        :type change_number: int

        :param rbs_change_number: Last known timestamp of a rule based segment modification.
        :type rbs_change_number: int

        :param fetch_options: Fetch options for getting feature flag definitions.
        :type fetch_options: splitio.api.commons.FetchOptions

        :return: Json representation of a splitChanges response.
        :rtype: dict
        """
        try:
            self._check_last_proxy_check_timestamp(change_number)
            change_number = self._check_old_spec_since(change_number)

            if self._spec_version == _SPEC_1_1:
                fetch_options = FetchOptions(fetch_options.cache_control_headers, fetch_options.change_number,
                                                                               None, fetch_options.sets, self._spec_version)
                rbs_change_number = None
            query, extra_headers = build_fetch(change_number, fetch_options, self._metadata, rbs_change_number)
            response = self._client.get(
                'sdk',
                'splitChanges',
                self._sdk_key,
                extra_headers=extra_headers,
                query=query,
            )
            if 200 <= response.status_code < 300:
                if self._spec_version == _SPEC_1_1:
                    return util.convert_to_new_spec(json.loads(response.body))
                
                self.clear_storage = self._last_proxy_check_timestamp != 0
                self._last_proxy_check_timestamp = 0
                return json.loads(response.body)

            else:
                if response.status_code == 414:
                    _LOGGER.error('Error fetching feature flags; the amount of flag sets provided are too big, causing uri length error.')
                    
                if self._client.is_sdk_endpoint_overridden() and response.status_code == 400 and self._spec_version == SPEC_VERSION:
                    _LOGGER.warning('Detected proxy response error, changing spec version from %s to %s and re-fetching.', self._spec_version, _SPEC_1_1)
                    self._spec_version = _SPEC_1_1
                    self._last_proxy_check_timestamp = utctime_ms()                    
                    return self.fetch_splits(change_number, None, FetchOptions(fetch_options.cache_control_headers, fetch_options.change_number,
                                                                               None, fetch_options.sets, self._spec_version))
                    
                raise APIException(response.body, response.status_code)
            
        except HttpClientException as exc:
            _LOGGER.error('Error fetching feature flags because an exception was raised by the HTTPClient')
            _LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Feature flags not fetched correctly.') from exc

class SplitsAPIAsync(SplitsAPIBase):  # pylint: disable=too-few-public-methods
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
        SplitsAPIBase.__init__(self, client, sdk_key, sdk_metadata, telemetry_runtime_producer)

    async def fetch_splits(self, change_number, rbs_change_number, fetch_options):
        """
        Fetch feature flags from backend.

        :param change_number: Last known timestamp of a split modification.
        :type change_number: int
        
        :param rbs_change_number: Last known timestamp of a rule based segment modification.
        :type rbs_change_number: int

        :param fetch_options: Fetch options for getting feature flag definitions.
        :type fetch_options: splitio.api.commons.FetchOptions

        :return: Json representation of a splitChanges response.
        :rtype: dict
        """
        try:
            self._check_last_proxy_check_timestamp(change_number)
            change_number = self._check_old_spec_since(change_number)
            if self._spec_version == _SPEC_1_1:
                fetch_options = FetchOptions(fetch_options.cache_control_headers, fetch_options.change_number,
                                                                               None, fetch_options.sets, self._spec_version)
                rbs_change_number = None
            
            query, extra_headers = build_fetch(change_number, fetch_options, self._metadata, rbs_change_number)
            response = await self._client.get(
                'sdk',
                'splitChanges',
                self._sdk_key,
                extra_headers=extra_headers,
                query=query,
            )
            if 200 <= response.status_code < 300:
                if self._spec_version == _SPEC_1_1:
                    return util.convert_to_new_spec(json.loads(response.body))
                
                self.clear_storage = self._last_proxy_check_timestamp != 0
                self._last_proxy_check_timestamp = 0
                return json.loads(response.body)

            else:
                if response.status_code == 414:
                    _LOGGER.error('Error fetching feature flags; the amount of flag sets provided are too big, causing uri length error.')
                    
                if self._client.is_sdk_endpoint_overridden() and response.status_code == 400 and self._spec_version == SPEC_VERSION:
                    _LOGGER.warning('Detected proxy response error, changing spec version from %s to %s and re-fetching.', self._spec_version, _SPEC_1_1)
                    self._spec_version = _SPEC_1_1
                    self._last_proxy_check_timestamp = utctime_ms()                    
                    return await self.fetch_splits(change_number, None, FetchOptions(fetch_options.cache_control_headers, fetch_options.change_number,
                                                                               None, fetch_options.sets, self._spec_version))

                raise APIException(response.body, response.status_code)
            
        except HttpClientException as exc:
            _LOGGER.error('Error fetching feature flags because an exception was raised by the HTTPClient')
            _LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Feature flags not fetched correctly.') from exc
