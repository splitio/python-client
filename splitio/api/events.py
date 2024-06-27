"""Events API module."""
import logging

from splitio.api import APIException, headers_from_metadata
from splitio.api.client import HttpClientException
from splitio.models.telemetry import HTTPExceptionsAndLatencies


_LOGGER = logging.getLogger(__name__)


class EventsAPIBase(object):  # pylint: disable=too-few-public-methods
    """Base Class that uses an httpClient to communicate with the events API."""

    @staticmethod
    def _build_bulk(events):
        """
        Build event bulk as expected by the API.

        :param events: Events to be bundled.
        :type events: list(splitio.models.events.Event)

        :return: Formatted bulk.
        :rtype: dict
        """
        return [
            {
                'key': event.key,
                'trafficTypeName': event.traffic_type_name,
                'eventTypeId': event.event_type_id,
                'value': event.value,
                'timestamp': event.timestamp,
                'properties': event.properties,
            }
            for event in events
        ]


class EventsAPI(EventsAPIBase):  # pylint: disable=too-few-public-methods
    """Class that uses an httpClient to communicate with the events API."""

    def __init__(self, http_client, sdk_key, sdk_metadata, telemetry_runtime_producer):
        """
        Class constructor.

        :param http_client: HTTP Client responsble for issuing calls to the backend.
        :type http_client: HttpClient
        :param sdk_key: sdk key.
        :type sdk_key: string
        :param sdk_metadata: SDK version & machine name & IP.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        """
        self._client = http_client
        self._sdk_key = sdk_key
        self._metadata = headers_from_metadata(sdk_metadata)
        self._telemetry_runtime_producer = telemetry_runtime_producer
        self._client.set_telemetry_data(HTTPExceptionsAndLatencies.EVENT, self._telemetry_runtime_producer)

    def flush_events(self, events):
        """
        Send events to the backend.

        :param events: Events bulk
        :type events: list

        :return: True if flush was successful. False otherwise
        :rtype: bool
        """
        bulk = self._build_bulk(events)
        try:
            response = self._client.post(
                'events',
                'events/bulk',
                self._sdk_key,
                body=bulk,
                extra_headers=self._metadata,
            )
            if not 200 <= response.status_code < 300:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            _LOGGER.error('Error posting events because an exception was raised by the HTTPClient')
            _LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Events not flushed properly.') from exc

class EventsAPIAsync(EventsAPIBase):  # pylint: disable=too-few-public-methods
    """Async Class that uses an httpClient to communicate with the events API."""

    def __init__(self, http_client, sdk_key, sdk_metadata, telemetry_runtime_producer):
        """
        Class constructor.

        :param http_client: HTTP Client responsble for issuing calls to the backend.
        :type http_client: HttpClient
        :param sdk_key: sdk key.
        :type sdk_key: string
        :param sdk_metadata: SDK version & machine name & IP.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        """
        self._client = http_client
        self._sdk_key = sdk_key
        self._metadata = headers_from_metadata(sdk_metadata)
        self._telemetry_runtime_producer = telemetry_runtime_producer
        self._client.set_telemetry_data(HTTPExceptionsAndLatencies.EVENT, self._telemetry_runtime_producer)

    async def flush_events(self, events):
        """
        Send events to the backend.

        :param events: Events bulk
        :type events: list

        :return: True if flush was successful. False otherwise
        :rtype: bool
        """
        bulk = self._build_bulk(events)
        try:
            response = await self._client.post(
                'events',
                'events/bulk',
                self._sdk_key,
                body=bulk,
                extra_headers=self._metadata,
            )
            if not 200 <= response.status_code < 300:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            _LOGGER.error('Error posting events because an exception was raised by the HTTPClient')
            _LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Events not flushed properly.') from exc
