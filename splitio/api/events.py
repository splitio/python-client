"""Events API module."""
import logging

from splitio.api import APIException
from splitio.api.client import HttpClientException
from splitio.api.commons import headers_from_metadata


_LOGGER = logging.getLogger(__name__)


class EventsAPI(object):  # pylint: disable=too-few-public-methods
    """Class that uses an httpClient to communicate with the events API."""

    def __init__(self, http_client, apikey, sdk_metadata):
        """
        Class constructor.

        :param http_client: HTTP Client responsble for issuing calls to the backend.
        :type http_client: HttpClient
        :param apikey: User apikey token.
        :type apikey: string
        :param sdk_metadata: SDK version & machine name & IP.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        """
        self._client = http_client
        self._apikey = apikey
        self._metadata = headers_from_metadata(sdk_metadata)

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
                '/events/bulk',
                self._apikey,
                body=bulk,
                extra_headers=self._metadata
            )
            if not 200 <= response.status_code < 300:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            _LOGGER.error('Error posting events because an exception was raised by the HTTPClient')
            _LOGGER.debug('Error: ', exc_info=True)
            raise APIException('Events not flushed properly.') from exc
