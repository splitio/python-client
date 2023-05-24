import logging
import queue

from splitio.api import APIException


_LOGGER = logging.getLogger(__name__)


class EventSynchronizer(object):
    def __init__(self, events_api, storage, bulk_size):
        """
        Class constructor.

        :param events_api: Events Api object to send data to the backend
        :type events_api: splitio.api.events.EventsAPI
        :param storage: Events Storage
        :type storage: splitio.storage.EventStorage
        :param bulk_size: How many events to send per push.
        :type bulk_size: int

        """
        self._api = events_api
        self._event_storage = storage
        self._bulk_size = bulk_size
        self._failed = queue.Queue()

    def _get_failed(self):
        """Return up to <BULK_SIZE> events stored in the failed eventes queue."""
        events = []
        count = 0
        while count < self._bulk_size:
            try:
                events.append(self._failed.get(False))
                count += 1
            except queue.Empty:
                # If no more items in queue, break the loop
                break
        return events

    def _add_to_failed_queue(self, events):
        """
        Add events that were about to be sent to a secondary queue for failed sends.

        :param events: List of events that failed to be pushed.
        :type events: list
        """
        for event in events:
            self._failed.put(event, False)

    def synchronize_events(self):
        """Send events from both the failed and new queues."""
        to_send = self._get_failed()
        if len(to_send) < self._bulk_size:
            # If the amount of previously failed items is less than the bulk
            # size, try to complete with new events from storage
            to_send.extend(self._event_storage.pop_many(self._bulk_size - len(to_send)))

        if not to_send:
            return

        try:
            self._api.flush_events(to_send)
        except APIException:
            _LOGGER.error('Exception raised while reporting events')
            _LOGGER.debug('Exception information: ', exc_info=True)
            self._add_to_failed_queue(to_send)
