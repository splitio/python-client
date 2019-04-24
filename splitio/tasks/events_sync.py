"""Events syncrhonization task."""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import logging

from six.moves import queue
from splitio.api import APIException
from splitio.tasks import BaseSynchronizationTask
from splitio.tasks.util.asynctask import AsyncTask


class EventsSyncTask(BaseSynchronizationTask):
    """Events synchronization task uses an asynctask.AsyncTask to send events."""

    def __init__(self, events_api, storage, period, bulk_size):
        """
        Class constructor.

        :param events_api: Events Api object to send data to the backend
        :type events_api: splitio.api.events.EventsAPI
        :param storage: Events Storage
        :type storage: splitio.storage.EventStorage
        :param period: How many seconds to wait between subsequent event pushes to the BE.
        :type period: int
        :param bulk_size: How many events to send per push.
        :type bulk_size: int
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._events_api = events_api
        self._storage = storage
        self._period = period
        self._failed = queue.Queue()
        self._bulk_size = bulk_size
        self._task = AsyncTask(self._send_events, self._period, on_stop=self._send_events)

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

    def _send_events(self):
        """Send events from both the failed and new queues."""
        to_send = self._get_failed()
        if len(to_send) < self._bulk_size:
            # If the amount of previously failed items is less than the bulk
            # size, try to complete with new events from storage
            to_send.extend(self._storage.pop_many(self._bulk_size - len(to_send)))

        if not to_send:
            return

        try:
            self._events_api.flush_events(to_send)
        except APIException as exc:
            self._logger.error(
                'Exception raised while reporting events: %s -- %d',
                exc.message,
                exc.status_code
            )
            self._add_to_failed_queue(to_send)

    def start(self):
        """Start executing the events synchronization task."""
        self._task.start()

    def stop(self, event=None):
        """Stop executing the events synchronization task."""
        self._task.stop(event)

    def flush(self):
        """Flush events in storage."""
        self._task.force_execution()

    def is_running(self):
        """
        Return whether the task is running or not.

        :return: True if the task is running. False otherwise.
        :rtype: bool
        """
        return self._task.running()
