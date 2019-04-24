"""Impressions syncrhonization task."""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import logging

from six.moves import queue

from splitio.api import APIException
from splitio.tasks import BaseSynchronizationTask
from splitio.tasks.util.asynctask import AsyncTask


class ImpressionsSyncTask(BaseSynchronizationTask):
    """Impressions synchronization task uses an asynctask.AsyncTask to send impressions."""

    def __init__(self, impressions_api, storage, period, bulk_size):
        """
        Class constructor.

        :param impressions_api: Impressions Api object to send data to the backend
        :type impressions_api: splitio.api.impressions.ImpressionsAPI
        :param storage: Impressions Storage
        :type storage: splitio.storage.ImpressionsStorage
        :param period: How many seconds to wait between subsequent impressions pushes to the BE.
        :type period: int
        :param bulk_size: How many impressions to send per push.
        :type bulk_size: int
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._impressions_api = impressions_api
        self._storage = storage
        self._period = period
        self._failed = queue.Queue()
        self._bulk_size = bulk_size
        self._task = AsyncTask(self._send_impressions, self._period, on_stop=self._send_impressions)

    def _get_failed(self):
        """Return up to <BULK_SIZE> impressions stored in the failed impressions queue."""
        imps = []
        count = 0
        while count < self._bulk_size:
            try:
                imps.append(self._failed.get(False))
                count += 1
            except queue.Empty:
                # If no more items in queue, break the loop
                break
        return imps

    def _add_to_failed_queue(self, imps):
        """
        Add impressions that were about to be sent to a secondary queue for failed sends.

        :param imps: List of impressions that failed to be pushed.
        :type imps: list
        """
        for impression in imps:
            self._failed.put(impression, False)

    def _send_impressions(self):
        """Send impressions from both the failed and new queues."""
        to_send = self._get_failed()
        if len(to_send) < self._bulk_size:
            # If the amount of previously failed items is less than the bulk
            # size, try to complete with new impressions from storage
            to_send.extend(self._storage.pop_many(self._bulk_size - len(to_send)))

        if not to_send:
            return

        try:
            self._impressions_api.flush_impressions(to_send)
        except APIException as exc:
            self._logger.error(
                'Exception raised while reporting impressions: %s -- %d',
                exc.message,
                exc.status_code
            )
            self._add_to_failed_queue(to_send)

    def start(self):
        """Start executing the impressions synchronization task."""
        self._task.start()

    def stop(self, event=None):
        """Stop executing the impressions synchronization task."""
        self._task.stop(event)

    def is_running(self):
        """
        Return whether the task is running or not.

        :return: True if the task is running. False otherwise.
        :rtype: bool
        """
        return self._task.running()

    def flush(self):
        """Flush impressions in storage."""
        self._task.force_execution()
