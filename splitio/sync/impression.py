import logging
import queue

from splitio.api import APIException

_LOGGER = logging.getLogger(__name__)


class ImpressionSynchronizer(object):
    def __init__(self, impressions_api, storage, bulk_size):
        """
        Class constructor.

        :param impressions_api: Impressions Api object to send data to the backend
        :type impressions_api: splitio.api.impressions.ImpressionsAPI
        :param storage: Impressions Storage
        :type storage: splitio.storage.ImpressionsStorage
        :param bulk_size: How many impressions to send per push.
        :type bulk_size: int

        """
        self._api = impressions_api
        self._impression_storage = storage
        self._bulk_size = bulk_size
        self._failed = queue.Queue()

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

    def synchronize_impressions(self):
        """Send impressions from both the failed and new queues."""
        to_send = self._get_failed()
        if len(to_send) < self._bulk_size:
            # If the amount of previously failed items is less than the bulk
            # size, try to complete with new impressions from storage
            to_send.extend(self._impression_storage.pop_many(self._bulk_size - len(to_send)))

        if not to_send:
            return

        try:
            self._api.flush_impressions(to_send)
        except APIException:
            _LOGGER.error('Exception raised while reporting impressions')
            _LOGGER.debug('Exception information: ', exc_info=True)
            self._add_to_failed_queue(to_send)


class ImpressionsCountSynchronizer(object):
    def __init__(self, impressions_api, imp_counter):
        """
        Class constructor.

        :param impressions_api: Impressions Api object to send data to the backend
        :type impressions_api: splitio.api.impressions.ImpressionsAPI
        :param impressions_manager: Impressions manager instance
        :type impressions_manager: splitio.engine.impressions.Manager

        """
        self._impressions_api = impressions_api
        self._impressions_counter = imp_counter

    def synchronize_counters(self):
        """Send impressions from both the failed and new queues."""

        if self._impressions_counter == None:
            return

        to_send = self._impressions_counter.pop_all()
        if not to_send:
            return

        try:
            self._impressions_api.flush_counters(to_send)
        except APIException:
            _LOGGER.error('Exception raised while reporting impression counts')
            _LOGGER.debug('Exception information: ', exc_info=True)
