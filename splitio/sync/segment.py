import logging

from splitio.api import APIException
from splitio.models import splits
from splitio.tasks.util import workerpool
from splitio.models import segments


_LOGGER = logging.getLogger(__name__)


class SegmentSynchronizer(object):
    def __init__(self, segment_api, split_storage, segment_storage):
        """
        Class constructor.

        :param segment_api: API to retrieve segments from backend.
        :type segment_api: splitio.api.SegmentApi

        :param split_storage: Split Storage.
        :type split_storage: splitio.storage.InMemorySplitStorage

        :param segment_storage: Segment storage reference.
        :type segment_storage: splitio.storage.SegmentStorage

        """
        self._api = segment_api
        self._split_storage = split_storage
        self._segment_storage = segment_storage
        self._worker_pool = workerpool.WorkerPool(10, self.synchronize_segment)
        self._worker_pool.start()

    def recreate(self):
        """
        Create worker_pool on forked processes.

        """
        self._worker_pool = workerpool.WorkerPool(10, self.synchronize_segment)
        self._worker_pool.start()

    def shutdown(self):
        """
        Shutdown worker_pool

        """
        self._worker_pool.stop()

    def synchronize_segment(self, segment_name, till=None):
        """
        Update a segment from queue

        :param segment_name: Name of the segment to update.
        :type segment_name: str

        :param till: ChangeNumber received.
        :type till: int

        """
        while True:
            change_number = self._segment_storage.get_change_number(segment_name)
            if change_number is None:
                change_number = -1
            if till is not None and till < change_number:
                # the passed till is less than change_number, no need to perform updates
                return

            try:
                segment_changes = self._api.fetch_segment(segment_name, change_number)
            except APIException as exc:
                _LOGGER.error('Exception raised while fetching segment %s', segment_name)
                _LOGGER.debug('Exception information: ', exc_info=True)
                raise exc

            if change_number == -1:  # first time fetching the segment
                new_segment = segments.from_raw(segment_changes)
                self._segment_storage.put(new_segment)
            else:
                self._segment_storage.update(
                    segment_name,
                    segment_changes['added'],
                    segment_changes['removed'],
                    segment_changes['till']
                )

            if segment_changes['till'] == segment_changes['since']:
                return

    def synchronize_segments(self):
        """
        Submit all current segments and wait for them to finish, then set the ready flag.

        :return: True if no error occurs. False otherwise.
        :rtype: bool
        """
        segment_names = self._split_storage.get_segment_names()
        for segment_name in segment_names:
            self._worker_pool.submit_work(segment_name)
        return not self._worker_pool.wait_for_completion()
