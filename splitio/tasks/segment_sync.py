"""Segment syncrhonization module."""

import logging
from splitio.api import APIException
from splitio.tasks import BaseSynchronizationTask
from splitio.tasks.util import asynctask, workerpool
from splitio.models import segments


class SegmentSynchronizationTask(BaseSynchronizationTask):  #pylint: disable=too-many-instance-attributes
    """Segment Syncrhonization class."""

    def __init__(self, segment_api, segment_storage, split_storage, period):  #pylint: disable=too-many-arguments
        """
        Clas constructor.

        :param segment_api: API to retrieve segments from backend.
        :type segment_api: splitio.api.SegmentApi

        :param segment_storage: Segment storage reference.
        :type segment_storage: splitio.storage.SegmentStorage

        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._worker_pool = workerpool.WorkerPool(10, self._ensure_segment_is_updated)
        self._task = asynctask.AsyncTask(self.update_segments, period, on_init=self.update_segments)
        self._segment_api = segment_api
        self._segment_storage = segment_storage
        self._split_storage = split_storage
        self._worker_pool.start()

    def _update_segment(self, segment_name, till=None):
        """
        Update a segment by hitting the split backend.

        :param segment_name: Name of the segment to update.
        :type segment_name: str
        """
        change_number = self._segment_storage.get_change_number(segment_name)
        if change_number is None:
            change_number = -1
        if till is not None and till < change_number: # the passed till is less than change_number, no need to perform updates
            return True

        try:
            segment_changes = self._segment_api.fetch_segment(segment_name, change_number)
        except APIException:
            self._logger.error('Error fetching segments')
            return False

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

        return segment_changes['till'] == segment_changes['since'] or (till is not None and segment_changes['till'] >= till)

    def _main(self):
        """Submit all current segments and wait for them to finish."""
        segment_names = self._split_storage.get_segment_names()
        for segment_name in segment_names:
            self._worker_pool.submit_work(segment_name)

    def _ensure_segment_is_updated(self, segment_name):
        """
        Update a segment by hitting the split backend.

        :param segment_name: Name of the segment to update.
        :type segment_name: str
        """
        while not self._update_segment(segment_name):
            pass

    def start(self):
        """Start segment synchronization."""
        self._task.start()

    def pause(self):
        """Pause segment synchronization."""
        self._task.stop()

    def stop(self, event=None):
        """Stop segment synchronization."""
        self._task.stop()
        if self._worker_pool is not None:
            self._worker_pool.stop(event)

    def is_running(self):
        """
        Return whether the task is running or not.

        :return: True if the task is running. False otherwise.
        :rtype: bool
        """
        return self._task.running()
    
    def update_segment(self, segment_name, till=None):
        """Synchronize particular segment when is needed after receiving an update from streaming."""
        while not self._update_segment(segment_name, till):
            pass
        return True

    def update_segments(self):
        print('update_segments')
        """Submit all current segments and wait for them to finish, then set the ready flag."""
        self._main()
        self._worker_pool.wait_for_completion()
        return True
