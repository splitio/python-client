"""Segment syncrhonization module."""

import logging
from splitio.api import APIException
from splitio.tasks import BaseSynchronizationTask
from splitio.tasks.util import asynctask, workerpool
from splitio.models import segments


class SegmentSynchronizationTask(BaseSynchronizationTask):  #pylint: disable=too-many-instance-attributes
    """Segment Syncrhonization class."""

    def __init__(self, segment_api, segment_storage, split_storage, period, event):  #pylint: disable=too-many-arguments
        """
        Clas constructor.

        :param segment_api: API to retrieve segments from backend.
        :type segment_api: splitio.api.SegmentApi

        :param segment_storage: Segment storage reference.
        :type segment_storage: splitio.storage.SegmentStorage

        :param event: Event to signal when all segments have finished initial sync.
        :type event: threading.Event
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._worker_pool = workerpool.WorkerPool(20, self._ensure_segment_is_updated)
        self._task = asynctask.AsyncTask(self._main, period, on_init=self._on_init)
        self._segment_api = segment_api
        self._segment_storage = segment_storage
        self._split_storage = split_storage
        self._event = event
        self._pending_initialization = []

    def _update_segment(self, segment_name):
        """
        Update a segment by hitting the split backend.

        :param segment_name: Name of the segment to update.
        :type segment_name: str
        """
        since = self._segment_storage.get_change_number(segment_name)
        if since is None:
            since = -1

        try:
            segment_changes = self._segment_api.fetch_segment(segment_name, since)
        except APIException:
            self._logger.error('Error fetching segments')
            return False

        if since == -1:  # first time fetching the segment
            new_segment = segments.from_raw(segment_changes)
            self._segment_storage.put(new_segment)
        else:
            self._segment_storage.update(
                segment_name,
                segment_changes['added'],
                segment_changes['removed'],
                segment_changes['till']
            )

        return segment_changes['till'] == segment_changes['since']

    def _main(self):
        """Submit all current segments and wait for them to finish."""
        segment_names = self._split_storage.get_segment_names()
        for segment_name in segment_names:
            self._worker_pool.submit_work(segment_name)

    def _on_init(self):
        """Submit all current segments and wait for them to finish, then set the ready flag."""
        self._main()
        self._worker_pool.wait_for_completion()
        self._event.set()

    def _ensure_segment_is_updated(self, segment_name):
        """
        Update a segment by hitting the split backend.

        :param segment_name: Name of the segment to update.
        :type segment_name: str
        """
        while True:
            ready = self._update_segment(segment_name)
            if ready:
                break

    def start(self):
        """Start segment synchronization."""
        self._worker_pool.start()
        self._task.start()

    def stop(self, event=None):
        """Stop segment synchronization."""
        self._task.stop()
        self._worker_pool.stop(event)

    def is_running(self):
        """
        Return whether the task is running or not.

        :return: True if the task is running. False otherwise.
        :rtype: bool
        """
        return self._task.running()
