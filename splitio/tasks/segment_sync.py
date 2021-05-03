"""Segment syncrhonization module."""

import logging
from splitio.tasks import BaseSynchronizationTask
from splitio.tasks.util import asynctask


_LOGGER = logging.getLogger(__name__)


class SegmentSynchronizationTask(BaseSynchronizationTask):
    """Segment Syncrhonization class."""

    def __init__(self, synchronize_segments, period):
        """
        Clas constructor.

        :param synchronize_segments: handler for syncing segments
        :type synchronize_segments: func

        """
        self._task = asynctask.AsyncTask(synchronize_segments, period, on_init=None)

    def start(self):
        """Start segment synchronization."""
        self._task.start()

    def stop(self, event=None):
        """Stop segment synchronization."""
        self._task.stop(event)

    def is_running(self):
        """
        Return whether the task is running or not.

        :return: True if the task is running. False otherwise.
        :rtype: bool
        """
        return self._task.running()
