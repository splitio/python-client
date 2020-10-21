"""Segment syncrhonization module."""

import logging
from splitio.api import APIException
from splitio.tasks import BaseSynchronizationTask
from splitio.tasks.util import asynctask, workerpool


class SegmentSynchronizationTask(BaseSynchronizationTask):
    """Segment Syncrhonization class."""

    def __init__(self, synchronize_segments, worker_pool, period):
        """
        Clas constructor.

        :param synchronize_segments: handler for syncing segments
        :type synchronize_segments: func

        :param worker_pool: worker created by sync to be able to stop worker
        :type worker_pool:  splitio.tasks.util.WorkerPool

        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._worker_pool = worker_pool
        self._task = asynctask.AsyncTask(synchronize_segments, period, on_init=synchronize_segments)

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
