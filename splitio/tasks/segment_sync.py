"""Segment syncrhonization module."""

import logging
from splitio.tasks import BaseSynchronizationTask
from splitio.tasks.util import asynctask


_LOGGER = logging.getLogger(__name__)


class SegmentSynchronizationTaskBase(BaseSynchronizationTask):
    """Segment Syncrhonization base class."""

    def start(self):
        """Start segment synchronization."""
        self._task.start()

    def stop(self, event=None):
        """Stop segment synchronization."""
        pass

    def is_running(self):
        """
        Return whether the task is running or not.

        :return: True if the task is running. False otherwise.
        :rtype: bool
        """
        return self._task.running()


class SegmentSynchronizationTask(SegmentSynchronizationTaskBase):
    """Segment Syncrhonization class."""

    def __init__(self, synchronize_segments, period):
        """
        Clas constructor.

        :param synchronize_segments: handler for syncing segments
        :type synchronize_segments: func

        """
        self._task = asynctask.AsyncTask(synchronize_segments, period, on_init=None)

    def stop(self, event=None):
        """Stop segment synchronization."""
        self._task.stop(event)


class SegmentSynchronizationTaskAsync(SegmentSynchronizationTaskBase):
    """Segment Syncrhonization async class."""

    def __init__(self, synchronize_segments, period):
        """
        Clas constructor.

        :param synchronize_segments: handler for syncing segments
        :type synchronize_segments: func

        """
        self._task = asynctask.AsyncTaskAsync(synchronize_segments, period, on_init=None)

    async def stop(self):
        """Stop segment synchronization."""
        await self._task.stop(True)
