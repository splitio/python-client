"""Split Synchronization task."""

import logging
from splitio.tasks import BaseSynchronizationTask
from splitio.tasks.util.asynctask import AsyncTask


_LOGGER = logging.getLogger(__name__)


class SplitSynchronizationTask(BaseSynchronizationTask):
    """Split Synchronization task class."""
    def __init__(self, synchronize_splits, period):
        """
        Class constructor.

        :param synchronize_splits: Handler
        :type synchronize_splits: func
        :param period: Period of task
        :type period: int
        """
        self._period = period
        self._task = AsyncTask(synchronize_splits, period, on_init=None)

    def start(self):
        """Start the task."""
        self._task.start()

    def stop(self, event=None):
        """Stop the task. Accept an optional event to set when the task has finished."""
        self._task.stop(event)

    def is_running(self):
        """
        Return whether the task is running.

        :return: True if the task is running. False otherwise.
        :rtype bool
        """
        return self._task.running()
