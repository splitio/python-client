"""Split Synchronization task."""

import logging
from splitio_commons.tasks import BaseSynchronizationTask
from splitio_commons.tasks.util.asynctask import AsyncTask, AsyncTaskAsync


_LOGGER = logging.getLogger(__name__)


class SplitSynchronizationTaskBase(BaseSynchronizationTask):
    """Split Synchronization task class."""

    def start(self):
        """Start the task."""
        self._task.start()

    def stop(self, event=None):
        """Stop the task. Accept an optional event to set when the task has finished."""
        pass

    def is_running(self):
        """
        Return whether the task is running.

        :return: True if the task is running. False otherwise.
        :rtype bool
        """
        return self._task.running()


class SplitSynchronizationTask(SplitSynchronizationTaskBase):
    """Split Synchronization task class."""

    def __init__(self, synchronize_definitions, period):
        """
        Class constructor.

        :param synchronize_definitions: Handler
        :type synchronize_definitions: func
        :param period: Period of task
        :type period: int
        """
        self._period = period
        self._task = AsyncTask(synchronize_definitions, period, on_init=None)

    def stop(self, event=None):
        """Stop the task. Accept an optional event to set when the task has finished."""
        self._task.stop(event)


class SplitSynchronizationTaskAsync(SplitSynchronizationTaskBase):
    """Split Synchronization async task class."""

    def __init__(self, synchronize_definitions, period):
        """
        Class constructor.

        :param synchronize_definitions: Handler
        :type synchronize_definitions: func
        :param period: Period of task
        :type period: int
        """
        self._period = period
        self._task = AsyncTaskAsync(synchronize_definitions, period, on_init=None)

    async def stop(self, event=None):
        """Stop the task. Accept an optional event to set when the task has finished."""
        await self._task.stop(True)
