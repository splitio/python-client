"""Impressions syncrhonization task."""
import logging

from splitio.tasks import BaseSynchronizationTask
from splitio.tasks.util.asynctask import AsyncTask, AsyncTaskAsync


_LOGGER = logging.getLogger(__name__)


class ImpressionsSyncTaskBase(BaseSynchronizationTask):
    """Impressions synchronization task uses an asynctask.AsyncTask to send impressions."""

    def start(self):
        """Start executing the impressions synchronization task."""
        self._task.start()

    def stop(self, event=None):
        """Stop executing the impressions synchronization task."""
        pass

    def is_running(self):
        """
        Return whether the task is running or not.

        :return: True if the task is running. False otherwise.
        :rtype: bool
        """
        return self._task.running()

    def flush(self):
        """Flush impressions in storage."""
        _LOGGER.debug('Forcing flush execution for impressions')
        self._task.force_execution()


class ImpressionsSyncTask(ImpressionsSyncTaskBase):
    """Impressions synchronization task uses an asynctask.AsyncTask to send impressions."""

    def __init__(self, synchronize_impressions, period):
        """
        Class constructor.

        :param synchronize_impressions: sender
        :type synchronize_impressions: func
        :param period: How many seconds to wait between subsequent impressions pushes to the BE.
        :type period: int

        """
        self._period = period
        self._task = AsyncTask(synchronize_impressions, self._period,
                               on_stop=synchronize_impressions)

    def stop(self, event=None):
        """Stop executing the impressions synchronization task."""
        self._task.stop(event)


class ImpressionsSyncTaskAsync(ImpressionsSyncTaskBase):
    """Impressions synchronization task uses an asynctask.AsyncTask to send impressions."""

    def __init__(self, synchronize_impressions, period):
        """
        Class constructor.

        :param synchronize_impressions: sender
        :type synchronize_impressions: func
        :param period: How many seconds to wait between subsequent impressions pushes to the BE.
        :type period: int

        """
        self._period = period
        self._task = AsyncTaskAsync(synchronize_impressions, self._period,
                               on_stop=synchronize_impressions)

    async def stop(self, event=None):
        """Stop executing the impressions synchronization task."""
        await self._task.stop(True)


class ImpressionsCountSyncTaskBase(BaseSynchronizationTask):
    """Impressions synchronization task uses an asynctask.AsyncTask to send impressions."""

    _PERIOD = 1800  # 30 * 60 # 30 minutes

    def start(self):
        """Start executing the impressions synchronization task."""
        self._task.start()

    def stop(self, event=None):
        """Stop executing the impressions synchronization task."""
        pass

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


class ImpressionsCountSyncTask(ImpressionsCountSyncTaskBase):
    """Impressions synchronization task uses an asynctask.AsyncTask to send impressions."""

    def __init__(self, synchronize_counters):
        """
        Class constructor.

        :param synchronize_counters: Handler
        :type synchronize_counters: func

        """
        self._task = AsyncTask(synchronize_counters, self._PERIOD, on_stop=synchronize_counters)

    def stop(self, event=None):
        """Stop executing the impressions synchronization task."""
        self._task.stop(event)


class ImpressionsCountSyncTaskAsync(ImpressionsCountSyncTaskBase):
    """Impressions synchronization task uses an asynctask.AsyncTask to send impressions."""

    def __init__(self, synchronize_counters):
        """
        Class constructor.

        :param synchronize_counters: Handler
        :type synchronize_counters: func

        """
        self._task = AsyncTaskAsync(synchronize_counters, self._PERIOD, on_stop=synchronize_counters)

    async def stop(self):
        """Stop executing the impressions synchronization task."""
        await self._task.stop(True)
