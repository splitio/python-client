"""Impressions syncrhonization task."""
import logging

from splitio.tasks import BaseSynchronizationTask
from splitio.tasks.util.asynctask import AsyncTask


_LOGGER = logging.getLogger(__name__)
_UNIQUE_KEYS_SYNC_PERIOD = 15 * 60  # 15 minutes
_CLEAR_FILTER_SYNC_PERIOD = 60 * 60 * 24  # 24 hours


class UniqueKeysSyncTask(BaseSynchronizationTask):
    """Unique Keys synchronization task uses an asynctask.AsyncTask to send MTKs."""

    def __init__(self, synchronize_unique_keys, period = None):
        """
        Class constructor.

        :param synchronize_unique_keys: sender
        :type synchronize_unique_keys: func
        :param period: How many seconds to wait between subsequent unique keys pushes to the BE.
        :type period: int
        """

        if period == None:
            period = _UNIQUE_KEYS_SYNC_PERIOD
        self._task = AsyncTask(synchronize_unique_keys, period,
                               on_stop=synchronize_unique_keys)

    def start(self):
        """Start executing the unique keys synchronization task."""
        self._task.start()

    def stop(self, event=None):
        """Stop executing the unique keys synchronization task."""
        self._task.stop(event)

    def is_running(self):
        """
        Return whether the task is running or not.

        :return: True if the task is running. False otherwise.
        :rtype: bool
        """
        return self._task.running()

    def flush(self):
        """Flush unique keys."""
        _LOGGER.debug('Forcing flush execution for unique keys')
        self._task.force_execution()

class ClearFilterSyncTask(BaseSynchronizationTask):
    """Unique Keys synchronization task uses an asynctask.AsyncTask to send MTKs."""

    def __init__(self, clear_filter, period = None):
        """
        Class constructor.

        :param synchronize_unique_keys: sender
        :type synchronize_unique_keys: func
        :param period: How many seconds to wait between subsequent clearing of bloom filter
        :type period: int
        """
        if period == None:
            period = _CLEAR_FILTER_SYNC_PERIOD

        self._task = AsyncTask(clear_filter, period,
                               on_stop=clear_filter)

    def start(self):
        """Start executing the unique keys synchronization task."""

        self._task.start()

    def stop(self, event=None):
        """Stop executing the unique keys synchronization task."""

        self._task.stop(event)

    def is_running(self):
        """
        Return whether the task is running or not.

        :return: True if the task is running. False otherwise.
        :rtype: bool
        """
        return self._task.running()
