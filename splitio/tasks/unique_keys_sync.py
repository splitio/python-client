"""Impressions syncrhonization task."""
import logging

from splitio.tasks import BaseSynchronizationTask
from splitio.tasks.util.asynctask import AsyncTask


_LOGGER = logging.getLogger(__name__)


class UniqueKeysSyncTask(BaseSynchronizationTask):
    """Unique Keys synchronization task uses an asynctask.AsyncTask to send MTKs."""

    def __init__(self, synchronize_unique_keys):
        """
        Class constructor.

        :param synchronize_unique_keys: sender
        :type synchronize_unique_keys: func
        :param period: How many seconds to wait between subsequent unique keys pushes to the BE.
        :type period: int
        """
        _period = 15 * 60  # 15 minutes
        self._task = AsyncTask(synchronize_unique_keys, _period,
                               on_stop=synchronize_unique_keys)

    def start(self):
        """Start executing the unique keys synchronization task."""
        _LOGGER.debug('Starting periodic Unique Keys posting')
        self._task.start()

    def stop(self, event=None):
        """Stop executing the unique keys synchronization task."""
        _LOGGER.debug('Stopping periodic Unique Keys posting')
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

    def __init__(self, clear_filter):
        """
        Class constructor.

        :param synchronize_unique_keys: sender
        :type synchronize_unique_keys: func
        :param period: How many seconds to wait between subsequent clearing of bloom filter
        :type period: int
        """
        _period = 60 * 60 * 24  # 24 hours
        self._task = AsyncTask(clear_filter, _period,
                               on_stop=clear_filter)

    def start(self):
        """Start executing the unique keys synchronization task."""

        _LOGGER.debug('Starting periodic Unique Keys posting')
        self._task.start()

    def stop(self, event=None):
        """Stop executing the unique keys synchronization task."""

        _LOGGER.debug('Stopping periodic Unique Keys posting')
        self._task.stop(event)

    def is_running(self):
        """
        Return whether the task is running or not.

        :return: True if the task is running. False otherwise.
        :rtype: bool
        """
        return self._task.running()
