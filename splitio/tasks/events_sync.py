"""Events syncrhonization task."""
import logging

from splitio.tasks import BaseSynchronizationTask
from splitio.tasks.util.asynctask import AsyncTask


_LOGGER = logging.getLogger(__name__)


class EventsSyncTask(BaseSynchronizationTask):
    """Events synchronization task uses an asynctask.AsyncTask to send events."""

    def __init__(self, synchronize_events, period):
        """
        Class constructor.

        :param synchronize_events: Events Api object to send data to the backend
        :type synchronize_events: splitio.api.events.EventsAPI
        :param period: How many seconds to wait between subsequent event pushes to the BE.
        :type period: int

        """
        self._period = period
        self._task = AsyncTask(synchronize_events, self._period, on_stop=synchronize_events)

    def start(self):
        """Start executing the events synchronization task."""
        self._task.start()

    def stop(self, event=None):
        """Stop executing the events synchronization task."""
        self._task.stop(event)

    def flush(self):
        """Flush events in storage."""
        _LOGGER.debug('Forcing flush execution for events')
        self._task.force_execution()

    def is_running(self):
        """
        Return whether the task is running or not.

        :return: True if the task is running. False otherwise.
        :rtype: bool
        """
        return self._task.running()
