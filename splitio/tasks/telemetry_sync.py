"""Telemetry syncrhonization task."""
import logging

from splitio.tasks import BaseSynchronizationTask
from splitio.tasks.util.asynctask import AsyncTask, AsyncTaskAsync

_LOGGER = logging.getLogger(__name__)

class TelemetrySyncTaskBase(BaseSynchronizationTask):
    """Telemetry synchronization task uses an asynctask.AsyncTask to send MTKs."""

    def start(self):
        """Start executing the telemetry synchronization task."""
        self._task.start()

    def stop(self, event=None):
        """Stop executing the unique telemetry synchronization task."""
        pass

    def is_running(self):
        """
        Return whether the task is running or not.

        :return: True if the task is running. False otherwise.
        :rtype: bool
        """
        return self._task.running()

    def flush(self):
        """Flush unique keys."""
        _LOGGER.debug('Forcing flush execution for telemetry')
        self._task.force_execution()


class TelemetrySyncTask(TelemetrySyncTaskBase):
    """Unique Telemetry task uses an asynctask.AsyncTask to send MTKs."""

    def __init__(self, synchronize_telemetry, period):
        """
        Class constructor.

        :param synchronize_telemetry: sender
        :type synchronize_telemetry: func
        :param period: How many seconds to wait between subsequent unique keys pushes to the BE.
        :type period: int
        """

        self._task = AsyncTask(synchronize_telemetry, period,
                               on_stop=synchronize_telemetry)

    def stop(self, event=None):
        """Stop executing the unique telemetry synchronization task."""
        self._task.stop(event)


class TelemetrySyncTaskAsync(TelemetrySyncTaskBase):
    """Telemetry synchronization task uses an asynctask.AsyncTask to send MTKs."""

    def __init__(self, synchronize_telemetry, period):
        """
        Class constructor.

        :param synchronize_telemetry: sender
        :type synchronize_telemetry: func
        :param period: How many seconds to wait between subsequent unique keys pushes to the BE.
        :type period: int
        """

        self._task = AsyncTaskAsync(synchronize_telemetry, period,
                               on_stop=synchronize_telemetry)

    async def stop(self):
        """Stop executing the unique telemetry synchronization task."""
        await self._task.stop(True)
