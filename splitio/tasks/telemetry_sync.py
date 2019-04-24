"""Split Synchronization task."""

import logging
from splitio.api import APIException
from splitio.tasks import BaseSynchronizationTask
from splitio.tasks.util.asynctask import AsyncTask


class TelemetrySynchronizationTask(BaseSynchronizationTask):
    """Split Synchronization task class."""

    def __init__(self, api, storage, period):
        """
        Class constructor.

        :param api: Telemetry API Client.
        :type api: splitio.api.telemetry.TelemetryAPI
        :param storage: Telemetry Storage.
        :type storage: splitio.storage.InMemoryTelemetryStorage
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._api = api
        self._period = period
        self._storage = storage
        self._task = AsyncTask(self._flush_telemetry, period)

    def _flush_telemetry(self):
        """
        Send latencies, counters and gauges to split BE.

        :return: True if synchronization is complete.
        :rtype: bool
        """
        try:
            latencies = self._storage.pop_latencies()
            if latencies:
                self._api.flush_latencies(latencies)
        except APIException:
            self._logger.error('Failed send telemetry/latencies to split BE.')

        try:
            counters = self._storage.pop_counters()
            if counters:
                self._api.flush_counters(counters)
        except APIException:
            self._logger.error('Failed send telemetry/counters to split BE.')

        try:
            gauges = self._storage.pop_gauges()
            if gauges:
                self._api.flush_gauges(gauges)
        except APIException:
            self._logger.error('Failed send telemetry/gauges to split BE.')

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
