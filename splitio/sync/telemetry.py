import logging
from six.moves import queue

from splitio.api import APIException


_LOGGER = logging.getLogger(__name__)


class TelemetrySynchronizer(object):
    def __init__(self, api, storage):
        """
        Class constructor.

        :param api: Telemetry API Client.
        :type api: splitio.api.telemetry.TelemetryAPI
        :param storage: Telemetry Storage.
        :type storage: splitio.storage.InMemoryTelemetryStorage

        """
        self._api = api
        self._storage = storage

    def synchronize_telemetry(self):
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
            _LOGGER.error('Failed send telemetry/latencies to split BE.')
            _LOGGER.debug('Exception information: ', exc_info=True)

        try:
            counters = self._storage.pop_counters()
            if counters:
                self._api.flush_counters(counters)
        except APIException:
            _LOGGER.error('Failed send telemetry/counters to split BE.')
            _LOGGER.debug('Exception information: ', exc_info=True)

        try:
            gauges = self._storage.pop_gauges()
            if gauges:
                self._api.flush_gauges(gauges)
        except APIException:
            _LOGGER.error('Failed send telemetry/gauges to split BE.')
            _LOGGER.debug('Exception information: ', exc_info=True)
