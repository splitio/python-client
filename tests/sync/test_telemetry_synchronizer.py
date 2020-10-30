"""Split Worker tests."""

import threading
import time
import pytest

from splitio.api.client import HttpResponse
from splitio.api import APIException
from splitio.storage import TelemetryStorage
from splitio.sync.telemetry import TelemetrySynchronizer
from splitio.api.telemetry import TelemetryAPI


class TelemetrySynchronizerTests(object):
    """Telemetry synchronizer test cases."""

    def test_synchronize_impressions(self, mocker):
        """Test normal behaviour of sync task."""
        api = mocker.Mock(spec=TelemetryAPI)
        storage = mocker.Mock(spec=TelemetryStorage)
        storage.pop_latencies.return_value = {
            'some_latency1': [1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            'some_latency2': [0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        }
        storage.pop_gauges.return_value = {
            'gauge1': 123,
            'gauge2': 456
        }
        storage.pop_counters.return_value = {
            'counter1': 1,
            'counter2': 5
        }
        telemetry_synchronizer = TelemetrySynchronizer(api, storage)
        telemetry_synchronizer.synchronize_telemetry()

        assert mocker.call() in storage.pop_latencies.mock_calls
        assert mocker.call() in storage.pop_counters.mock_calls
        assert mocker.call() in storage.pop_gauges.mock_calls

        assert mocker.call({
            'some_latency1': [1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
            'some_latency2': [0, 1, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
        }) in api.flush_latencies.mock_calls

        assert mocker.call({
            'gauge1': 123,
            'gauge2': 456
        }) in api.flush_gauges.mock_calls

        assert mocker.call({
            'counter1': 1,
            'counter2': 5
        }) in api.flush_counters.mock_calls
