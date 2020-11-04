"""Telemetry synchronization task unit test module."""
# pylint: disable=no-self-use
import time
import threading
from splitio.storage import TelemetryStorage
from splitio.api.telemetry import TelemetryAPI
from splitio.tasks.telemetry_sync import TelemetrySynchronizationTask
from splitio.sync.telemetry import TelemetrySynchronizer


class TelemetrySyncTests(object):  # pylint: disable=too-few-public-methods
    """Impressions Syncrhonization task test cases."""

    def test_normal_operation(self, mocker):
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
        telemtry_synchronizer = TelemetrySynchronizer(api, storage)
        task = TelemetrySynchronizationTask(telemtry_synchronizer.synchronize_telemetry, 1)
        task.start()
        time.sleep(2)
        assert task.is_running()

        stop_event = threading.Event()
        task.stop(stop_event)
        stop_event.wait()

        assert stop_event.is_set()
        assert not task.is_running()
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
