"""Impressions synchronization task test module."""
import pytest
import threading
import time
from splitio.api.client import HttpResponse
from splitio.tasks.telemetry_sync import TelemetrySyncTask, TelemetrySyncTaskAsync
from splitio.api.telemetry import TelemetryAPI, TelemetryAPIAsync
from splitio.sync.telemetry import TelemetrySynchronizer, TelemetrySynchronizerAsync, InMemoryTelemetrySubmitter, InMemoryTelemetrySubmitterAsync
from splitio.storage.inmemmory import InMemoryTelemetryStorage, InMemoryTelemetryStorageAsync
from splitio.engine.telemetry import TelemetryStorageConsumer, TelemetryStorageConsumerAsync
from splitio.optional.loaders import asyncio


class TelemetrySyncTaskTests(object):
    """Unique Keys Syncrhonization task test cases."""

    def test_record_stats(self, mocker):
        """Test that the task works properly under normal circumstances."""
        api = mocker.Mock(spec=TelemetryAPI)
        api.record_stats.return_value = HttpResponse(200, '', {})
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        telemetry_submitter = InMemoryTelemetrySubmitter(telemetry_consumer, mocker.Mock(), mocker.Mock(), api)
        def _build_stats():
            return {}
        telemetry_submitter._build_stats = _build_stats

        telemetry_synchronizer = TelemetrySynchronizer(telemetry_submitter)
        task = TelemetrySyncTask(telemetry_synchronizer.synchronize_stats, 1)
        task.start()
        time.sleep(2)
        assert task.is_running()
        assert len(api.record_stats.mock_calls) >= 1
        stop_event = threading.Event()
        task.stop(stop_event)
        stop_event.wait(5)
        assert stop_event.is_set()


class TelemetrySyncTaskAsyncTests(object):
    """Unique Keys Syncrhonization task test cases."""

    @pytest.mark.asyncio
    async def test_record_stats(self, mocker):
        """Test that the task works properly under normal circumstances."""
        api = mocker.Mock(spec=TelemetryAPIAsync)
        self.called = False
        async def record_stats(stats):
            self.called = True
            return HttpResponse(200, '', {})
        api.record_stats = record_stats

        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_consumer = TelemetryStorageConsumerAsync(telemetry_storage)
        telemetry_submitter = InMemoryTelemetrySubmitterAsync(telemetry_consumer, mocker.Mock(), mocker.Mock(), api)
        async def _build_stats():
            return {}
        telemetry_submitter._build_stats = _build_stats

        telemetry_synchronizer = TelemetrySynchronizerAsync(telemetry_submitter)
        task = TelemetrySyncTaskAsync(telemetry_synchronizer.synchronize_stats, 1)
        task.start()
        await asyncio.sleep(2)
        assert task.is_running()
        assert self.called
        await task.stop()
        assert not task.is_running()
