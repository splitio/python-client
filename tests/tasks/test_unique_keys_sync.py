"""Impressions synchronization task test module."""
import asyncio
import threading
import time
import pytest

from splitio.api.client import HttpResponse
from splitio.tasks.unique_keys_sync import UniqueKeysSyncTask, ClearFilterSyncTask,\
    ClearFilterSyncTaskAsync, UniqueKeysSyncTaskAsync
from splitio.api.telemetry import TelemetryAPI
from splitio.sync.unique_keys import UniqueKeysSynchronizer, ClearFilterSynchronizer,\
    UniqueKeysSynchronizerAsync, ClearFilterSynchronizerAsync
from splitio.engine.impressions.unique_keys_tracker import UniqueKeysTracker, UniqueKeysTrackerAsync


class UniqueKeysSyncTests(object):
    """Unique Keys Syncrhonization task test cases."""

    def test_normal_operation(self, mocker):
        """Test that the task works properly under normal circumstances."""
        api = mocker.Mock(spec=TelemetryAPI)
        api.record_unique_keys.return_value = HttpResponse(200, '', {})

        unique_keys_tracker = UniqueKeysTracker()
        unique_keys_tracker.track("key1", "split1")
        unique_keys_tracker.track("key2", "split1")

        unique_keys_sync = UniqueKeysSynchronizer(mocker.Mock(), unique_keys_tracker)
        task = UniqueKeysSyncTask(unique_keys_sync.send_all, 1)
        task.start()
        time.sleep(2)
        assert task.is_running()
        assert api.record_unique_keys.mock_calls == mocker.call()
        stop_event = threading.Event()
        task.stop(stop_event)
        stop_event.wait(5)
        assert stop_event.is_set()

class ClearFilterSyncTests(object):
    """Clear Filter Syncrhonization task test cases."""

    def test_normal_operation(self, mocker):
        """Test that the task works properly under normal circumstances."""

        unique_keys_tracker = UniqueKeysTracker()
        unique_keys_tracker.track("key1", "split1")
        unique_keys_tracker.track("key2", "split1")

        clear_filter_sync = ClearFilterSynchronizer(unique_keys_tracker)
        task = ClearFilterSyncTask(clear_filter_sync.clear_all, 1)
        task.start()
        time.sleep(2)
        assert task.is_running()
        assert not unique_keys_tracker._filter.contains("split1key1")
        assert not unique_keys_tracker._filter.contains("split1key2")
        stop_event = threading.Event()
        task.stop(stop_event)
        stop_event.wait(5)
        assert stop_event.is_set()

class UniqueKeysSyncAsyncTests(object):
    """Unique Keys Syncrhonization task test cases."""

    @pytest.mark.asyncio
    async def test_normal_operation(self, mocker):
        """Test that the task works properly under normal circumstances."""
        api = mocker.Mock(spec=TelemetryAPI)
        api.record_unique_keys.return_value = HttpResponse(200, '', {})

        unique_keys_tracker = UniqueKeysTrackerAsync()
        await unique_keys_tracker.track("key1", "split1")
        await unique_keys_tracker.track("key2", "split1")

        unique_keys_sync = UniqueKeysSynchronizerAsync(mocker.Mock(), unique_keys_tracker)
        task = UniqueKeysSyncTaskAsync(unique_keys_sync.send_all, 1)
        task.start()
        await asyncio.sleep(2)
        assert task.is_running()
        assert api.record_unique_keys.mock_calls == mocker.call()
        await task.stop()
        assert not task.is_running()

class ClearFilterSyncTests(object):
    """Clear Filter Syncrhonization task test cases."""

    @pytest.mark.asyncio
    async def test_normal_operation(self, mocker):
        """Test that the task works properly under normal circumstances."""

        unique_keys_tracker = UniqueKeysTrackerAsync()
        await unique_keys_tracker.track("key1", "split1")
        await unique_keys_tracker.track("key2", "split1")

        clear_filter_sync = ClearFilterSynchronizerAsync(unique_keys_tracker)
        task = ClearFilterSyncTaskAsync(clear_filter_sync.clear_all, 1)
        task.start()
        await asyncio.sleep(2)
        assert task.is_running()
        assert not unique_keys_tracker._filter.contains("split1key1")
        assert not unique_keys_tracker._filter.contains("split1key2")
        await task.stop()
        assert not task.is_running()
