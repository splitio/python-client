"""Impressions synchronization task test module."""

import threading
import time
from splitio.api.client import HttpResponse
from splitio.tasks import impressions_sync
from splitio.storage import ImpressionStorage
from splitio.models.impressions import Impression
from splitio.api.impressions import ImpressionsAPI
from splitio.engine.impressions import Manager as ImpressionsManager
from splitio.engine.impressions import Counter


class ImpressionsSyncTests(object):
    """Impressions Syncrhonization task test cases."""

    def test_normal_operation(self, mocker):
        """Test that the task works properly under normal circumstances."""
        storage = mocker.Mock(spec=ImpressionStorage)
        impressions = [
            Impression('key1', 'split1', 'on', 'l1', 123456, 'b1', 321654),
            Impression('key2', 'split1', 'on', 'l1', 123456, 'b1', 321654),
            Impression('key3', 'split2', 'off', 'l1', 123456, 'b1', 321654),
            Impression('key4', 'split2', 'on', 'l1', 123456, 'b1', 321654),
            Impression('key5', 'split3', 'off', 'l1', 123456, 'b1', 321654)
        ]
        storage.pop_many.return_value = impressions
        api = mocker.Mock(spec=ImpressionsAPI)
        api.flush_impressions.return_value = HttpResponse(200, '')
        task = impressions_sync.ImpressionsSyncTask(api, storage, 1, 5)
        task.start()
        time.sleep(2)
        assert task.is_running()
        assert storage.pop_many.mock_calls[0] == mocker.call(5)
        assert api.flush_impressions.mock_calls[0] == mocker.call(impressions)
        stop_event = threading.Event()
        calls_now = len(api.flush_impressions.mock_calls)
        task.stop(stop_event)
        stop_event.wait(5)
        assert stop_event.is_set()
        assert len(api.flush_impressions.mock_calls) > calls_now


class ImpressionsCountSyncTests(object):
    """Impressions Syncrhonization task test cases."""

    def test_normal_operation(self, mocker):
        """Test that the task works properly under normal circumstances."""
        manager = mocker.Mock(spec=ImpressionsManager)

        counters = [
            Counter.CountPerFeature('f1', 123, 2),
            Counter.CountPerFeature('f2', 123, 123),
            Counter.CountPerFeature('f1', 456, 111),
            Counter.CountPerFeature('f2', 456, 222)
        ]

        manager.get_counts.return_value = counters
        api = mocker.Mock(spec=ImpressionsAPI)
        api.flush_counters.return_value = HttpResponse(200, '')
        impressions_sync.ImpressionsCountSyncTask._PERIOD = 1
        task = impressions_sync.ImpressionsCountSyncTask(api, manager)
        task.start()
        time.sleep(2)
        assert task.is_running()
        assert manager.get_counts.mock_calls[0] == mocker.call()
        assert api.flush_counters.mock_calls[0] == mocker.call(counters)
        stop_event = threading.Event()
        calls_now = len(api.flush_counters.mock_calls)
        task.stop(stop_event)
        stop_event.wait(5)
        assert stop_event.is_set()
        assert len(api.flush_counters.mock_calls) > calls_now
