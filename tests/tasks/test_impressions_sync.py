"""Impressions synchronization task test module."""

import threading
import time
import pytest

from splitio.api.client import HttpResponse
from splitio.tasks import impressions_sync
from splitio.storage import ImpressionStorage
from splitio.models.impressions import Impression
from splitio.api.impressions import ImpressionsAPI
from splitio.sync.impression import ImpressionSynchronizer, ImpressionsCountSynchronizer, ImpressionSynchronizerAsync, ImpressionsCountSynchronizerAsync
from splitio.engine.impressions.manager import Counter
from splitio.optional.loaders import asyncio

class ImpressionsSyncTaskTests(object):
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
        api.flush_impressions.return_value = HttpResponse(200, '', {})
        impression_synchronizer = ImpressionSynchronizer(api, storage, 5)
        task = impressions_sync.ImpressionsSyncTask(
            impression_synchronizer.synchronize_impressions,
            1
        )
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


class ImpressionsSyncTaskAsyncTests(object):
    """Impressions Syncrhonization task test cases."""

    @pytest.mark.asyncio
    async def test_normal_operation(self, mocker):
        """Test that the task works properly under normal circumstances."""
        storage = mocker.Mock(spec=ImpressionStorage)
        impressions = [
            Impression('key1', 'split1', 'on', 'l1', 123456, 'b1', 321654),
            Impression('key2', 'split1', 'on', 'l1', 123456, 'b1', 321654),
            Impression('key3', 'split2', 'off', 'l1', 123456, 'b1', 321654),
            Impression('key4', 'split2', 'on', 'l1', 123456, 'b1', 321654),
            Impression('key5', 'split3', 'off', 'l1', 123456, 'b1', 321654)
        ]
        self.pop_called = 0
        async def pop_many(*args):
            self.pop_called += 1
            return impressions
        storage.pop_many = pop_many

        api = mocker.Mock(spec=ImpressionsAPI)
        self.flushed = None
        self.called = 0
        async def flush_impressions(imps):
            self.called += 1
            self.flushed = imps
            return  HttpResponse(200, '', {})
        api.flush_impressions = flush_impressions

        impression_synchronizer = ImpressionSynchronizerAsync(api, storage, 5)
        task = impressions_sync.ImpressionsSyncTaskAsync(
            impression_synchronizer.synchronize_impressions,
            1
        )
        task.start()
        await asyncio.sleep(2)
        assert task.is_running()
        assert self.pop_called == 1
        assert self.flushed == impressions

        calls_now = self.called
        await task.stop()
        assert self.called > calls_now


class ImpressionsCountSyncTaskTests(object):
    """Impressions Syncrhonization task test cases."""

    def test_normal_operation(self, mocker):
        """Test that the task works properly under normal circumstances."""
        counter = mocker.Mock(spec=Counter)

        counters = [
            Counter.CountPerFeature('f1', 123, 2),
            Counter.CountPerFeature('f2', 123, 123),
            Counter.CountPerFeature('f1', 456, 111),
            Counter.CountPerFeature('f2', 456, 222)
        ]

        counter.pop_all.return_value = counters
        api = mocker.Mock(spec=ImpressionsAPI)
        api.flush_counters.return_value = HttpResponse(200, '', {})
        impressions_sync.ImpressionsCountSyncTask._PERIOD = 1
        impression_synchronizer = ImpressionsCountSynchronizer(api, counter)
        task = impressions_sync.ImpressionsCountSyncTask(
            impression_synchronizer.synchronize_counters
        )
        task.start()
        time.sleep(2)
        assert task.is_running()
        assert counter.pop_all.mock_calls[0] == mocker.call()
        assert api.flush_counters.mock_calls[0] == mocker.call(counters)
        stop_event = threading.Event()
        calls_now = len(api.flush_counters.mock_calls)
        task.stop(stop_event)
        stop_event.wait(5)
        assert stop_event.is_set()
        assert len(api.flush_counters.mock_calls) > calls_now


class ImpressionsCountSyncTaskAsyncTests(object):
    """Impressions Syncrhonization task test cases."""

    @pytest.mark.asyncio
    async def test_normal_operation(self, mocker):
        """Test that the task works properly under normal circumstances."""
        counter = mocker.Mock(spec=Counter)
        counters = [
            Counter.CountPerFeature('f1', 123, 2),
            Counter.CountPerFeature('f2', 123, 123),
            Counter.CountPerFeature('f1', 456, 111),
            Counter.CountPerFeature('f2', 456, 222)
        ]
        self._pop_called = 0
        async def pop_all():
            self._pop_called += 1
            return counters
        counter.pop_all = pop_all

        api = mocker.Mock(spec=ImpressionsAPI)
        self.flushed = None
        self.called = 0
        async def flush_counters(imps):
            self.called += 1
            self.flushed = imps
            return  HttpResponse(200, '', {})
        api.flush_counters = flush_counters

        impressions_sync.ImpressionsCountSyncTaskAsync._PERIOD = 1
        impression_synchronizer = ImpressionsCountSynchronizerAsync(api, counter)
        task = impressions_sync.ImpressionsCountSyncTaskAsync(
            impression_synchronizer.synchronize_counters
        )
        task.start()
        await asyncio.sleep(2)
        assert task.is_running()

        assert self._pop_called == 1
        assert self.flushed == counters

        calls_now = self.called
        await task.stop()
        assert self.called > calls_now
