"""Split Worker tests."""

import threading
import time
import pytest

from splitio.api.client import HttpResponse
from splitio.api import APIException
from splitio.engine.impressions.impressions import Manager as ImpressionsManager
from splitio.engine.impressions.manager import Counter
from splitio.engine.impressions.strategies import StrategyOptimizedMode
from splitio.sync.impression import ImpressionsCountSynchronizer, ImpressionsCountSynchronizerAsync
from splitio.api.impressions import ImpressionsAPI


class ImpressionsCountSynchronizerTests(object):
    """ImpressionsCount synchronizer test cases."""

    def test_synchronize_impressions_counts(self, mocker):
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
        impression_count_synchronizer = ImpressionsCountSynchronizer(api, counter)
        assert impression_count_synchronizer._LOGGER.name == 'splitio.sync.impression'
        impression_count_synchronizer.synchronize_counters()

        assert counter.pop_all.mock_calls[0] == mocker.call()
        assert api.flush_counters.mock_calls[0] == mocker.call(counters)

        assert len(api.flush_counters.mock_calls) == 1


class ImpressionsCountSynchronizerAsyncTests(object):
    """ImpressionsCount synchronizer test cases."""

    @pytest.mark.asyncio
    async def test_synchronize_impressions_counts(self, mocker):
        counter = mocker.Mock(spec=Counter)

        self.called = 0
        async def pop_all():
            self.called += 1
            return [
                Counter.CountPerFeature('f1', 123, 2),
                Counter.CountPerFeature('f2', 123, 123),
                Counter.CountPerFeature('f1', 456, 111),
                Counter.CountPerFeature('f2', 456, 222)
            ]
        counter.pop_all = pop_all

        self.counters = None
        async def flush_counters(counters):
            self.counters = counters
            return HttpResponse(200, '', {})
        api = mocker.Mock(spec=ImpressionsAPI)
        api.flush_counters = flush_counters

        impression_count_synchronizer = ImpressionsCountSynchronizerAsync(api, counter)
        assert impression_count_synchronizer._LOGGER.name == 'asyncio'
        await impression_count_synchronizer.synchronize_counters()

        assert self.counters == [
                Counter.CountPerFeature('f1', 123, 2),
                Counter.CountPerFeature('f2', 123, 123),
                Counter.CountPerFeature('f1', 456, 111),
                Counter.CountPerFeature('f2', 456, 222)
            ]
        assert self.called == 1
