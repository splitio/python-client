"""Split Worker tests."""

import threading
import time
import pytest

from splitio.api.client import HttpResponse
from splitio.api import APIException
from splitio.engine.impressions.impressions import Manager as ImpressionsManager
from splitio.engine.impressions.manager import Counter
from splitio.engine.impressions.strategies import StrategyOptimizedMode
from splitio.sync.impression import ImpressionsCountSynchronizer
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
        api.flush_counters.return_value = HttpResponse(200, '')
        impression_count_synchronizer = ImpressionsCountSynchronizer(api, counter)
        impression_count_synchronizer.synchronize_counters()

        assert counter.pop_all.mock_calls[0] == mocker.call()
        assert api.flush_counters.mock_calls[0] == mocker.call(counters)

        assert len(api.flush_counters.mock_calls) == 1
