"""Split Worker tests."""

import threading
import time
import pytest

from splitio.api.client import HttpResponse
from splitio.api import APIException
from splitio.engine.impressions import Manager as ImpressionsManager
from splitio.engine.impressions import Counter
from splitio.synchronizers.impression import ImpressionsCountSynchronizer
from splitio.api.impressions import ImpressionsAPI


class ImpressionsCountSynchronizerTests(object):
    """ImpressionsCount synchronizer test cases."""

    def test_synchronize_impressions_counts(self, mocker):
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
        impression_count_synchronizer = ImpressionsCountSynchronizer(api, manager)
        impression_count_synchronizer.synchronize_counters()

        assert manager.get_counts.mock_calls[0] == mocker.call()
        assert api.flush_counters.mock_calls[0] == mocker.call(counters)

        assert len(api.flush_counters.mock_calls) == 1