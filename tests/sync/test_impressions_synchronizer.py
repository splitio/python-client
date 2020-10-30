"""Split Worker tests."""

import threading
import time
import pytest

from splitio.api.client import HttpResponse
from splitio.api import APIException
from splitio.storage import ImpressionStorage
from splitio.models.impressions import Impression
from splitio.sync.impression import ImpressionSynchronizer


class ImpressionsSynchronizerTests(object):
    """Impressions synchronizer test cases."""

    def test_synchronize_impressions_error(self, mocker):
        storage = mocker.Mock(spec=ImpressionStorage)
        storage.pop_many.return_value = [
            Impression('key1', 'split1', 'on', 'l1', 123456, 'b1', 321654),
            Impression('key2', 'split1', 'on', 'l1', 123456, 'b1', 321654),
        ]

        api = mocker.Mock()

        def run(x):
            raise APIException("something broke")
        api.flush_impressions.side_effect = run

        impression_synchronizer = ImpressionSynchronizer(api, storage, 5)
        impression_synchronizer.synchronize_impressions()
        assert impression_synchronizer._failed.qsize() == 2

    def test_synchronize_impressions_empty(self, mocker):
        storage = mocker.Mock(spec=ImpressionStorage)
        storage.pop_many.return_value = []

        api = mocker.Mock()

        def run(x):
            run._called += 1

        run._called = 0
        api.flush_impressions.side_effect = run
        impression_synchronizer = ImpressionSynchronizer(api, storage, 5)
        impression_synchronizer.synchronize_impressions()
        assert run._called == 0

    def test_synchronize_impressions(self, mocker):
        storage = mocker.Mock(spec=ImpressionStorage)
        storage.pop_many.return_value = [
            Impression('key1', 'split1', 'on', 'l1', 123456, 'b1', 321654),
            Impression('key2', 'split1', 'on', 'l1', 123456, 'b1', 321654),
        ]

        api = mocker.Mock()

        def run(x):
            run._called += 1
            return HttpResponse(200, '')

        api.flush_impressions.side_effect = run
        run._called = 0

        impression_synchronizer = ImpressionSynchronizer(api, storage, 5)
        impression_synchronizer.synchronize_impressions()
        assert run._called == 1
        assert impression_synchronizer._failed.qsize() == 0
