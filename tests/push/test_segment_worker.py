"""Split Worker tests."""
import time
import queue
import pytest

from splitio.api import APIException
from splitio.push.segmentworker import SegmentWorker
from splitio.models.notification import SegmentChangeNotification

change_number_received = None
segment_name_received = None


def handler_sync(segment_name, change_number):
    global change_number_received
    global segment_name_received
    change_number_received = change_number
    segment_name_received = segment_name
    return


class SegmentWorkerTests(object):
    def test_on_error(self):
        q = queue.Queue()

        def handler_sync(change_number):
            raise APIException('some')

        segment_worker = SegmentWorker(handler_sync, q)
        segment_worker.start()
        assert segment_worker.is_running()

        q.put(SegmentChangeNotification('some', 'SEGMENT_UPDATE', 123456789, 'some'))

        with pytest.raises(Exception):
            segment_worker._handler()

        assert segment_worker.is_running()
        assert segment_worker._worker.is_alive()
        segment_worker.stop()
        time.sleep(1)
        assert not segment_worker.is_running()
        assert not segment_worker._worker.is_alive()

    def test_handler(self):
        q = queue.Queue()
        segment_worker = SegmentWorker(handler_sync, q)
        global change_number_received
        assert not segment_worker.is_running()
        segment_worker.start()
        assert segment_worker.is_running()

        q.put(SegmentChangeNotification('some', 'SEGMENT_UPDATE', 123456789, 'some'))

        time.sleep(0.1)
        assert change_number_received == 123456789
        assert segment_name_received == 'some'

        segment_worker.stop()
        assert not segment_worker.is_running()
