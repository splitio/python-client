"""Split Worker tests."""
import time
import queue

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
    q = queue.Queue()
    segment_worker = SegmentWorker(handler_sync, q)

    def test_handler(self):
        global change_number_received
        assert self.segment_worker.is_running() == False
        self.segment_worker.start()
        assert self.segment_worker.is_running() == True

        self.q.put(SegmentChangeNotification('some', 'SEGMENT_UPDATE', 123456789, 'some'))

        time.sleep(0.1)
        assert change_number_received == 123456789
        assert segment_name_received == 'some'

        self.segment_worker.stop()
        assert self.segment_worker.is_running() == False
