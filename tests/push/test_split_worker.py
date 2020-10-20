"""Split Worker tests."""
import time
import queue

from splitio.push.splitworker import SplitWorker
from splitio.models.notification import SplitChangeNotification

change_number_received = None

def handler_sync(change_number):
    global change_number_received
    change_number_received = change_number
    return


class SplitWorkerTests(object):
    q = queue.Queue()
    split_worker = SplitWorker(handler_sync, q)

    def test_handler(self):
        global change_number_received
        assert self.split_worker.is_running() == False
        self.split_worker.start()
        assert self.split_worker.is_running() == True

        self.q.put(SplitChangeNotification('some', 'SPLIT_UPDATE', 123456789))

        time.sleep(0.1)
        assert change_number_received == 123456789

        self.split_worker.stop()
        assert self.split_worker.is_running() == False
