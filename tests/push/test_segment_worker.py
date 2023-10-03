"""Split Worker tests."""
import time
import queue
import pytest

from splitio.api import APIException
from splitio.push.workers import SegmentWorker, SegmentWorkerAsync
from splitio.models.notification import SegmentChangeNotification
from splitio.optional.loaders import asyncio

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

class SegmentWorkerAsyncTests(object):

    @pytest.mark.asyncio
    async def test_on_error(self):
        q = asyncio.Queue()

        def handler_sync(change_number):
            raise APIException('some')

        segment_worker = SegmentWorkerAsync(handler_sync, q)
        segment_worker.start()
        assert segment_worker.is_running()

        await q.put(SegmentChangeNotification('some', 'SEGMENT_UPDATE', 123456789, 'some'))

        with pytest.raises(Exception):
            segment_worker._handler()

        assert segment_worker.is_running()
        assert(self._worker_running())
        await segment_worker.stop()
        await asyncio.sleep(.1)
        assert not segment_worker.is_running()
        assert(not self._worker_running())

    def _worker_running(self):
        worker_running = False
        for task in asyncio.Task.all_tasks():
            if task._coro.cr_code.co_name == '_run' and not task.done():
                worker_running = True
                break
        return worker_running

    @pytest.mark.asyncio
    async def test_handler(self):
        q = asyncio.Queue()
        segment_worker = SegmentWorkerAsync(handler_sync, q)
        global change_number_received
        assert not segment_worker.is_running()
        segment_worker.start()
        assert segment_worker.is_running()

        await q.put(SegmentChangeNotification('some', 'SEGMENT_UPDATE', 123456789, 'some'))

        await asyncio.sleep(.1)
        assert change_number_received == 123456789
        assert segment_name_received == 'some'

        await segment_worker.stop()
        await asyncio.sleep(.1)
        assert(not self._worker_running())
