"""Split Worker tests."""
import time
import queue
import pytest

from splitio.api import APIException
from splitio.push.splitworker import SplitWorker, SplitWorkerAsync
from splitio.models.notification import SplitChangeNotification
from splitio.optional.loaders import asyncio

change_number_received = None


def handler_sync(change_number):
    global change_number_received
    change_number_received = change_number
    return

async def handler_async(change_number):
    global change_number_received
    change_number_received = change_number
    return


class SplitWorkerTests(object):

    def test_on_error(self):
        q = queue.Queue()

        def handler_sync(change_number):
            raise APIException('some')

        split_worker = SplitWorker(handler_sync, q)
        split_worker.start()
        assert split_worker.is_running()

        q.put(SplitChangeNotification('some', 'SPLIT_UPDATE', 123456789))
        with pytest.raises(Exception):
            split_worker._handler()

        assert split_worker.is_running()
        assert split_worker._worker.is_alive()
        split_worker.stop()
        time.sleep(1)
        assert not split_worker.is_running()
        assert not split_worker._worker.is_alive()

    def test_handler(self):
        q = queue.Queue()
        split_worker = SplitWorker(handler_sync, q)

        global change_number_received
        assert not split_worker.is_running()
        split_worker.start()
        assert split_worker.is_running()

        q.put(SplitChangeNotification('some', 'SPLIT_UPDATE', 123456789))

        time.sleep(0.1)
        assert change_number_received == 123456789

        split_worker.stop()
        assert not split_worker.is_running()

class SplitWorkerAsyncTests(object):

    async def test_on_error(self):
        q = asyncio.Queue()

        def handler_sync(change_number):
            raise APIException('some')

        split_worker = SplitWorkerAsync(handler_sync, q)
        split_worker.start()
        assert split_worker.is_running()

        await q.put(SplitChangeNotification('some', 'SPLIT_UPDATE', 123456789))
        with pytest.raises(Exception):
            split_worker._handler()

        assert split_worker.is_running()
        assert(self._worker_running())

        await split_worker.stop()
        await asyncio.sleep(.1)

        assert not split_worker.is_running()
        assert(not self._worker_running())

    def _worker_running(self):
        worker_running = False
        for task in asyncio.Task.all_tasks():
            if task._coro.cr_code.co_name == '_run' and not task.done():
                worker_running = True
                break
        return worker_running

    async def test_handler(self):
        q = asyncio.Queue()
        split_worker = SplitWorkerAsync(handler_async, q)

        assert not split_worker.is_running()
        split_worker.start()
        assert split_worker.is_running()
        assert(self._worker_running())

        global change_number_received
        await q.put(SplitChangeNotification('some', 'SPLIT_UPDATE', 123456789))
        await asyncio.sleep(1)

        assert change_number_received == 123456789

        await split_worker.stop()
        await asyncio.sleep(.1)

        assert not split_worker.is_running()
        assert(not self._worker_running())
