"""Workerpool test module."""
# pylint: disable=no-self-use,too-few-public-methods,missing-docstring
import time
import threading
import pytest

from splitio.tasks.util import workerpool
from splitio.optional.loaders import asyncio

class WorkerPoolTests(object):
    """Worker pool test cases."""

    def test_normal_operation(self, mocker):
        """Test normal opeation works properly."""
        worker_func = mocker.Mock()
        wpool = workerpool.WorkerPool(10, worker_func)
        wpool.start()
        for num in range(0, 100):
            wpool.submit_work(str(num))

        stop_event = threading.Event()
        wpool.stop(stop_event)
        stop_event.wait(5)
        assert stop_event.is_set()

        calls = worker_func.mock_calls
        for num in range(0, 100):
            assert mocker.call(str(num)) in calls

    def test_fail_in_msg_doesnt_break(self):
        """Test that if a message cannot be parsed it is ignored and others are processed."""
        class Worker(object):  #pylint: disable=
            def __init__(self):
                self.worked = set()

            def do_work(self, work):
                if work == '55':
                    raise Exception('something')
                self.worked.add(work)

        worker = Worker()
        wpool = workerpool.WorkerPool(50, worker.do_work)
        wpool.start()
        for num in range(0, 100):
            wpool.submit_work(str(num))

        stop_event = threading.Event()
        wpool.stop(stop_event)
        stop_event.wait(5)
        assert stop_event.is_set()

        for num in range(0, 100):
            if num != 55:
                assert str(num) in worker.worked
            else:
                assert str(num) not in worker.worked

    def test_msg_acked_after_processed(self):
        """Test that events are only set after all the work in the pipeline is done."""
        class Worker(object):
            def __init__(self):
                self.worked = set()

            def do_work(self, work):
                self.worked.add(work)
                time.sleep(0.02)  # will wait 2 seconds in total for 100 elements

        worker = Worker()
        wpool = workerpool.WorkerPool(50, worker.do_work)
        wpool.start()
        for num in range(0, 100):
            wpool.submit_work(str(num))

        wpool.wait_for_completion()
        assert len(worker.worked) == 100


class WorkerPoolAsyncTests(object):
    """Worker pool async test cases."""

    @pytest.mark.asyncio
    async def test_normal_operation(self, mocker):
        """Test normal opeation works properly."""
        self.calls = 0
        calls = []
        async def worker_func(num):
            self.calls += 1
            calls.append(num)

        wpool = workerpool.WorkerPoolAsync(10, worker_func)
        wpool.start()
        for num in range(0, 11):
            await wpool.submit_work(str(num))

        await asyncio.sleep(1)
        await wpool.stop()
        assert wpool._running == False
        for num in range(0, 11):
            assert str(num) in calls

    @pytest.mark.asyncio
    async def test_fail_in_msg_doesnt_break(self):
        """Test that if a message cannot be parsed it is ignored and others are processed."""
        class Worker(object):  #pylint: disable=
            def __init__(self):
                self.worked = set()

            async def do_work(self, work):
                if work == '55':
                    raise Exception('something')
                self.worked.add(work)

        worker = Worker()
        wpool = workerpool.WorkerPoolAsync(50, worker.do_work)
        wpool.start()
        for num in range(0, 100):
            await wpool.submit_work(str(num))
        await asyncio.sleep(1)
        await wpool.stop()

        for num in range(0, 100):
            if num != 55:
                assert str(num) in worker.worked
            else:
                assert str(num) not in worker.worked

    @pytest.mark.asyncio
    async def test_msg_acked_after_processed(self):
        """Test that events are only set after all the work in the pipeline is done."""
        class Worker(object):
            def __init__(self):
                self.worked = set()

            async def do_work(self, work):
                self.worked.add(work)
                await asyncio.sleep(0.02)  # will wait 2 seconds in total for 100 elements

        worker = Worker()
        wpool = workerpool.WorkerPoolAsync(50, worker.do_work)
        wpool.start()
        for num in range(0, 100):
            await wpool.submit_work(str(num))

        await asyncio.sleep(1)
        await wpool.wait_for_completion()
        assert len(worker.worked) == 100
