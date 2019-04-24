"""Workerpool test module."""
import time
import threading
from splitio.tasks.util import workerpool


class WorkerPoolTests(object):
    """Worker pool test cases."""

    def test_normal_operation(self, mocker):
        """Test normal opeation works properly."""
        worker_func = mocker.Mock()
        wp = workerpool.WorkerPool(10, worker_func)
        wp.start()
        for x in range(0, 100):
            wp.submit_work(str(x))

        stop_event = threading.Event()
        wp.stop(stop_event)
        stop_event.wait(5)
        assert stop_event.is_set()

        calls = worker_func.mock_calls
        for x in range(0, 100):
            assert mocker.call(str(x)) in calls

    def test_failure_in_message_doesnt_breal(self, mocker):
        """Test that if a message cannot be parsed it is ignored and others are processed."""
        class Worker:
            def __init__(self):
                self._worked = set()

            def do_work(self, w):
                if w == '55':
                    raise Exception('something')
                self._worked.add(w)

        worker = Worker()
        wp = workerpool.WorkerPool(50, worker.do_work)
        wp.start()
        for x in range(0, 100):
            wp.submit_work(str(x))

        stop_event = threading.Event()
        wp.stop(stop_event)
        stop_event.wait(5)
        assert stop_event.is_set()

        for x in range(0, 100):
            if x != 55:
                assert str(x) in worker._worked
            else:
                assert str(x) not in worker._worked
