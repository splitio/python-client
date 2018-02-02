"""
Asynchronous tasks that can be controlled
"""
import threading
import logging

from six.moves import queue


__TASK_STOP__ = 0
__TASK_FORCE_RUN__ = 1

_LOGGER = logging.getLogger(__name__)


def _safe_run(func):
    """
    Executes a function wrapped in a try-except statement set
    If anything goes wrong returns false instead of propagating the exception.

    :param func: Function to be executed, receives no arguments and it's return
        value is ignored.
    """
    try:
        func()
        return True
    except Exception as exc:
        # Catch any exception that might happen to avoid the periodic task
        # from ending and allowing for a recovery, as well as preventing
        # an exception from propagating and breaking the main thread
        _LOGGER.exception(exc)
        return False


class AsyncTask(object):
    """
    This class creates is used to wrap around a function to treat it as a
    periodic task. This task can be stopped, it's execution can be forced, and
    it's status (whether it's running or not) can be obtained from the task
    object.
    It also allows for "on init" and "on stop" functions to be passed.
    """

    def __init__(self, main, period, on_init=None, on_stop=None):
        """
        Class constructor

        :param main: Main function to be executed periodically
        :param period: How many seconds to wait between executions
        :param onInit: Function to be executed ONCE before the main one
        :Param onStop: Function to be executed ONCE after the task has finished
        """
        self._on_init = on_init
        self._main = main
        self._on_stop = on_stop
        self._period = period
        self._messages = queue.Queue()
        self._running = False
        self._thread = None

    def _execution_wrapper(self):
        """
        This function will be run in a separate thread.
        It will execute the "on init" hook is available. If an exception is
        raised it will abort execution, otherwise it will enter an infinite
        loop in which the main function is executed every <period> seconds.
        After stop has been called the "on stop" hook will be invoked if
        available.

        All custom functions are run within a _safe_run() function which
        prevents exceptions from being propagated.
        """
        if self._on_init is not None:
            if not _safe_run(self._on_init):
                _LOGGER.error("Error running task initialization function, aborting execution")
                return

        while True:
            try:
                msg = self._messages.get(True, self._period)
                if msg == __TASK_STOP__:
                    _LOGGER.info("Stop signal received. finishing task execution")
                    break
                elif msg == __TASK_FORCE_RUN__:
                    _LOGGER.info("Force execution signal received. Running now")
                    if not _safe_run(self._main):
                        _LOGGER.error(
                            "An error occurred when executing the task. "
                            "Retrying after perio expires"
                        )
                    continue
            except queue.Empty:
                # If no message was received, the timeout has expired
                # and we're ready for a new execution
                if not _safe_run(self._main):
                    _LOGGER.error(
                        "An error occurred when executing the task. "
                        "Retrying after perio expires"
                    )

        if self._on_stop is not None:
            if not _safe_run(self._on_stop):
                _LOGGER.error("An error occurred when executing the task's OnStop hook. ")

        self._running = False

    def start(self):
        """
        Creates the thread that will execute the function periodically and
        starts it.
        """
        if self._running:
            _LOGGER.warning("Task is already running. Ignoring .start() call")
            return

        # Start execution
        self._thread = threading.Thread(target=self._execution_wrapper)
        self._thread.setDaemon(True)
        try:
            self._thread.start()
            self._running = True
        except RuntimeError as exc:
            _LOGGER.error("Couldn't create new thread for async task")
            _LOGGER.exception(exc)

    def stop(self):
        """
        Sends a signal to the thread in order to stop it.
        If the task is not running it does nothing.
        """
        if not self._running:
            return
        # Queue is of infinite size, should not raise an exception
        self._messages.put(__TASK_STOP__, False)

    def force_execution(self):
        """
        Forces an execution of the task withouth waiting for the period to end.
        If the task is not running it does nothing.
        """
        if not self._running:
            return
        # Queue is of infinite size, should not raise an exception
        self._messages.put(__TASK_FORCE_RUN__, False)

    def running(self):
        """
        Returns whether the task is running or not
        """
        return self._running
