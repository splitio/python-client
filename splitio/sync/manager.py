"""Synchronization manager module."""
import logging
import time
from threading import Thread
from queue import Queue
from splitio.push.manager import PushManager, Status
from splitio.api import APIException
from splitio.util.backoff import Backoff


_LOGGER = logging.getLogger(__name__)


class Manager(object):
    """Manager Class."""

    def __init__(self, ready_flag, synchronizer, auth_api, streaming_enabled, sse_url=None):  # pylint:disable=too-many-arguments
        """
        Construct Manager.

        :param ready_flag: Flag to set when splits initial sync is complete.
        :type ready_flag: threading.Event

        :param split_synchronizers: synchronizers for performing start/stop logic
        :type split_synchronizers: splitio.sync.synchronizer.Synchronizer

        :param auth_api: Authentication api client
        :type auth_api: splitio.api.auth.AuthAPI

        :param streaming_enabled: whether to use streaming or not
        :type streaming_enabled: bool
        """
        self._streaming_enabled = streaming_enabled
        self._ready_flag = ready_flag
        self._synchronizer = synchronizer
        if self._streaming_enabled:
            self._backoff = Backoff()
            self._queue = Queue()
            self._push = PushManager(auth_api, synchronizer, self._queue, sse_url)
            self._push_status_handler = Thread(target=self._streaming_feedback_handler,
                                               name='PushStatusHandler')
            self._push_status_handler.setDaemon(True)

    def start(self):
        """Start the SDK synchronization tasks."""
        try:
            self._synchronizer.sync_all()
            self._ready_flag.set()
            self._synchronizer.start_periodic_data_recording()
            if self._streaming_enabled:
                self._push_status_handler.start()
                self._push.start()
            else:
                self._synchronizer.start_periodic_fetching()

        except (APIException, RuntimeError):
            _LOGGER.error('Exception raised starting Split Manager')
            _LOGGER.debug('Exception information: ', exc_info=True)
            raise

    def stop(self):
        """Stop manager logic."""
        _LOGGER.info('Stopping manager tasks')
        if self._streaming_enabled:
            self._push.stop()
        self._synchronizer.stop_periodic_fetching(True)
        self._synchronizer.stop_periodic_data_recording()

    def _streaming_feedback_handler(self):
        """
        Handle status updates from the streaming subsystem.

        :param status: current status of the streaming pipeline.
        :type status: splitio.push.status_stracker.Status
        """
        while True:
            status = self._queue.get()
            if status == Status.PUSH_SUBSYSTEM_UP:
                _LOGGER.info('streaming up and running. disabling periodic fetching.')
                self._synchronizer.stop_periodic_fetching()
                self._push.update_workers_status(True)
                self._synchronizer.sync_all()
                self._backoff.reset()
            elif status == Status.PUSH_SUBSYSTEM_DOWN:
                _LOGGER.info('streaming temporarily down. starting periodic fetching')
                self._push.update_workers_status(False)
                self._synchronizer.start_periodic_fetching()
            elif status == Status.PUSH_RETRYABLE_ERROR:
                _LOGGER.info('error in streaming. restarting flow')
                self._synchronizer.start_periodic_fetching()
                self._push.stop(True)
                time.sleep(self._backoff.get())
                self._push.start()
            elif status == Status.PUSH_NONRETRYABLE_ERROR:
                _LOGGER.info('non-recoverable error in streaming. switching to polling.')
                self._synchronizer.start_periodic_fetching()
                self._push.stop(False)
                return
