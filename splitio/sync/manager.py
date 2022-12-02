"""Synchronization manager module."""
import logging
import time
import threading
from threading import Thread
from queue import Queue
from splitio.push.manager import PushManager, Status
from splitio.api import APIException
from splitio.util.backoff import Backoff
from splitio.sync.synchronizer import _SYNC_ALL_NO_RETRIES

_LOGGER = logging.getLogger(__name__)


class Manager(object):  # pylint:disable=too-many-instance-attributes
    """Manager Class."""

    _CENTINEL_EVENT = object()

    def __init__(self, ready_flag, synchronizer, auth_api, streaming_enabled, sdk_metadata, sse_url=None, client_key=None):  # pylint:disable=too-many-arguments
        """
        Construct Manager.

        :param ready_flag: Flag to set when splits initial sync is complete.
        :type ready_flag: threading.Event

        :param split_synchronizers: synchronizers for performing start/stop logic
        :type split_synchronizers: splitio.sync.synchronizer.Synchronizer

        :param auth_api: Authentication api client
        :type auth_api: splitio.api.auth.AuthAPI

        :param sdk_metadata: SDK version & machine name & IP.
        :type sdk_metadata: splitio.client.util.SdkMetadata

        :param streaming_enabled: whether to use streaming or not
        :type streaming_enabled: bool

        :param sse_url: streaming base url.
        :type sse_url: str

        :param client_key: client key.
        :type client_key: str
        """
        self._streaming_enabled = streaming_enabled
        self._ready_flag = ready_flag
        self._synchronizer = synchronizer
        if self._streaming_enabled:
            self._push_status_handler_active = True
            self._backoff = Backoff()
            self._queue = Queue()
            self._push = PushManager(auth_api, synchronizer, self._queue, sdk_metadata, sse_url, client_key)
            self._push_status_handler = Thread(target=self._streaming_feedback_handler,
                                               name='PushStatusHandler')
            self._push_status_handler.setDaemon(True)

    def recreate(self):
        """Recreate poolers for forked processes."""
        self._synchronizer._split_synchronizers._segment_sync.recreate()

    def start(self, max_retry_attempts=_SYNC_ALL_NO_RETRIES):
        """
        Start the SDK synchronization tasks.
        """
        try:
            self._synchronizer.sync_all(max_retry_attempts)
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

    def stop(self, blocking):
        """
        Stop manager logic.

        :param blocking: flag to wait until tasks are stopped
        :type blocking: bool
        """
        _LOGGER.info('Stopping manager tasks')
        if self._streaming_enabled:
            self._push_status_handler_active = False
            self._queue.put(self._CENTINEL_EVENT)
            self._push.stop()
        self._synchronizer.shutdown(blocking)

    def _streaming_feedback_handler(self):
        """
        Handle status updates from the streaming subsystem.

        :param status: current status of the streaming pipeline.
        :type status: splitio.push.status_stracker.Status
        """
        while self._push_status_handler_active:
            status = self._queue.get()
            if status == self._CENTINEL_EVENT:
                continue

            if status == Status.PUSH_SUBSYSTEM_UP:
                self._synchronizer.stop_periodic_fetching()
                self._synchronizer.sync_all()
                self._push.update_workers_status(True)
                self._backoff.reset()
                _LOGGER.info('streaming up and running. disabling periodic fetching.')
            elif status == Status.PUSH_SUBSYSTEM_DOWN:
                self._push.update_workers_status(False)
                self._synchronizer.sync_all()
                self._synchronizer.start_periodic_fetching()
                _LOGGER.info('streaming temporarily down. starting periodic fetching')
            elif status == Status.PUSH_RETRYABLE_ERROR:
                self._push.update_workers_status(False)
                self._push.stop(True)
                self._synchronizer.sync_all()
                self._synchronizer.start_periodic_fetching()
                how_long = self._backoff.get()
                _LOGGER.info('error in streaming. restarting flow in %d seconds', how_long)
                time.sleep(how_long)
                self._push.start()
            elif status == Status.PUSH_NONRETRYABLE_ERROR:
                self._push.update_workers_status(False)
                self._push.stop(False)
                self._synchronizer.sync_all()
                self._synchronizer.start_periodic_fetching()
                _LOGGER.info('non-recoverable error in streaming. switching to polling.')
                return

class RedisManager(object):  # pylint:disable=too-many-instance-attributes
    """Manager Class."""

    def __init__(self, synchronizer):  # pylint:disable=too-many-arguments
        """
        Construct Manager.

        :param unique_keys_task: unique keys task instance
        :type unique_keys_task: splitio.tasks.unique_keys_sync.UniqueKeysSyncTask

        :param clear_filter_task: clear filter task instance
        :type clear_filter_task: splitio.tasks.clear_filter_task.ClearFilterSynchronizer

        """
        self._ready_flag = True
        self._synchronizer = synchronizer

    def recreate(self):
        """Not implemented"""
        return

    def start(self):
        """Start the SDK synchronization tasks."""
        try:
            self._synchronizer.start_periodic_data_recording()

        except (APIException, RuntimeError):
            _LOGGER.error('Exception raised starting Split Manager')
            _LOGGER.debug('Exception information: ', exc_info=True)
            raise

    def stop(self, blocking):
        """
        Stop manager logic.

        :param blocking: flag to wait until tasks are stopped
        :type blocking: bool
        """
        _LOGGER.info('Stopping manager tasks')
        self._synchronizer.shutdown(blocking)