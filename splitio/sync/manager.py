"""Synchronization manager module."""
import logging
import time
import threading
from threading import Thread
from queue import Queue

from splitio.optional.loaders import asyncio
from splitio.push.manager import PushManager, PushManagerAsync, Status
from splitio.api import APIException
from splitio.util.backoff import Backoff
from splitio.util.time import get_current_epoch_time_ms
from splitio.models.telemetry import SSESyncMode, StreamingEventTypes
from splitio.sync.synchronizer import _SYNC_ALL_NO_RETRIES
from splitio.util import log_helper


class Manager(object):  # pylint:disable=too-many-instance-attributes
    """Manager Class."""

    _LOGGER = logging.getLogger(__name__)
    _CENTINEL_EVENT = object()

    def __init__(self, ready_flag, synchronizer, auth_api, streaming_enabled, sdk_metadata, telemetry_runtime_producer, sse_url=None, client_key=None):  # pylint:disable=too-many-arguments
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
        self._telemetry_runtime_producer = telemetry_runtime_producer
        if self._streaming_enabled:
            self._push_status_handler_active = True
            self._backoff = Backoff()
            self._queue = Queue()
            self._push = PushManager(auth_api, synchronizer, self._queue, sdk_metadata, telemetry_runtime_producer, sse_url, client_key)
            self._push_status_handler = Thread(target=self._streaming_feedback_handler,
                                               name='PushStatusHandler', daemon=True)

    def recreate(self):
        """Recreate poolers for forked processes."""
        self._synchronizer._split_synchronizers._segment_sync.recreate()

    def start(self, max_retry_attempts=_SYNC_ALL_NO_RETRIES):
        """Start the SDK synchronization tasks."""
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
            self._LOGGER.error('Exception raised starting Split Manager')
            self._LOGGER.debug('Exception information: ', exc_info=True)
            raise

    def stop(self, blocking):
        """
        Stop manager logic.

        :param blocking: flag to wait until tasks are stopped
        :type blocking: bool
        """
        self._LOGGER.info('Stopping manager tasks')
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
                self._LOGGER.info('streaming up and running. disabling periodic fetching.')
                self._telemetry_runtime_producer.record_streaming_event((StreamingEventTypes.SYNC_MODE_UPDATE, SSESyncMode.STREAMING.value,  get_current_epoch_time_ms()))
            elif status == Status.PUSH_SUBSYSTEM_DOWN:
                self._push.update_workers_status(False)
                self._synchronizer.sync_all()
                self._synchronizer.start_periodic_fetching()
                self._LOGGER.info('streaming temporarily down. starting periodic fetching')
                self._telemetry_runtime_producer.record_streaming_event((StreamingEventTypes.SYNC_MODE_UPDATE, SSESyncMode.POLLING.value,  get_current_epoch_time_ms()))
            elif status == Status.PUSH_RETRYABLE_ERROR:
                self._push.update_workers_status(False)
                self._push.stop(True)
                self._synchronizer.sync_all()
                self._synchronizer.start_periodic_fetching()
                how_long = self._backoff.get()
                self._LOGGER.info('error in streaming. restarting flow in %d seconds', how_long)
                time.sleep(how_long)
                self._push.start()
            elif status == Status.PUSH_NONRETRYABLE_ERROR:
                self._push.update_workers_status(False)
                self._push.stop(False)
                self._synchronizer.sync_all()
                self._synchronizer.start_periodic_fetching()
                self._LOGGER.info('non-recoverable error in streaming. switching to polling.')
                self._telemetry_runtime_producer.record_streaming_event((StreamingEventTypes.SYNC_MODE_UPDATE, SSESyncMode.POLLING.value,  get_current_epoch_time_ms()))
                return


class ManagerAsync(object):  # pylint:disable=too-many-instance-attributes
    """Manager Class."""

    _LOGGER = logging.getLogger('asyncio')
    _CENTINEL_EVENT = object()

    def __init__(self, synchronizer, auth_api, streaming_enabled, sdk_metadata, telemetry_runtime_producer, sse_url=None, client_key=None):  # pylint:disable=too-many-arguments
        """
        Construct Manager.

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
        self._synchronizer = synchronizer
        self._telemetry_runtime_producer = telemetry_runtime_producer
        if self._streaming_enabled:
            self._push_status_handler_active = True
            self._backoff = Backoff()
            self._queue = asyncio.Queue()
            self._push = PushManagerAsync(auth_api, synchronizer, self._queue, sdk_metadata, telemetry_runtime_producer, sse_url, client_key)
            self._push_status_handler_task = None

    async def start(self, max_retry_attempts=_SYNC_ALL_NO_RETRIES):
        """Start the SDK synchronization tasks."""
        try:
            await self._synchronizer.sync_all(max_retry_attempts)
            self._synchronizer.start_periodic_data_recording()
            if self._streaming_enabled:
                self._push_status_handler_task = asyncio.get_running_loop().create_task(self._streaming_feedback_handler())
                self._push.start()
            else:
                self._synchronizer.start_periodic_fetching()

        except (APIException, RuntimeError):
            self._LOGGER.error('Exception raised starting Split Manager')
            self._LOGGER.debug('Exception information: ', exc_info=True)
            raise

    async def stop(self, blocking):
        """
        Stop manager logic.

        :param blocking: flag to wait until tasks are stopped
        :type blocking: bool
        """
        self._LOGGER.info('Stopping manager tasks')
        if self._streaming_enabled:
            self._push_status_handler_active = False
            await self._queue.put(self._CENTINEL_EVENT)
            await self._push.stop()
        await self._synchronizer.shutdown(blocking)

    async def _streaming_feedback_handler(self):
        """
        Handle status updates from the streaming subsystem.

        :param status: current status of the streaming pipeline.
        :type status: splitio.push.status_stracker.Status
        """
        while self._push_status_handler_active:
            status = await self._queue.get()
            if status == self._CENTINEL_EVENT:
                continue
            if status == Status.PUSH_SUBSYSTEM_UP:
                await self._synchronizer.stop_periodic_fetching()
                await self._synchronizer.sync_all()
                await self._push.update_workers_status(True)
                self._backoff.reset()
                self._LOGGER.info('streaming up and running. disabling periodic fetching.')
                await self._telemetry_runtime_producer.record_streaming_event((StreamingEventTypes.SYNC_MODE_UPDATE, SSESyncMode.STREAMING.value,  get_current_epoch_time_ms()))
            elif status == Status.PUSH_SUBSYSTEM_DOWN:
                await self._push.update_workers_status(False)
                await self._synchronizer.sync_all()
                self._synchronizer.start_periodic_fetching()
                self._LOGGER.info('streaming temporarily down. starting periodic fetching')
                await self._telemetry_runtime_producer.record_streaming_event((StreamingEventTypes.SYNC_MODE_UPDATE, SSESyncMode.POLLING.value,  get_current_epoch_time_ms()))
            elif status == Status.PUSH_RETRYABLE_ERROR:
                await self._push.update_workers_status(False)
                await self._push.stop(True)
                await self._synchronizer.sync_all()
                self._synchronizer.start_periodic_fetching()
                how_long = self._backoff.get()
                self._LOGGER.info('error in streaming. restarting flow in %d seconds', how_long)
                await asyncio.sleep(how_long)
                self._push.start()
            elif status == Status.PUSH_NONRETRYABLE_ERROR:
                await self._push.update_workers_status(False)
                await self._push.stop(False)
                await self._synchronizer.sync_all()
                self._synchronizer.start_periodic_fetching()
                self._LOGGER.info('non-recoverable error in streaming. switching to polling.')
                await self._telemetry_runtime_producer.record_streaming_event((StreamingEventTypes.SYNC_MODE_UPDATE, SSESyncMode.POLLING.value,  get_current_epoch_time_ms()))
                return


class RedisManagerBase(object):  # pylint:disable=too-many-instance-attributes
    """Manager base Class."""

    def __init__(self, synchronizer):  # pylint:disable=too-many-arguments
        """
        Construct Manager.

        :param synchronizer: synchronizers for performing start/stop logic
        :type synchronizer: splitio.sync.synchronizer.Synchronizer
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
            self._LOGGER.error('Exception raised starting Split Manager')
            self._LOGGER.debug('Exception information: ', exc_info=True)
            raise


class RedisManager(RedisManagerBase):  # pylint:disable=too-many-instance-attributes
    """Manager Class."""

    _LOGGER = logging.getLogger(__name__)

    def __init__(self, synchronizer):  # pylint:disable=too-many-arguments
        """
        Construct Manager.

        :param synchronizer: synchronizers for performing start/stop logic
        :type synchronizer: splitio.sync.synchronizer.Synchronizer
        """
        super().__init__(synchronizer)

    def stop(self, blocking):
        """
        Stop manager logic.

        :param blocking: flag to wait until tasks are stopped
        :type blocking: bool
        """
        self._LOGGER.info('Stopping manager tasks')
        self._synchronizer.shutdown(blocking)


class RedisManagerAsync(RedisManagerBase):  # pylint:disable=too-many-instance-attributes
    """Manager async Class."""

    _LOGGER = logging.getLogger('asyncio')

    def __init__(self, synchronizer):  # pylint:disable=too-many-arguments
        """
        Construct Manager.

        :param synchronizer: synchronizers for performing start/stop logic
        :type synchronizer: splitio.sync.synchronizer.Synchronizer
        """
        super().__init__(synchronizer)

    async def stop(self, blocking):
        """
        Stop manager logic.

        :param blocking: flag to wait until tasks are stopped
        :type blocking: bool
        """
        self._LOGGER.info('Stopping manager tasks')
        await self._synchronizer.shutdown(blocking)
