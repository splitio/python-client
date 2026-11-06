"""Synchronization manager module."""
import logging
from threading import Thread, Event
from queue import Queue

from splitio.optional.loaders import asyncio
from splitio.push.manager import PushManager, PushManagerAsync, Status
from splitio.api import APIException
from splitio.util.backoff import Backoff
from splitio.util.time import get_current_epoch_time_ms
from splitio.models.telemetry import SSESyncMode, StreamingEventTypes
from splitio.sync.synchronizer import _SYNC_ALL_NO_RETRIES

_LOGGER = logging.getLogger(__name__)


class Manager(object):  # pylint:disable=too-many-instance-attributes
    """Manager Class."""

    _CENTINEL_EVENT = object()

    def __init__(self, ready_flag, synchronizer, auth_api, streaming_enabled, sdk_metadata, telemetry_runtime_producer, sse_url=None, client_key=None, proxy_url=None):  # pylint:disable=too-many-arguments
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

        :param proxy_url: proxy url.
        :type proxy_url: str
        """
        self._streaming_enabled = streaming_enabled
        self._ready_flag = ready_flag
        self._synchronizer = synchronizer
        self._telemetry_runtime_producer = telemetry_runtime_producer
        if self._streaming_enabled:
            self._push_status_handler_active = True
            self._stop_event = Event()  # interrupts the backoff between streaming retries
            self._backoff = Backoff()
            self._queue = Queue()
            self._push = PushManager(auth_api, synchronizer, self._queue, sdk_metadata, telemetry_runtime_producer, sse_url, client_key, proxy_url)
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
            self._stop_event.set()
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

            try:
                if status == Status.PUSH_SUBSYSTEM_UP:
                    self._synchronizer.stop_periodic_fetching()
                    self._synchronizer.sync_all()
                    self._push.update_workers_status(True)
                    self._backoff.reset()
                    _LOGGER.info('streaming up and running. disabling periodic fetching.')
                    self._telemetry_runtime_producer.record_streaming_event((StreamingEventTypes.SYNC_MODE_UPDATE, SSESyncMode.STREAMING.value,  get_current_epoch_time_ms()))
                elif status == Status.PUSH_SUBSYSTEM_DOWN:
                    self._push.update_workers_status(False)
                    self._synchronizer.sync_all()
                    self._synchronizer.start_periodic_fetching()
                    _LOGGER.info('streaming temporarily down. starting periodic fetching')
                    self._telemetry_runtime_producer.record_streaming_event((StreamingEventTypes.SYNC_MODE_UPDATE, SSESyncMode.POLLING.value,  get_current_epoch_time_ms()))
                elif status == Status.PUSH_RETRYABLE_ERROR:
                    self._push.update_workers_status(False)
                    self._push.stop(True)
                    self._synchronizer.sync_all()
                    self._synchronizer.start_periodic_fetching()
                    how_long = self._backoff.get()
                    _LOGGER.info('error in streaming. restarting flow in %d seconds', how_long)
                    if self._stop_event.wait(how_long):
                        return  # manager stopped while waiting: don't restart streaming
                    self._push.start()
                elif status == Status.PUSH_NONRETRYABLE_ERROR:
                    self._push.update_workers_status(False)
                    self._push.stop(False)
                    self._synchronizer.sync_all()
                    self._synchronizer.start_periodic_fetching()
                    _LOGGER.info('non-recoverable error in streaming. switching to polling.')
                    self._telemetry_runtime_producer.record_streaming_event((StreamingEventTypes.SYNC_MODE_UPDATE, SSESyncMode.POLLING.value,  get_current_epoch_time_ms()))
                    return
            except Exception:  # pylint:disable=broad-except
                # Keep this thread alive: if it dies, no further streaming status change is
                # handled and the SDK can be left without streaming and without polling.
                _LOGGER.error('unexpected error handling streaming status %s. '
                              'falling back to periodic fetching.', status)
                _LOGGER.debug('Exception information: ', exc_info=True)
                self._fall_back_to_polling()
                if status == Status.PUSH_NONRETRYABLE_ERROR:
                    # streaming was being disabled for good: stay in polling mode.
                    return

                # A local error has no streaming error code, so it is handled as retryable:
                # restart the streaming flow with backoff.
                how_long = self._backoff.get()
                _LOGGER.info('retrying streaming in %d seconds', how_long)
                if self._stop_event.wait(how_long):
                    return  # manager stopped while waiting: don't restart streaming
                self._queue.put(Status.PUSH_RETRYABLE_ERROR)

    def _fall_back_to_polling(self):
        """Start periodic fetching after an unexpected error in the streaming status handler."""
        try:
            self._synchronizer.start_periodic_fetching()
        except Exception:  # pylint:disable=broad-except
            _LOGGER.error('could not start periodic fetching.')
            _LOGGER.debug('Exception information: ', exc_info=True)


class ManagerAsync(object):  # pylint:disable=too-many-instance-attributes
    """Manager Class."""

    _CENTINEL_EVENT = object()

    def __init__(self, synchronizer, auth_api, streaming_enabled, sdk_metadata, telemetry_runtime_producer, sse_url=None, client_key=None, proxy=None):  # pylint:disable=too-many-arguments
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
            self._push = PushManagerAsync(auth_api, synchronizer, self._queue, sdk_metadata, telemetry_runtime_producer, sse_url, client_key, proxy=proxy)
        self._stopped = False

    async def start(self, max_retry_attempts=_SYNC_ALL_NO_RETRIES):
        """Start the SDK synchronization tasks."""
        self._stopped = False
        try:
            await self._synchronizer.sync_all(max_retry_attempts)
            if not self._stopped:
                self._synchronizer.start_periodic_data_recording()
                if self._streaming_enabled:
                    asyncio.get_running_loop().create_task(self._streaming_feedback_handler())
                    self._push.start()
                else:
                    self._synchronizer.start_periodic_fetching()
        except (APIException, RuntimeError):
            _LOGGER.error('Exception raised starting Split Manager')
            _LOGGER.debug('Exception information: ', exc_info=True)
            raise

    async def stop(self, blocking):
        """
        Stop manager logic.

        :param blocking: flag to wait until tasks are stopped
        :type blocking: bool
        """
        _LOGGER.info('Stopping manager tasks')
        if self._streaming_enabled:
            self._push_status_handler_active = False
            await self._queue.put(self._CENTINEL_EVENT)
            await self._push.stop(blocking)
            await self._push.close_sse_http_client()
        await self._synchronizer.shutdown(blocking)
        self._stopped = True

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
            try:
                if status == Status.PUSH_SUBSYSTEM_UP:
                    await self._synchronizer.stop_periodic_fetching()
                    await self._synchronizer.sync_all()
                    await self._push.update_workers_status(True)
                    self._backoff.reset()
                    _LOGGER.info('streaming up and running. disabling periodic fetching.')
                    await self._telemetry_runtime_producer.record_streaming_event((StreamingEventTypes.SYNC_MODE_UPDATE, SSESyncMode.STREAMING.value,  get_current_epoch_time_ms()))
                elif status == Status.PUSH_SUBSYSTEM_DOWN:
                    await self._push.update_workers_status(False)
                    await self._synchronizer.sync_all()
                    self._synchronizer.start_periodic_fetching()
                    _LOGGER.info('streaming temporarily down. starting periodic fetching')
                    await self._telemetry_runtime_producer.record_streaming_event((StreamingEventTypes.SYNC_MODE_UPDATE, SSESyncMode.POLLING.value,  get_current_epoch_time_ms()))
                elif status == Status.PUSH_RETRYABLE_ERROR:
                    await self._push.update_workers_status(False)
                    await self._push.stop(True)
                    await self._synchronizer.sync_all()
                    self._synchronizer.start_periodic_fetching()
                    how_long = self._backoff.get()
                    _LOGGER.info('error in streaming. restarting flow in %d seconds', how_long)
                    await asyncio.sleep(how_long)
                    if not self._push_status_handler_active:
                        return  # manager stopped while waiting: don't restart streaming
                    self._push.start()
                elif status == Status.PUSH_NONRETRYABLE_ERROR:
                    await self._push.update_workers_status(False)
                    await self._push.stop(False)
                    await self._synchronizer.sync_all()
                    self._synchronizer.start_periodic_fetching()
                    _LOGGER.info('non-recoverable error in streaming. switching to polling.')
                    await self._telemetry_runtime_producer.record_streaming_event((StreamingEventTypes.SYNC_MODE_UPDATE, SSESyncMode.POLLING.value,  get_current_epoch_time_ms()))
                    return
            except asyncio.CancelledError:  # subclass of Exception in python 3.7
                raise
            except Exception:  # pylint:disable=broad-except
                # Keep this task alive: if it ends, no further streaming status change is
                # handled and the SDK can be left without streaming and without polling.
                _LOGGER.error('unexpected error handling streaming status %s. '
                              'falling back to periodic fetching.', status)
                _LOGGER.debug('Exception information: ', exc_info=True)
                self._fall_back_to_polling()
                if status == Status.PUSH_NONRETRYABLE_ERROR:
                    # streaming was being disabled for good: stay in polling mode.
                    return

                # A local error has no streaming error code, so it is handled as retryable:
                # restart the streaming flow with backoff.
                how_long = self._backoff.get()
                _LOGGER.info('retrying streaming in %d seconds', how_long)
                await asyncio.sleep(how_long)
                if not self._push_status_handler_active:
                    return  # manager stopped while waiting: don't restart streaming
                await self._queue.put(Status.PUSH_RETRYABLE_ERROR)

    def _fall_back_to_polling(self):
        """Start periodic fetching after an unexpected error in the streaming status handler."""
        try:
            self._synchronizer.start_periodic_fetching()
        except Exception:  # pylint:disable=broad-except
            _LOGGER.error('could not start periodic fetching.')
            _LOGGER.debug('Exception information: ', exc_info=True)


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
            _LOGGER.error('Exception raised starting Split Manager')
            _LOGGER.debug('Exception information: ', exc_info=True)
            raise


class RedisManager(RedisManagerBase):  # pylint:disable=too-many-instance-attributes
    """Manager Class."""

    def __init__(self, synchronizer):  # pylint:disable=too-many-arguments
        """
        Construct Manager.

        :param synchronizer: synchronizers for performing start/stop logic
        :type synchronizer: splitio.sync.synchronizer.Synchronizer
        """
        RedisManagerBase.__init__(self, synchronizer)

    def stop(self, blocking):
        """
        Stop manager logic.

        :param blocking: flag to wait until tasks are stopped
        :type blocking: bool
        """
        _LOGGER.info('Stopping manager tasks')
        self._synchronizer.shutdown(blocking)


class RedisManagerAsync(RedisManagerBase):  # pylint:disable=too-many-instance-attributes
    """Manager async Class."""

    def __init__(self, synchronizer):  # pylint:disable=too-many-arguments
        """
        Construct Manager.

        :param synchronizer: synchronizers for performing start/stop logic
        :type synchronizer: splitio.sync.synchronizer.Synchronizer
        """
        RedisManagerBase.__init__(self, synchronizer)

    async def stop(self, blocking):
        """
        Stop manager logic.

        :param blocking: flag to wait until tasks are stopped
        :type blocking: bool
        """
        _LOGGER.info('Stopping manager tasks')
        await self._synchronizer.shutdown(blocking)
