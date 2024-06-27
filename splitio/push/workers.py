"""Segment changes processing worker."""
import logging
import threading
import abc
import gzip
import zlib
import base64
import json
from enum import Enum

from splitio.models.splits import from_raw
from splitio.models.telemetry import UpdateFromSSE
from splitio.push import SplitStorageException
from splitio.push.parser import UpdateType
from splitio.optional.loaders import asyncio
from splitio.util.storage_helper import update_feature_flag_storage, update_feature_flag_storage_async

_LOGGER = logging.getLogger(__name__)

class CompressionMode(Enum):
    """Compression modes """

    NO_COMPRESSION = 0
    GZIP_COMPRESSION = 1
    ZLIB_COMPRESSION = 2

_compression_handlers = {
    CompressionMode.NO_COMPRESSION: lambda event: base64.b64decode(event.feature_flag_definition),
    CompressionMode.GZIP_COMPRESSION: lambda event: gzip.decompress(base64.b64decode(event.feature_flag_definition)).decode('utf-8'),
    CompressionMode.ZLIB_COMPRESSION: lambda event: zlib.decompress(base64.b64decode(event.feature_flag_definition)).decode('utf-8'),
}

class WorkerBase(object, metaclass=abc.ABCMeta):
    """Worker template."""

    @abc.abstractmethod
    def is_running(self):
        """Return whether the working is running."""

    @abc.abstractmethod
    def start(self):
        """Start worker."""

    @abc.abstractmethod
    def stop(self):
        """Stop worker."""

    def _get_feature_flag_definition(self, event):
        """return feature flag definition in event."""
        cm = CompressionMode(event.compression) # will throw if the number is not defined in compression mode
        return _compression_handlers[cm](event)

class SegmentWorker(WorkerBase):
    """Segment Worker for processing updates."""

    _centinel = object()

    def __init__(self, synchronize_segment, segment_queue):
        """
        Class constructor.

        :param synchronize_segment: handler to perform segment synchronization on incoming event
        :type synchronize_segment: function

        :param segment_queue: queue with segment updates notifications
        :type segment_queue: queue
        """
        self._segment_queue = segment_queue
        self._handler = synchronize_segment
        self._running = False
        self._worker = None

    def is_running(self):
        """Return whether the working is running."""
        return self._running

    def _run(self):
        """Run worker handler."""
        while self.is_running():
            event = self._segment_queue.get()
            if not self.is_running():
                break
            if event == self._centinel:
                continue
            _LOGGER.debug('Processing segment_update: %s, change_number: %d',
                          event.segment_name, event.change_number)
            try:
                self._handler(event.segment_name, event.change_number)
            except Exception:
                _LOGGER.error('Exception raised in segment synchronization')
                _LOGGER.debug('Exception information: ', exc_info=True)

    def start(self):
        """Start worker."""
        if self.is_running():
            _LOGGER.debug('Worker is already running')
            return
        self._running = True

        _LOGGER.debug('Starting Segment Worker')
        self._worker = threading.Thread(target=self._run, name='PushSegmentWorker', daemon=True)
        self._worker.start()

    def stop(self):
        """Stop worker."""
        _LOGGER.debug('Stopping Segment Worker')
        if not self.is_running():
            _LOGGER.debug('Worker is not running. Ignoring.')
            return
        self._running = False
        self._segment_queue.put(self._centinel)

class SegmentWorkerAsync(WorkerBase):
    """Segment Worker for processing updates."""

    _centinel = object()

    def __init__(self, synchronize_segment, segment_queue):
        """
        Class constructor.

        :param synchronize_segment: handler to perform segment synchronization on incoming event
        :type synchronize_segment: function

        :param segment_queue: queue with segment updates notifications
        :type segment_queue: asyncio.Queue
        """
        self._segment_queue = segment_queue
        self._handler = synchronize_segment
        self._running = False

    def is_running(self):
        """Return whether the working is running."""
        return self._running

    async def _run(self):
        """Run worker handler."""
        while self.is_running():
            event = await self._segment_queue.get()
            if not self.is_running():
                break
            if event == self._centinel:
                continue
            _LOGGER.debug('Processing segment_update: %s, change_number: %d',
                          event.segment_name, event.change_number)
            try:
                await self._handler(event.segment_name, event.change_number)
            except Exception:
                _LOGGER.error('Exception raised in segment synchronization')
                _LOGGER.debug('Exception information: ', exc_info=True)

    def start(self):
        """Start worker."""
        if self.is_running():
            _LOGGER.debug('Worker is already running')
            return
        self._running = True

        _LOGGER.debug('Starting Segment Worker')
        asyncio.get_running_loop().create_task(self._run())

    async def stop(self):
        """Stop worker."""
        _LOGGER.debug('Stopping Segment Worker')
        if not self.is_running():
            _LOGGER.debug('Worker is not running. Ignoring.')
            return
        self._running = False
        await self._segment_queue.put(self._centinel)

class SplitWorker(WorkerBase):
    """Feature Flag Worker for processing updates."""

    _centinel = object()

    def __init__(self, synchronize_feature_flag, synchronize_segment, feature_flag_queue, feature_flag_storage, segment_storage, telemetry_runtime_producer):
        """
        Class constructor.

        :param synchronize_feature_flag: handler to perform feature flag synchronization on incoming event
        :type synchronize_feature_flag: callable
        :param synchronize_segment: handler to perform segment synchronization on incoming event
        :type synchronize_segment: function
        :param feature_flag_queue: queue with feature flag updates notifications
        :type feature_flag_queue: queue
        :param feature_flag_storage: feature flag storage instance
        :type feature_flag_storage: splitio.storage.inmemory.InMemorySplitStorage
        :param segment_storage: segment storage instance
        :type segment_storage: splitio.storage.inmemory.InMemorySegmentStorage
        :param telemetry_runtime_producer: Telemetry runtime producer instance
        :type telemetry_runtime_producer: splitio.engine.telemetry.TelemetryRuntimeProducer
        """
        self._feature_flag_queue = feature_flag_queue
        self._handler = synchronize_feature_flag
        self._segment_handler = synchronize_segment
        self._running = False
        self._worker = None
        self._feature_flag_storage = feature_flag_storage
        self._segment_storage = segment_storage
        self._telemetry_runtime_producer = telemetry_runtime_producer

    def is_running(self):
        """Return whether the working is running."""
        return self._running

    def _apply_iff_if_needed(self, event):
        if not self._check_instant_ff_update(event):
            return False

        try:
            new_feature_flag = from_raw(json.loads(self._get_feature_flag_definition(event)))
            segment_list = update_feature_flag_storage(self._feature_flag_storage, [new_feature_flag], event.change_number)
            for segment_name in segment_list:
                if self._segment_storage.get(segment_name) is None:
                    _LOGGER.debug('Fetching new segment %s', segment_name)
                    self._segment_handler(segment_name, event.change_number)

            self._telemetry_runtime_producer.record_update_from_sse(UpdateFromSSE.SPLIT_UPDATE)
            return True

        except Exception as e:
            raise SplitStorageException(e)

    def _check_instant_ff_update(self, event):
        if event.update_type == UpdateType.SPLIT_UPDATE and event.compression is not None and event.previous_change_number == self._feature_flag_storage.get_change_number():
            return True

        return False

    def _run(self):
        """Run worker handler."""
        while self.is_running():
            event = self._feature_flag_queue.get()
            if not self.is_running():
                break
            if event == self._centinel:
                continue
            _LOGGER.debug('Processing feature flag update %d', event.change_number)
            try:
                if self._apply_iff_if_needed(event):
                    continue

                sync_result = self._handler(event.change_number)
                if not sync_result.success and sync_result.error_code is not None and sync_result.error_code == 414:
                    _LOGGER.error("URI too long exception caught, sync failed")

                if not sync_result.success:
                    _LOGGER.error("feature flags sync failed")

            except SplitStorageException as e:  # pylint: disable=broad-except
                _LOGGER.error('Exception Updating Feature Flag')
                _LOGGER.debug('Exception information: ', exc_info=True)
            except Exception as e:  # pylint: disable=broad-except
                _LOGGER.error('Exception raised in feature flag synchronization')
                _LOGGER.debug('Exception information: ', exc_info=True)

    def start(self):
        """Start worker."""
        if self.is_running():
            _LOGGER.debug('Worker is already running')
            return
        self._running = True

        _LOGGER.debug('Starting Feature Flag Worker')
        self._worker = threading.Thread(target=self._run, name='PushFeatureFlagWorker', daemon=True)
        self._worker.start()

    def stop(self):
        """Stop worker."""
        _LOGGER.debug('Stopping Feature Flag Worker')
        if not self.is_running():
            _LOGGER.debug('Worker is not running')
            return
        self._running = False
        self._feature_flag_queue.put(self._centinel)

class SplitWorkerAsync(WorkerBase):
    """Split Worker for processing updates."""

    _centinel = object()

    def __init__(self, synchronize_feature_flag, synchronize_segment, feature_flag_queue, feature_flag_storage, segment_storage, telemetry_runtime_producer):
        """
        Class constructor.

        :param synchronize_feature_flag: handler to perform feature_flag synchronization on incoming event
        :type synchronize_feature_flag: callable
        :param synchronize_segment: handler to perform segment synchronization on incoming event
        :type synchronize_segment: function
        :param feature_flag_queue: queue with feature_flag updates notifications
        :type feature_flag_queue: queue
        :param feature_flag_storage: feature flag storage instance
        :type feature_flag_storage: splitio.storage.inmemory.InMemorySplitStorage
        :param segment_storage: segment storage instance
        :type segment_storage: splitio.storage.inmemory.InMemorySegmentStorage
        :param telemetry_runtime_producer: Telemetry runtime producer instance
        :type telemetry_runtime_producer: splitio.engine.telemetry.TelemetryRuntimeProducer
        """
        self._feature_flag_queue = feature_flag_queue
        self._handler = synchronize_feature_flag
        self._segment_handler = synchronize_segment
        self._running = False
        self._feature_flag_storage = feature_flag_storage
        self._segment_storage = segment_storage
        self._telemetry_runtime_producer = telemetry_runtime_producer

    def is_running(self):
        """Return whether the working is running."""
        return self._running

    async def _apply_iff_if_needed(self, event):
        if not await self._check_instant_ff_update(event):
            return False
        try:
            new_feature_flag = from_raw(json.loads(self._get_feature_flag_definition(event)))
            segment_list = await update_feature_flag_storage_async(self._feature_flag_storage, [new_feature_flag], event.change_number)
            for segment_name in segment_list:
                if await self._segment_storage.get(segment_name) is None:
                    _LOGGER.debug('Fetching new segment %s', segment_name)
                    await self._segment_handler(segment_name, event.change_number)

            await self._telemetry_runtime_producer.record_update_from_sse(UpdateFromSSE.SPLIT_UPDATE)
            return True

        except Exception as e:
            raise SplitStorageException(e)


    async def _check_instant_ff_update(self, event):
        if event.update_type == UpdateType.SPLIT_UPDATE and event.compression is not None and event.previous_change_number == await self._feature_flag_storage.get_change_number():
            return True
        return False

    async def _run(self):
        """Run worker handler."""
        while self.is_running():
            event = await self._feature_flag_queue.get()
            if not self.is_running():
                break
            if event == self._centinel:
                continue
            _LOGGER.debug('Processing split_update %d', event.change_number)
            try:
                if await self._apply_iff_if_needed(event):
                    continue
                await self._handler(event.change_number)
            except SplitStorageException as e:  # pylint: disable=broad-except
                _LOGGER.error('Exception Updating Feature Flag')
                _LOGGER.debug('Exception information: ', exc_info=True)
            except Exception as e:  # pylint: disable=broad-except
                _LOGGER.error('Exception raised in split synchronization')
                _LOGGER.debug('Exception information: ', exc_info=True)

    def start(self):
        """Start worker."""
        if self.is_running():
            _LOGGER.debug('Worker is already running')
            return
        self._running = True

        _LOGGER.debug('Starting Split Worker')
        asyncio.get_running_loop().create_task(self._run())

    async def stop(self):
        """Stop worker."""
        _LOGGER.debug('Stopping Split Worker')
        if not self.is_running():
            _LOGGER.debug('Worker is not running')
            return
        self._running = False
        await self._feature_flag_queue.put(self._centinel)
