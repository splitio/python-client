"""Segment changes processing worker."""
import logging
import threading
import abc

from splitio.optional.loaders import asyncio


_LOGGER = logging.getLogger(__name__)

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

    def __init__(self, synchronize_feature_flag, feature_flag_queue):
        """
        Class constructor.

        :param synchronize_feature_flag: handler to perform feature flag synchronization on incoming event
        :type synchronize_feature_flag: callable

        :param feature_flag_queue: queue with feature flag updates notifications
        :type feature_flag_queue: queue
        """
        self._feature_flag_queue = feature_flag_queue
        self._handler = synchronize_feature_flag
        self._running = False
        self._worker = None

    def is_running(self):
        """Return whether the working is running."""
        return self._running

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
                self._handler(event.change_number)
            except Exception:  # pylint: disable=broad-except
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

    def __init__(self, synchronize_split, split_queue):
        """
        Class constructor.

        :param synchronize_split: handler to perform split synchronization on incoming event
        :type synchronize_split: callable

        :param split_queue: queue with split updates notifications
        :type split_queue: queue
        """
        self._split_queue = split_queue
        self._handler = synchronize_split
        self._running = False

    def is_running(self):
        """Return whether the working is running."""
        return self._running

    async def _run(self):
        """Run worker handler."""
        while self.is_running():
            event = await self._split_queue.get()
            if not self.is_running():
                break
            if event == self._centinel:
                continue
            _LOGGER.debug('Processing split_update %d', event.change_number)
            try:
                _LOGGER.error(event.change_number)
                await self._handler(event.change_number)
            except Exception:  # pylint: disable=broad-except
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
        await self._split_queue.put(self._centinel)
