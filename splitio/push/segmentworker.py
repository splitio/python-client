"""Segment changes processing worker."""
import logging
import threading


_LOGGER = logging.getLogger(__name__)


class SegmentWorker(object):
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
        self._worker = threading.Thread(target=self._run, name='PushSegmentWorker')
        self._worker.setDaemon(True)
        self._worker.start()

    def stop(self):
        """Stop worker."""
        _LOGGER.debug('Stopping Segment Worker')
        if not self.is_running():
            _LOGGER.debug('Worker is not running. Ignoring.')
            return
        self._running = False
        self._segment_queue.put(self._centinel)
