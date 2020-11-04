"""Split changes processing worker."""
import logging
import threading


_LOGGER = logging.getLogger(__name__)


class SplitWorker(object):
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
        self._worker = None

    def is_running(self):
        """Return whether the working is running."""
        return self._running

    def _run(self):
        """Run worker handler."""
        while self.is_running():
            event = self._split_queue.get()
            if not self.is_running():
                break
            if event == self._centinel:
                continue
            _LOGGER.debug('Processing split_update %d', event.change_number)
            try:
                self._handler(event.change_number)
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
        self._worker = threading.Thread(target=self._run, name='PushSplitWorker')
        self._worker.setDaemon(True)
        self._worker.start()

    def stop(self):
        """Stop worker."""
        _LOGGER.debug('Stopping Split Worker')
        if not self.is_running():
            _LOGGER.debug('Worker is not running')
            return
        self._running = False
        self._split_queue.put(self._centinel)
