"""sdk internal events task."""
import logging
import threading
import abc

_LOGGER = logging.getLogger(__name__)

class EventsTaskBase(object, metaclass=abc.ABCMeta):
    """task template."""

    @abc.abstractmethod
    def is_running(self):
        """Return whether the task is running."""

    @abc.abstractmethod
    def start(self):
        """Start task."""

    @abc.abstractmethod
    def stop(self):
        """Stop task."""

class EventsTask(EventsTaskBase):
    """sdk internal events processing task."""

    _centinel = object()

    def __init__(self, notify_internal_events, internal_events_queue):
        """
        Class constructor.

        :param synchronize_segment: handler to perform segment synchronization on incoming event
        :type synchronize_segment: function

        :param segment_queue: queue with segment updates notifications
        :type segment_queue: queue
        """
        self._internal_events_queue = internal_events_queue
        self._handler = notify_internal_events
        self._running = False
        self._worker = None

    def is_running(self):
        """Return whether the working is running."""
        return self._running

    def _run(self):
        """Run worker handler."""
        while self.is_running():
            event = self._internal_events_queue.get()
            if not self.is_running():
                break
            
            if event == self._centinel:
                continue
            
            _LOGGER.debug('Processing sdk internal event: %s', event.internal_event)
            try:
                self._handler(event.internal_event, event.metadata)
            except Exception:
                _LOGGER.error('Exception raised in events manager')
                _LOGGER.debug('Exception information: ', exc_info=True)

    def start(self):
        """Start worker."""
        if self.is_running():
            _LOGGER.debug('SDK Event Worker is already running')
            return
        
        self._running = True
        _LOGGER.debug('Starting SDK Event Task worker')
        self._worker = threading.Thread(target=self._run, name='EventsTaskWorker', daemon=True)
        self._worker.start()

    def stop(self, stop_flag=None):
        """Stop worker."""
        _LOGGER.debug('Stopping SDK Event Task worker')
        if not self.is_running():
            _LOGGER.debug('SDK Event Worker is not running. Ignoring.')
            return
        
        self._running = False
        self._internal_events_queue.put(self._centinel)