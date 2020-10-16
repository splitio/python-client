import logging
import threading


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
        self._logger = logging.getLogger(self.__class__.__name__)
    
    def set_running(self, value):
        """
        Enables/Disable mode

        :param value: flag for enabling/disabling
        :type value: bool
        """
        self._running = value

    def is_running(self):
        """
        Return running
        """
        return self._running
    
    def _run(self):
        """
        Run worker handler
        """
        while self.is_running():
            event = self._segment_queue.get()
            if not self.is_running():
                break
            if event == self._centinel:
                continue
            self._logger.debug('Processing segment_update: %s, change_number: %d', event.segment_name, event.change_number)
            self._handler(event.segment_name, event.change_number)
    
    def start(self):
        """
        Start worker
        """
        if self.is_running():
            self._logger.debug('Worker is already running')
            return
        self._logger.debug('Starting Segment Worker')
        self.set_running(True)
        self._worker = threading.Thread(target=self._run, daemon=True)
        self._worker.start()
    
    def stop(self):
        """
        Stop worker
        """
        self._logger.debug('Stopping Segment Worker')
        self.set_running(False)
        self._segment_queue.put(self._centinel)
