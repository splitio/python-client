import logging
import threading


class SplitWorker(object):
    """Split Worker for processing updates."""
    _centinel = object()
    
    def __init__(self, synchronize_split, split_queue):
        """
        Class constructor.

        :param synchronize_split: handler to perform split synchronization on incoming event
        :type synchronize_split: function

        :param split_queue: queue with split updates notifications
        :type split_queue: queue
        """
        self._split_queue = split_queue
        self._handler = synchronize_split
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
            event = self._split_queue.get()
            if not self.is_running():
                break
            if event == self._centinel:
                continue
            self._logger.debug('Processing split_update %d', event.change_number)
            self._handler(event.change_number)
    
    def start(self):
        """
        Start worker
        """
        if self.is_running():
            self._logger.debug('Worker is already running')
            return
        self._logger.debug('Starting Split Worker')
        self.set_running(True)
        self._worker = threading.Thread(target=self._run)
        self._worker.setDaemon(True)
        self._worker.start()
    
    def stop(self):
        """
        Stop worker
        """
        self._logger.debug('Stopping Split Worker')
        self.set_running(False)
        self._split_queue.put(self._centinel)
