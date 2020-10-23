import logging

from splitio.push.synchronizer import Synchronizer
from splitio.api import APIException

_LOGGER = logging.getLogger(__name__)


class Manager(object):
    """Manager Class."""

    def __init__(self, ready_flag, synchronizer):
        """
        Construct Manager.

        :param ready_flag: Flag to set when splits initial sync is complete.
        :type ready_flag: threading.Event
        :param split_synchronizers: synchronizers for performing start/stop logic
        :type split_synchronizers: splitio.push.synchronizer.Synchronizer
        """
        self._ready_flag = ready_flag
        self._synchronizer = synchronizer

    def start(self):
        """Start manager logic."""
        try:
            self._synchronizer.sync_all()
            self._ready_flag.set()
            self._synchronizer.start_periodic_fetching()
            self._synchronizer.start_periodic_data_recording()
        except APIException:
            _LOGGER.error('Exception raised starting Split Manager')
            _LOGGER.debug('Exception information: ', exc_info=True)
            raise
        except RuntimeError:
            _LOGGER.error('Exception raised starting Split Manager')
            _LOGGER.debug('Exception information: ', exc_info=True)
            raise

    def stop(self):
        """Stop manager logic."""
        _LOGGER.info('Stopping manager tasks')
        self._synchronizer.stop_periodic_fetching(True)
        self._synchronizer.stop_periodic_data_recording()
