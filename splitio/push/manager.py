import logging

from splitio.push.synchronizer import Synchronizer
from splitio.api import APIException

_LOGGER = logging.getLogger(__name__)


class Manager(object):
    """Manager Class."""

    def __init__(self, synchronizer):
        """
        Construct Manager.

        :param split_synchronizers: synchronizers for performing start/stop logic
        :type split_synchronizers: splitio.push.synchronizer.Synchronizer
        """
        if not isinstance(synchronizer, Synchronizer):
            _LOGGER.error('Wrong parameter passed for instantiating Manager')
            return None
        self._synchronizer = synchronizer

    def start(self):
        """Start manager logic."""
        try:
            self._synchronizer.sync_all()
            self._synchronizer.start_periodic_fetching()
            self._synchronizer.start_periodic_data_recording()
        except APIException:
            _LOGGER.error('Exception raised starting Split Manager')
            _LOGGER.debug('Exception information: ', exc_info=True)
        except Exception:
            _LOGGER.error('Exception raised starting Split Manager')
            _LOGGER.debug('Exception information: ', exc_info=True)

    def stop(self):
        """Stop manager logic."""
        _LOGGER.info('Stopping manager tasks')
        self._synchronizer.stop_periodic_fetching(True)
        self._synchronizer.stop_periodic_data_recording()
