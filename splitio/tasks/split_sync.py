"""Split Synchronization task."""

import logging
from splitio.models import splits
from splitio.api import APIException
from splitio.tasks import BaseSynchronizationTask
from splitio.tasks.util.asynctask import AsyncTask


class SplitSynchronizationTask(BaseSynchronizationTask):
    """Split Synchronization task class."""

    def __init__(self, split_api, split_storage, period, ready_flag=None):
        """
        Class constructor.

        :param split_api: Split API Client.
        :type split_api: splitio.api.splits.SplitsAPI
        :param split_storage: Split Storage.
        :type split_storage: splitio.storage.InMemorySplitStorage
        :param ready_flag: Flag to set when splits initial sync is complete.
        :type ready_flag: threading.Event
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._ready_flag = ready_flag
        self._api = split_api
        self._period = period
        self._split_storage = split_storage
        self._task = AsyncTask(self.update_splits, period, on_init=self.update_splits)

    def _update_splits(self, till=None):
        """
        Hit endpoint, update storage and return True if sync is complete.

        :param till: Passed till from Streaming.
        :type till: int

        :return: True if synchronization is complete.
        :rtype: bool
        """
        change_number = self._split_storage.get_change_number()
        if change_number is None:
            change_number = -1
        if till is not None and till < change_number: # the passed till is less than change_number, no need to perform updates
            return True

        try:
            split_changes = self._api.fetch_splits(change_number)
        except APIException:
            self._logger.error('Failed to fetch split from servers')
            return False

        for split in split_changes.get('splits', []):
            if split['status'] == splits.Status.ACTIVE.value:
                self._split_storage.put(splits.from_raw(split))
            else:
                self._split_storage.remove(split['name'])

        self._split_storage.set_change_number(split_changes['till'])
        return split_changes['till'] == split_changes['since'] or (till is not None and split_changes['till'] >= till)

    def start(self):
        """Start the task."""
        self._task.start()

    def stop(self, event=None):
        """Stop the task. Accept an optional event to set when the task has finished."""
        self._task.stop(event)

    def is_running(self):
        """
        Return whether the task is running.

        :return: True if the task is running. False otherwise.
        :rtype bool
        """
        return self._task.running()
    
    def update_splits(self, till=None):
        """
        Perform entire synchronization of splits.

        :param till: Passed till from Streaming.
        :type till: int
        """
        while not self._update_splits(till):
            pass
        if self._ready_flag is not None:
            self._ready_flag.set()
        return True
