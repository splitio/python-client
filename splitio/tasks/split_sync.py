"""Split Synchronization task."""

import logging
from splitio.models import splits
from splitio.api import APIException
from splitio.tasks import BaseSynchronizationTask
from splitio.tasks.util.asynctask import AsyncTask


class SplitSynchronizationTask(BaseSynchronizationTask):
    """Split Synchronization task class."""

    def __init__(self, split_api, split_storage, period, ready_flag):
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
        self._api = split_api
        self._ready_flag = ready_flag
        self._period = period
        self._split_storage = split_storage
        self._task = AsyncTask(self._update_splits, period, self._on_start)

    def _update_splits(self):
        """
        Hit endpoint, update storage and return True if sync is complete.

        :return: True if synchronization is complete.
        :rtype: bool
        """
        till = self._split_storage.get_change_number()
        if till is None:
            till = -1

        try:
            split_changes = self._api.fetch_splits(till)
        except APIException:
            self._logger.error('Failed to fetch split from servers')
            return False

        for split in split_changes.get('splits', []):
            if split['status'] == splits.Status.ACTIVE.value:
                self._split_storage.put(splits.from_raw(split))
            else:
                self._split_storage.remove(split['name'])

        self._split_storage.set_change_number(split_changes['till'])
        return split_changes['till'] == split_changes['since']

    def _on_start(self):
        """Wait until splits are in sync and set the flag to true."""
        while True:
            ready = self._update_splits()
            if ready:
                break

        self._ready_flag.set()
        return True

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
