import logging
from splitio.api import APIException
from splitio.models import splits


_LOGGER = logging.getLogger(__name__)


class SplitSynchronizer(object):
    def __init__(self, split_api, split_storage):
        """
        Class constructor.

        :param split_api: Split API Client.
        :type split_api: splitio.api.splits.SplitsAPI

        :param split_storage: Split Storage.
        :type split_storage: splitio.storage.InMemorySplitStorage

        """
        self._api = split_api
        self._split_storage = split_storage

    def synchronize_splits(self, till=None):
        """
        Hit endpoint, update storage and return True if sync is complete.

        :param till: Passed till from Streaming.
        :type till: int

        """
        while True:
            change_number = self._split_storage.get_change_number()
            if change_number is None:
                change_number = -1
            if till is not None and till < change_number:
                # the passed till is less than change_number, no need to perform updates
                return True

            try:
                split_changes = self._api.fetch_splits(change_number)
            except APIException as exc:
                _LOGGER.error('Failed to fetch split from servers')
                raise exc

            for split in split_changes.get('splits', []):
                if split['status'] == splits.Status.ACTIVE.value:
                    self._split_storage.put(splits.from_raw(split))
                else:
                    self._split_storage.remove(split['name'])

            self._split_storage.set_change_number(split_changes['till'])
            if split_changes['till'] == split_changes['since'] \
               or (till is not None and split_changes['till'] >= till):
                return
