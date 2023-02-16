"""Pluggable Storage classes."""

import logging

from splitio.models import splits
from splitio.storage import SplitStorage

_LOGGER = logging.getLogger(__name__)

class PluggableSplitStorage(SplitStorage):
    """InMemory implementation of a split storage."""

    def __init__(self, pluggable_adapter, prefix):
        """Constructor."""
        self._pluggable_adapter = pluggable_adapter
        self._prefix = prefix + ".split."
        self._traffic_type_prefix = prefix + ".trafficType."
        self._split_till_prefix = prefix + ".splits.till"

    def get(self, split_name):
        """
        Retrieve a split.

        :param split_name: Name of the feature to fetch.
        :type split_name: str

        :rtype: splitio.models.splits.Split
        """
        split = self._pluggable_adapter.get(self._prefix + split_name)
        if not split:
            return None
        return splits.from_raw(split)

    def fetch_many(self, split_names):
        """
        Retrieve splits.

        :param split_names: Names of the features to fetch.
        :type split_name: list(str)

        :return: A dict with split objects parsed from queue.
        :rtype: dict(split_name, splitio.models.splits.Split)
        """
        return {split_name: self.get(split_name) for split_name in split_names}

    def put_many(self, splits, change_number):
        """
        Store a split.

        :param split: Split object.
        :type split: splitio.models.split.Split
        """
        for split in splits:
            self.put(split)
        self._pluggable_adapter.set(self._split_till_prefix, change_number)

    def remove(self, split_name):
        """
        Remove a split from storage.

        :param split_name: Name of the feature to remove.
        :type split_name: str

        :return: True if the split was found and removed. False otherwise.
        :rtype: bool
        """
        split = self.get(split_name)
        if not split:
            _LOGGER.warning("Tried to delete nonexistant split %s. Skipping", split_name)
            return False

        self._pluggable_adapter.delete(self._prefix + split_name)
        self._pluggable_adapter.decrement(self._traffic_type_prefix + split.traffic_type_name, 1)
        if self._pluggable_adapter.get(self._traffic_type_prefix + split.traffic_type_name) == 0:
            self._pluggable_adapter.delete(self._traffic_type_prefix + split.traffic_type_name)
        return True

    def get_change_number(self):
        """
        Retrieve latest split change number.

        :rtype: int
        """
        return self._pluggable_adapter.get(self._split_till_prefix)

    def set_change_number(self, new_change_number):
        """
        Set the latest change number.

        :param new_change_number: New change number.
        :type new_change_number: int
        """
        self._pluggable_adapter.set(self._split_till_prefix, new_change_number)

    def get_split_names(self):
        """
        Retrieve a list of all split names.

        :return: List of split names.
        :rtype: list(str)
        """
        return [split.name for split in self.get_all()]


    def get_all(self):
        """
        Return all the splits.

        :return: List of all the splits.
        :rtype: list
        """
        return [splits.from_raw(split) for split in self._pluggable_adapter.get_keys_by_prefix(self._prefix)]

    def traffic_type_exists(self, traffic_type_name):
        """
        Return whether the traffic type exists in at least one split in cache.

        :param traffic_type_name: Traffic type to validate.
        :type traffic_type_name: str

        :return: True if the traffic type is valid. False otherwise.
        :rtype: bool
        """
        return self._pluggable_adapter.get(self._traffic_type_prefix + traffic_type_name) != None

    def kill_locally(self, split_name, default_treatment, change_number):
        """
        Local kill for split

        :param split_name: name of the split to perform kill
        :type split_name: str
        :param default_treatment: name of the default treatment to return
        :type default_treatment: str
        :param change_number: change_number
        :type change_number: int
        """
        split = self.get(split_name)
        if not split:
            return
        if self.get_change_number() > change_number:
            return
        split.local_kill(default_treatment, change_number)
        self._pluggable_adapter.set(self._prefix + split_name, split.to_json())
        self.set_change_number(change_number)

    def increase_traffic_type_count(self, traffic_type_name):
        """
        Increase by one the count for a specific traffic type name.

        :param traffic_type_name: Traffic type to increase the count.
        :type traffic_type_name: str
        """
        self._pluggable_adapter.increment(self._traffic_type_prefix + traffic_type_name, 1)

    def decrease_traffic_type_count(self, traffic_type_name):
        """
        Decrease by one the count for a specific traffic type name.

        :param traffic_type_name: Traffic type to decrease the count.
        :type traffic_type_name: str
        """
        self._pluggable_adapter.decrement(self._traffic_type_prefix + traffic_type_name, 1)
        if self._pluggable_adapter.get(self._traffic_type_prefix + traffic_type_name) == 0:
            self._pluggable_adapter.delete(self._traffic_type_prefix + traffic_type_name)

    def get_all_splits(self):
        """
        Return all the splits.

        :return: List of all the splits.
        :rtype: list
        """
        return self.get_all()

    def is_valid_traffic_type(self, traffic_type_name):
        """
        Return whether the traffic type exists in at least one split in cache.

        :param traffic_type_name: Traffic type to validate.
        :type traffic_type_name: str

        :return: True if the traffic type is valid. False otherwise.
        :rtype: bool
        """
        return self.traffic_type_exists(traffic_type_name)

    def put(self, split):
        """
        Store a split.

        :param split: Split object.
        :type split: splitio.models.split.Split
        """
        self._pluggable_adapter.set(self._prefix + split.name, split.to_json())
        self._pluggable_adapter.increment(self._traffic_type_prefix + split.traffic_type_name, 1)
