"""Pluggable Storage classes."""

import logging

from splitio.models import splits, segments
from splitio.storage import SplitStorage, SegmentStorage

_LOGGER = logging.getLogger(__name__)

class PluggableSplitStorage(SplitStorage):
    """InMemory implementation of a split storage."""

    def __init__(self, pluggable_adapter, prefix=None):
        """Constructor."""
        self._pluggable_adapter = pluggable_adapter
        self._prefix = "SPLITIO.split."
        self._traffic_type_prefix = "SPLITIO.trafficType."
        self._split_till_prefix = "SPLITIO.splits.till"
        if prefix is not None:
            self._prefix = prefix + "." + self._prefix
            self._traffic_type_prefix = prefix + "." + self._traffic_type_prefix
            self._split_till_prefix = prefix + "." + self._split_till_prefix

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
        prefix_added = [self._prefix + split for split in split_names]
        return {split['name']: splits.from_raw(split) for split in self._pluggable_adapter.get_many(prefix_added)}

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
        self._decrease_traffic_type_count(split.traffic_type_name)
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
        return [splits.from_raw(self._pluggable_adapter.get(key)) for key in self._pluggable_adapter.get_keys_by_prefix(self._prefix)]

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

    def _increase_traffic_type_count(self, traffic_type_name):
        """
        Increase by one the count for a specific traffic type name.

        :param traffic_type_name: Traffic type to increase the count.
        :type traffic_type_name: str

        :return: existing count of traffic type
        :rtype: int
        """
        return self._pluggable_adapter.increment(self._traffic_type_prefix + traffic_type_name, 1)

    def _decrease_traffic_type_count(self, traffic_type_name):
        """
        Decrease by one the count for a specific traffic type name.

        :param traffic_type_name: Traffic type to decrease the count.
        :type traffic_type_name: str

        :return: existing count of traffic type
        :rtype: int
        """
        return_count = self._pluggable_adapter.decrement(self._traffic_type_prefix + traffic_type_name, 1)
        if return_count == 0:
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
        existing_split = self.get(split.name)
        self._pluggable_adapter.set(self._prefix + split.name, split.to_json())
        if existing_split is None:
            self._increase_traffic_type_count(split.traffic_type_name)
            return

        if existing_split is not None and existing_split.traffic_type_name != split.traffic_type_name:
            self._increase_traffic_type_count(split.traffic_type_name)
            self._decrease_traffic_type_count(existing_split.traffic_type_name)


class PluggableSegmentStorage(SegmentStorage):
    """Pluggable implementation of segment storage."""
    _SEGMENT_NAME_LENGTH = 14
    _TILL_LENGTH = 4

    def __init__(self, pluggable_adapter, prefix=None):
        """Constructor."""
        self._pluggable_adapter = pluggable_adapter
        self._prefix = "SPLITIO.segment.{segment_name}"
        self._segment_till_prefix = "SPLITIO.segment.{segment_name}.till"
        if prefix is not None:
            self._prefix = prefix + "." + self._prefix
            self._segment_till_prefix = prefix + "." + self._segment_till_prefix

    def update(self, segment_name, to_add, to_remove, change_number=None):
        """
        Update a segment. Create it if it doesn't exist.

        :param segment_name: Name of the segment to update.
        :type segment_name: str
        :param to_add: Set of members to add to the segment.
        :type to_add: set
        :param to_remove: List of members to remove from the segment.
        :type to_remove: Set
        """
        try:
            if to_add is not None:
                self._pluggable_adapter.add_items(self._prefix.format(segment_name=segment_name), to_add)
            if to_remove is not None:
                self._pluggable_adapter.remove_items(self._prefix.format(segment_name=segment_name), to_remove)
            if change_number is not None:
                self.set_change_number(segment_name, change_number)
        except Exception:
            _LOGGER.error('Error updating segment storage')
            _LOGGER.debug('Error: ', exc_info=True)

    def set_change_number(self, segment_name, change_number):
        """
        Store a segment change number.

        :param segment_name: segment name
        :type segment_name: str
        :param change_number: change number
        :type segment_name: int
        """
        try:
            self._pluggable_adapter.set(self._segment_till_prefix.format(segment_name=segment_name), change_number)
        except Exception:
            _LOGGER.error('Error updating segment change number')
            _LOGGER.debug('Error: ', exc_info=True)

    def get_change_number(self, segment_name):
        """
        Get a segment change number.

        :param segment_name: segment name
        :type segment_name: str

        :return: change number
        :rtype: int
        """
        try:
            return self._pluggable_adapter.get(self._segment_till_prefix.format(segment_name=segment_name))
        except Exception:
            _LOGGER.error('Error fetching segment change number')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    def get_segment_names(self):
        """
        Get list of segment names.

        :return: list of segment names
        :rtype: str[]
        """
        try:
            keys = []
            for key in self._pluggable_adapter.get_keys_by_prefix(self._prefix[:-self._SEGMENT_NAME_LENGTH]):
                if key[-self._TILL_LENGTH:] != 'till':
                    keys.append(key[len(self._prefix[:-self._SEGMENT_NAME_LENGTH]):])
            return keys
        except Exception:
            _LOGGER.error('Error getting segments')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    # TODO: To be added in the future because this data is not being sent by telemetry in consumer/synchronizer mode
#    def get_keys(self, segment_name):
#        """
#        Get keys of a segment.
#
#        :param segment_name: segment name
#        :type segment_name: str
#
#        :return: list of segment keys
#        :rtype: str[]
#        """
#        try:
#            return list(self._pluggable_adapter.get(self._prefix.format(segment_name=segment_name)))
#        except Exception:
#            _LOGGER.error('Error getting segments keys')
#            _LOGGER.debug('Error: ', exc_info=True)
#            return None

    def segment_contains(self, segment_name, key):
        """
        Check if segment contains a key

        :param segment_name: segment name
        :type segment_name: str
        :param key: key
        :type key: str

        :return: True if found, otherwise False
        :rtype: bool
        """
        try:
            return self._pluggable_adapter.item_contains(self._prefix.format(segment_name=segment_name), key)
        except Exception:
            _LOGGER.error('Error checking segment key')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    def get_segment_keys_count(self):
        """
        Get count of all keys in segments.

        :return: keys count
        :rtype: int
        """
        try:
            return sum([self._pluggable_adapter.get_items_count(key) for key in self._pluggable_adapter.get_keys_by_prefix(self._prefix)])
        except Exception:
            _LOGGER.error('Error getting segment keys')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    def get(self, segment_name):
        """
        Get a segment

        :param segment_name: segment name
        :type segment_name: str

        :return: segment object
        :rtype: splitio.models.segments.Segment
        """
        try:
            return segments.from_raw({'name': segment_name, 'added': list(self._pluggable_adapter.get(self._prefix.format(segment_name=segment_name))), 'removed': [], 'till': self._pluggable_adapter.get(self._segment_till_prefix.format(segment_name=segment_name))})
        except Exception:
            _LOGGER.error('Error getting segment')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    def put(self, segment):
        """
        Store a segment.

        :param segment: Segment to store.
        :type segment: splitio.models.segment.Segment
        """
        try:
            self._pluggable_adapter.add_items(self._prefix.format(segment_name=segment.name), list(segment.keys))
            if segment.change_number is not None:
                self._pluggable_adapter.set(self._segment_till_prefix.format(segment_name=segment.name), segment.change_number)
        except Exception:
            _LOGGER.error('Error updating segment storage')
            _LOGGER.debug('Error: ', exc_info=True)
