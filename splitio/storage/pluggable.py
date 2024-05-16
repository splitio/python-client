"""Pluggable Storage classes."""

import logging
import json
import threading

from splitio.optional.loaders import asyncio
from splitio.models import splits, segments
from splitio.models.impressions import Impression
from splitio.models.telemetry import MethodExceptions, MethodLatencies, TelemetryConfig, MAX_TAGS,\
    MethodLatenciesAsync, MethodExceptionsAsync, TelemetryConfigAsync
from splitio.storage import FlagSetsFilter, SplitStorage, SegmentStorage, ImpressionStorage, EventStorage, TelemetryStorage
from splitio.util.storage_helper import get_valid_flag_sets, combine_valid_flag_sets

_LOGGER = logging.getLogger(__name__)

class PluggableSplitStorageBase(SplitStorage):
    """InMemory implementation of a feature flag storage."""

    _FEATURE_FLAG_NAME_LENGTH = 19
    _TILL_LENGTH = 4

    def __init__(self, pluggable_adapter, prefix=None, config_flag_sets=[]):
        """
        Class constructor.

        :param pluggable_adapter: Storage client or compliant interface.
        :type pluggable_adapter: TBD
        :param prefix: optional, prefix to storage keys
        :type prefix: str
        """
        self._pluggable_adapter = pluggable_adapter
        self._prefix = "SPLITIO.split.{feature_flag_name}"
        self._traffic_type_prefix = "SPLITIO.trafficType.{traffic_type_name}"
        self._feature_flag_till_prefix = "SPLITIO.splits.till"
        self._flag_set_prefix = 'SPLITIO.flagSet.{flag_set}'
        self.flag_set_filter = FlagSetsFilter(config_flag_sets)
        if prefix is not None:
            self._prefix = prefix + "." + self._prefix
            self._traffic_type_prefix = prefix + "." + self._traffic_type_prefix
            self._feature_flag_till_prefix = prefix + "." + self._feature_flag_till_prefix
            self._flag_set_prefix = prefix + "." + self._flag_set_prefix

    def get(self, feature_flag_name):
        """
        Retrieve a feature flag.

        :param feature_flag_name: Name of the feature to fetch.
        :type feature_flag_name: str

        :rtype: splitio.models.splits.Split
        """
        pass

    def fetch_many(self, feature_flag_names):
        """
        Retrieve feature flags.

        :param feature_flag_names: Names of the features to fetch.
        :type feature_flag_name: list(str)

        :return: A dict with feature flag objects parsed from queue.
        :rtype: dict(feature_flag_name, splitio.models.splits.Split)
        """
        pass

    # TODO: To be added when producer mode is supported
#    def put_many(self, splits, change_number):
#        """
#        Store multiple splits.
#
#        :param split: array of Split objects.
#        :type split: splitio.models.split.Split[]
#        """
#        try:
#            for split in splits:
#                self.put(split)
#            self._pluggable_adapter.set(self._split_till_prefix, change_number)
#        except Exception:
#            _LOGGER.error('Error storing splits in storage')
#            _LOGGER.debug('Error: ', exc_info=True)

    def update(self, to_add, to_delete, new_change_number):
        """
        Update feature flag storage.
        :param to_add: List of feature flags to add
        :type to_add: list[splitio.models.splits.Split]
        :param to_delete: List of feature flags to delete
        :type to_delete: list[splitio.models.splits.Split]
        :param new_change_number: New change number.
        :type new_change_number: int
        """
#        pass
#        try:
#            split = self.get(feature_flag_name)
#            if not split:
#                _LOGGER.warning("Tried to delete nonexistant split %s. Skipping", feature_flag_name)
#                return False
#            self._pluggable_adapter.delete(self._prefix.format(feature_flag_name=feature_flag_name))
#            self._decrease_traffic_type_count(split.traffic_type_name)
#            return True
#        except Exception:
#            _LOGGER.error('Error removing split from storage')
#            _LOGGER.debug('Error: ', exc_info=True)
#            return False

    def get_change_number(self):
        """
        Retrieve latest feature flag change number.

        :rtype: int
        """
        pass

    # TODO: To be added when producer mode is aupported
#    def _set_change_number(self, new_change_number):
        """
        Set the latest change number.

        :param new_change_number: New change number.
        :type new_change_number: int
        """
#        pass
#        try:
#            self._pluggable_adapter.set(self._split_till_prefix, new_change_number)
#        except Exception:
#            _LOGGER.error('Error setting change number in split storage')
#            _LOGGER.debug('Error: ', exc_info=True)
#            return None

    def get_split_names(self):
        """
        Retrieve a list of all feature flag names.

        :return: List of feature flag names.
        :rtype: list(str)
        """
        pass

    def traffic_type_exists(self, traffic_type_name):
        """
        Return whether the traffic type exists in at least one feature flag in cache.

        :param traffic_type_name: Traffic type to validate.
        :type traffic_type_name: str

        :return: True if the traffic type is valid. False otherwise.
        :rtype: bool
        """
        pass

    def kill_locally(self, feature_flag_name, default_treatment, change_number):
        """
        Local kill for feature flag

        :param feature_flag_name: name of the feature flag to perform kill
        :type feature_flag_name: str
        :param default_treatment: name of the default treatment to return
        :type default_treatment: str
        :param change_number: change_number
        :type change_number: int
        """
        pass
        # TODO: To be added when producer mode is aupported
#        try:
#            split = self.get(feature_flag_name)
#            if not split:
#                return
#            if self.get_change_number() > change_number:
#                return
#            split.local_kill(default_treatment, change_number)
#            self._pluggable_adapter.set(self._prefix.format(feature_flag_name=feature_flag_name), split.to_json())
#        except Exception:
#            _LOGGER.error('Error updating split in storage')
#            _LOGGER.debug('Error: ', exc_info=True)

    # TODO: To be added when producer mode is aupported
#    def _increase_traffic_type_count(self, traffic_type_name):
#        """
#        Increase by one the count for a specific traffic type name.
#
#        :param traffic_type_name: Traffic type to increase the count.
#        :type traffic_type_name: str
#
#        :return: existing count of traffic type
#        :rtype: int
#        """
#        try:
#            return self._pluggable_adapter.increment(self._traffic_type_prefix.format(traffic_type_name=traffic_type_name), 1)
#        except Exception:
#            _LOGGER.error('Error updating traffic type count in split storage')
#            _LOGGER.debug('Error: ', exc_info=True)
#            return None

    # TODO: To be added when producer mode is aupported
#   def _decrease_traffic_type_count(self, traffic_type_name):
#        """
#        Decrease by one the count for a specific traffic type name.
#
#        :param traffic_type_name: Traffic type to decrease the count.
#        :type traffic_type_name: str
#
#        :return: existing count of traffic type
#        :rtype: int
#        """
#        try:
#            return_count = self._pluggable_adapter.decrement(self._traffic_type_prefix.format(traffic_type_name=traffic_type_name), 1)
#            if return_count == 0:
#                self._pluggable_adapter.delete(self._traffic_type_prefix.format(traffic_type_name=traffic_type_name))
#        except Exception:
#            _LOGGER.error('Error updating traffic type count in split storage')
#            _LOGGER.debug('Error: ', exc_info=True)
#            return None

    def get_all_splits(self):
        """
        Return all the feature flags.

        :return: List of all the feature flags.
        :rtype: list
        """
        pass

    def is_valid_traffic_type(self, traffic_type_name):
        """
        Return whether the traffic type exists in at least one feature flag in cache.

        :param traffic_type_name: Traffic type to validate.
        :type traffic_type_name: str

        :return: True if the traffic type is valid. False otherwise.
        :rtype: bool
        """
        pass

class PluggableSplitStorage(PluggableSplitStorageBase):
    """InMemory implementation of a feature flag storage."""

    def __init__(self, pluggable_adapter, prefix=None, config_flag_sets=[]):
        """
        Class constructor.

        :param pluggable_adapter: Storage client or compliant interface.
        :type pluggable_adapter: TBD
        :param prefix: optional, prefix to storage keys
        :type prefix: str
        """
        PluggableSplitStorageBase.__init__(self, pluggable_adapter, prefix)

    def get(self, feature_flag_name):
        """
        Retrieve a feature flag.

        :param feature_flag_name: Name of the feature to fetch.
        :type feature_flag_name: str

        :rtype: splitio.models.splits.Split
        """
        try:
            feature_flag = self._pluggable_adapter.get(self._prefix.format(feature_flag_name=feature_flag_name))
            if not feature_flag:
                return None

            return splits.from_raw(feature_flag)

        except Exception:
            _LOGGER.error('Error getting feature flag from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    def fetch_many(self, feature_flag_names):
        """
        Retrieve feature flags.

        :param feature_flag_names: Names of the features to fetch.
        :type feature_flag_name: list(str)

        :return: A dict with feature flag objects parsed from queue.
        :rtype: dict(feature_flag_name, splitio.models.splits.Split)
        """
        try:
            prefix_added = [self._prefix.format(feature_flag_name=feature_flag_name) for feature_flag_name in feature_flag_names]
            return {feature_flag['name']: splits.from_raw(feature_flag) for feature_flag in self._pluggable_adapter.get_many(prefix_added)}

        except Exception:
            _LOGGER.error('Error getting feature flag from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    def get_feature_flags_by_sets(self, flag_sets):
        """
        Retrieve feature flags by flag set.
        :param flag_sets: List of flag sets to fetch.
        :type flag_sets: list(str)
        :return: Feature flag names that are tagged with the flag set
        :rtype: listt(str)
        """
        try:
            sets_to_fetch = get_valid_flag_sets(flag_sets, self.flag_set_filter)
            if sets_to_fetch == []:
                return []

            keys = [self._flag_set_prefix.format(flag_set=flag_set) for flag_set in sets_to_fetch]
            result_sets = []
            [result_sets.append(set(key)) for key in self._pluggable_adapter.get_many(keys)]
            return list(combine_valid_flag_sets(result_sets))

        except Exception:
            _LOGGER.error('Error fetching feature flag from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    def get_change_number(self):
        """
        Retrieve latest feature flag change number.

        :rtype: int
        """
        try:
            return self._pluggable_adapter.get(self._feature_flag_till_prefix)

        except Exception:
            _LOGGER.error('Error getting change number in feature flag storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    def get_split_names(self):
        """
        Retrieve a list of all feature flag names.

        :return: List of feature flag names.
        :rtype: list(str)
        """
        try:
            keys = []
            for key in self._pluggable_adapter.get_keys_by_prefix(self._prefix[:-self._FEATURE_FLAG_NAME_LENGTH]):
                if key[-self._TILL_LENGTH:] != 'till':
                    keys.append(key[len(self._prefix[:-self._FEATURE_FLAG_NAME_LENGTH]):])
            return keys

        except Exception:
            _LOGGER.error('Error getting feature flag names from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    def traffic_type_exists(self, traffic_type_name):
        """
        Return whether the traffic type exists in at least one feature flag in cache.

        :param traffic_type_name: Traffic type to validate.
        :type traffic_type_name: str

        :return: True if the traffic type is valid. False otherwise.
        :rtype: bool
        """
        try:
            return self._pluggable_adapter.get(self._traffic_type_prefix.format(traffic_type_name=traffic_type_name)) != None

        except Exception:
            _LOGGER.error('Error getting feature flag info from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    def get_all_splits(self):
        """
        Return all the feature flags.

        :return: List of all the feature flags.
        :rtype: list
        """
        try:
            keys = []
            for key in self._pluggable_adapter.get_keys_by_prefix(self._prefix[:-self._FEATURE_FLAG_NAME_LENGTH]):
                if key[-self._TILL_LENGTH:] != 'till':
                    keys.append(key)
            return [splits.from_raw(feature_flag) for feature_flag in self._pluggable_adapter.get_many(keys)]

        except Exception:
            _LOGGER.error('Error fetching feature flags from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    def is_valid_traffic_type(self, traffic_type_name):
        """
        Return whether the traffic type exists in at least one feature flag in cache.

        :param traffic_type_name: Traffic type to validate.
        :type traffic_type_name: str

        :return: True if the traffic type is valid. False otherwise.
        :rtype: bool
        """
        try:
            return self.traffic_type_exists(traffic_type_name)

        except Exception:
            _LOGGER.error('Error getting traffic type info from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

class PluggableSplitStorageAsync(PluggableSplitStorageBase):
    """InMemory async implementation of a feature flag storage."""

    def __init__(self, pluggable_adapter, prefix=None):
        """
        Class constructor.

        :param pluggable_adapter: Storage client or compliant interface.
        :type pluggable_adapter: TBD
        :param prefix: optional, prefix to storage keys
        :type prefix: str
        """
        PluggableSplitStorageBase.__init__(self, pluggable_adapter, prefix)

    async def get(self, feature_flag_name):
        """
        Retrieve a feature flag.

        :param feature_flag_name: Name of the feature to fetch.
        :type feature_flag_name: str

        :rtype: splitio.models.splits.Split
        """
        try:
            feature_flag = await self._pluggable_adapter.get(self._prefix.format(feature_flag_name=feature_flag_name))
            if not feature_flag:
                return None

            return splits.from_raw(feature_flag)

        except Exception:
            _LOGGER.error('Error getting feature flag from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    async def fetch_many(self, feature_flag_names):
        """
        Retrieve feature flags.

        :param feature_flag_names: Names of the features to fetch.
        :type feature_flag_name: list(str)

        :return: A dict with feature_flag objects parsed from queue.
        :rtype: dict(split_feature_flag, splitio.models.splits.Split)
        """
        try:
            prefix_added = [self._prefix.format(feature_flag_name=feature_flag_name) for feature_flag_name in feature_flag_names]
            return {feature_flag['name']: splits.from_raw(feature_flag) for feature_flag in await self._pluggable_adapter.get_many(prefix_added)}

        except Exception:
            _LOGGER.error('Error getting feature flag from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    async def get_feature_flags_by_sets(self, flag_sets):
        """
        Retrieve feature flags by flag set.
        :param flag_sets: List of flag sets to fetch.
        :type flag_sets: list(str)
        :return: Feature flag names that are tagged with the flag set
        :rtype: listt(str)
        """
        try:
            sets_to_fetch = get_valid_flag_sets(flag_sets, self.flag_set_filter)
            if sets_to_fetch == []:
                return []

            keys = [self._flag_set_prefix.format(flag_set=flag_set) for flag_set in sets_to_fetch]
            result_sets = []
            [result_sets.append(set(key)) for key in await self._pluggable_adapter.get_many(keys)]
            return list(combine_valid_flag_sets(result_sets))

        except Exception:
            _LOGGER.error('Error fetching feature flag from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    async def get_change_number(self):
        """
        Retrieve latest feature flag change number.

        :rtype: int
        """
        try:
            return await self._pluggable_adapter.get(self._feature_flag_till_prefix)

        except Exception:
            _LOGGER.error('Error getting change number in feature flag storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    async def get_split_names(self):
        """
        Retrieve a list of all feature flag names.

        :return: List of feature flag names.
        :rtype: list(str)
        """
        try:
            keys = []
            for key in await self._pluggable_adapter.get_keys_by_prefix(self._prefix[:-self._FEATURE_FLAG_NAME_LENGTH]):
                if key[-self._TILL_LENGTH:] != 'till':
                    keys.append(key[len(self._prefix[:-self._FEATURE_FLAG_NAME_LENGTH]):])
            return keys

        except Exception:
            _LOGGER.error('Error getting feature flag names from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    async def traffic_type_exists(self, traffic_type_name):
        """
        Return whether the traffic type exists in at least one feature flag in cache.

        :param traffic_type_name: Traffic type to validate.
        :type traffic_type_name: str

        :return: True if the traffic type is valid. False otherwise.
        :rtype: bool
        """
        try:
            return await self._pluggable_adapter.get(self._traffic_type_prefix.format(traffic_type_name=traffic_type_name)) != None

        except Exception:
            _LOGGER.error('Error getting traffic type info from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    async def get_all_splits(self):
        """
        Return all the feature flags.

        :return: List of all the feature flags.
        :rtype: list
        """
        try:
            keys = []
            for key in await self._pluggable_adapter.get_keys_by_prefix(self._prefix[:-self._FEATURE_FLAG_NAME_LENGTH]):
                if key[-self._TILL_LENGTH:] != 'till':
                    keys.append(key)
            return [splits.from_raw(feature_flag) for feature_flag in await self._pluggable_adapter.get_many(keys)]

        except Exception:
            _LOGGER.error('Error fetching feature flags from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    async def is_valid_traffic_type(self, traffic_type_name):
        """
        Return whether the traffic type exists in at least one feature flag in cache.

        :param traffic_type_name: Traffic type to validate.
        :type traffic_type_name: str

        :return: True if the traffic type is valid. False otherwise.
        :rtype: bool
        """
        try:
            return await self.traffic_type_exists(traffic_type_name)

        except Exception:
            _LOGGER.error('Error getting feature flag info from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

class PluggableSegmentStorageBase(SegmentStorage):
    """Pluggable async implementation of segment storage."""
    _SEGMENT_NAME_LENGTH = 14
    _TILL_LENGTH = 4

    def __init__(self, pluggable_adapter, prefix=None):
        """
        Class constructor.

        :param pluggable_adapter: Storage client or compliant interface.
        :type pluggable_adapter: TBD
        :param prefix: optional, prefix to storage keys
        :type prefix: str
        """
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
        pass
        # TODO: To be added when producer mode is aupported
#        try:
#            if to_add is not None:
#                self._pluggable_adapter.add_items(self._prefix.format(segment_name=segment_name), to_add)
#            if to_remove is not None:
#                self._pluggable_adapter.remove_items(self._prefix.format(segment_name=segment_name), to_remove)
#            if change_number is not None:
#                self._pluggable_adapter.set(self._segment_till_prefix.format(segment_name=segment_name), change_number)
#        except Exception:
#            _LOGGER.error('Error updating segment storage')
#            _LOGGER.debug('Error: ', exc_info=True)

    def set_change_number(self, segment_name, change_number):
        """
        Store a segment change number.

        :param segment_name: segment name
        :type segment_name: str
        :param change_number: change number
        :type segment_name: int
        """
        pass
        # TODO: To be added when producer mode is aupported
#        try:
#            self._pluggable_adapter.set(self._segment_till_prefix.format(segment_name=segment_name), change_number)
#        except Exception:
#            _LOGGER.error('Error updating segment change number')
#            _LOGGER.debug('Error: ', exc_info=True)

    def get_change_number(self, segment_name):
        """
        Get a segment change number.

        :param segment_name: segment name
        :type segment_name: str

        :return: change number
        :rtype: int
        """
        pass

    def get_segment_names(self):
        """
        Get list of segment names.

        :return: list of segment names
        :rtype: str[]
        """
        pass

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
        pass

    def get_segment_keys_count(self):
        """
        Get count of all keys in segments.

        :return: keys count
        :rtype: int
        """
        pass
        # TODO: To be added when producer mode is aupported
#        try:
#            return sum([self._pluggable_adapter.get_items_count(key) for key in self._pluggable_adapter.get_keys_by_prefix(self._prefix)])
#        except Exception:
#            _LOGGER.error('Error getting segment keys')
#            _LOGGER.debug('Error: ', exc_info=True)
#            return None

    def get(self, segment_name):
        """
        Get a segment

        :param segment_name: segment name
        :type segment_name: str

        :return: segment object
        :rtype: splitio.models.segments.Segment
        """
        pass

    def put(self, segment):
        """
        Store a segment.

        :param segment: Segment to store.
        :type segment: splitio.models.segment.Segment
        """
        pass
        # TODO: To be added when producer mode is aupported
#       try:
#            self._pluggable_adapter.add_items(self._prefix.format(segment_name=segment.name), list(segment.keys))
#            if segment.change_number is not None:
#                self._pluggable_adapter.set(self._segment_till_prefix.format(segment_name=segment.name), segment.change_number)
#        except Exception:
#            _LOGGER.error('Error updating segment storage')
#            _LOGGER.debug('Error: ', exc_info=True)


class PluggableSegmentStorage(PluggableSegmentStorageBase):
    """Pluggable implementation of segment storage."""

    def __init__(self, pluggable_adapter, prefix=None):
        """
        Class constructor.

        :param pluggable_adapter: Storage client or compliant interface.
        :type pluggable_adapter: TBD
        :param prefix: optional, prefix to storage keys
        :type prefix: str
        """
        PluggableSegmentStorageBase.__init__(self, pluggable_adapter, prefix)

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
            return False

    def get(self, segment_name):
        """
        Get a segment

        :param segment_name: segment name
        :type segment_name: str

        :return: segment object
        :rtype: splitio.models.segments.Segment
        """
        try:
            return segments.from_raw({'name': segment_name, 'added': self._pluggable_adapter.get_items(self._prefix.format(segment_name=segment_name)), 'removed': [], 'till': self._pluggable_adapter.get(self._segment_till_prefix.format(segment_name=segment_name))})

        except Exception:
            _LOGGER.error('Error getting segment')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

class PluggableSegmentStorageAsync(PluggableSegmentStorageBase):
    """Pluggable async implementation of segment storage."""

    def __init__(self, pluggable_adapter, prefix=None):
        """
        Class constructor.

        :param pluggable_adapter: Storage client or compliant interface.
        :type pluggable_adapter: TBD
        :param prefix: optional, prefix to storage keys
        :type prefix: str
        """
        PluggableSegmentStorageBase.__init__(self, pluggable_adapter, prefix)

    async def get_change_number(self, segment_name):
        """
        Get a segment change number.

        :param segment_name: segment name
        :type segment_name: str

        :return: change number
        :rtype: int
        """
        try:
            return await self._pluggable_adapter.get(self._segment_till_prefix.format(segment_name=segment_name))

        except Exception:
            _LOGGER.error('Error fetching segment change number')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    async def get_segment_names(self):
        """
        Get list of segment names.

        :return: list of segment names
        :rtype: str[]
        """
        try:
            keys = []
            for key in await self._pluggable_adapter.get_keys_by_prefix(self._prefix[:-self._SEGMENT_NAME_LENGTH]):
                if key[-self._TILL_LENGTH:] != 'till':
                    keys.append(key[len(self._prefix[:-self._SEGMENT_NAME_LENGTH]):])
            return keys

        except Exception:
            _LOGGER.error('Error getting segments')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    async def segment_contains(self, segment_name, key):
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
            return await self._pluggable_adapter.item_contains(self._prefix.format(segment_name=segment_name), key)

        except Exception:
            _LOGGER.error('Error checking segment key')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    async def get(self, segment_name):
        """
        Get a segment

        :param segment_name: segment name
        :type segment_name: str

        :return: segment object
        :rtype: splitio.models.segments.Segment
        """
        try:
            return segments.from_raw({'name': segment_name, 'added': await self._pluggable_adapter.get_items(self._prefix.format(segment_name=segment_name)), 'removed': [], 'till': await self._pluggable_adapter.get(self._segment_till_prefix.format(segment_name=segment_name))})

        except Exception:
            _LOGGER.error('Error getting segment')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

class PluggableImpressionsStorageBase(ImpressionStorage):
    """Pluggable Impressions storage class."""

    IMPRESSIONS_KEY_DEFAULT_TTL = 3600

    def __init__(self, pluggable_adapter, sdk_metadata, prefix=None):
        """
        Class constructor.

        :param pluggable_adapter: Storage client or compliant interface.
        :type pluggable_adapter: TBD
        :param sdk_metadata: SDK & Machine information.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        :param prefix: optional, prefix to storage keys
        :type prefix: str
        """
        self._pluggable_adapter = pluggable_adapter
        self._sdk_metadata = {
                                's': sdk_metadata.sdk_version,
                                'n': sdk_metadata.instance_name,
                                'i': sdk_metadata.instance_ip,
                            }
        self._impressions_queue_key = 'SPLITIO.impressions'
        if prefix is not None:
            self._impressions_queue_key = prefix + "." + self._impressions_queue_key

    def _wrap_impressions(self, impressions):
        """
        Wrap impressions to be stored in storage

        :param impressions: Impression to add to the queue.
        :type impressions: splitio.models.impressions.Impression

        :return: Processed impressions.
        :rtype: list[splitio.models.impressions.Impression]
        """
        bulk_impressions = []
        for impression in impressions:
            if isinstance(impression, Impression):
                to_store = {
                    'm': self._sdk_metadata,
                    'i': {
                        'k': impression.matching_key,
                        'b': impression.bucketing_key,
                        'f': impression.feature_name,
                        't': impression.treatment,
                        'r': impression.label,
                        'c': impression.change_number,
                        'm': impression.time,
                    }
                }
            bulk_impressions.append(json.dumps(to_store))
        return bulk_impressions

    def put(self, impressions):
        """
        Add an impression to the pluggable storage.

        :param impressions: Impression to add to the queue.
        :type impressions: splitio.models.impressions.Impression

        :return: Whether the impression has been added or not.
        :rtype: bool
        """
        pass

    def expire_key(self, total_keys, inserted):
        """
        Set expire

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        pass

    def pop_many(self, count):
        """
        Pop the oldest N events from storage.

        :param count: Number of events to pop.
        :type count: int
        """
        raise NotImplementedError('Only consumer mode is supported.')

    def clear(self):
        """
        Clear data.
        """
        raise NotImplementedError('Only consumer mode is supported.')


class PluggableImpressionsStorage(PluggableImpressionsStorageBase):
    """Pluggable Impressions storage class."""

    def __init__(self, pluggable_adapter, sdk_metadata, prefix=None):
        """
        Class constructor.

        :param pluggable_adapter: Storage client or compliant interface.
        :type pluggable_adapter: TBD
        :param sdk_metadata: SDK & Machine information.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        :param prefix: optional, prefix to storage keys
        :type prefix: str
        """
        PluggableImpressionsStorageBase.__init__(self, pluggable_adapter, sdk_metadata, prefix)

    def put(self, impressions):
        """
        Add an impression to the pluggable storage.

        :param impressions: Impression to add to the queue.
        :type impressions: splitio.models.impressions.Impression

        :return: Whether the impression has been added or not.
        :rtype: bool
        """
        bulk_impressions = self._wrap_impressions(impressions)
        try:
            total_keys = self._pluggable_adapter.push_items(self._impressions_queue_key, *bulk_impressions)
            self.expire_key(total_keys, len(bulk_impressions))
            return True

        except Exception:
            _LOGGER.error('Something went wrong when trying to add impression to storage')
            _LOGGER.error('Error: ', exc_info=True)
            return False

    def expire_key(self, total_keys, inserted):
        """
        Set expire

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        if total_keys == inserted:
            self._pluggable_adapter.expire(self._impressions_queue_key, self.IMPRESSIONS_KEY_DEFAULT_TTL)


class PluggableImpressionsStorageAsync(PluggableImpressionsStorageBase):
    """Pluggable Impressions storage class."""

    def __init__(self, pluggable_adapter, sdk_metadata, prefix=None):
        """
        Class constructor.

        :param pluggable_adapter: Storage client or compliant interface.
        :type pluggable_adapter: TBD
        :param sdk_metadata: SDK & Machine information.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        :param prefix: optional, prefix to storage keys
        :type prefix: str
        """
        PluggableImpressionsStorageBase.__init__(self, pluggable_adapter, sdk_metadata, prefix)

    async def put(self, impressions):
        """
        Add an impression to the pluggable storage.

        :param impressions: Impression to add to the queue.
        :type impressions: splitio.models.impressions.Impression

        :return: Whether the impression has been added or not.
        :rtype: bool
        """
        bulk_impressions = self._wrap_impressions(impressions)
        try:
            total_keys = await self._pluggable_adapter.push_items(self._impressions_queue_key, *bulk_impressions)
            await self.expire_key(total_keys, len(bulk_impressions))
            return True

        except Exception:
            _LOGGER.error('Something went wrong when trying to add impression to storage')
            _LOGGER.error('Error: ', exc_info=True)
            return False

    async def expire_key(self, total_keys, inserted):
        """
        Set expire

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        if total_keys == inserted:
            await self._pluggable_adapter.expire(self._impressions_queue_key, self.IMPRESSIONS_KEY_DEFAULT_TTL)


class PluggableEventsStorageBase(EventStorage):
    """Pluggable Event storage class."""

    _EVENTS_KEY_DEFAULT_TTL = 3600

    def __init__(self, pluggable_adapter, sdk_metadata, prefix=None):
        """
        Class constructor.

        :param pluggable_adapter: Storage client or compliant interface.
        :type pluggable_adapter: TBD
        :param sdk_metadata: SDK & Machine information.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        :param prefix: optional, prefix to storage keys
        :type prefix: str
        """
        self._pluggable_adapter = pluggable_adapter
        self._sdk_metadata = {
                                's': sdk_metadata.sdk_version,
                                'n': sdk_metadata.instance_name,
                                'i': sdk_metadata.instance_ip,
                            }
        self._events_queue_key = 'SPLITIO.events'
        if prefix is not None:
            self._events_queue_key = prefix + "." + self._events_queue_key

    def _wrap_events(self, events):
        return [
        json.dumps({
            'e': {
                'key': e.event.key,
                'trafficTypeName': e.event.traffic_type_name,
                'eventTypeId': e.event.event_type_id,
                'value': e.event.value,
                'timestamp': e.event.timestamp,
                'properties': e.event.properties,
            },
            'm': self._sdk_metadata
        })
        for e in events
    ]

    def put(self, events):
        """
        Add an event to the redis storage.

        :param event: Event to add to the queue.
        :type event: splitio.models.events.Event

        :return: Whether the event has been added or not.
        :rtype: bool
        """
        pass

    def expire_key(self, total_keys, inserted):
        """
        Set expire

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        pass

    def pop_many(self, count):
        """
        Pop the oldest N events from storage.

        :param count: Number of events to pop.
        :type count: int
        """
        raise NotImplementedError('Only redis-consumer mode is supported.')

    def clear(self):
        """
        Clear data.
        """
        raise NotImplementedError('Not supported for redis.')

class PluggableEventsStorage(PluggableEventsStorageBase):
    """Pluggable Event storage class."""

    def __init__(self, pluggable_adapter, sdk_metadata, prefix=None):
        """
        Class constructor.

        :param pluggable_adapter: Storage client or compliant interface.
        :type pluggable_adapter: TBD
        :param sdk_metadata: SDK & Machine information.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        :param prefix: optional, prefix to storage keys
        :type prefix: str
        """
        PluggableEventsStorageBase.__init__(self, pluggable_adapter, sdk_metadata, prefix)

    def put(self, events):
        """
        Add an event to the redis storage.

        :param event: Event to add to the queue.
        :type event: splitio.models.events.Event

        :return: Whether the event has been added or not.
        :rtype: bool
        """
        to_store = self._wrap_events(events)
        try:
            total_keys = self._pluggable_adapter.push_items(self._events_queue_key, *to_store)
            self.expire_key(total_keys, len(to_store))
            return True

        except Exception:
            _LOGGER.error('Something went wrong when trying to add event to redis')
            _LOGGER.debug('Error: ', exc_info=True)
            return False

    def expire_key(self, total_keys, inserted):
        """
        Set expire

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        if total_keys == inserted:
            self._pluggable_adapter.expire(self._events_queue_key, self._EVENTS_KEY_DEFAULT_TTL)


class PluggableEventsStorageAsync(PluggableEventsStorageBase):
    """Pluggable Event storage class."""

    def __init__(self, pluggable_adapter, sdk_metadata, prefix=None):
        """
        Class constructor.

        :param pluggable_adapter: Storage client or compliant interface.
        :type pluggable_adapter: TBD
        :param sdk_metadata: SDK & Machine information.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        :param prefix: optional, prefix to storage keys
        :type prefix: str
        """
        PluggableEventsStorageBase.__init__(self, pluggable_adapter, sdk_metadata, prefix)

    async def put(self, events):
        """
        Add an event to the redis storage.

        :param event: Event to add to the queue.
        :type event: splitio.models.events.Event

        :return: Whether the event has been added or not.
        :rtype: bool
        """
        to_store = self._wrap_events(events)
        try:
            total_keys = await self._pluggable_adapter.push_items(self._events_queue_key, *to_store)
            await self.expire_key(total_keys, len(to_store))
            return True

        except Exception:
            _LOGGER.error('Something went wrong when trying to add event to redis')
            _LOGGER.debug('Error: ', exc_info=True)
            return False

    async def expire_key(self, total_keys, inserted):
        """
        Set expire

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        if total_keys == inserted:
            await self._pluggable_adapter.expire(self._events_queue_key, self._EVENTS_KEY_DEFAULT_TTL)


class PluggableTelemetryStorageBase(TelemetryStorage):
    """Pluggable telemetry storage class."""

    _TELEMETRY_KEY_DEFAULT_TTL = 3600

    def _reset_config_tags(self):
        """Reset config tags."""
        pass

    def add_config_tag(self, tag):
        """
        Record tag string.

        :param tag: tag to be added
        :type tag: str
        """
        pass

    def record_config(self, config, extra_config, total_flag_sets, invalid_flag_sets):
        """
        initilize telemetry objects

        :param config: factory configuration parameters
        :type config: Dict
        :param extra_config: any extra configs
        :type extra_config: Dict
        """
        pass

    def pop_config_tags(self):
        """Get and reset configs."""
        pass

    def push_config_stats(self):
        """push config stats to storage."""
        pass

    def _format_config_stats(self):
        """format only selected config stats to json"""
        pass

    def record_active_and_redundant_factories(self, active_factory_count, redundant_factory_count):
        """
        Record active and redundant factories.

        :param active_factory_count: active factory count
        :type active_factory_count: int
        :param redundant_factory_count: redundant factory count
        :type redundant_factory_count: int
        """
        pass

    def record_latency(self, method, bucket):
        """
        record latency data

        :param method: method name
        :type method: string
        :param latency: latency
        :type latency: int64
        """
        pass

    def record_exception(self, method):
        """
        record an exception

        :param method: method name
        :type method: string
        """
        pass

    def record_not_ready_usage(self):
        """Not implemented"""
        pass

    def record_bur_time_out(self):
        """Not implemented"""
        pass

    def record_impression_stats(self, data_type, count):
        """Not implemented"""
        pass

    def expire_latency_keys(self, total_keys, inserted):
        """
        Set expire ttl for a latency key in storage

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        pass

    def expire_keys(self, queue_key, key_default_ttl, total_keys, inserted):
        """
        Set expire ttl for a key in storage if total keys equal inserted

        :param queue_keys: key to be set
        :type queue_keys: str
        :param ey_default_ttl: ttl value
        :type ey_default_ttl: int
        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        pass


class PluggableTelemetryStorage(PluggableTelemetryStorageBase):
    """Pluggable telemetry storage class."""

    def __init__(self, pluggable_adapter, sdk_metadata, prefix=None):
        """
        Class constructor.

        :param pluggable_adapter: Storage client or compliant interface.
        :type pluggable_adapter: TBD
        :param sdk_metadata: SDK & Machine information.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        :param prefix: optional, prefix to storage keys
        :type prefix: str
        """
        self._pluggable_adapter = pluggable_adapter
        self._sdk_metadata = sdk_metadata.sdk_version + '/' + sdk_metadata.instance_name + '/' + sdk_metadata.instance_ip
        self._telemetry_config_key = 'SPLITIO.telemetry.init'
        self._telemetry_latencies_key = 'SPLITIO.telemetry.latencies'
        self._telemetry_exceptions_key = 'SPLITIO.telemetry.exceptions'
        if prefix is not None:
            self._telemetry_config_key = prefix + "." + self._telemetry_config_key
            self._telemetry_latencies_key = prefix + "." + self._telemetry_latencies_key
            self._telemetry_exceptions_key = prefix + "." + self._telemetry_exceptions_key

        self._lock = threading.RLock()
        self._reset_config_tags()
        self._method_latencies = MethodLatencies()
        self._method_exceptions = MethodExceptions()
        self._tel_config = TelemetryConfig()

    def _reset_config_tags(self):
        """Reset config tags."""
        with self._lock:
            self._config_tags = []

    def add_config_tag(self, tag):
        """
        Record tag string.

        :param tag: tag to be added
        :type tag: str
        """
        with self._lock:
            if len(self._config_tags) < MAX_TAGS:
                self._config_tags.append(tag)

    def record_config(self, config, extra_config, total_flag_sets, invalid_flag_sets):
        """
        initilize telemetry objects

        :param config: factory configuration parameters
        :type config: Dict
        :param extra_config: any extra configs
        :type extra_config: Dict
        """
        self._tel_config.record_config(config, extra_config, total_flag_sets, invalid_flag_sets)

    def pop_config_tags(self):
        """Get and reset configs."""
        with self._lock:
            tags = self._config_tags
            self._reset_config_tags()
            return tags

    def push_config_stats(self):
        """push config stats to storage."""
        self._pluggable_adapter.set(self._telemetry_config_key + "::" + self._sdk_metadata, str(self._format_config_stats()))

    def _format_config_stats(self):
        """format only selected config stats to json"""
        config_stats = self._tel_config.get_stats()
        return json.dumps({
            'aF': config_stats['aF'],
            'rF': config_stats['rF'],
            'sT': config_stats['sT'],
            'oM': config_stats['oM'],
            't': self.pop_config_tags()
        })

    def record_active_and_redundant_factories(self, active_factory_count, redundant_factory_count):
        """
        Record active and redundant factories.

        :param active_factory_count: active factory count
        :type active_factory_count: int
        :param redundant_factory_count: redundant factory count
        :type redundant_factory_count: int
        """
        self._tel_config.record_active_and_redundant_factories(active_factory_count, redundant_factory_count)

    def record_latency(self, method, bucket):
        """
        record latency data

        :param method: method name
        :type method: string
        :param latency: latency
        :type latency: int64
        """
        latency_key = self._telemetry_latencies_key + '::' + self._sdk_metadata + '/' + method.value + '/' + str(bucket)
        result = self._pluggable_adapter.increment(latency_key, 1)
        self.expire_keys(latency_key, self._TELEMETRY_KEY_DEFAULT_TTL, 1, result)

    def record_exception(self, method):
        """
        record an exception

        :param method: method name
        :type method: string
        """
        except_key = self._telemetry_exceptions_key + "::" + self._sdk_metadata + '/' + method.value
        result = self._pluggable_adapter.increment(except_key, 1)
        self.expire_keys(except_key, self._TELEMETRY_KEY_DEFAULT_TTL, 1, result)

    def expire_latency_keys(self, total_keys, inserted):
        """
        Set expire ttl for a latency key in storage

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        self.expire_keys(self._telemetry_latencies_key, self._TELEMETRY_KEY_DEFAULT_TTL, total_keys, inserted)

    def expire_keys(self, queue_key, key_default_ttl, total_keys, inserted):
        """
        Set expire ttl for a key in storage if total keys equal inserted

        :param queue_keys: key to be set
        :type queue_keys: str
        :param ey_default_ttl: ttl value
        :type ey_default_ttl: int
        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        if total_keys == inserted:
            self._pluggable_adapter.expire(queue_key, key_default_ttl)

    def record_bur_time_out(self):
        """record BUR timeouts"""
        pass

    def record_ready_time(self, ready_time):
        """Record ready time."""
        pass


class PluggableTelemetryStorageAsync(PluggableTelemetryStorageBase):
    """Pluggable telemetry storage class."""

    @classmethod
    async def create(cls, pluggable_adapter, sdk_metadata, prefix=None):
        """
        Class constructor.

        :param pluggable_adapter: Storage client or compliant interface.
        :type pluggable_adapter: TBD
        :param sdk_metadata: SDK & Machine information.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        :param prefix: optional, prefix to storage keys
        :type prefix: str
        """
        self = cls()
        self._pluggable_adapter = pluggable_adapter
        self._sdk_metadata = sdk_metadata.sdk_version + '/' + sdk_metadata.instance_name + '/' + sdk_metadata.instance_ip
        self._telemetry_config_key = 'SPLITIO.telemetry.init'
        self._telemetry_latencies_key = 'SPLITIO.telemetry.latencies'
        self._telemetry_exceptions_key = 'SPLITIO.telemetry.exceptions'
        if prefix is not None:
            self._telemetry_config_key = prefix + "." + self._telemetry_config_key
            self._telemetry_latencies_key = prefix + "." + self._telemetry_latencies_key
            self._telemetry_exceptions_key = prefix + "." + self._telemetry_exceptions_key

        self._lock = asyncio.Lock()
        await self._reset_config_tags()
        self._method_latencies = await MethodLatenciesAsync.create()
        self._method_exceptions = await MethodExceptionsAsync.create()
        self._tel_config = await TelemetryConfigAsync.create()
        return self

    async def _reset_config_tags(self):
        """Reset config tags."""
        async with self._lock:
            self._config_tags = []

    async def add_config_tag(self, tag):
        """
        Record tag string.

        :param tag: tag to be added
        :type tag: str
        """
        async with self._lock:
            if len(self._config_tags) < MAX_TAGS:
                self._config_tags.append(tag)

    async def record_config(self, config, extra_config, total_flag_sets, invalid_flag_sets):
        """
        initilize telemetry objects

        :param config: factory configuration parameters
        :type config: Dict
        :param extra_config: any extra configs
        :type extra_config: Dict
        """
        await self._tel_config.record_config(config, extra_config, total_flag_sets, invalid_flag_sets)

    async def pop_config_tags(self):
        """Get and reset configs."""
        tags = self._config_tags
        await self._reset_config_tags()
        return tags

    async def push_config_stats(self):
        """push config stats to storage."""
        await self._pluggable_adapter.set(self._telemetry_config_key + "::" + self._sdk_metadata, str(await self._format_config_stats()))

    async def _format_config_stats(self):
        """format only selected config stats to json"""
        config_stats = await self._tel_config.get_stats()
        return json.dumps({
            'aF': config_stats['aF'],
            'rF': config_stats['rF'],
            'sT': config_stats['sT'],
            'oM': config_stats['oM'],
            't': await self.pop_config_tags()
        })

    async def record_active_and_redundant_factories(self, active_factory_count, redundant_factory_count):
        """
        Record active and redundant factories.

        :param active_factory_count: active factory count
        :type active_factory_count: int
        :param redundant_factory_count: redundant factory count
        :type redundant_factory_count: int
        """
        await self._tel_config.record_active_and_redundant_factories(active_factory_count, redundant_factory_count)

    async def record_latency(self, method, bucket):
        """
        record latency data

        :param method: method name
        :type method: string
        :param latency: latency
        :type latency: int64
        """
        latency_key = self._telemetry_latencies_key + '::' + self._sdk_metadata + '/' + method.value + '/' + str(bucket)
        result = await self._pluggable_adapter.increment(latency_key, 1)
        await self.expire_keys(latency_key, self._TELEMETRY_KEY_DEFAULT_TTL, 1, result)

    async def record_exception(self, method):
        """
        record an exception

        :param method: method name
        :type method: string
        """
        except_key = self._telemetry_exceptions_key + "::" + self._sdk_metadata + '/' + method.value
        result = await self._pluggable_adapter.increment(except_key, 1)
        await self.expire_keys(except_key, self._TELEMETRY_KEY_DEFAULT_TTL, 1, result)

    async def expire_latency_keys(self, total_keys, inserted):
        """
        Set expire ttl for a latency key in storage

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        await self.expire_keys(self._telemetry_latencies_key, self._TELEMETRY_KEY_DEFAULT_TTL, total_keys, inserted)

    async def expire_keys(self, queue_key, key_default_ttl, total_keys, inserted):
        """
        Set expire ttl for a key in storage if total keys equal inserted

        :param queue_keys: key to be set
        :type queue_keys: str
        :param ey_default_ttl: ttl value
        :type ey_default_ttl: int
        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        if total_keys == inserted:
            await self._pluggable_adapter.expire(queue_key, key_default_ttl)

    async def record_bur_time_out(self):
        """record BUR timeouts"""
        pass

    async def record_ready_time(self, ready_time):
        """Record ready time."""
        pass

    async def record_not_ready_usage(self):
        """Not implemented"""
        pass

    async def record_impression_stats(self, data_type, count):
        """Not implemented"""
        pass
