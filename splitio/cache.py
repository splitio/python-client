"""This module contains everything related split and segment caches"""
from __future__ import absolute_import, division, print_function, unicode_literals


from collections import defaultdict
from copy import deepcopy
from threading import RLock


class SplitCache(object):  # pragma: no cover
    """
    The basic interface for a Split cache. It should be able to store and retrieve Split
    instances, as well as keeping track of the change number.
    """
    def add_split(self, split_name, split):
        """
        Stores a Split under a name.
        :param split_name: Name of the split (feature)
        :type split_name: str
        :param split: The split to store
        :type split: Split
        """
        pass  # Do nothing

    def remove_split(self, split_name):
        """
        Evicts a Split from the cache.
        :param split_name: Name of the split (feature)
        :type split_name: str
        """
        pass  # Do nothing

    def get_split(self, split_name):
        """
        Retrieves a Split from the cache.
        :param split_name: Name of the split (feature)
        :type split_name: str
        :return: The split under the name if it exists, None otherwise
        :rtype: Split
        """
        return None

    def set_change_number(self, change_number):
        """
        Sets the value for the change number
        :param change_number: The change number
        :type change_number: int
        """
        pass  # Do nothing

    def get_change_number(self):
        """
        Retrieves the value of the change number
        :return: The current change number value, -1 otherwise
        :rtype: int
        """
        return -1


class SegmentCache(object):  # pragma: no cover
    """
    The basic interface for a Segment cache. It should be able to store and retrieve Segment
    information, as well as keeping track of the change number.
    """
    def add_keys_to_segment(self, segment_name, segment_keys):
        """
        Adds a set of keys to a segment
        :param segment_name: Name of the segment
        :type segment_name: str
        :param segment_keys: Keys to add to the segment
        :type segment_keys: list
        """
        pass  # Do nothing

    def remove_keys_from_segment(self, segment_name, segment_keys):
        """
        Removes a set of keys from a segment
        :param segment_name: Name of the segment
        :type segment_name: str
        :param segment_keys: Keys to remove from the segment
        :type segment_keys: list
        """
        pass  # Do nothing

    def is_in_segment(self, segment_name, key):
        """
        Checks if a key is in a segment
        :param segment_name: Name of the segment
        :type segment_name: str
        :param key: Key to check
        :type key: str
        :return: True if the key is in the segment, False otherwise
        :rtype: bool
        """
        return False

    def set_change_number(self, segment_name, change_number):
        """
        Sets the value for the change number
        :param segment_name: Name of the segment
        :type segment_name: str
        :param change_number: The change number
        :type change_number: int
        """
        pass  # Do nothing

    def get_change_number(self, segment_name):
        """
        Retrieves the value of the change number of a segment
        :param segment_name: Name of the segment
        :type segment_name: str
        :return: The current change number value, -1 otherwise
        :rtype: int
        """
        return -1


class InMemorySplitCache(SplitCache):
    def __init__(self, change_number=-1, entries=None):
        """
        A SplitCache that stores splits in a dictionary.
        :param change_number: Initial value for the change number.
        :type change_number: int
        :param entries: Initial set of dictionary entries
        :type entries: dict
        """
        self._change_number = change_number
        self._entries = entries if entries is not None else dict()

    def add_split(self, split_name, split):
        self._entries[split_name] = split

    def remove_split(self, split_name):
        self._entries.pop(split_name, None)

    def get_split(self, split_name):
        return self._entries.get(split_name, None)

    def set_change_number(self, change_number):
        self._change_number = change_number

    def get_change_number(self):
        return self._change_number


class InMemorySegmentCache(SegmentCache):
    def __init__(self):
        """A SegmentCache implementation that stores segments in a dictionary"""
        self._entries = defaultdict(lambda: {'change_number': -1, 'key_set': frozenset()})

    def add_keys_to_segment(self, segment_name, segment_keys):
        segment = self._entries[segment_name]
        segment['key_set'] = segment['key_set'] | frozenset(segment_keys)

    def remove_keys_from_segment(self, segment_name, segment_keys):
        segment = self._entries[segment_name]
        segment['key_set'] = segment['key_set'] - frozenset(segment_keys)

    def is_in_segment(self, segment_name, key):
        return key in self._entries[segment_name]['key_set']

    def set_change_number(self, segment_name, change_number):
        self._entries[segment_name]['change_number'] = change_number

    def get_change_number(self, segment_name):
        return self._entries[segment_name]['change_number']


class ImpressionsCache(object):  # pragma: no cover
    """The basic interface for an Impressions cache."""
    def add_impression(self, impression):
        """Add an impression to a feature
        :param impression: An impression
        :type impression: Impression
        :return: How many impressions have been added so far
        :rtype: int
        """
        pass  # Do nothing

    def fetch_all(self):
        """ List all impressions.
        :return: A list of Impression tuples
        :rtype: list
        """
        return []

    def clear(self):
        """Clears all impressions."""
        pass  # Do nothing

    def fetch_all_and_clear(self):
        """ List all impressions and clear the cache.
        :return: A list of Impression tuples
        :rtype: list
        """
        return []


class InMemoryImpressionsCache(ImpressionsCache):  # pragma: no cover
    def __init__(self, impressions=None):
        """An in memory implementation of an Impressions cache.
        :param impressions: Initial set of impressions
        :type impressions: dict
        """
        self._impressions = defaultdict(list)
        if impressions is not None:
            self._impressions.update(impressions)
        self._rlock = RLock()

    def add_impression(self, impression):
        """Add an impression to a feature
        :param impression: An impression
        :type impression: Impression
        """
        with self._rlock:
            self._impressions[impression.feature].append(impression)

    def fetch_all(self):
        """ List all impressions.
        :return: A list of Impression tuples
        :rtype: dict
        """
        return deepcopy(self._impressions)

    def clear(self):
        """Clears all impressions."""
        with self._rlock:
            self._impressions = defaultdict(list)

    def fetch_all_and_clear(self):
        """ List all impressions and clear the cache.
        :return: A list of Impression tuples
        :rtype: list
        """
        with self._rlock:
            impressions = self.fetch_all()
            self.clear()

        return impressions


class MetricsCache(object):  # pragma: no cover
    """A default implementation of a Metrics cache."""
    def set_count(self, counter, value):
        """Sets a counter value.
        :param counter: Name of the counter
        :type counter: str
        :param value: Value for the counter
        :type value: 1
        """
        pass  # Do nothing

    def increment_count(self, counter, delta=1):
        """Increments the value of a counter by a given value.
        :param counter: Name of the counter
        :type counter: str
        :param delta: The value to be added to the counter
        :type delta: int
        """
        pass  # Do nothing

    def get_count(self, counter):
        """
        :param counter: Name of the counter
        :type counter: str
        :return: The current value of the counter
        :rtype: int
        """
        return 0

    def set_gauge(self, gauge, value):
        """Sets the value of a gauge.
        :param gauge: The name of the gauge
        :type gauge: str
        :param value: The value of the gauge
        :type value: float
        """
        pass  # Do nothing

    def get_gauge(self, gauge):
        """
        :param gauge: The name of the gauge
        :type gauge: str
        :return: The current value of the gauge
        :rtype: float
        """
        return 0

    def set_latency_bucket_counter(self, operation, bucket_index, value):
        """Sets the value of a bucket of a latency tracker for an operation.
        :param operation: The name of the operation
        :type operation: str
        :param bucket_index: The index for the latency bucket
        :type bucket_index: int
        :param value: The new value for the bucket
        :type value: int
        """
        pass  # Do nothing

    def increment_latency_bucket_counter(self, operation, bucket_index, delta=1):
        """Increments the value of a bucket of a latency tracker for an operation
        :param operation: The name of the operation
        :type operation: str
        :param bucket_index: The index for the latency bucket
        :type bucket_index: int
        :param delta: The value to add to the bucket
        :type delta: int
        """
        pass  # Do nothing

    def get_latency_bucket_counter(self, operation, bucket_index):
        """
        :param operation: The name of the operation
        :type operation: str
        :param bucket_index: The index for the latency bucket
        :type bucket_index: int
        :return: The current value of a bucket of a latency tracker
        :rtype: int
        """
        return 0

    def get_latency(self, operation):
        """
        :param operation: The name of the operation
        :type operation: str
        :return: All the buckets of a latency tracker
        :rtype: list
        """
        return [0] * 23
