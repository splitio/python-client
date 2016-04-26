"""This module contains everything related split and segment caches"""
from __future__ import absolute_import, division, print_function, unicode_literals


from collections import defaultdict


class SplitCache(object):
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
        raise NotImplementedError()

    def remove_split(self, split_name):
        """
        Evicts a Split from the cache.
        :param split_name: Name of the split (feature)
        :type split_name: str
        """
        raise NotImplementedError()

    def get_split(self, split_name):
        """
        Retrieves a Split from the cache.
        :param split_name: Name of the split (feature)
        :type split_name: str
        :return: The split under the name if it exists, None otherwise
        :rtype: Split
        """
        raise NotImplementedError()

    def set_change_number(self, change_number):
        """
        Sets the value for the change number
        :param change_number: The change number
        :type change_number: int
        """
        raise NotImplementedError()

    def get_change_number(self):
        """
        Retrieves the value of the change number
        :return: The current change number value, -1 otherwise
        :rtype: int
        """
        raise NotImplementedError()


class SegmentCache(object):
    """
    The basic interface for a Segment cache. It should be able to store and retrieve Segment
    instances, as well as keeping track of the change number.
    """
    def add_to_segment(self, segment_name, segment_keys):
        """
        Adds a set of keys to a segment
        :param segment_name: Name of the segment
        :type segment_name: str
        :param segment_keys: Keys to add to the segment
        :type segment_keys: list
        """
        raise NotImplementedError()

    def remove_from_segment(self, segment_name, segment_keys):
        """
        Removes a set of keys from a segment
        :param segment_name: Name of the segment
        :type segment_name: str
        :param segment_keys: Keys to remove from the segment
        :type segment_keys: list
        """
        raise NotImplementedError()

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
        raise NotImplementedError()

    def set_change_number(self, change_number):
        """
        Sets the value for the change number
        :param change_number: The change number
        :type change_number: int
        """
        raise NotImplementedError()

    def get_change_number(self):
        """
        Retrieves the value of the change number
        :return: The current change number value, -1 otherwise
        :rtype: int
        """
        raise NotImplementedError()


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
    def __init__(self, change_number=-1, entries=None):
        """
        A SegmentCache implementation that stores segments in a dictionary.
        :param change_number: Initial value for the change number.
        :type change_number: int
        :param entries: Initial set of dictionary entries
        :type entries: dict
        """
        self._change_number = change_number
        self._entries = defaultdict(set)
        if entries is not None:
            self._entries.update(entries)

    def add_to_segment(self, segment_name, segment_keys):
        existing_segment_keys = self._entries[segment_name]
        self._entries[segment_name] = existing_segment_keys | set(segment_keys)

    def remove_from_segment(self, segment_name, segment_keys):
        existing_segment_keys = self._entries[segment_name]
        self._entries[segment_name] = existing_segment_keys - set(segment_keys)

    def is_in_segment(self, segment_name, key):
        return key in self._entries[segment_name]

    def set_change_number(self, change_number):
        self._change_number = change_number

    def get_change_number(self):
        return self._change_number
