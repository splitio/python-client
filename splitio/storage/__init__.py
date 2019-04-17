"""Base storage interfaces."""
from __future__ import absolute_import

import abc

from six import add_metaclass

@add_metaclass(abc.ABCMeta)
class SplitStorage(object):
    """Split storage interface implemented as an abstract class."""

    @abc.abstractmethod
    def get(self, split_name):
        """
        Retrieve a split.

        :param split_name: Name of the feature to fetch.
        :type split_name: str

        :rtype: str
        """
        pass

    @abc.abstractmethod
    def put(self, split):
        """
        Store a split.

        :param split: Split object to store
        :type split_name: splitio.models.splits.Split
        """
        pass

    @abc.abstractmethod
    def remove(self, split_name):
        """
        Remove a split from storage.

        :param split_name: Name of the feature to remove.
        :type split_name: str

        :return: True if the split was found and removed. False otherwise.
        :rtype: bool
        """
        pass

    @abc.abstractmethod
    def get_change_number(self):
        """
        Retrieve latest split change number.

        :rtype: int
        """
        pass

    @abc.abstractmethod
    def set_change_number(self, new_change_number):
        """
        Set the latest change number.

        :param new_change_number: New change number.
        :type new_change_number: int
        """
        pass

    @abc.abstractmethod
    def get_split_names(self):
        """
        Retrieve a list of all split names.

        :return: List of split names.
        :rtype: list(str)
        """
        pass

    @abc.abstractmethod
    def get_all_splits(self):
        """
        Return all the splits.

        :return: List of all the splits.
        :rtype: list
        """
        pass

    def get_segment_names(self):
        """
        Return a set of all segments referenced by splits in storage.

        :return: Set of all segment names.
        :rtype: set(string)
        """
        return set([name for spl in self.get_all_splits() for name in spl.get_segment_names()])


@add_metaclass(abc.ABCMeta)
class SegmentStorage(object):
    """Segment storage interface implemented as an abstract class."""

    @abc.abstractmethod
    def get(self, segment_name):
        """
        Retrieve a segment.

        :param segment_name: Name of the segment to fetch.
        :type segment_name: str

        :rtype: str
        """
        pass

    @abc.abstractmethod
    def put(self, segment):
        """
        Store a segment.

        :param segment: Segment to store.
        :type segment: splitio.models.segment.Segment
        """
        pass

    @abc.abstractmethod
    def update(self, segment_name, to_add, to_remove, change_number=None):
        """
        Store a split.

        :param segment_name: Name of the segment to update.
        :type segment_name: str
        :param to_add: List of members to add to the segment.
        :type to_add: list
        :param to_remove: List of members to remove from the segment.
        :type to_remove: list
        """
        pass

    @abc.abstractmethod
    def get_change_number(self, segment_name):
        """
        Retrieve latest change number for a segment.

        :param segment_name: Name of the segment.
        :type segment_name: str

        :rtype: int
        """
        pass

    @abc.abstractmethod
    def set_change_number(self, segment_name, new_change_number):
        """
        Set the latest change number.

        :param segment_name: Name of the segment.
        :type segment_name: str
        :param new_change_number: New change number.
        :type new_change_number: int
        """
        pass

    @abc.abstractmethod
    def segment_contains(self, segment_name, key):
        """
        Check whether a specific key belongs to a segment in storage.

        :param segment_name: Name of the segment to search in.
        :type segment_name: str
        :param key: Key to search for.
        :type key: str

        :return: True if the segment contains the key. False otherwise.
        :rtype: bool
        """
        pass


@add_metaclass(abc.ABCMeta)
class ImpressionStorage(object):
    """Impressions storage interface."""

    @abc.abstractmethod
    def put(self, impressions):
        """
        Put one or more impressions in storage.

        :param impressions: List of one or more impressions to store.
        :type impressions: list
        """
        pass

    @abc.abstractmethod
    def pop_many(self, count):
        """
        Pop the oldest N impressions from storage.

        :param count: Number of impressions to pop.
        :type count: int
        """
        pass


@add_metaclass(abc.ABCMeta)
class EventStorage(object):
    """Events storage interface."""

    @abc.abstractmethod
    def put(self, events):
        """
        Put one or more events in storage.

        :param events: List of one or more events to store.
        :type events: list
        """
        pass

    @abc.abstractmethod
    def pop_many(self, count):
        """
        Pop the oldest N events from storage.

        :param count: Number of events to pop.
        :type count: int
        """
        pass


@add_metaclass(abc.ABCMeta)
class TelemetryStorage(object):
    """Telemetry storage interface."""

    @abc.abstractmethod
    def inc_latency(self, name, bucket):
        """
        Add a latency.

        :param name: Name of the latency metric.
        :type name: str
        :param value: Value of the latency metric.
        :tyoe value: int
        """
        pass

    @abc.abstractmethod
    def inc_counter(self, name):
        """
        Increment a counter.

        :param name: Name of the counter metric.
        :type name: str
        """
        pass

    @abc.abstractmethod
    def put_gauge(self, name, value):
        """
        Add a gauge metric.

        :param name: Name of the gauge metric.
        :type name: str
        :param value: Value of the gauge metric.
        :type value: int
        """
        pass

    @abc.abstractmethod
    def pop_counters(self):
        """
        Get all the counters.

        :rtype: list
        """
        pass

    @abc.abstractmethod
    def pop_gauges(self):
        """
        Get all the gauges.

        :rtype: list

        """
        pass

    @abc.abstractmethod
    def pop_latencies(self):
        """
        Get all latencies.

        :rtype: list
        """
        pass
