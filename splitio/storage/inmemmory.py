"""In memory storage classes."""
from __future__ import absolute_import

import logging
import threading
from six.moves import queue
from splitio.models.segments import Segment
from splitio.storage import SplitStorage, SegmentStorage, ImpressionStorage, EventStorage, \
    TelemetryStorage


class InMemorySplitStorage(SplitStorage):
    """InMemory implementation of a split storage."""

    def __init__(self):
        """Constructor."""
        self._logger = logging.getLogger(self.__class__.__name__)
        self._lock = threading.RLock()
        self._splits = {}
        self._change_number = -1

    def get(self, split_name):
        """
        Retrieve a split.

        :param split_name: Name of the feature to fetch.
        :type split_name: str

        :rtype: splitio.models.splits.Split
        """
        with self._lock:
            return self._splits.get(split_name)

    def put(self, split):
        """
        Store a split.

        :param split: Split object.
        :type split: splitio.models.split.Split
        """
        with self._lock:
            self._splits[split.name] = split

    def remove(self, split_name):
        """
        Remove a split from storage.

        :param split_name: Name of the feature to remove.
        :type split_name: str

        :return: True if the split was found and removed. False otherwise.
        :rtype: bool
        """
        with self._lock:
            try:
                self._splits.pop(split_name)
                return True
            except KeyError:
                self._logger.warning("Tried to delete nonexistant split %s. Skipping", split_name)
                return False

    def get_change_number(self):
        """
        Retrieve latest split change number.

        :rtype: int
        """
        with self._lock:
            return self._change_number

    def set_change_number(self, new_change_number):
        """
        Set the latest change number.

        :param new_change_number: New change number.
        :type new_change_number: int
        """
        with self._lock:
            self._change_number = new_change_number

    def get_split_names(self):
        """
        Retrieve a list of all split names.

        :return: List of split names.
        :rtype: list(str)
        """
        with self._lock:
            return list(self._splits.keys())

    def get_all_splits(self):
        """
        Return all the splits.

        :return: List of all the splits.
        :rtype: list
        """
        with self._lock:
            return list(self._splits.values())


class InMemorySegmentStorage(SegmentStorage):
    """In-memory implementation of a segment storage."""

    def __init__(self):
        """Constructor."""
        self._logger = logging.getLogger(self.__class__.__name__)
        self._segments = {}
        self._change_numbers = {}
        self._lock = threading.RLock()

    def get(self, segment_name):
        """
        Retrieve a segment.

        :param segment_name: Name of the segment to fetch.
        :type segment_name: str

        :rtype: str
        """
        with self._lock:
            fetched = self._segments.get(segment_name)
            if fetched is None:
                self._logger.warning(
                    "Tried to retrieve nonexistant segment %s. Skipping",
                    segment_name
                )
            return fetched

    def put(self, segment):
        """
        Store a segment.

        :param segment: Segment to store.
        :type segment: splitio.models.segment.Segment
        """
        with self._lock:
            self._segments[segment.name] = segment

    def update(self, segment_name, to_add, to_remove, change_number=None):
        """
        Update a split. Create it if it doesn't exist.

        :param segment_name: Name of the segment to update.
        :type segment_name: str
        :param to_add: Set of members to add to the segment.
        :type to_add: set
        :param to_remove: List of members to remove from the segment.
        :type to_remove: Set
        """
        with self._lock:
            if not segment_name in self._segments:
                self._segments[segment_name] = Segment(segment_name, to_add, change_number)
                return

            self._segments[segment_name].update(to_add, to_remove)
            if change_number is not None:
                self._segments[segment_name].change_number = change_number

    def get_change_number(self, segment_name):
        """
        Retrieve latest change number for a segment.

        :param segment_name: Name of the segment.
        :type segment_name: str

        :rtype: int
        """
        with self._lock:
            if not segment_name in self._segments:
                return None
            return self._segments[segment_name].change_number

    def set_change_number(self, segment_name, new_change_number):
        """
        Set the latest change number.

        :param segment_name: Name of the segment.
        :type segment_name: str
        :param new_change_number: New change number.
        :type new_change_number: int
        """
        with self._lock:
            if not segment_name in self._segments:
                return
            self._segments[segment_name].change_number = new_change_number

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
        with self._lock:
            if not segment_name in self._segments:
                self._logger.warning(
                    "Tried to query members for nonexistant segment %s. Returning False",
                    segment_name
                )
                return False
            return self._segments[segment_name].contains(key)


class InMemoryImpressionStorage(ImpressionStorage):
    """In memory implementation of an impressions storage."""

    def __init__(self, queue_size):
        """
        Construct an instance.

        :param eventsQueueSize: How many events to queue before forcing a submission
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._impressions = queue.Queue(maxsize=queue_size)
        self._lock = threading.Lock()
        self._queue_full_hook = None

    def set_queue_full_hook(self, hook):
        """
        Set a hook to be called when the queue is full.

        :param h: Hook to be called when the queue is full
        """
        if callable(hook):
            self._queue_full_hook = hook

    def put(self, impressions):
        """
        Put one or more impressions in storage.

        :param impressions: List of one or more impressions to store.
        :type impressions: list
        """
        try:
            with self._lock:
                for impression in impressions:
                    self._impressions.put(impression, False)
            return True
        except queue.Full:
            if self._queue_full_hook is not None and callable(self._queue_full_hook):
                self._queue_full_hook()
            self._logger.warning(
                'Event queue is full, failing to add more events. \n'
                'Consider increasing parameter `eventQueueSize` in configuration'
            )
            return False

    def pop_many(self, count):
        """
        Pop the oldest N impressions from storage.

        :param count: Number of impressions to pop.
        :type count: int
        """
        impressions = []
        with self._lock:
            while not self._impressions.empty() and count > 0:
                impressions.append(self._impressions.get(False))
                count -= 1
        return impressions


class InMemoryEventStorage(EventStorage):
    """
    In memory storage for events.

    Supports adding and popping events.
    """

    def __init__(self, eventsQueueSize):
        """
        Construct an instance.

        :param eventsQueueSize: How many events to queue before forcing a submission
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._lock = threading.Lock()
        self._events = queue.Queue(maxsize=eventsQueueSize)
        self._queue_full_hook = None

    def set_queue_full_hook(self, hook):
        """
        Set a hook to be called when the queue is full.

        :param h: Hook to be called when the queue is full
        """
        if callable(hook):
            self._queue_full_hook = hook

    def put(self, events):
        """
        Add an avent to storage.

        :param event: Event to be added in the storage
        """
        try:
            with self._lock:
                for event in events:
                    self._events.put(event, False)
            return True
        except queue.Full:
            if self._queue_full_hook is not None and callable(self._queue_full_hook):
                self._queue_full_hook()
            self._logger.warning(
                'Events queue is full, failing to add more events. \n'
                'Consider increasing parameter `impressionsQueueSize` in configuration'
            )
            return False

    def pop_many(self, count):
        """
        Pop multiple items from the storage.

        :param count: number of items to be retrieved and removed from the queue.
        """
        events = []
        with self._lock:
            while not self._events.empty() and count > 0:
                events.append(self._events.get(False))
                count -= 1
        return events


class InMemoryTelemetryStorage(TelemetryStorage):
    """In-Memory implementation of telemetry storage interface."""

    def __init__(self):
        """Constructor."""
        self._logger = logging.getLogger(self.__class__.__name__)
        self._latencies = {}
        self._gauges = {}
        self._counters = {}
        self._latencies_lock = threading.Lock()
        self._gauges_lock = threading.Lock()
        self._counters_lock = threading.Lock()

    def inc_latency(self, name, bucket):
        """
        Add a latency.

        :param name: Name of the latency metric.
        :type name: str
        :param value: Value of the latency metric.
        :tyoe value: int
        """
        if not 0 <= bucket <= 21:
            self._logger.warning('Incorect bucket "%d" for latency "%s". Ignoring.', bucket, name)
            return

        with self._latencies_lock:
            latencies = self._latencies.get(name, [0] * 22)
            latencies[bucket] += 1
            self._latencies[name] = latencies

    def inc_counter(self, name):
        """
        Increment a counter.

        :param name: Name of the counter metric.
        :type name: str
        """
        with self._counters_lock:
            counter = self._counters.get(name, 0)
            counter += 1
            self._counters[name] = counter

    def put_gauge(self, name, value):
        """
        Add a gauge metric.

        :param name: Name of the gauge metric.
        :type name: str
        :param value: Value of the gauge metric.
        :type value: int
        """
        with self._gauges_lock:
            self._gauges[name] = value

    def pop_counters(self):
        """
        Get all the counters.

        :rtype: list
        """
        with self._counters_lock:
            try:
                return self._counters
            finally:
                self._counters = {}

    def pop_gauges(self):
        """
        Get all the gauges.

        :rtype: list

        """
        with self._gauges_lock:
            try:
                return self._gauges
            finally:
                self._gauges = {}

    def pop_latencies(self):
        """
        Get all latencies.

        :rtype: list
        """
        with self._latencies_lock:
            try:
                return self._latencies
            finally:
                self._latencies = {}
