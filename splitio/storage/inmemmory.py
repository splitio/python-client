"""In memory storage classes."""
import logging
import threading
import queue
from collections import Counter

from splitio.models.segments import Segment
from splitio.storage import SplitStorage, SegmentStorage, ImpressionStorage, EventStorage

MAX_SIZE_BYTES = 5 * 1024 * 1024


_LOGGER = logging.getLogger(__name__)


class InMemorySplitStorage(SplitStorage):
    """InMemory implementation of a split storage."""

    def __init__(self):
        """Constructor."""
        self._lock = threading.RLock()
        self._splits = {}
        self._change_number = -1
        self._traffic_types = Counter()

    def get(self, split_name):
        """
        Retrieve a split.

        :param split_name: Name of the feature to fetch.
        :type split_name: str

        :rtype: splitio.models.splits.Split
        """
        with self._lock:
            return self._splits.get(split_name)

    def fetch_many(self, split_names):
        """
        Retrieve splits.

        :param split_names: Names of the features to fetch.
        :type split_name: list(str)

        :return: A dict with split objects parsed from queue.
        :rtype: dict(split_name, splitio.models.splits.Split)
        """
        return {split_name: self.get(split_name) for split_name in split_names}

    def put(self, split):
        """
        Store a split.

        :param split: Split object.
        :type split: splitio.models.split.Split
        """
        with self._lock:
            if split.name in self._splits:
                self._decrease_traffic_type_count(self._splits[split.name].traffic_type_name)
            self._splits[split.name] = split
            self._increase_traffic_type_count(split.traffic_type_name)

    def remove(self, split_name):
        """
        Remove a split from storage.

        :param split_name: Name of the feature to remove.
        :type split_name: str

        :return: True if the split was found and removed. False otherwise.
        :rtype: bool
        """
        with self._lock:
            split = self._splits.get(split_name)
            if not split:
                _LOGGER.warning("Tried to delete nonexistant split %s. Skipping", split_name)
                return False

            self._splits.pop(split_name)
            self._decrease_traffic_type_count(split.traffic_type_name)
            return True

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

    def is_valid_traffic_type(self, traffic_type_name):
        """
        Return whether the traffic type exists in at least one split in cache.

        :param traffic_type_name: Traffic type to validate.
        :type traffic_type_name: str

        :return: True if the traffic type is valid. False otherwise.
        :rtype: bool
        """
        with self._lock:
            return traffic_type_name in self._traffic_types

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
        with self._lock:
            if self.get_change_number() > change_number:
                return
            split = self._splits.get(split_name)
            if not split:
                return
            split.local_kill(default_treatment, change_number)
            self.put(split)

    def _increase_traffic_type_count(self, traffic_type_name):
        """
        Increase by one the count for a specific traffic type name.

        :param traffic_type_name: Traffic type to increase the count.
        :type traffic_type_name: str
        """
        self._traffic_types.update([traffic_type_name])

    def _decrease_traffic_type_count(self, traffic_type_name):
        """
        Decrease by one the count for a specific traffic type name.

        :param traffic_type_name: Traffic type to decrease the count.
        :type traffic_type_name: str
        """
        self._traffic_types.subtract([traffic_type_name])
        self._traffic_types += Counter()


class InMemorySegmentStorage(SegmentStorage):
    """In-memory implementation of a segment storage."""

    def __init__(self):
        """Constructor."""
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
                _LOGGER.debug(
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
            if segment_name not in self._segments:
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
            if segment_name not in self._segments:
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
            if segment_name not in self._segments:
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
            if segment_name not in self._segments:
                _LOGGER.warning(
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
        self._queue_size = queue_size
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
            _LOGGER.warning(
                'Impression queue is full, failing to add more impressions. \n'
                'Consider increasing parameter `impressionsQueueSize` in configuration'
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

    def clear(self):
        """
        Clear data.
        """
        with self._lock:
            self._impressions = queue.Queue(maxsize=self._queue_size)


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
        self._queue_size = eventsQueueSize
        self._lock = threading.Lock()
        self._events = queue.Queue(maxsize=eventsQueueSize)
        self._queue_full_hook = None
        self._size = 0

    def set_queue_full_hook(self, hook):
        """
        Set a hook to be called when the queue is full.

        :param h: Hook to be called when the queue is full
        """
        if callable(hook):
            self._queue_full_hook = hook

    def put(self, events):
        """
        Add an event to storage.

        :param event: Event to be added in the storage
        """
        try:
            with self._lock:
                for event in events:
                    self._size += event.size

                    if self._size >= MAX_SIZE_BYTES:
                        self._queue_full_hook()
                        return False

                    self._events.put(event.event, False)
            return True
        except queue.Full:
            if self._queue_full_hook is not None and callable(self._queue_full_hook):
                self._queue_full_hook()
            _LOGGER.warning(
                'Events queue is full, failing to add more events. \n'
                'Consider increasing parameter `eventsQueueSize` in configuration'
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
        self._size = 0
        return events

    def clear(self):
        """
        Clear data.
        """
        with self._lock:
            self._events = queue.Queue(maxsize=self._queue_size)
