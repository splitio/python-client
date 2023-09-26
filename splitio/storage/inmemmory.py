"""In memory storage classes."""
import logging
import threading
import queue
from collections import Counter

from splitio.models.segments import Segment
from splitio.models.telemetry import HTTPErrors, HTTPLatencies, MethodExceptions, MethodLatencies, LastSynchronization, StreamingEvents, TelemetryConfig, TelemetryCounters, CounterConstants, \
    HTTPErrorsAsync, HTTPLatenciesAsync, MethodExceptionsAsync, MethodLatenciesAsync, LastSynchronizationAsync, StreamingEventsAsync, TelemetryConfigAsync, TelemetryCountersAsync
from splitio.storage import SplitStorage, SegmentStorage, ImpressionStorage, EventStorage, TelemetryStorage
from splitio.optional.loaders import asyncio

MAX_SIZE_BYTES = 5 * 1024 * 1024
MAX_TAGS = 10

_LOGGER = logging.getLogger(__name__)


class InMemorySplitStorageBase(SplitStorage):
    """InMemory implementation of a split storage base."""

    def get(self, split_name):
        """
        Retrieve a split.

        :param split_name: Name of the feature to fetch.
        :type split_name: str

        :rtype: splitio.models.splits.Split
        """
        pass

    def fetch_many(self, split_names):
        """
        Retrieve splits.

        :param split_names: Names of the features to fetch.
        :type split_name: list(str)

        :return: A dict with split objects parsed from queue.
        :rtype: dict(split_name, splitio.models.splits.Split)
        """
        pass

    def put(self, split):
        """
        Store a split.

        :param split: Split object.
        :type split: splitio.models.split.Split
        """
        pass

    def remove(self, split_name):
        """
        Remove a split from storage.

        :param split_name: Name of the feature to remove.
        :type split_name: str

        :return: True if the split was found and removed. False otherwise.
        :rtype: bool
        """
        pass

    def get_change_number(self):
        """
        Retrieve latest split change number.

        :rtype: int
        """
        pass

    def set_change_number(self, new_change_number):
        """
        Set the latest change number.

        :param new_change_number: New change number.
        :type new_change_number: int
        """
        pass

    def get_split_names(self):
        """
        Retrieve a list of all split names.

        :return: List of split names.
        :rtype: list(str)
        """
        pass

    def get_all_splits(self):
        """
        Return all the splits.

        :return: List of all the splits.
        :rtype: list
        """
        pass

    def get_splits_count(self):
        """
        Return splits count.

        :rtype: int
        """
        pass

    def is_valid_traffic_type(self, traffic_type_name):
        """
        Return whether the traffic type exists in at least one split in cache.

        :param traffic_type_name: Traffic type to validate.
        :type traffic_type_name: str

        :return: True if the traffic type is valid. False otherwise.
        :rtype: bool
        """
        pass

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
        pass

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


class InMemorySplitStorage(InMemorySplitStorageBase):
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

    def get_splits_count(self):
        """
        Return splits count.

        :rtype: int
        """
        with self._lock:
            return len(self._splits)

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


class InMemorySplitStorageAsync(InMemorySplitStorageBase):
    """InMemory implementation of a split async storage."""

    def __init__(self):
        """Constructor."""
        self._lock = asyncio.Lock()
        self._splits = {}
        self._change_number = -1
        self._traffic_types = Counter()

    async def get(self, split_name):
        """
        Retrieve a split.

        :param split_name: Name of the feature to fetch.
        :type split_name: str

        :rtype: splitio.models.splits.Split
        """
        async with self._lock:
            return self._splits.get(split_name)

    async def fetch_many(self, split_names):
        """
        Retrieve splits.

        :param split_names: Names of the features to fetch.
        :type split_name: list(str)

        :return: A dict with split objects parsed from queue.
        :rtype: dict(split_name, splitio.models.splits.Split)
        """
        return {split_name: await self.get(split_name) for split_name in split_names}

    async def put(self, split):
        """
        Store a split.

        :param split: Split object.
        :type split: splitio.models.split.Split
        """
        async with self._lock:
            if split.name in self._splits:
                self._decrease_traffic_type_count(self._splits[split.name].traffic_type_name)
            self._splits[split.name] = split
            self._increase_traffic_type_count(split.traffic_type_name)

    async def remove(self, split_name):
        """
        Remove a split from storage.

        :param split_name: Name of the feature to remove.
        :type split_name: str

        :return: True if the split was found and removed. False otherwise.
        :rtype: bool
        """
        async with self._lock:
            split = self._splits.get(split_name)
            if not split:
                _LOGGER.warning("Tried to delete nonexistant split %s. Skipping", split_name)
                return False

            self._splits.pop(split_name)
            self._decrease_traffic_type_count(split.traffic_type_name)
            return True

    async def get_change_number(self):
        """
        Retrieve latest split change number.

        :rtype: int
        """
        async with self._lock:
            return self._change_number

    async def set_change_number(self, new_change_number):
        """
        Set the latest change number.

        :param new_change_number: New change number.
        :type new_change_number: int
        """
        async with self._lock:
            self._change_number = new_change_number

    async def get_split_names(self):
        """
        Retrieve a list of all split names.

        :return: List of split names.
        :rtype: list(str)
        """
        async with self._lock:
            return list(self._splits.keys())

    async def get_all_splits(self):
        """
        Return all the splits.

        :return: List of all the splits.
        :rtype: list
        """
        async with self._lock:
            return list(self._splits.values())

    async def get_splits_count(self):
        """
        Return splits count.

        :rtype: int
        """
        async with self._lock:
            return len(self._splits)

    async def is_valid_traffic_type(self, traffic_type_name):
        """
        Return whether the traffic type exists in at least one split in cache.

        :param traffic_type_name: Traffic type to validate.
        :type traffic_type_name: str

        :return: True if the traffic type is valid. False otherwise.
        :rtype: bool
        """
        async with self._lock:
            return traffic_type_name in self._traffic_types

    async def kill_locally(self, split_name, default_treatment, change_number):
        """
        Local kill for split

        :param split_name: name of the split to perform kill
        :type split_name: str
        :param default_treatment: name of the default treatment to return
        :type default_treatment: str
        :param change_number: change_number
        :type change_number: int
        """
        if await self.get_change_number() > change_number:
            return
        async with self._lock:
            split = self._splits.get(split_name)
            if not split:
                return
            split.local_kill(default_treatment, change_number)
        await self.put(split)

    async def get_segment_names(self):
        """
        Return a set of all segments referenced by splits in storage.

        :return: Set of all segment names.
        :rtype: set(string)
        """
        return set([name for spl in await self.get_all_splits() for name in spl.get_segment_names()])

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

    def get_segments_count(self):
        """
        Retrieve segments count.

        :rtype: int
        """
        with self._lock:
            return len(self._segments)

    def get_segments_keys_count(self):
        """
        Retrieve segments keys count.

        :rtype: int
        """
        total_count = 0
        with self._lock:
            for segment in self._segments:
                total_count += len(self._segments[segment]._keys)
            return total_count


class InMemorySegmentStorageAsync(SegmentStorage):
    """In-memory implementation of a segment async storage."""

    def __init__(self):
        """Constructor."""
        self._segments = {}
        self._change_numbers = {}
        self._lock = asyncio.Lock()

    async def get(self, segment_name):
        """
        Retrieve a segment.

        :param segment_name: Name of the segment to fetch.
        :type segment_name: str

        :rtype: str
        """
        async with self._lock:
            fetched = self._segments.get(segment_name)
            if fetched is None:
                _LOGGER.debug(
                    "Tried to retrieve nonexistant segment %s. Skipping",
                    segment_name
                )
            return fetched

    async def put(self, segment):
        """
        Store a segment.

        :param segment: Segment to store.
        :type segment: splitio.models.segment.Segment
        """
        async with self._lock:
            self._segments[segment.name] = segment

    async def update(self, segment_name, to_add, to_remove, change_number=None):
        """
        Update a split. Create it if it doesn't exist.

        :param segment_name: Name of the segment to update.
        :type segment_name: str
        :param to_add: Set of members to add to the segment.
        :type to_add: set
        :param to_remove: List of members to remove from the segment.
        :type to_remove: Set
        """
        async with self._lock:
            if segment_name not in self._segments:
                self._segments[segment_name] = Segment(segment_name, to_add, change_number)
                return

            self._segments[segment_name].update(to_add, to_remove)
            if change_number is not None:
                self._segments[segment_name].change_number = change_number

    async def get_change_number(self, segment_name):
        """
        Retrieve latest change number for a segment.

        :param segment_name: Name of the segment.
        :type segment_name: str

        :rtype: int
        """
        async with self._lock:
            if segment_name not in self._segments:
                return None
            return self._segments[segment_name].change_number

    async def set_change_number(self, segment_name, new_change_number):
        """
        Set the latest change number.

        :param segment_name: Name of the segment.
        :type segment_name: str
        :param new_change_number: New change number.
        :type new_change_number: int
        """
        async with self._lock:
            if segment_name not in self._segments:
                return
            self._segments[segment_name].change_number = new_change_number

    async def segment_contains(self, segment_name, key):
        """
        Check whether a specific key belongs to a segment in storage.

        :param segment_name: Name of the segment to search in.
        :type segment_name: str
        :param key: Key to search for.
        :type key: str

        :return: True if the segment contains the key. False otherwise.
        :rtype: bool
        """
        async with self._lock:
            if segment_name not in self._segments:
                _LOGGER.warning(
                    "Tried to query members for nonexistant segment %s. Returning False",
                    segment_name
                )
                return False
            return self._segments[segment_name].contains(key)

    async def get_segments_count(self):
        """
        Retrieve segments count.

        :rtype: int
        """
        async with self._lock:
            return len(self._segments)

    async def get_segments_keys_count(self):
        """
        Retrieve segments keys count.

        :rtype: int
        """
        total_count = 0
        async with self._lock:
            for segment in self._segments:
                total_count += len(self._segments[segment]._keys)
            return total_count


class InMemoryImpressionStorageBase(ImpressionStorage):
    """In memory implementation of an impressions base storage."""

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
        pass

    def pop_many(self, count):
        """
        Pop the oldest N impressions from storage.

        :param count: Number of impressions to pop.
        :type count: int
        """
        pass

    def clear(self):
        """
        Clear data.
        """
        pass

class InMemoryImpressionStorage(InMemoryImpressionStorageBase):
    """In memory implementation of an impressions storage."""

    def __init__(self, queue_size, telemetry_runtime_producer):
        """
        Construct an instance.

        :param eventsQueueSize: How many events to queue before forcing a submission
        """
        self._queue_size = queue_size
        self._impressions = queue.Queue(maxsize=queue_size)
        self._lock = threading.Lock()
        self._queue_full_hook = None
        self._telemetry_runtime_producer = telemetry_runtime_producer

    def put(self, impressions):
        """
        Put one or more impressions in storage.

        :param impressions: List of one or more impressions to store.
        :type impressions: list
        """
        impressions_stored = 0
        try:
            with self._lock:
                for impression in impressions:
                    self._impressions.put(impression, False)
                    impressions_stored += 1
            self._telemetry_runtime_producer.record_impression_stats(CounterConstants.IMPRESSIONS_QUEUED, len(impressions))
            return True
        except queue.Full:
            self._telemetry_runtime_producer.record_impression_stats(CounterConstants.IMPRESSIONS_DROPPED, len(impressions) - impressions_stored)
            self._telemetry_runtime_producer.record_impression_stats(CounterConstants.IMPRESSIONS_QUEUED, impressions_stored)
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


class InMemoryImpressionStorageAsync(InMemoryImpressionStorageBase):
    """In memory implementation of an impressions async storage."""

    def __init__(self, queue_size, telemetry_runtime_producer):
        """
        Construct an instance.

        :param eventsQueueSize: How many events to queue before forcing a submission
        """
        self._queue_size = queue_size
        self._impressions = asyncio.Queue(maxsize=queue_size)
        self._lock = asyncio.Lock()
        self._queue_full_hook = None
        self._telemetry_runtime_producer = telemetry_runtime_producer

    async def put(self, impressions):
        """
        Put one or more impressions in storage.

        :param impressions: List of one or more impressions to store.
        :type impressions: list
        """
        impressions_stored = 0
        try:
            async with self._lock:
                for impression in impressions:
                    if self._impressions.qsize() == self._queue_size:
                        raise asyncio.QueueFull
                    await self._impressions.put(impression)
                    impressions_stored += 1
            await self._telemetry_runtime_producer.record_impression_stats(CounterConstants.IMPRESSIONS_QUEUED, len(impressions))
            return True
        except asyncio.QueueFull:
            await self._telemetry_runtime_producer.record_impression_stats(CounterConstants.IMPRESSIONS_DROPPED, len(impressions) - impressions_stored)
            await self._telemetry_runtime_producer.record_impression_stats(CounterConstants.IMPRESSIONS_QUEUED, impressions_stored)
            if self._queue_full_hook is not None and callable(self._queue_full_hook):
                await self._queue_full_hook()
            _LOGGER.warning(
                'Impression queue is full, failing to add more impressions. \n'
                'Consider increasing parameter `impressionsQueueSize` in configuration'
            )
            return False

    async def pop_many(self, count):
        """
        Pop the oldest N impressions from storage.

        :param count: Number of impressions to pop.
        :type count: int
        """
        impressions = []
        async with self._lock:
            while not self._impressions.empty() and count > 0:
                impressions.append(await self._impressions.get())
                count -= 1
        return impressions

    async def clear(self):
        """
        Clear data.
        """
        async with self._lock:
            self._impressions = asyncio.Queue(maxsize=self._queue_size)


class InMemoryEventStorageBase(EventStorage):
    """
    In memory storage base class for events.
    Supports adding and popping events.
    """
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
        pass

    def pop_many(self, count):
        """
        Pop multiple items from the storage.

        :param count: number of items to be retrieved and removed from the queue.
        """
        pass

    def clear(self):
        """
        Clear data.
        """
        pass


class InMemoryEventStorage(InMemoryEventStorageBase):
    """
    In memory storage for events.

    Supports adding and popping events.
    """

    def __init__(self, eventsQueueSize, telemetry_runtime_producer):
        """
        Construct an instance.

        :param eventsQueueSize: How many events to queue before forcing a submission
        """
        self._queue_size = eventsQueueSize
        self._lock = threading.Lock()
        self._events = queue.Queue(maxsize=eventsQueueSize)
        self._queue_full_hook = None
        self._size = 0
        self._telemetry_runtime_producer = telemetry_runtime_producer

    def put(self, events):
        """
        Add an event to storage.

        :param event: Event to be added in the storage
        """
        events_stored = 0
        try:
            with self._lock:
                for event in events:
                    self._size += event.size

                    if self._size >= MAX_SIZE_BYTES:
                        self._queue_full_hook()
                        return False
                    self._events.put(event.event, False)
                    events_stored += 1
            self._telemetry_runtime_producer.record_event_stats(CounterConstants.EVENTS_QUEUED, len(events))
            return True
        except queue.Full:
            self._telemetry_runtime_producer.record_event_stats(CounterConstants.EVENTS_DROPPED, len(events) - events_stored)
            self._telemetry_runtime_producer.record_event_stats(CounterConstants.EVENTS_QUEUED, events_stored)
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


class InMemoryEventStorageAsync(InMemoryEventStorageBase):
    """
    In memory async storage for events.
    Supports adding and popping events.
    """
    def __init__(self, eventsQueueSize, telemetry_runtime_producer):
        """
        Construct an instance.

        :param eventsQueueSize: How many events to queue before forcing a submission
        """
        self._queue_size = eventsQueueSize
        self._lock = asyncio.Lock()
        self._events = asyncio.Queue(maxsize=eventsQueueSize)
        self._queue_full_hook = None
        self._size = 0
        self._telemetry_runtime_producer = telemetry_runtime_producer

    async def put(self, events):
        """
        Add an event to storage.

        :param event: Event to be added in the storage
        """
        events_stored = 0
        try:
            async with self._lock:
                for event in events:
                    if self._events.qsize() == self._queue_size:
                        raise asyncio.QueueFull

                    self._size += event.size
                    if self._size >= MAX_SIZE_BYTES:
                        await self._queue_full_hook()
                        return False
                    await self._events.put(event.event)
                    events_stored += 1
            await self._telemetry_runtime_producer.record_event_stats(CounterConstants.EVENTS_QUEUED, len(events))
            return True
        except asyncio.QueueFull:
            await self._telemetry_runtime_producer.record_event_stats(CounterConstants.EVENTS_DROPPED, len(events) - events_stored)
            await self._telemetry_runtime_producer.record_event_stats(CounterConstants.EVENTS_QUEUED, events_stored)
            if self._queue_full_hook is not None and callable(self._queue_full_hook):
                await self._queue_full_hook()
            _LOGGER.warning(
                'Events queue is full, failing to add more events. \n'
                'Consider increasing parameter `eventsQueueSize` in configuration'
            )
            return False

    async def pop_many(self, count):
        """
        Pop multiple items from the storage.

        :param count: number of items to be retrieved and removed from the queue.
        """
        events = []
        async with self._lock:
            while not self._events.empty() and count > 0:
                events.append(await self._events.get())
                count -= 1
        self._size = 0
        return events

    async def clear(self):
        """
        Clear data.
        """
        async with self._lock:
            self._events = asyncio.Queue(maxsize=self._queue_size)


class InMemoryTelemetryStorageBase(TelemetryStorage):
    """In-memory telemetry storage base."""

    def _reset_tags(self):
        self._tags = []

    def _reset_config_tags(self):
        self._config_tags = []

    def record_config(self, config, extra_config):
        """Record configurations."""
        pass

    def record_active_and_redundant_factories(self, active_factory_count, redundant_factory_count):
        """Record active and redundant factories."""
        pass

    def record_ready_time(self, ready_time):
        """Record ready time."""
        pass

    def add_tag(self, tag):
        """Record tag string."""
        pass

    def add_config_tag(self, tag):
        """Record tag string."""
        pass

    def record_bur_time_out(self):
        """Record block until ready timeout."""
        pass

    def record_not_ready_usage(self):
        """record non-ready usage."""
        pass

    def record_latency(self, method, latency):
        """Record method latency time."""
        pass

    def record_exception(self, method):
        """Record method exception."""
        pass

    def record_impression_stats(self, data_type, count):
        """Record impressions stats."""
        pass

    def record_event_stats(self, data_type, count):
        """Record events stats."""
        pass

    def record_successful_sync(self, resource, time):
        """Record successful sync."""
        pass

    def record_sync_error(self, resource, status):
        """Record sync http error."""
        pass

    def record_sync_latency(self, resource, latency):
        """Record latency time."""
        pass

    def record_auth_rejections(self):
        """Record auth rejection."""
        pass

    def record_token_refreshes(self):
        """Record sse token refresh."""
        pass

    def record_streaming_event(self, streaming_event):
        """Record incoming streaming event."""
        pass

    def record_session_length(self, session):
        """Record session length."""
        pass

    def get_bur_time_outs(self):
        """Get block until ready timeout."""
        pass

    def get_non_ready_usage(self):
        """Get non-ready usage."""
        pass

    def get_config_stats(self):
        """Get all config info."""
        pass

    def pop_exceptions(self):
        """Get and reset method exceptions."""
        pass

    def pop_tags(self):
        """Get and reset tags."""
        pass

    def pop_config_tags(self):
        """Get and reset tags."""
        pass

    def pop_latencies(self):
        """Get and reset eval latencies."""
        pass

    def get_impressions_stats(self, type):
        """Get impressions stats"""
        pass

    def get_events_stats(self, type):
        """Get events stats"""
        pass

    def get_last_synchronization(self):
        """Get last sync"""
        pass

    def pop_http_errors(self):
        """Get and reset http errors."""
        pass

    def pop_http_latencies(self):
        """Get and reset http latencies."""
        pass

    def pop_auth_rejections(self):
        """Get and reset auth rejections."""
        pass

    def pop_token_refreshes(self):
        """Get and reset token refreshes."""
        pass

    def pop_streaming_events(self):
        """Get and reset streaming events"""
        pass

    def get_session_length(self):
        """Get session length"""
        pass


class InMemoryTelemetryStorage(InMemoryTelemetryStorageBase):
    """In-memory telemetry storage."""

    def __init__(self):
        """Constructor"""
        self._lock = threading.RLock()
        self._method_exceptions = MethodExceptions()
        self._last_synchronization = LastSynchronization()
        self._counters = TelemetryCounters()
        self._http_sync_errors = HTTPErrors()
        self._method_latencies = MethodLatencies()
        self._http_latencies = HTTPLatencies()
        self._streaming_events = StreamingEvents()
        self._tel_config = TelemetryConfig()
        with self._lock:
            self._reset_tags()
            self._reset_config_tags()

    def record_config(self, config, extra_config):
        """Record configurations."""
        self._tel_config.record_config(config, extra_config)

    def record_active_and_redundant_factories(self, active_factory_count, redundant_factory_count):
        """Record active and redundant factories."""
        self._tel_config.record_active_and_redundant_factories(active_factory_count, redundant_factory_count)

    def record_ready_time(self, ready_time):
        """Record ready time."""
        self._tel_config.record_ready_time(ready_time)

    def add_tag(self, tag):
        """Record tag string."""
        with self._lock:
            if len(self._tags) < MAX_TAGS:
                self._tags.append(tag)

    def add_config_tag(self, tag):
        """Record tag string."""
        with self._lock:
            if len(self._config_tags) < MAX_TAGS:
                self._config_tags.append(tag)

    def record_bur_time_out(self):
        """Record block until ready timeout."""
        self._tel_config.record_bur_time_out()

    def record_not_ready_usage(self):
        """record non-ready usage."""
        self._tel_config.record_not_ready_usage()

    def record_latency(self, method, latency):
        """Record method latency time."""
        self._method_latencies.add_latency(method,latency)

    def record_exception(self, method):
        """Record method exception."""
        self._method_exceptions.add_exception(method)

    def record_impression_stats(self, data_type, count):
        """Record impressions stats."""
        self._counters.record_impressions_value(data_type, count)

    def record_event_stats(self, data_type, count):
        """Record events stats."""
        self._counters.record_events_value(data_type, count)

    def record_successful_sync(self, resource, time):
        """Record successful sync."""
        self._last_synchronization.add_latency(resource, time)

    def record_sync_error(self, resource, status):
        """Record sync http error."""
        self._http_sync_errors.add_error(resource, status)

    def record_sync_latency(self, resource, latency):
        """Record latency time."""
        self._http_latencies.add_latency(resource, latency)

    def record_auth_rejections(self):
        """Record auth rejection."""
        self._counters.record_auth_rejections()

    def record_token_refreshes(self):
        """Record sse token refresh."""
        self._counters.record_token_refreshes()

    def record_streaming_event(self, streaming_event):
        """Record incoming streaming event."""
        self._streaming_events.record_streaming_event(streaming_event)

    def record_session_length(self, session):
        """Record session length."""
        self._counters.record_session_length(session)

    def get_bur_time_outs(self):
        """Get block until ready timeout."""
        return self._tel_config.get_bur_time_outs()

    def get_non_ready_usage(self):
        """Get non-ready usage."""
        return self._tel_config.get_non_ready_usage()

    def get_config_stats(self):
        """Get all config info."""
        return self._tel_config.get_stats()

    def pop_exceptions(self):
        """Get and reset method exceptions."""
        return self._method_exceptions.pop_all()

    def pop_tags(self):
        """Get and reset tags."""
        with self._lock:
            tags = self._tags
            self._reset_tags()
            return tags

    def pop_config_tags(self):
        """Get and reset tags."""
        with self._lock:
            tags = self._config_tags
            self._reset_config_tags()
            return tags

    def pop_latencies(self):
        """Get and reset eval latencies."""
        return self._method_latencies.pop_all()

    def get_impressions_stats(self, type):
        """Get impressions stats"""
        return self._counters.get_counter_stats(type)

    def get_events_stats(self, type):
        """Get events stats"""
        return self._counters.get_counter_stats(type)

    def get_last_synchronization(self):
        """Get last sync"""
        return self._last_synchronization.get_all()

    def pop_http_errors(self):
        """Get and reset http errors."""
        return self._http_sync_errors.pop_all()

    def pop_http_latencies(self):
        """Get and reset http latencies."""
        return self._http_latencies.pop_all()

    def pop_auth_rejections(self):
        """Get and reset auth rejections."""
        return self._counters.pop_auth_rejections()

    def pop_token_refreshes(self):
        """Get and reset token refreshes."""
        return self._counters.pop_token_refreshes()

    def pop_streaming_events(self):
        return self._streaming_events.pop_streaming_events()

    def get_session_length(self):
        """Get session length"""
        return self._counters.get_session_length()


class InMemoryTelemetryStorageAsync(InMemoryTelemetryStorageBase):
    """In-memory telemetry async storage."""

    async def create():
        """Constructor"""
        self = InMemoryTelemetryStorageAsync()
        self._lock = asyncio.Lock()
        self._method_exceptions = await MethodExceptionsAsync.create()
        self._last_synchronization = await LastSynchronizationAsync.create()
        self._counters = await TelemetryCountersAsync.create()
        self._http_sync_errors = await HTTPErrorsAsync.create()
        self._method_latencies = await MethodLatenciesAsync.create()
        self._http_latencies = await HTTPLatenciesAsync.create()
        self._streaming_events = await StreamingEventsAsync.create()
        self._tel_config = await TelemetryConfigAsync.create()
        async with self._lock:
            self._reset_tags()
            self._reset_config_tags()
        return self

    async def record_config(self, config, extra_config):
        """Record configurations."""
        await self._tel_config.record_config(config, extra_config)

    async def record_active_and_redundant_factories(self, active_factory_count, redundant_factory_count):
        """Record active and redundant factories."""
        await self._tel_config.record_active_and_redundant_factories(active_factory_count, redundant_factory_count)

    async def record_ready_time(self, ready_time):
        """Record ready time."""
        await self._tel_config.record_ready_time(ready_time)

    async def add_tag(self, tag):
        """Record tag string."""
        async with self._lock:
            if len(self._tags) < MAX_TAGS:
                self._tags.append(tag)

    async def add_config_tag(self, tag):
        """Record tag string."""
        async with self._lock:
            if len(self._config_tags) < MAX_TAGS:
                self._config_tags.append(tag)

    async def record_bur_time_out(self):
        """Record block until ready timeout."""
        await self._tel_config.record_bur_time_out()

    async def record_not_ready_usage(self):
        """record non-ready usage."""
        await self._tel_config.record_not_ready_usage()

    async def record_latency(self, method, latency):
        """Record method latency time."""
        await self._method_latencies.add_latency(method,latency)

    async def record_exception(self, method):
        """Record method exception."""
        await self._method_exceptions.add_exception(method)

    async def record_impression_stats(self, data_type, count):
        """Record impressions stats."""
        await self._counters.record_impressions_value(data_type, count)

    async def record_event_stats(self, data_type, count):
        """Record events stats."""
        await self._counters.record_events_value(data_type, count)

    async def record_successful_sync(self, resource, time):
        """Record successful sync."""
        await self._last_synchronization.add_latency(resource, time)

    async def record_sync_error(self, resource, status):
        """Record sync http error."""
        await self._http_sync_errors.add_error(resource, status)

    async def record_sync_latency(self, resource, latency):
        """Record latency time."""
        await self._http_latencies.add_latency(resource, latency)

    async def record_auth_rejections(self):
        """Record auth rejection."""
        await self._counters.record_auth_rejections()

    async def record_token_refreshes(self):
        """Record sse token refresh."""
        await self._counters.record_token_refreshes()

    async def record_streaming_event(self, streaming_event):
        """Record incoming streaming event."""
        await self._streaming_events.record_streaming_event(streaming_event)

    async def record_session_length(self, session):
        """Record session length."""
        await self._counters.record_session_length(session)

    async def get_bur_time_outs(self):
        """Get block until ready timeout."""
        return await self._tel_config.get_bur_time_outs()

    async def get_non_ready_usage(self):
        """Get non-ready usage."""
        return await self._tel_config.get_non_ready_usage()

    async def get_config_stats(self):
        """Get all config info."""
        return await self._tel_config.get_stats()

    async def pop_exceptions(self):
        """Get and reset method exceptions."""
        return await self._method_exceptions.pop_all()

    async def pop_tags(self):
        """Get and reset tags."""
        async with self._lock:
            tags = self._tags
            self._reset_tags()
            return tags

    async def pop_config_tags(self):
        """Get and reset tags."""
        async with self._lock:
            tags = self._config_tags
            self._reset_config_tags()
            return tags

    async def pop_latencies(self):
        """Get and reset eval latencies."""
        return await self._method_latencies.pop_all()

    async def get_impressions_stats(self, type):
        """Get impressions stats"""
        return await self._counters.get_counter_stats(type)

    async def get_events_stats(self, type):
        """Get events stats"""
        return await self._counters.get_counter_stats(type)

    async def get_last_synchronization(self):
        """Get last sync"""
        return await self._last_synchronization.get_all()

    async def pop_http_errors(self):
        """Get and reset http errors."""
        return await self._http_sync_errors.pop_all()

    async def pop_http_latencies(self):
        """Get and reset http latencies."""
        return await self._http_latencies.pop_all()

    async def pop_auth_rejections(self):
        """Get and reset auth rejections."""
        return await self._counters.pop_auth_rejections()

    async def pop_token_refreshes(self):
        """Get and reset token refreshes."""
        return await self._counters.pop_token_refreshes()

    async def pop_streaming_events(self):
        return await self._streaming_events.pop_streaming_events()

    async def get_session_length(self):
        """Get session length"""
        return await self._counters.get_session_length()


class LocalhostTelemetryStorage():
    """Localhost telemetry storage."""
    def do_nothing(*_, **__):
        return {}

    def __getattr__(self, _):
        return self.do_nothing