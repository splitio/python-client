"""In memory storage classes."""
import logging
import threading
import queue
from collections import Counter

from splitio.models.segments import Segment
from splitio.models.telemetry import HTTPErrors, HTTPLatencies, MethodExceptions, MethodLatencies, LastSynchronization, StreamingEvents, TelemetryConfig, TelemetryCounters, CounterConstants
from splitio.storage import FlagSetsFilter
from splitio.storage import SplitStorage, SegmentStorage, ImpressionStorage, EventStorage, TelemetryStorage

MAX_SIZE_BYTES = 5 * 1024 * 1024
MAX_TAGS = 10

_LOGGER = logging.getLogger(__name__)

class FlagSets(object):
    """InMemory Flagsets storage."""

    def __init__(self, flag_sets=[]):
        """Constructor."""
        self._lock = threading.RLock()
        self.sets_feature_flag_map = {}
        for flag_set in flag_sets:
            self.sets_feature_flag_map[flag_set] = set()

    def flag_set_exist(self, flag_set):
        """
        Check if a flagset exist in stored flagset

        :param flag_set: set name
        :type flag_set: str

        :rtype: bool
        """
        with self._lock:
            return flag_set in self.sets_feature_flag_map.keys()

    def get_flag_set(self, flag_set):
        """
        fetch feature flags stored in a flag set

        :param flag_set: set name
        :type flag_set: str

        :rtype: list(str)
        """
        with self._lock:
            return self.sets_feature_flag_map.get(flag_set)

    def add_flag_set(self, flag_set):
        """
        Add new flag set to storage

        :param flag_set: set name
        :type flag_set: str
        """
        with self._lock:
            if not self.flag_set_exist(flag_set):
                self.sets_feature_flag_map[flag_set] = set()

    def remove_flag_set(self, flag_set):
        """
        Remove existing flag set from storage

        :param flag_set: set name
        :type flag_set: str
        """
        with self._lock:
            if self.flag_set_exist(flag_set):
                del self.sets_feature_flag_map[flag_set]

    def add_feature_flag_to_flag_set(self, flag_set, feature_flag):
        """
        Add a feature flag to existing flag set

        :param flag_set: set name
        :type flag_set: str
        :param feature_flag: feature flag name
        :type feature_flag: str
        """
        with self._lock:
            if self.flag_set_exist(flag_set):
                self.sets_feature_flag_map[flag_set].add(feature_flag)

    def remove_feature_flag_to_flag_set(self, flag_set, feature_flag):
        """
        Remove a feature flag from existing flag set

        :param flag_set: set name
        :type flag_set: str
        :param feature_flag: feature flag name
        :type feature_flag: str
        """
        with self._lock:
            if self.flag_set_exist(flag_set):
                self.sets_feature_flag_map[flag_set].remove(feature_flag)


class InMemorySplitStorage(SplitStorage):
    """InMemory implementation of a feature flag storage."""

    def __init__(self, flag_sets=[]):
        """Constructor."""
        self._lock = threading.RLock()
        self._splits = {}
        self._change_number = -1
        self._traffic_types = Counter()
        self.flag_set = FlagSets(flag_sets)
        self.flag_set_filter = FlagSetsFilter(flag_sets)

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

    def update(self, to_add, to_delete, new_change_number):
        """
        Update feature flag storage.

        :param to_add: List of feature flags to add
        :type to_add: list[splitio.models.splits.Split]
        :param to_delete: List of feature flags to delete
        :type to_delete: list[str]
        :param new_change_number: New change number.
        :type new_change_number: int
        """
        [self._put(add_split) for add_split in to_add]
        [self._remove(delete_split) for delete_split in to_delete]
        self._set_change_number(new_change_number)

    def _put(self, split):
        """
        Store a split.

        :param split: Split object.
        :type split: splitio.models.split.Split
        """
        with self._lock:
            if split.name in self._splits:
                self._remove_from_flag_sets(self._splits[split.name])
                self._decrease_traffic_type_count(self._splits[split.name].traffic_type_name)
            self._splits[split.name] = split
            self._increase_traffic_type_count(split.traffic_type_name)
            if split.sets is not None:
                for flag_set in split.sets:
                    if not self.flag_set.flag_set_exist(flag_set):
                        if self.flag_set_filter.should_filter:
                            continue
                        self.flag_set.add_flag_set(flag_set)
                    self.flag_set.add_feature_flag_to_flag_set(flag_set, split.name)

    def _remove(self, split_name):
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
            self._remove_from_flag_sets(split)
            return True

    def _remove_from_flag_sets(self, feature_flag):
        """
        Remove flag sets associated to a split

        :param feature_flag: feature flag object
        :type feature_flag: splitio.models.splits.Split
        """
        if feature_flag.sets is not None:
            for flag_set in feature_flag.sets:
                self.flag_set.remove_feature_flag_to_flag_set(flag_set, feature_flag.name)
                if self.is_flag_set_exist(flag_set) and len(self.flag_set.get_flag_set(flag_set)) == 0 and not self.flag_set_filter.should_filter:
                    self.flag_set.remove_flag_set(flag_set)

    def get_feature_flags_by_sets(self, sets):
        """
        Get list of feature flag names associated to a set, if it does not exist will return empty list

        :param set: flag set
        :type set: str

        :return: list of feature flag names
        :rtype: list
        """
        with self._lock:
            sets_to_fetch = []
            for flag_set in sets:
                if not self.flag_set.flag_set_exist(flag_set):
                    _LOGGER.warning("Flag set %s is not part of the configured flag set list, ignoring it." % (flag_set))
                    continue
                sets_to_fetch.append(flag_set)

            to_return = set()
            [to_return.update(self.flag_set.get_flag_set(flag_set)) for flag_set in sets_to_fetch]
            return list(to_return)

    def get_change_number(self):
        """
        Retrieve latest feature flag change number.

        :rtype: int
        """
        with self._lock:
            return self._change_number

    def _set_change_number(self, new_change_number):
        """
        Set the latest change number.

        :param new_change_number: New change number.
        :type new_change_number: int
        """
        with self._lock:
            self._change_number = new_change_number

    def get_split_names(self):
        """
        Retrieve a list of all feature flag names.

        :return: List of feature flag names.
        :rtype: list(str)
        """
        with self._lock:
            return list(self._splits.keys())

    def get_all_splits(self):
        """
        Return all the feature flags.

        :return: List of all the feature flags.
        :rtype: list
        """
        with self._lock:
            return list(self._splits.values())

    def get_splits_count(self):
        """
        Return feature flags count.

        :rtype: int
        """
        with self._lock:
            return len(self._splits)

    def is_valid_traffic_type(self, traffic_type_name):
        """
        Return whether the traffic type exists in at least one feature flag in cache.

        :param traffic_type_name: Traffic type to validate.
        :type traffic_type_name: str

        :return: True if the traffic type is valid. False otherwise.
        :rtype: bool
        """
        with self._lock:
            return traffic_type_name in self._traffic_types

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
        with self._lock:
            if self.get_change_number() > change_number:
                return
            split = self._splits.get(feature_flag_name)
            if not split:
                return
            split.local_kill(default_treatment, change_number)
            self._put(split)

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

    def is_flag_set_exist(self, flag_set):
        """
        Return whether a flag set exists in at least one feature flag in cache.

        :param flag_set: Flag set to validate.
        :type flag_set: str

        :return: True if the flag_set exist. False otherwise.
        :rtype: bool
        """
        return self.flag_set.flag_set_exist(flag_set)

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


class InMemoryImpressionStorage(ImpressionStorage):
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


class InMemoryEventStorage(EventStorage):
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

class InMemoryTelemetryStorage(TelemetryStorage):
    """In-memory telemetry storage."""

    def __init__(self):
        """Constructor"""
        self._lock = threading.RLock()
        self._reset_tags()
        self._reset_config_tags()
        self._method_exceptions = MethodExceptions()
        self._last_synchronization = LastSynchronization()
        self._counters = TelemetryCounters()
        self._http_sync_errors = HTTPErrors()
        self._method_latencies = MethodLatencies()
        self._http_latencies = HTTPLatencies()
        self._streaming_events = StreamingEvents()
        self._tel_config = TelemetryConfig()

    def _reset_tags(self):
        with self._lock:
            self._tags = []

    def _reset_config_tags(self):
        with self._lock:
            self._config_tags = []

    def record_config(self, config, extra_config, total_flag_sets, invalid_flag_sets):
        """Record configurations."""
        self._tel_config.record_config(config, extra_config, total_flag_sets, invalid_flag_sets)

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

    def record_update_from_sse(self, event):
        """Record update from sse."""
        self._counters.record_update_from_sse(event)

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

    def pop_update_from_sse(self, event):
        """Get and reset update from sse."""
        return self._counters.pop_update_from_sse(event)

class LocalhostTelemetryStorage():
    """Localhost telemetry storage."""
    def do_nothing(*_, **__):
        return {}

    def __getattr__(self, _):
        return self.do_nothing