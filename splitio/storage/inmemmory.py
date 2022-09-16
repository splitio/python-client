"""In memory storage classes."""
import logging
import threading
import queue
from collections import Counter
import os

from splitio.models.segments import Segment
from splitio.storage import SplitStorage, SegmentStorage, ImpressionStorage, EventStorage, TelemetryStorage

MAX_SIZE_BYTES = 5 * 1024 * 1024
MAX_LATENCY_BUCKET_COUNT = 23
MAX_STREAMING_EVENTS = 20
MAX_TAGS = 10

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
                _LOGGER.warning(
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
                total_count = total_count + len(segment)
        return total_count


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

class InMemoryTelemetryStorage(TelemetryStorage):
    """In-memory telemetry storage."""

    def __init__(self):
        """Constructor"""
        self._counters = {'iQ': 0, 'iDe': 0, 'iDr': 0, 'eQ': 0, 'eD': 0, 'sL': 0,
                        'aR': 0, 'tR': 0}
        self._latencies = {'mL': {'t': [], 'ts': [], 'tc': [], 'tcs': [], 'tr': []},
                           'hL': {'sp': [], 'se': [], 'ms': [], 'im': [], 'ic': [], 'ev': [], 'te': [], 'to': []}}
        self._exceptions = {'mE': {'t': 0, 'ts': 0, 'tc': 0, 'tcs': 0, 'tr': 0}}
        self._records = {'IS': {'sp': 0, 'se': 0, 'ms': 0, 'im': 0, 'ic': 0, 'ev': 0, 'te': 0, 'to': 0},
                         'sL': 0}
        self._http_errors = {'sp': {}, 'se': {}, 'ms': {}, 'im': {}, 'ic': {}, 'ev': {}, 'te': {}, 'to': {}}
        self._streaming_events = []
        self._tags = []
        self._integrations = {}
        self._config = {'bT':0, 'nR':0, 'uC': 0}
        self._map_latencies = {'Treatment': 't', 'Treatments': 'ts', 'TreatmentWithConfig': 'tc', 'TreatmentsWithConfig': 'tcs', 'Track': 'tr'}

    def record_config(self, config):
        """Record configurations."""
        self._config['oM'] = self._get_operation_mode(config['operationMode'])
        self._config['st'] = self._get_storage_type(config['operationMode'])
        self._config['sE'] = config['streamingEnabled']
        self._config['rR'] = self._get_refresh_rates(config)
        self._config['uO'] = self._get_url_overrides(config)
        self._config['iQ'] = config['impressionsQueueSize']
        self._config['eQ'] = config['eventsQueueSize']
        self._config['iM'] = self._get_impressions_mode(config['impressionsMode'])
        self._config['iL'] = True if config['impressionListener'] is not None else False
        self._config['hp'] = self._check_if_proxy_detected()
        self._config['aF'] = config['activeFactoryCount']
        self._config['rF'] = config['redundantFactoryCount']

    def record_ready_time(self, ready_time):
        """Record ready time."""
        self._config['tR'] = ready_time

    def add_tag(self, tag):
        """Record tag string."""
        if len(self._tags) <= MAX_TAGS:
            self._tags.append(tag)

    def record_bur_timeout(self):
        """Record block until ready timeout."""
        self._config['bT'] = self._config['bT'] + 1

    def record_non_ready_usage(self):
        """record non-ready usage."""
        self._config['nR'] = self._config['nR'] + 1

    def record_latency(self, method, latency):
        """Record method latency time."""
        if self._latencies['mL'][self._map_latencies[method]] < MAX_LATENCY_BUCKET_COUNT:
            self._latencies['mL'][self._map_latencies[method]].append(latency)

    def record_exceptions(self, method):
        """Record method exception."""
        self._exceptions['mE'][self._map_latencies[method]] = self._exceptions['mE'][self._map_latencies[method]] + 1

    def record_impression_stats(self, data_type, count):
        """Record impressions stats."""
        self._counters[data_type] = self._counters[data_type] + count

    def record_event_stats(self, data_type, count):
        """Record events stats."""
        self._counters[data_type] = self._counters[data_type] + count

    def record_suceessful_sync(self, resource, time):
        """Record successful sync."""
        self._records['IS'][resource] = time

    def record_sync_error(self, resource, status):
        """Record sync http error."""
        self._http_errors[resource][status] = self._http_errors[resource][status] + 1

    def record_sync_latency(self, resource, latency):
        """Record latency time."""
        if self._latencies['hL'][self._map_latencies[resource]] < MAX_LATENCY_BUCKET_COUNT:
            self._latencies['hL'][self._map_latencies[resource]].append(latency)

    def record_auth_rejections(self):
        """Record auth rejection."""
        self._counters['aR'] = self._counters['aR'] + 1

    def record_token_refreshes(self):
        """Record sse token refresh."""
        self._counters['tR'] = self._counters['tR'] + 1

    def record_streaming_event(self, streaming_event):
        """Record incoming streaming event."""
        if len(self._streaming_events) < MAX_STREAMING_EVENTS:
            self._streaming_events.append({'e': streaming_event.type, 'd': streaming_event.data, 't': streaming_event.time})

    def record_session_length(self, session):
        """Record session length."""
        self._records['sL'] = session

    def get_bur_timeouts(self):
        """Get block until ready timeout."""
        return self._config['bT']

    def get_non_ready_usage(self):
        """Get non-ready usage."""
        return self._config['nR']

    def get_config_stats(self):
        """Get all config info."""
        return self._config

    def pop_exceptions(self):
        """Get and reset method exceptions."""
        exceptions = self._exceptions['mE']
        self._exceptions = {'mE': {'t': 0, 'ts': 0, 'tc': 0, 'tcs': 0, 'tr': 0}}
        return exceptions

    def pop_tags(self):
        """Get and reset tags."""
        tags = self._tags
        self._tags = []
        return tags

    def pop_latencies(self):
        """Get and reset eval latencies."""
        latencies = self._latencies['mL']
        self._latencies['mL'] =  {'t': [], 'ts': [], 'tc': [], 'tcs': [], 'tr': []}
        return latencies

    def get_impressions_stats(self, type):
        """Get impressions stats"""
        return self._counters[type]

    def get_events_stats(self, type):
        """Get events stats"""
        return self._counters[type]

    def get_last_synchronization(self):
        """Get last sync"""
        return self._records['IS']

    def pop_http_errors(self):
        """Get and reset http errors."""
        https_errors = self._http_errors
        self._http_errors = {'sp': {}, 'se': {}, 'ms': {}, 'im': {}, 'ic': {}, 'ev': {}, 'te': {}, 'to': {}}
        return https_errors

    def pop_http_latencies(self):
        """Get and reset http latencies."""
        latencies = self._latencies['hL']
        self._latencies['hL'] = {'sp': [], 'se': [], 'ms': [], 'im': [], 'ic': [], 'ev': [], 'te': [], 'to': []}
        return latencies

    def pop_auth_rejections(self):
        """Get and reset auth rejections."""
        auth_rejections = self._counters['aR']
        self._counters['aR'] = 0
        return auth_rejections

    def pop_token_refreshes(self):
        """Get and reset token refreshes."""
        token_refreshes = self._counters['tR']
        self._counters['tR'] = 0
        return token_refreshes

    def pop_streaming_events(self):
        """Get and reset streaming events."""
        streaming_events = self._streaming_events
        self._streaming_events = []
        return streaming_events

    def get_session_length(self):
        """Get session length"""
        return self._records['sL']

    def _get_operation_mode(self, op_mode):
        if 'in-memory' in op_mode:
           return 0
        elif op_mode == 'redis-consumer':
           return 1
        else:
           return 2

    def _get_storage_type(self, op_mode):
        if 'in-memory' in op_mode:
           return 'memory'
        elif 'redis' in op_mode:
           return 'redis'
        else:
           return 'localstorage'

    def _get_refresh_rates(self, config):
        rr = {}
        rr['sp'] == config['featuresRefreshRate']
        rr['se'] == config['segmentsRefreshRate']
        rr['im'] == config['impressionsRefreshRate']
        rr['ev'] == config['eventsPushRate']
        rr['te'] == config['metrcsRefreshRate']
        return rr

    def _get_url_overrides(self, config):
        rr = {}
        rr['s'] == True if config['sdk_url'] is not None else False
        rr['e'] == True if config['events_url'] is not None else False
        rr['a'] == True if config['auth_url'] is not None else False
        rr['st'] == True if config['streaming_url'] is not None else False
        rr['t'] == True if config['telemetry_url'] is not None else False
        return rr

    def _get_impressions_mode(self, imp_mode):
        if imp_mode == 'DEBUG':
           return 1
        elif imp_mode == 'OPTIMIZED':
           return 0
        else:
            return 3

    def _check_if_proxy_detected(self):
        for x in os.environ:
            if 'https_proxy' in os.getenv(x):
                return True
        return False