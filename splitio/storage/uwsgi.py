"""UWSGI Cache based storages implementation module."""
import logging
import json

from splitio.storage import SplitStorage, SegmentStorage, ImpressionStorage, EventStorage, \
    TelemetryStorage
from splitio.models import splits, segments
from splitio.models.impressions import Impression
from splitio.models.events import Event
from splitio.storage.adapters.uwsgi_cache import _SPLITIO_CHANGE_NUMBERS, \
    _SPLITIO_EVENTS_CACHE_NAMESPACE, _SPLITIO_IMPRESSIONS_CACHE_NAMESPACE, \
     _SPLITIO_METRICS_CACHE_NAMESPACE, _SPLITIO_MISC_NAMESPACE, UWSGILock, \
    _SPLITIO_SEGMENTS_CACHE_NAMESPACE, _SPLITIO_SPLITS_CACHE_NAMESPACE, \
    _SPLITIO_LOCK_CACHE_NAMESPACE


class UWSGISplitStorage(SplitStorage):
    """UWSGI-Cache based implementation of a split storage."""

    _KEY_TEMPLATE = 'split.{suffix}'
    _KEY_TILL = 'splits.till'
    _KEY_FEATURE_LIST_LOCK = 'splits.list.lock'
    _KEY_FEATURE_LIST = 'splits.list'
    _OVERWRITE_LOCK_SECONDS = 5

    def __init__(self, uwsgi_entrypoint):
        """
        Class constructor.

        :param uwsgi_entrypoint: UWSGI module. Can be the actual module or a mock.
        :type uwsgi_entrypoint: module
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._uwsgi = uwsgi_entrypoint

    def get(self, split_name):
        """
        Retrieve a split.

        :param split_name: Name of the feature to fetch.
        :type split_name: str

        :rtype: str
        """
        raw = self._uwsgi.cache_get(
            self._KEY_TEMPLATE.format(suffix=split_name),
            _SPLITIO_SPLITS_CACHE_NAMESPACE
        )
        to_return = splits.from_raw(json.loads(raw)) if raw is not None else None
        if not to_return:
            self._logger.warning("Trying to retrieve nonexistant split %s. Ignoring.", split_name)
        return to_return

    def put(self, split):
        """
        Store a split.

        :param split: Split object to store
        :type split: splitio.models.splits.Split
        """
        self._uwsgi.cache_update(
            self._KEY_TEMPLATE.format(suffix=split.name),
            json.dumps(split.to_json()),
            0,
            _SPLITIO_SPLITS_CACHE_NAMESPACE
        )

        with UWSGILock(self._uwsgi, self._KEY_FEATURE_LIST_LOCK):
            try:
                current = set(json.loads(
                    self._uwsgi.cache_get(self._KEY_FEATURE_LIST, _SPLITIO_MISC_NAMESPACE)
                ))
            except TypeError:
                current = set()
            current.add(split.name)
            self._uwsgi.cache_update(
                self._KEY_FEATURE_LIST,
                json.dumps(list(current)),
                0,
                _SPLITIO_MISC_NAMESPACE
            )

    def remove(self, split_name):
        """
        Remove a split from storage.

        :param split_name: Name of the feature to remove.
        :type split_name: str

        :return: True if the split was found and removed. False otherwise.
        :rtype: bool
        """
        with UWSGILock(self._uwsgi, self._KEY_FEATURE_LIST_LOCK):
            try:
                current = set(json.loads(
                    self._uwsgi.cache_get(self._KEY_FEATURE_LIST, _SPLITIO_MISC_NAMESPACE)
                ))
                current.remove(split_name)
                self._uwsgi.cache_update(
                    self._KEY_FEATURE_LIST,
                    json.dumps(list(current)),
                    0,
                    _SPLITIO_MISC_NAMESPACE
                )
            except TypeError:
                # Split list not found, no need to delete anything
                pass
            except KeyError:
                # Split not found in list. nothing to do.
                pass

        result = self._uwsgi.cache_del(
            self._KEY_TEMPLATE.format(suffix=split_name),
            _SPLITIO_SPLITS_CACHE_NAMESPACE
        )
        if not result is False:
            self._logger.warning("Trying to retrieve nonexistant split %s. Ignoring.", split_name)
        return result

    def get_change_number(self):
        """
        Retrieve latest split change number.

        :rtype: int
        """
        try:
            return json.loads(self._uwsgi.cache_get(self._KEY_TILL, _SPLITIO_CHANGE_NUMBERS))
        except TypeError:
            return None

    def set_change_number(self, new_change_number):
        """
        Set the latest change number.

        :param new_change_number: New change number.
        :type new_change_number: int
        """
        self._uwsgi.cache_update(self._KEY_TILL, str(new_change_number), 0, _SPLITIO_CHANGE_NUMBERS)

    def get_split_names(self):
        """
        Return a list of all the split names.

        :return: List of split names in cache.
        :rtype: list(str)
        """
        if self._uwsgi.cache_exists(self._KEY_FEATURE_LIST, _SPLITIO_MISC_NAMESPACE):
            try:
                return json.loads(
                    self._uwsgi.cache_get(self._KEY_FEATURE_LIST, _SPLITIO_MISC_NAMESPACE)
                )
            except TypeError: # Thrown by json.loads when passing none
                pass # Fall back to default return statement (empty list)
        return []

    def get_all_splits(self):
        """
        Return a list of all splits in cache.

        :return: List of splits.
        :rtype: list(splitio.models.splits.Split)
        """
        return [self.get(split_name) for split_name in self.get_split_names()]


class UWSGISegmentStorage(SegmentStorage):
    """UWSGI-Cache based implementation of a split storage."""

    _KEY_TEMPLATE = 'segments.{suffix}'
    _SEGMENT_DATA_KEY_TEMPLATE = 'segmentData.{segment_name}'
    _SEGMENT_CHANGE_NUMBER_KEY_TEMPLATE = 'segment.{segment_name}.till'

    def __init__(self, uwsgi_entrypoint):
        """
        Class constructor.

        :param uwsgi_entrypoint: UWSGI module. Can be the actual module or a mock.
        :type uwsgi_entrypoint: module
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._uwsgi = uwsgi_entrypoint

    def get(self, segment_name):
        """
        Retrieve a segment.

        :param segment_name: Name of the segment to fetch.
        :type segment_name: str

        :return: Parsed segment if present. None otherwise.
        :rtype: splitio.models.segments.Segment
        """
        key = self._SEGMENT_DATA_KEY_TEMPLATE.format(segment_name=segment_name)
        cn_key = self._SEGMENT_CHANGE_NUMBER_KEY_TEMPLATE.format(segment_name=segment_name)
        try:
            segment_data = json.loads(self._uwsgi.cache_get(key, _SPLITIO_SEGMENTS_CACHE_NAMESPACE))
            change_number = json.loads(self._uwsgi.cache_get(cn_key, _SPLITIO_CHANGE_NUMBERS))
            return segments.from_raw({
                'name': segment_name,
                'added': segment_data,
                'removed': [],
                'till': change_number
            })
        except TypeError:
            self._logger.warning(
                "Trying to retrieve nonexistant segment %s. Ignoring.",
                segment_name
            )
            return None

    def update(self, segment_name, to_add, to_remove, change_number=None):
        """
        Update a segment.

        :param segment_name: Name of the segment to update.
        :type segment_name: str
        :param to_add: List of members to add to the segment.
        :type to_add: list
        :param to_remove: List of members to remove from the segment.
        :type to_remove: list
        """
        key = self._SEGMENT_DATA_KEY_TEMPLATE.format(segment_name=segment_name)
        try:
            segment_data = json.loads(self._uwsgi.cache_get(key, _SPLITIO_SEGMENTS_CACHE_NAMESPACE))
        except TypeError:
            segment_data = []
        updated = set(segment_data).union(set(to_add)).difference(to_remove)
        self._uwsgi.cache_update(
            key,
            json.dumps(list(updated)),
            0,
            _SPLITIO_SEGMENTS_CACHE_NAMESPACE
        )

        if change_number is not None:
            self.set_change_number(segment_name, change_number)

    def put(self, segment):
        """
        Put a new segment in storage.

        :param segment: Segment to store.
        :type segment: splitio.models.segments.Segent
        """
        key = self._SEGMENT_DATA_KEY_TEMPLATE.format(segment_name=segment.name)
        self._uwsgi.cache_update(
            key,
            json.dumps(list(segment.keys)),
            0,
            _SPLITIO_SEGMENTS_CACHE_NAMESPACE
        )
        self.set_change_number(segment.name, segment.change_number)


    def get_change_number(self, segment_name):
        """
        Retrieve latest change number for a segment.

        :param segment_name: Name of the segment.
        :type segment_name: str

        :rtype: int
        """
        cnkey = self._SEGMENT_CHANGE_NUMBER_KEY_TEMPLATE.format(segment_name=segment_name)
        try:
            return json.loads(self._uwsgi.cache_get(cnkey, _SPLITIO_CHANGE_NUMBERS))

        except TypeError:
            return None

    def set_change_number(self, segment_name, new_change_number):
        """
        Set the latest change number.

        :param segment_name: Name of the segment.
        :type segment_name: str
        :param new_change_number: New change number.
        :type new_change_number: int
        """
        cn_key = self._SEGMENT_CHANGE_NUMBER_KEY_TEMPLATE.format(segment_name=segment_name)
        self._uwsgi.cache_update(cn_key, json.dumps(new_change_number), 0, _SPLITIO_CHANGE_NUMBERS)

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
        segment = self.get(segment_name)
        return segment.contains(key)


class UWSGIImpressionStorage(ImpressionStorage):
    """Impressions storage interface."""

    _IMPRESSIONS_KEY = 'SPLITIO.impressions.'
    _LOCK_IMPRESSION_KEY = 'SPLITIO.impressions_lock'
    _IMPRESSIONS_FLUSH = 'SPLITIO.impressions_flush'
    _OVERWRITE_LOCK_SECONDS = 5

    def __init__(self, adapter):
        """
        Class Constructor.

        :param adapter: UWSGI Adapter/Emulator/Module.
        :type: object
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._uwsgi = adapter

    def put(self, impressions):
        """
        Put one or more impressions in storage.

        :param impressions: List of one or more impressions to store.
        :type impressions: list
        """
        to_store = [i._asdict() for i in impressions]
        with UWSGILock(self._uwsgi, self._LOCK_IMPRESSION_KEY):
            try:
                current = json.loads(self._uwsgi.cache_get(
                    self._IMPRESSIONS_KEY, _SPLITIO_IMPRESSIONS_CACHE_NAMESPACE
                ))
            except TypeError:
                current = []

            self._uwsgi.cache_update(
                self._IMPRESSIONS_KEY,
                json.dumps(current + to_store),
                0,
                _SPLITIO_IMPRESSIONS_CACHE_NAMESPACE
            )

    def pop_many(self, count):
        """
        Pop the oldest N impressions from storage.

        :param count: Number of impressions to pop.
        :type count: int
        """
        with UWSGILock(self._uwsgi, self._LOCK_IMPRESSION_KEY):
            try:
                current = json.loads(self._uwsgi.cache_get(
                    self._IMPRESSIONS_KEY, _SPLITIO_IMPRESSIONS_CACHE_NAMESPACE
                ))
            except TypeError:
                return []

            self._uwsgi.cache_update(
                self._IMPRESSIONS_KEY,
                json.dumps(current[count:]),
                0,
                _SPLITIO_IMPRESSIONS_CACHE_NAMESPACE
            )

        return [
            Impression(
                impression['matching_key'],
                impression['feature_name'],
                impression['treatment'],
                impression['label'],
                impression['change_number'],
                impression['bucketing_key'],
                impression['time']
            ) for impression in current[:count]
        ]

    def request_flush(self):
        """Set a marker in the events cache to indicate that a flush has been requested."""
        self._uwsgi.cache_set(self._IMPRESSIONS_FLUSH, 'ok', 0, _SPLITIO_LOCK_CACHE_NAMESPACE)

    def should_flush(self):
        """
        Return True if a flush has been requested.

        :return: Whether a flush has been requested.
        :rtype: bool
        """
        value = self._uwsgi.cache_get(self._IMPRESSIONS_FLUSH, _SPLITIO_LOCK_CACHE_NAMESPACE)
        return True if value is not None else False

    def acknowledge_flush(self):
        """Acknowledge that a flush has been requested."""
        self._uwsgi.cache_del(self._IMPRESSIONS_FLUSH, _SPLITIO_LOCK_CACHE_NAMESPACE)


class UWSGIEventStorage(EventStorage):
    """Events storage interface."""

    _EVENTS_KEY = 'events'
    _LOCK_EVENTS_KEY = 'events_lock'
    _EVENTS_FLUSH = 'events_flush'
    _OVERWRITE_LOCK_SECONDS = 5

    def __init__(self, adapter):
        """
        Class Constructor.

        :param adapter: UWSGI Adapter/Emulator/Module.
        :type: object
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._uwsgi = adapter

    def put(self, events):
        """
        Put one or more events in storage.

        :param events: List of one or more events to store.
        :type events: list
        """
        with UWSGILock(self._uwsgi, self._LOCK_EVENTS_KEY):
            try:
                current = json.loads(self._uwsgi.cache_get(
                    self._EVENTS_KEY, _SPLITIO_EVENTS_CACHE_NAMESPACE
                ))
            except TypeError:
                current = []
            self._uwsgi.cache_update(
                self._EVENTS_KEY,
                json.dumps(current + [e._asdict() for e in events]),
                0,
                _SPLITIO_EVENTS_CACHE_NAMESPACE
            )

    def pop_many(self, count):
        """
        Pop the oldest N events from storage.

        :param count: Number of events to pop.
        :type count: int
        """
        with UWSGILock(self._uwsgi, self._LOCK_EVENTS_KEY):
            try:
                current = json.loads(self._uwsgi.cache_get(
                    self._EVENTS_KEY, _SPLITIO_EVENTS_CACHE_NAMESPACE
                ))
            except TypeError:
                return []

            self._uwsgi.cache_update(
                self._EVENTS_KEY,
                json.dumps(current[count:]),
                0,
                _SPLITIO_EVENTS_CACHE_NAMESPACE
            )

        return [
            Event(
                event['key'],
                event['traffic_type_name'],
                event['event_type_id'],
                event['value'],
                event['timestamp']
            )   for event in current[:count]
        ]

    def request_flush(self):
        """Set a marker in the events cache to indicate that a flush has been requested."""
        self._uwsgi.cache_set(self._EVENTS_FLUSH, 'requested', 0, _SPLITIO_LOCK_CACHE_NAMESPACE)

    def should_flush(self):
        """
        Return True if a flush has been requested.

        :return: Whether a flush has been requested.
        :rtype: bool
        """
        value = self._uwsgi.cache_get(self._EVENTS_FLUSH, _SPLITIO_LOCK_CACHE_NAMESPACE)
        return True if value is not None else False

    def acknowledge_flush(self):
        """Acknowledge that a flush has been requested."""
        self._uwsgi.cache_del(self._EVENTS_FLUSH, _SPLITIO_LOCK_CACHE_NAMESPACE)


class UWSGITelemetryStorage(TelemetryStorage):
    """Telemetry storage interface."""

    _LATENCIES_KEY = 'SPLITIO.latencies'
    _GAUGES_KEY = 'SPLITIO.gauges'
    _COUNTERS_KEY = 'SPLITIO.counters'

    _LATENCIES_LOCK_KEY = 'SPLITIO.latencies.lock'
    _GAUGES_LOCK_KEY = 'SPLITIO.gauges.lock'
    _COUNTERS_LOCK_KEY = 'SPLITIO.counters.lock'

    def __init__(self, uwsgi_entrypoint):
        """
        Class constructor.

        :param uwsgi_entrypoint: uwsgi module/emulator
        :type uwsgi_entrypoint: object
        """
        self._uwsgi = uwsgi_entrypoint
        self._logger = logging.getLogger(self.__class__.__name__)


    def inc_latency(self, name, bucket):
        """
        Add a latency.

        :param name: Name of the latency metric.
        :type name: str
        :param value: Value of the latency metric.
        :tyoe value: int
        """
        if not 0 <= bucket <= 21:
            self._logger.error('Incorect bucket "%d" for latency "%s". Ignoring.', bucket, name)
            return

        with UWSGILock(self._uwsgi, self._LATENCIES_LOCK_KEY):
            latencies_raw = self._uwsgi.cache_get(self._LATENCIES_KEY, _SPLITIO_METRICS_CACHE_NAMESPACE)
            latencies = json.loads(latencies_raw) if latencies_raw else {}
            to_update = latencies.get(name, [0] * 22)
            to_update[bucket] += 1
            latencies[name] = to_update
            self._uwsgi.cache_set(
                self._LATENCIES_KEY,
                json.dumps(latencies),
                0,
                _SPLITIO_METRICS_CACHE_NAMESPACE
            )

    def inc_counter(self, name):
        """
        Increment a counter.

        :param name: Name of the counter metric.
        :type name: str
        """
        with UWSGILock(self._uwsgi, self._COUNTERS_LOCK_KEY):
            counters_raw = self._uwsgi.cache_get(self._COUNTERS_KEY, _SPLITIO_METRICS_CACHE_NAMESPACE)
            counters = json.loads(counters_raw) if counters_raw else {}
            value = counters.get(name, 0)
            value += 1
            counters[name] = value
            self._uwsgi.cache_set(
                self._COUNTERS_KEY,
                json.dumps(counters),
                0,
                _SPLITIO_METRICS_CACHE_NAMESPACE
            )

    def put_gauge(self, name, value):
        """
        Add a gauge metric.

        :param name: Name of the gauge metric.
        :type name: str
        :param value: Value of the gauge metric.
        :type value: int
        """
        with UWSGILock(self._uwsgi, self._GAUGES_LOCK_KEY):
            gauges_raw = self._uwsgi.cache_get(self._GAUGES_KEY, _SPLITIO_METRICS_CACHE_NAMESPACE)
            gauges = json.loads(gauges_raw) if gauges_raw else {}
            gauges[name] = value
            self._uwsgi.cache_set(
                self._GAUGES_KEY,
                json.dumps(gauges),
                0,
                _SPLITIO_METRICS_CACHE_NAMESPACE
            )

    def pop_counters(self):
        """
        Get all the counters.

        :rtype: list
        """
        with UWSGILock(self._uwsgi, self._COUNTERS_LOCK_KEY):
            counters_raw = self._uwsgi.cache_get(self._COUNTERS_KEY, _SPLITIO_METRICS_CACHE_NAMESPACE)
            self._uwsgi.cache_del(self._COUNTERS_KEY, _SPLITIO_METRICS_CACHE_NAMESPACE)
            return json.loads(counters_raw) if counters_raw else {}

    def pop_gauges(self):
        """
        Get all the gauges.

        :rtype: list

        """
        with UWSGILock(self._uwsgi, self._GAUGES_LOCK_KEY):
            gauges_raw = self._uwsgi.cache_get(self._GAUGES_KEY, _SPLITIO_METRICS_CACHE_NAMESPACE)
            self._uwsgi.cache_del(self._GAUGES_KEY, _SPLITIO_METRICS_CACHE_NAMESPACE)
            return json.loads(gauges_raw) if gauges_raw else {}

    def pop_latencies(self):
        """
        Get all latencies.

        :rtype: list
        """
        with UWSGILock(self._uwsgi, self._LATENCIES_LOCK_KEY):
            latencies_raw = self._uwsgi.cache_get(self._LATENCIES_KEY, _SPLITIO_METRICS_CACHE_NAMESPACE)
            self._uwsgi.cache_del(self._LATENCIES_KEY, _SPLITIO_METRICS_CACHE_NAMESPACE)
            return json.loads(latencies_raw) if latencies_raw else {}
