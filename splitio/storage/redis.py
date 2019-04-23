"""Redis storage module."""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import json
import logging

from splitio.models.impressions import Impression
from splitio.models import splits, segments
from splitio.storage import SplitStorage, SegmentStorage, ImpressionStorage, EventStorage
from splitio.storage.adapters.redis import RedisAdapterException


class RedisSplitStorage(SplitStorage):
    """Redis-based storage for splits."""

    _SPLIT_KEY = 'SPLITIO.split.{split_name}'
    _SPLIT_TILL_KEY = 'SPLITIO.splits.till'

    def __init__(self, redis_client):
        """
        Class constructor.

        :param redis_client: Redis client or compliant interface.
        :type redis_client: splitio.storage.adapters.redis.RedisAdapter
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._redis = redis_client

    def _get_key(self, split_name):
        """
        Use the provided split_name to build the appropriate redis key.

        :param split_name: Name of the split to interact with in redis.
        :type split_name: str

        :return: Redis key.
        :rtype: str.
        """
        return self._SPLIT_KEY.format(split_name=split_name)

    def get(self, split_name):
        """
        Retrieve a split.

        :param split_name: Name of the feature to fetch.
        :type split_name: str

        :return: A split object parsed from redis if the key exists. None otherwise
        :rtype: splitio.models.splits.Split
        """
        try:
            raw = self._redis.get(self._get_key(split_name))
            return splits.from_raw(json.loads(raw)) if raw is not None else None
        except RedisAdapterException:
            self._logger.error('Error fetching split from storage')
            self._logger.debug('Error: ', exc_info=True)
            return None

    def put(self, split):
        """
        Store a split.

        :param split: Split object to store
        :type split_name: splitio.models.splits.Split
        """
        raise NotImplementedError('Only redis-consumer mode is supported.')

    def remove(self, split_name):
        """
        Remove a split from storage.

        :param split_name: Name of the feature to remove.
        :type split_name: str

        :return: True if the split was found and removed. False otherwise.
        :rtype: bool
        """
        raise NotImplementedError('Only redis-consumer mode is supported.')

    def get_change_number(self):
        """
        Retrieve latest split change number.

        :rtype: int
        """
        try:
            stored_value = self._redis.get(self._SPLIT_TILL_KEY)
            return json.loads(stored_value) if stored_value is not None else None
        except RedisAdapterException:
            self._logger.error('Error fetching split change number from storage')
            self._logger.debug('Error: ', exc_info=True)
            return None

    def set_change_number(self, new_change_number):
        """
        Set the latest change number.

        :param new_change_number: New change number.
        :type new_change_number: int
        """
        raise NotImplementedError('Only redis-consumer mode is supported.')

    def get_split_names(self):
        """
        Retrieve a list of all split names.

        :return: List of split names.
        :rtype: list(str)
        """
        try:
            keys = self._redis.keys(self._get_key('*'))
            return [key.replace(self._get_key(''), '') for key in keys]
        except RedisAdapterException:
            self._logger.error('Error fetching split names from storage')
            self._logger.debug('Error: ', exc_info=True)
            return []

    def get_all_splits(self):
        """
        Return all the splits in cache.

        :return: List of all splits in cache.
        :rtype: list(splitio.models.splits.Split)
        """
        keys = self._redis.keys(self._get_key('*'))
        to_return = []
        try:
            raw_splits = self._redis.mget(keys)
            for raw in raw_splits:
                try:
                    to_return.append(splits.from_raw(json.loads(raw)))
                except ValueError:
                    self._logger.error('Could not parse split. Skipping')
        except RedisAdapterException:
            self._logger.error('Error fetching all splits from storage')
            self._logger.debug('Error: ', exc_info=True)
        return to_return


class RedisSegmentStorage(SegmentStorage):
    """Redis based segment storage class."""

    _SEGMENTS_KEY = 'SPLITIO.segment.{segment_name}'
    _SEGMENTS_TILL_KEY = 'SPLITIO.segment.{segment_name}.till'

    def __init__(self, redis_client):
        """
        Class constructor.

        :param redis_client: Redis client or compliant interface.
        :type redis_client: splitio.storage.adapters.redis.RedisAdapter
        """
        self._redis = redis_client
        self._logger = logging.getLogger(self.__class__.__name__)

    def _get_till_key(self, segment_name):
        """
        Use the provided segment_name to build the appropriate redis key.

        :param segment_name: Name of the segment to interact with in redis.
        :type segment_name: str

        :return: Redis key.
        :rtype: str.
        """
        return self._SEGMENTS_TILL_KEY.format(segment_name=segment_name)

    def _get_key(self, segment_name):
        """
        Use the provided segment_name to build the appropriate redis key.

        :param segment_name: Name of the segment to interact with in redis.
        :type segment_name: str

        :return: Redis key.
        :rtype: str.
        """
        return self._SEGMENTS_KEY.format(segment_name=segment_name)

    def get(self, segment_name):
        """
        Retrieve a segment.

        :param segment_name: Name of the segment to fetch.
        :type segment_name: str

        :return: Segment object is key exists. None otherwise.
        :rtype: splitio.models.segments.Segment
        """
        try:
            keys = (self._redis.smembers(self._get_key(segment_name)))
            till = self.get_change_number(segment_name)
            if not keys or till is None:
                return None
            return segments.Segment(segment_name, keys, till)
        except RedisAdapterException:
            self._logger.error('Error fetching segment from storage')
            self._logger.debug('Error: ', exc_info=True)
            return None

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
        raise NotImplementedError('Only redis-consumer mode is supported.')

    def get_change_number(self, segment_name):
        """
        Retrieve latest change number for a segment.

        :param segment_name: Name of the segment.
        :type segment_name: str

        :rtype: int
        """
        try:
            stored_value = self._redis.get(self._get_till_key(segment_name))
            return json.loads(stored_value) if stored_value is not None else None
        except RedisAdapterException:
            self._logger.error('Error fetching segment change number from storage')
            self._logger.debug('Error: ', exc_info=True)
            return None

    def set_change_number(self, segment_name, new_change_number):
        """
        Set the latest change number.

        :param segment_name: Name of the segment.
        :type segment_name: str
        :param new_change_number: New change number.
        :type new_change_number: int
        """
        raise NotImplementedError('Only redis-consumer mode is supported.')

    def put(self, segment):
        """
        Store a segment.

        :param segment: Segment to store.
        :type segment: splitio.models.segment.Segment
        """
        raise NotImplementedError('Only redis-consumer mode is supported.')

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
        try:
            return self._redis.sismember(self._get_key(segment_name), key)
        except RedisAdapterException:
            self._logger.error('Error testing members in segment stored in redis')
            self._logger.debug('Error: ', exc_info=True)
            return None


class RedisImpressionsStorage(ImpressionStorage):
    """Redis based event storage class."""

    IMPRESSIONS_QUEUE_KEY = 'SPLITIO.impressions'
    IMPRESSIONS_KEY_DEFAULT_TTL = 3600

    def __init__(self, redis_client, sdk_metadata):
        """
        Class constructor.

        :param redis_client: Redis client or compliant interface.
        :type redis_client: splitio.storage.adapters.redis.RedisAdapter
        :param sdk_metadata: SDK & Machine information.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        """
        self._redis = redis_client
        self._sdk_metadata = sdk_metadata
        self._logger = logging.getLogger(self.__class__.__name__)

    def put(self, impressions):
        """
        Add an event to the redis storage.

        :param event: Event to add to the queue.
        :type event: splitio.models.events.Event

        :return: Whether the event has been added or not.
        :rtype: bool
        """
        bulk_impressions = []
        for impression in impressions:
            if isinstance(impression, Impression):
                to_store = {
                    'm': {  # METADATA PORTION
                        's': self._sdk_metadata.sdk_version,
                        'n': self._sdk_metadata.instance_name,
                        'i': self._sdk_metadata.instance_ip,
                    },
                    'i': {  # IMPRESSION PORTION
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
        try:
            inserted = self._redis.rpush(self.IMPRESSIONS_QUEUE_KEY, *bulk_impressions)
            if inserted == len(bulk_impressions):
                self._logger.debug("SET EXPIRE KEY FOR QUEUE")
                self._redis.expire(self.IMPRESSIONS_QUEUE_KEY, self.IMPRESSIONS_KEY_DEFAULT_TTL)
            return True
        except RedisAdapterException:
            self._logger.error('Something went wrong when trying to add impression to redis')
            self._logger.error('Error: ', exc_info=True)
            return False

    def pop_many(self, count):
        """
        Pop the oldest N events from storage.

        :param count: Number of events to pop.
        :type count: int
        """
        raise NotImplementedError('Only redis-consumer mode is supported.')


class RedisEventsStorage(EventStorage):
    """Redis based event storage class."""

    _KEY_TEMPLATE = 'SPLITIO.events'

    def __init__(self, redis_client, sdk_metadata):
        """
        Class constructor.

        :param redis_client: Redis client or compliant interface.
        :type redis_client: splitio.storage.adapters.redis.RedisAdapter
        :param sdk_metadata: SDK & Machine information.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        """
        self._redis = redis_client
        self._sdk_metadata = sdk_metadata
        self._logger = logging.getLogger(self.__class__.__name__)

    def put(self, events):
        """
        Add an event to the redis storage.

        :param event: Event to add to the queue.
        :type event: splitio.models.events.Event

        :return: Whether the event has been added or not.
        :rtype: bool
        """
        key = self._KEY_TEMPLATE
        to_store = [
            json.dumps({
                'e': {
                    'key': event.key,
                    'trafficTypeName': event.traffic_type_name,
                    'eventTypeId': event.event_type_id,
                    'value': event.value,
                    'timestamp': event.timestamp
                },
                'm': {
                    's': self._sdk_metadata.sdk_version,
                    'n': self._sdk_metadata.instance_name,
                    'i': self._sdk_metadata.instance_ip,
                }
            })
            for event in events
        ]
        try:
            self._redis.rpush(key, *to_store)
            return True
        except RedisAdapterException:
            self._logger.error('Something went wrong when trying to add event to redis')
            self._logger.debug('Error: ', exc_info=True)
            return False

    def pop_many(self, count):
        """
        Pop the oldest N events from storage.

        :param count: Number of events to pop.
        :type count: int
        """
        raise NotImplementedError('Only redis-consumer mode is supported.')


class RedisTelemetryStorage(object):
    """Redis-based Telemetry storage."""

    _LATENCY_KEY_TEMPLATE = "SPLITIO/{sdk}/{instance}/latency.{name}.bucket.{bucket}"
    _COUNTER_KEY_TEMPLATE = "SPLITIO/{sdk}/{instance}/count.{name}"
    _GAUGE_KEY_TEMPLATE = "SPLITIO/{sdk}/{instance}/gauge.{name}"

    def __init__(self, redis_client, sdk_metadata):
        """
        Class constructor.

        :param redis_client: Redis client or compliant interface.
        :type redis_client: splitio.storage.adapters.redis.RedisAdapter
        :param sdk_metadata: SDK & Machine information.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        """
        self._redis = redis_client
        self._metadata = sdk_metadata
        self._logger = logging.getLogger(self.__class__.__name__)

    def _get_latency_key(self, name, bucket):
        """
        Instantiate and return the latency key template.

        :param name: Name of the latency metric.
        :type name: str
        :param bucket: Number of bucket.
        :type bucket: int

        :return: Redis latency key.
        :rtype: str
        """
        return self._LATENCY_KEY_TEMPLATE.format(
            sdk=self._metadata.sdk_version,
            instance=self._metadata.instance_name,
            name=name,
            bucket=bucket
        )

    def _get_counter_key(self, name):
        """
        Instantiate and return the counter key template.

        :param name: Name of the counter metric.
        :type name: str

        :return: Redis counter key.
        :rtype: str
        """
        return self._COUNTER_KEY_TEMPLATE.format(
            sdk=self._metadata.sdk_version,
            instance=self._metadata.instance_name,
            name=name
        )

    def _get_gauge_key(self, name):
        """
        Instantiate and return the latency key template.

        :param name: Name of the latency metric.
        :type name: str

        :return: Redis latency key.
        :rtype: str
        """
        return self._GAUGE_KEY_TEMPLATE.format(
            sdk=self._metadata.sdk_version,
            instance=self._metadata.instance_name,
            name=name,
        )

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

        key = self._get_latency_key(name, bucket)
        try:
            self._redis.incr(key)
        except RedisAdapterException:
            self._logger.error('Something went wrong when trying to store latency in redis')
            self._logger.debug('Error: ', exc_info=True)

    def inc_counter(self, name):
        """
        Increment a counter.

        :param name: Name of the counter metric.
        :type name: str
        """
        key = self._get_counter_key(name)
        try:
            self._redis.incr(key)
        except RedisAdapterException:
            self._logger.error('Something went wrong when trying to increment counter in redis')
            self._logger.debug('Error: ', exc_info=True)

    def put_gauge(self, name, value):
        """
        Add a gauge metric.

        :param name: Name of the gauge metric.
        :type name: str
        :param value: Value of the gauge metric.
        :type value: int
        """
        key = self._get_gauge_key(name)
        try:
            self._redis.set(key, value)
        except RedisAdapterException:
            self._logger.error('Something went wrong when trying to set gauge in redis')
            self._logger.debug('Error: ', exc_info=True)

    def pop_counters(self):
        """
        Get all the counters.

        :rtype: list
        """
        raise NotImplementedError('Only redis-consumer mode is supported.')

    def pop_gauges(self):
        """
        Get all the gauges.

        :rtype: list

        """
        raise NotImplementedError('Only redis-consumer mode is supported.')

    def pop_latencies(self):
        """
        Get all latencies.

        :rtype: list
        """
        raise NotImplementedError('Only redis-consumer mode is supported.')
