"""Redis storage module."""
import json
import logging

from splitio.models.impressions import Impression
from splitio.models import splits, segments
from splitio.storage import SplitStorage, SegmentStorage, ImpressionStorage, EventStorage, \
    ImpressionPipelinedStorage
from splitio.storage.adapters.redis import RedisAdapterException
from splitio.storage.adapters.cache_trait import decorate as add_cache, DEFAULT_MAX_AGE


_LOGGER = logging.getLogger(__name__)


class RedisSplitStorage(SplitStorage):
    """Redis-based storage for splits."""

    _SPLIT_KEY = 'SPLITIO.split.{split_name}'
    _SPLIT_TILL_KEY = 'SPLITIO.splits.till'
    _TRAFFIC_TYPE_KEY = 'SPLITIO.trafficType.{traffic_type_name}'

    def __init__(self, redis_client, enable_caching=False, max_age=DEFAULT_MAX_AGE):
        """
        Class constructor.

        :param redis_client: Redis client or compliant interface.
        :type redis_client: splitio.storage.adapters.redis.RedisAdapter
        """
        self._redis = redis_client
        if enable_caching:
            self.get = add_cache(lambda *p, **_: p[0], max_age)(self.get)
            self.is_valid_traffic_type = add_cache(lambda *p, **_: p[0], max_age)(self.is_valid_traffic_type)  # pylint: disable=line-too-long
            self.fetch_many = add_cache(lambda *p, **_: frozenset(p[0]), max_age)(self.fetch_many)

    def _get_key(self, split_name):
        """
        Use the provided split_name to build the appropriate redis key.

        :param split_name: Name of the split to interact with in redis.
        :type split_name: str

        :return: Redis key.
        :rtype: str.
        """
        return self._SPLIT_KEY.format(split_name=split_name)

    def _get_traffic_type_key(self, traffic_type_name):
        """
        Use the provided split_name to build the appropriate redis key.

        :param split_name: Name of the split to interact with in redis.
        :type split_name: str

        :return: Redis key.
        :rtype: str.
        """
        return self._TRAFFIC_TYPE_KEY.format(traffic_type_name=traffic_type_name)

    def get(self, split_name):  # pylint: disable=method-hidden
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
            _LOGGER.error('Error fetching split from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    def fetch_many(self, split_names):
        """
        Retrieve splits.

        :param split_names: Names of the features to fetch.
        :type split_name: list(str)

        :return: A dict with split objects parsed from redis.
        :rtype: dict(split_name, splitio.models.splits.Split)
        """
        to_return = dict()
        try:
            keys = [self._get_key(split_name) for split_name in split_names]
            raw_splits = self._redis.mget(keys)
            for i in range(len(split_names)):
                split = None
                try:
                    split = splits.from_raw(json.loads(raw_splits[i]))
                except (ValueError, TypeError):
                    _LOGGER.error('Could not parse split.')
                    _LOGGER.debug("Raw split that failed parsing attempt: %s", raw_splits[i])
                to_return[split_names[i]] = split
        except RedisAdapterException:
            _LOGGER.error('Error fetching splits from storage')
            _LOGGER.debug('Error: ', exc_info=True)
        return to_return

    def is_valid_traffic_type(self, traffic_type_name):  # pylint: disable=method-hidden
        """
        Return whether the traffic type exists in at least one split in cache.

        :param traffic_type_name: Traffic type to validate.
        :type traffic_type_name: str

        :return: True if the traffic type is valid. False otherwise.
        :rtype: bool
        """
        try:
            raw = self._redis.get(self._get_traffic_type_key(traffic_type_name))
            count = json.loads(raw) if raw else 0
            return count > 0
        except RedisAdapterException:
            _LOGGER.error('Error fetching split from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return False

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
            _LOGGER.error('Error fetching split change number from storage')
            _LOGGER.debug('Error: ', exc_info=True)
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
            _LOGGER.error('Error fetching split names from storage')
            _LOGGER.debug('Error: ', exc_info=True)
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
                except (ValueError, TypeError):
                    _LOGGER.error('Could not parse split. Skipping')
                    _LOGGER.debug("Raw split that failed parsing attempt: %s", raw)
        except RedisAdapterException:
            _LOGGER.error('Error fetching all splits from storage')
            _LOGGER.debug('Error: ', exc_info=True)
        return to_return

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
        raise NotImplementedError('Not supported for redis.')


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
            _LOGGER.error('Error fetching segment from storage')
            _LOGGER.debug('Error: ', exc_info=True)
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
            _LOGGER.error('Error fetching segment change number from storage')
            _LOGGER.debug('Error: ', exc_info=True)
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
            _LOGGER.error('Error testing members in segment stored in redis')
            _LOGGER.debug('Error: ', exc_info=True)
            return None


class RedisImpressionsStorage(ImpressionStorage, ImpressionPipelinedStorage):
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

    def _wrap_impressions(self, impressions):
        """
        Wrap impressions to be stored in redis

        :param impressions: Impression to add to the queue.
        :type impressions: splitio.models.impressions.Impression

        :return: Processed impressions.
        :rtype: list[splitio.models.impressions.Impression]
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
        return bulk_impressions

    def expire_key(self, total_keys, inserted):
        """
        Set expire

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        if total_keys == inserted:
            _LOGGER.debug("SET EXPIRE KEY FOR QUEUE")
            self._redis.expire(self.IMPRESSIONS_QUEUE_KEY, self.IMPRESSIONS_KEY_DEFAULT_TTL)

    def add_impressions_to_pipe(self, impressions, pipe):
        """
        Add put operation to pipeline

        :param impressions: List of one or more impressions to store.
        :type impressions: list
        :param pipe: Redis pipe.
        :type pipe: redis.pipe
        """
        bulk_impressions = self._wrap_impressions(impressions)
        pipe.rpush(self.IMPRESSIONS_QUEUE_KEY, *bulk_impressions)

    def put(self, impressions):
        """
        Add an impression to the redis storage.

        :param impressions: Impression to add to the queue.
        :type impressions: splitio.models.impressions.Impression

        :return: Whether the impression has been added or not.
        :rtype: bool
        """
        bulk_impressions = self._wrap_impressions(impressions)
        try:
            inserted = self._redis.rpush(self.IMPRESSIONS_QUEUE_KEY, *bulk_impressions)
            self.expire_key(inserted, len(bulk_impressions))
            return True
        except RedisAdapterException:
            _LOGGER.error('Something went wrong when trying to add impression to redis')
            _LOGGER.error('Error: ', exc_info=True)
            return False

    def pop_many(self, count):
        """
        Pop the oldest N events from storage.

        :param count: Number of events to pop.
        :type count: int
        """
        raise NotImplementedError('Only redis-consumer mode is supported.')

    def clear(self):
        """
        Clear data.
        """
        raise NotImplementedError('Not supported for redis.')


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
                    'key': e.event.key,
                    'trafficTypeName': e.event.traffic_type_name,
                    'eventTypeId': e.event.event_type_id,
                    'value': e.event.value,
                    'timestamp': e.event.timestamp,
                    'properties': e.event.properties,
                },
                'm': {
                    's': self._sdk_metadata.sdk_version,
                    'n': self._sdk_metadata.instance_name,
                    'i': self._sdk_metadata.instance_ip,
                }
            })
            for e in events
        ]
        try:
            self._redis.rpush(key, *to_store)
            return True
        except RedisAdapterException:
            _LOGGER.error('Something went wrong when trying to add event to redis')
            _LOGGER.debug('Error: ', exc_info=True)
            return False

    def pop_many(self, count):
        """
        Pop the oldest N events from storage.

        :param count: Number of events to pop.
        :type count: int
        """
        raise NotImplementedError('Only redis-consumer mode is supported.')

    def clear(self):
        """
        Clear data.
        """
        raise NotImplementedError('Not supported for redis.')
