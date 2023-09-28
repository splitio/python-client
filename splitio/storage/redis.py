"""Redis storage module."""
import json
import logging
import threading

from splitio.models.impressions import Impression
from splitio.models import splits, segments
from splitio.models.telemetry import TelemetryConfig, get_latency_bucket_index, TelemetryConfigAsync
from splitio.storage import SplitStorage, SegmentStorage, ImpressionStorage, EventStorage, \
    ImpressionPipelinedStorage, TelemetryStorage
from splitio.storage.adapters.redis import RedisAdapterException
from splitio.storage.adapters.cache_trait import decorate as add_cache, DEFAULT_MAX_AGE
from splitio.optional.loaders import asyncio
from splitio.storage.adapters.cache_trait import LocalMemoryCache

_LOGGER = logging.getLogger(__name__)
MAX_TAGS = 10

class RedisSplitStorageBase(SplitStorage):
    """Redis-based storage base for splits."""

    _SPLIT_KEY = 'SPLITIO.split.{split_name}'
    _SPLIT_TILL_KEY = 'SPLITIO.splits.till'
    _TRAFFIC_TYPE_KEY = 'SPLITIO.trafficType.{traffic_type_name}'

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
        pass

    def fetch_many(self, split_names):
        """
        Retrieve splits.

        :param split_names: Names of the features to fetch.
        :type split_name: list(str)

        :return: A dict with split objects parsed from redis.
        :rtype: dict(split_name, splitio.models.splits.Split)
        """
        pass

    def is_valid_traffic_type(self, traffic_type_name):  # pylint: disable=method-hidden
        """
        Return whether the traffic type exists in at least one split in cache.

        :param traffic_type_name: Traffic type to validate.
        :type traffic_type_name: str

        :return: True if the traffic type is valid. False otherwise.
        :rtype: bool
        """
        pass

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
        pass

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
        pass

    def get_splits_count(self):
        """
        Return splits count.

        :rtype: int
        """
        return 0

    def get_all_splits(self):
        """
        Return all the splits in cache.
        :return: List of all splits in cache.
        :rtype: list(splitio.models.splits.Split)
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
        raise NotImplementedError('Not supported for redis.')


class RedisSplitStorage(RedisSplitStorageBase):
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
            _LOGGER.debug("Fetchting Split [%s] from redis" % split_name)
            _LOGGER.debug(raw)
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
            _LOGGER.debug("Fetchting Splits [%s] from redis" % split_names)
            _LOGGER.debug(raw_splits)
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
            _LOGGER.debug("Fetching TrafficType [%s] count in redis: %s" % (traffic_type_name, count))
            return count > 0
        except RedisAdapterException:
            _LOGGER.error('Error fetching split from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return False

    def get_change_number(self):
        """
        Retrieve latest split change number.

        :rtype: int
        """
        try:
            stored_value = self._redis.get(self._SPLIT_TILL_KEY)
            _LOGGER.debug("Fetching Split Change Number from redis: %s" % stored_value)
            return json.loads(stored_value) if stored_value is not None else None
        except RedisAdapterException:
            _LOGGER.error('Error fetching split change number from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    def get_split_names(self):
        """
        Retrieve a list of all split names.

        :return: List of split names.
        :rtype: list(str)
        """
        try:
            keys = self._redis.keys(self._get_key('*'))
            _LOGGER.debug("Fetchting Split names from redis: %s" % keys)
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
            _LOGGER.debug("Fetchting all Splits from redis: %s" % keys)
            raw_splits = self._redis.mget(keys)
            _LOGGER.debug(raw_splits)
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


class RedisSplitStorageAsync(RedisSplitStorage):
    """Async Redis-based storage for splits."""

    def __init__(self, redis_client, enable_caching=False, max_age=DEFAULT_MAX_AGE):
        """
        Class constructor.
        :param split_name: name of the split to perform kill
        :param redis_client: Redis client or compliant interface.
        :type redis_client: splitio.storage.adapters.redis.RedisAdapter
        """
        self.redis = redis_client
        self._enable_caching = enable_caching
        if enable_caching:
            self._cache = LocalMemoryCache(None, None, max_age)

    async def get(self, split_name):  # pylint: disable=method-hidden
        """
        Retrieve a split.
        :param split_name: Name of the feature to fetch.
        :type split_name: str

        :param default_treatment: name of the default treatment to return
        :type default_treatment: str
        return: A split object parsed from redis if the key exists. None otherwise

        :param change_number: change_number
        :rtype: splitio.models.splits.Split
        :type change_number: int
        """
        try:
            if self._enable_caching and await self._cache.get_key(split_name) is not None:
                raw = await self._cache.get_key(split_name)
            else:
                raw = await self.redis.get(self._get_key(split_name))
                if self._enable_caching:
                    await self._cache.add_key(split_name, raw)
                _LOGGER.debug("Fetchting Split [%s] from redis" % split_name)
                _LOGGER.debug(raw)
            return splits.from_raw(json.loads(raw)) if raw is not None else None
        except RedisAdapterException:
            _LOGGER.error('Error fetching split from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    async def fetch_many(self, split_names):
        """
        Retrieve splits.
        :param split_names: Names of the features to fetch.
        :type split_name: list(str)
        :return: A dict with split objects parsed from redis.
        :rtype: dict(split_name, splitio.models.splits.Split)
        """
        to_return = dict()
        try:
            if self._enable_caching and await self._cache.get_key(frozenset(split_names)) is not None:
                raw_splits = await self._cache.get_key(frozenset(split_names))
            else:
                keys = [self._get_key(split_name) for split_name in split_names]
                raw_splits = await self.redis.mget(keys)
                if self._enable_caching:
                    await self._cache.add_key(frozenset(split_names), raw_splits)
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

    async def is_valid_traffic_type(self, traffic_type_name):  # pylint: disable=method-hidden
        """
        Return whether the traffic type exists in at least one split in cache.
        :param traffic_type_name: Traffic type to validate.
        :type traffic_type_name: str
        :return: True if the traffic type is valid. False otherwise.
        :rtype: bool
        """
        try:
            if self._enable_caching and await self._cache.get_key(traffic_type_name) is not None:
                raw = await self._cache.get_key(traffic_type_name)
            else:
                raw = await self.redis.get(self._get_traffic_type_key(traffic_type_name))
                if self._enable_caching:
                    await self._cache.add_key(traffic_type_name, raw)
            count = json.loads(raw) if raw else 0
            return count > 0
        except RedisAdapterException:
            _LOGGER.error('Error fetching split from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return False

    async def get_change_number(self):
        """
        Retrieve latest split change number.
        :rtype: int
        """
        try:
            stored_value = await self.redis.get(self._SPLIT_TILL_KEY)
            return json.loads(stored_value) if stored_value is not None else None
        except RedisAdapterException:
            _LOGGER.error('Error fetching split change number from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    async def get_split_names(self):
        """
        Retrieve a list of all split names.
        :return: List of split names.
        :rtype: list(str)
        """
        try:
            keys = await self.redis.keys(self._get_key('*'))
            return [key.decode('utf-8').replace(self._get_key(''), '') for key in keys]
        except RedisAdapterException:
            _LOGGER.error('Error fetching split names from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return []

    async def get_all_splits(self):
        """
        Return all the splits in cache.
        :return: List of all splits in cache.
        :rtype: list(splitio.models.splits.Split)
        """
        keys = await self.redis.keys(self._get_key('*'))
        to_return = []
        try:
            raw_splits = await self.redis.mget(keys)
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


class RedisSegmentStorageBase(SegmentStorage):
    """Redis based segment storage base class."""

    _SEGMENTS_KEY = 'SPLITIO.segment.{segment_name}'
    _SEGMENTS_TILL_KEY = 'SPLITIO.segment.{segment_name}.till'

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
        """Retrieve a segment."""
        pass

    def update(self, segment_name, to_add, to_remove, change_number=None):
        """
        Store a segment.

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
        pass

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
        pass

    def get_segments_count(self):
        """
        Return segment count.

        :return: 0
        :rtype: int
        """
        return 0

    def get_segments_keys_count(self):
        """
        Return segment count.

        :rtype: int
        """
        return 0


class RedisSegmentStorage(RedisSegmentStorageBase):
    """Redis based segment storage class."""

    def __init__(self, redis_client):
        """
        Class constructor.

        :param redis_client: Redis client or compliant interface.
        :type redis_client: splitio.storage.adapters.redis.RedisAdapter
        """
        self._redis = redis_client

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
            _LOGGER.debug("Fetchting Segment [%s] from redis" % segment_name)
            _LOGGER.debug(keys)
            till = self.get_change_number(segment_name)
            if not keys or till is None:
                return None
            return segments.Segment(segment_name, keys, till)
        except RedisAdapterException:
            _LOGGER.error('Error fetching segment from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    def get_change_number(self, segment_name):
        """
        Retrieve latest change number for a segment.

        :param segment_name: Name of the segment.
        :type segment_name: str

        :rtype: int
        """
        try:
            stored_value = self._redis.get(self._get_till_key(segment_name))
            _LOGGER.debug("Fetchting Change Number for Segment [%s] from redis: " % stored_value)
            return json.loads(stored_value) if stored_value is not None else None
        except RedisAdapterException:
            _LOGGER.error('Error fetching segment change number from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

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
            res = self._redis.sismember(self._get_key(segment_name), key)
            _LOGGER.debug("Checking Segment [%s] contain key [%s] in redis: %s" % (segment_name, key, res))
            return res
        except RedisAdapterException:
            _LOGGER.error('Error testing members in segment stored in redis')
            _LOGGER.debug('Error: ', exc_info=True)
            return None


class RedisSegmentStorageAsync(RedisSegmentStorageBase):
    """Redis based segment storage async class."""

    def __init__(self, redis_client):
        """
        Class constructor.

        :param redis_client: Redis client or compliant interface.
        :type redis_client: splitio.storage.adapters.redis.RedisAdapter
        """
        self._redis = redis_client

    async def get(self, segment_name):
        """
        Retrieve a segment.

        :param segment_name: Name of the segment to fetch.
        :type segment_name: str

        :return: Segment object is key exists. None otherwise.
        :rtype: splitio.models.segments.Segment
        """
        try:
            keys = (await self._redis.smembers(self._get_key(segment_name)))
            _LOGGER.debug("Fetchting Segment [%s] from redis" % segment_name)
            _LOGGER.debug(keys)
            till = await self.get_change_number(segment_name)
            if not keys or till is None:
                return None
            return segments.Segment(segment_name, keys, till)
        except RedisAdapterException:
            _LOGGER.error('Error fetching segment from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    async def get_change_number(self, segment_name):
        """
        Retrieve latest change number for a segment.

        :param segment_name: Name of the segment.
        :type segment_name: str

        :rtype: int
        """
        try:
            stored_value = await self._redis.get(self._get_till_key(segment_name))
            _LOGGER.debug("Fetchting Change Number for Segment [%s] from redis: " % stored_value)
            return json.loads(stored_value) if stored_value is not None else None
        except RedisAdapterException:
            _LOGGER.error('Error fetching segment change number from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

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
        try:
            res = await self._redis.sismember(self._get_key(segment_name), key)
            _LOGGER.debug("Checking Segment [%s] contain key [%s] in redis: %s" % (segment_name, key, res))
            return res
        except RedisAdapterException:
            _LOGGER.error('Error testing members in segment stored in redis')
            _LOGGER.debug('Error: ', exc_info=True)
            return None


class RedisImpressionsStorageBase(ImpressionStorage, ImpressionPipelinedStorage):
    """Redis based event storage base class."""

    IMPRESSIONS_QUEUE_KEY = 'SPLITIO.impressions'
    IMPRESSIONS_KEY_DEFAULT_TTL = 3600

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
        pass

    def add_impressions_to_pipe(self, impressions, pipe):
        """
        Add put operation to pipeline

        :param impressions: List of one or more impressions to store.
        :type impressions: list
        :param pipe: Redis pipe.
        :type pipe: redis.pipe
        """
        bulk_impressions = self._wrap_impressions(impressions)
        _LOGGER.debug("Adding Impressions to redis key %s" % (self.IMPRESSIONS_QUEUE_KEY))
        _LOGGER.debug(bulk_impressions)
        pipe.rpush(self.IMPRESSIONS_QUEUE_KEY, *bulk_impressions)

    def put(self, impressions):
        """
        Add an impression to the redis storage.

        :param impressions: Impression to add to the queue.
        :type impressions: splitio.models.impressions.Impression

        :return: Whether the impression has been added or not.
        :rtype: bool
        """
        pass

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


class RedisImpressionsStorage(RedisImpressionsStorageBase):
    """Redis based event storage class."""

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

    def expire_key(self, total_keys, inserted):
        """
        Set expire

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        if total_keys == inserted:
            self._redis.expire(self.IMPRESSIONS_QUEUE_KEY, self.IMPRESSIONS_KEY_DEFAULT_TTL)

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
            _LOGGER.debug("Adding Impressions to redis key %s" % (self.IMPRESSIONS_QUEUE_KEY))
            _LOGGER.debug(bulk_impressions)
            inserted = self._redis.rpush(self.IMPRESSIONS_QUEUE_KEY, *bulk_impressions)
            self.expire_key(inserted, len(bulk_impressions))
            return True
        except RedisAdapterException:
            _LOGGER.error('Something went wrong when trying to add impression to redis')
            _LOGGER.error('Error: ', exc_info=True)
            return False


class RedisImpressionsStorageAsync(RedisImpressionsStorageBase):
    """Redis based event storage async class."""

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

    async def expire_key(self, total_keys, inserted):
        """
        Set expire

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        if total_keys == inserted:
            await self._redis.expire(self.IMPRESSIONS_QUEUE_KEY, self.IMPRESSIONS_KEY_DEFAULT_TTL)

    async def put(self, impressions):
        """
        Add an impression to the redis storage.

        :param impressions: Impression to add to the queue.
        :type impressions: splitio.models.impressions.Impression

        :return: Whether the impression has been added or not.
        :rtype: bool
        """
        bulk_impressions = self._wrap_impressions(impressions)
        try:
            _LOGGER.debug("Adding Impressions to redis key %s" % (self.IMPRESSIONS_QUEUE_KEY))
            _LOGGER.debug(bulk_impressions)
            inserted = await self._redis.rpush(self.IMPRESSIONS_QUEUE_KEY, *bulk_impressions)
            await self.expire_key(inserted, len(bulk_impressions))
            return True
        except RedisAdapterException:
            _LOGGER.error('Something went wrong when trying to add impression to redis')
            _LOGGER.error('Error: ', exc_info=True)
            return False


class RedisEventsStorageBase(EventStorage):
    """Redis based event storage base class."""

    _EVENTS_KEY_TEMPLATE = 'SPLITIO.events'
    _EVENTS_KEY_DEFAULT_TTL = 3600

    def add_events_to_pipe(self, events, pipe):
        """
        Add put operation to pipeline

        :param impressions: List of one or more impressions to store.
        :type impressions: list
        :param pipe: Redis pipe.
        :type pipe: redis.pipe
        """
        bulk_events = self._wrap_events(events)
        _LOGGER.debug("Adding Events to redis key %s" % (self._EVENTS_KEY_TEMPLATE))
        _LOGGER.debug(bulk_events)
        pipe.rpush(self._EVENTS_KEY_TEMPLATE, *bulk_events)

    def _wrap_events(self, events):
        return [
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

    def put(self, events):
        """
        Add an event to the redis storage.

        :param event: Event to add to the queue.
        :type event: splitio.models.events.Event

        :return: Whether the event has been added or not.
        :rtype: bool
        """
        pass

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

    def expire_keys(self, total_keys, inserted):
        """
        Set expire

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        pass

class RedisEventsStorage(RedisEventsStorageBase):
    """Redis based event storage class."""

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
        key = self._EVENTS_KEY_TEMPLATE
        to_store = self._wrap_events(events)
        try:
            _LOGGER.debug("Adding Events to redis key %s" % (key))
            _LOGGER.debug(to_store)
            self._redis.rpush(key, *to_store)
            return True
        except RedisAdapterException:
            _LOGGER.error('Something went wrong when trying to add event to redis')
            _LOGGER.debug('Error: ', exc_info=True)
            return False

    def expire_keys(self, total_keys, inserted):
        """
        Set expire

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        if total_keys == inserted:
            self._redis.expire(self._EVENTS_KEY_TEMPLATE, self._EVENTS_KEY_DEFAULT_TTL)


class RedisEventsStorageAsync(RedisEventsStorageBase):
    """Redis based event async storage class."""

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

    async def put(self, events):
        """
        Add an event to the redis storage.

        :param event: Event to add to the queue.
        :type event: splitio.models.events.Event

        :return: Whether the event has been added or not.
        :rtype: bool
        """
        key = self._EVENTS_KEY_TEMPLATE
        to_store = self._wrap_events(events)
        try:
            _LOGGER.debug("Adding Events to redis key %s" % (key))
            _LOGGER.debug(to_store)
            await self._redis.rpush(key, *to_store)
            return True
        except RedisAdapterException:
            _LOGGER.error('Something went wrong when trying to add event to redis')
            _LOGGER.debug('Error: ', exc_info=True)
            return False

    async def expire_keys(self, total_keys, inserted):
        """
        Set expire

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        if total_keys == inserted:
            await self._redis.expire(self._EVENTS_KEY_TEMPLATE, self._EVENTS_KEY_DEFAULT_TTL)


class RedisTelemetryStorageBase(TelemetryStorage):
    """Redis based telemetry storage class."""

    _TELEMETRY_CONFIG_KEY = 'SPLITIO.telemetry.init'
    _TELEMETRY_LATENCIES_KEY = 'SPLITIO.telemetry.latencies'
    _TELEMETRY_EXCEPTIONS_KEY = 'SPLITIO.telemetry.exceptions'
    _TELEMETRY_KEY_DEFAULT_TTL = 3600

    def _reset_config_tags(self):
        """Reset all config tags"""
        pass

    def add_config_tag(self, tag):
        """Record tag string."""
        pass

    def record_config(self, config, extra_config):
        """
        initilize telemetry objects

        :param congif: factory configuration parameters
        :type config: splitio.client.config
        """
        pass

    def pop_config_tags(self):
        """Get and reset tags."""
        pass

    def push_config_stats(self):
        """push config stats to redis."""
        pass

    def _format_config_stats(self, config_stats, tags):
        """format only selected config stats to json"""
        return json.dumps({
            'aF': config_stats['aF'],
            'rF': config_stats['rF'],
            'sT': config_stats['sT'],
            'oM': config_stats['oM'],
            't': tags
        })

    def record_active_and_redundant_factories(self, active_factory_count, redundant_factory_count):
        """Record active and redundant factories."""
        pass

    def add_latency_to_pipe(self, method, bucket, pipe):
        """
        record latency data

        :param method: method name
        :type method: string
        :param latency: latency
        :type latency: int64
        :param pipe: Redis pipe.
        :type pipe: redis.pipe
        """
        _LOGGER.debug("Adding Latency stats to redis key %s" % (self._TELEMETRY_LATENCIES_KEY))
        _LOGGER.debug(self._sdk_metadata.sdk_version + '/' + self._sdk_metadata.instance_name + '/' + self._sdk_metadata.instance_ip + '/' +
            method.value + '/' + str(bucket))
        pipe.hincrby(self._TELEMETRY_LATENCIES_KEY, self._sdk_metadata.sdk_version + '/' + self._sdk_metadata.instance_name + '/' + self._sdk_metadata.instance_ip + '/' +
            method.value + '/' + str(bucket), 1)

    def record_latency(self, method, latency):
        """
        Not implemented
        """
        raise NotImplementedError('Only redis pipe is used.')

    def record_exception(self, method):
        """
        record an exception

        :param method: method name
        :type method: string
        """
        pass

    def record_not_ready_usage(self):
        """
        record not ready time

        """
        pass

    def record_bur_time_out(self):
        """
        record BUR timeouts

        """
        pass

    def record_impression_stats(self, data_type, count):
        pass

    def expire_latency_keys(self, total_keys, inserted):
        pass

    def expire_keys(self, queue_key, key_default_ttl, total_keys, inserted):
        """
        Set expire

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        pass


class RedisTelemetryStorage(RedisTelemetryStorageBase):
    """Redis based telemetry storage class."""

    def __init__(self, redis_client, sdk_metadata):
        """
        Class constructor.

        :param redis_client: Redis client or compliant interface.
        :type redis_client: splitio.storage.adapters.redis.RedisAdapter
        :param sdk_metadata: SDK & Machine information.
        :type sdk_metadata: splitio.client.util.SdkMetadata
        """
        self._lock = threading.RLock()
        self._reset_config_tags()
        self._redis_client = redis_client
        self._sdk_metadata = sdk_metadata
        self._tel_config = TelemetryConfig()
        self._make_pipe = redis_client.pipeline

    def _reset_config_tags(self):
        """Reset all config tags"""
        with self._lock:
            self._config_tags = []

    def add_config_tag(self, tag):
        """Record tag string."""
        with self._lock:
            if len(self._config_tags) < MAX_TAGS:
                self._config_tags.append(tag)

    def record_config(self, config, extra_config):
        """
        initilize telemetry objects

        :param congif: factory configuration parameters
        :type config: splitio.client.config
        """
        self._tel_config.record_config(config, extra_config)

    def pop_config_tags(self):
        """Get and reset tags."""
        with self._lock:
            tags = self._config_tags
            self._reset_config_tags()
            return tags

    def push_config_stats(self):
        """push config stats to redis."""
        _LOGGER.debug("Adding Config stats to redis key %s" % (self._TELEMETRY_CONFIG_KEY))
        _LOGGER.debug(str(self._format_config_stats(self._tel_config.get_stats(), self.pop_config_tags())))
        self._redis_client.hset(self._TELEMETRY_CONFIG_KEY, self._sdk_metadata.sdk_version + '/' + self._sdk_metadata.instance_name + '/' + self._sdk_metadata.instance_ip, str(self._format_config_stats(self._tel_config.get_stats(), self.pop_config_tags())))

    def record_active_and_redundant_factories(self, active_factory_count, redundant_factory_count):
        """Record active and redundant factories."""
        self._tel_config.record_active_and_redundant_factories(active_factory_count, redundant_factory_count)

    def record_exception(self, method):
        """
        record an exception

        :param method: method name
        :type method: string
        """
        _LOGGER.debug("Adding Excepction stats to redis key %s" % (self._TELEMETRY_EXCEPTIONS_KEY))
        _LOGGER.debug(self._sdk_metadata.sdk_version + '/' + self._sdk_metadata.instance_name + '/' + self._sdk_metadata.instance_ip + '/' +
                    method.value)
        pipe = self._make_pipe()
        pipe.hincrby(self._TELEMETRY_EXCEPTIONS_KEY, self._sdk_metadata.sdk_version + '/' + self._sdk_metadata.instance_name + '/' + self._sdk_metadata.instance_ip + '/' +
                    method.value, 1)
        result = pipe.execute()
        self.expire_keys(self._TELEMETRY_EXCEPTIONS_KEY, self._TELEMETRY_KEY_DEFAULT_TTL, 1, result[0])

    def record_active_and_redundant_factories(self, active_factory_count, redundant_factory_count):
        """Record active and redundant factories."""
        self._tel_config.record_active_and_redundant_factories(active_factory_count, redundant_factory_count)

    def expire_latency_keys(self, total_keys, inserted):
        """
        Expire lstency keys

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        self.expire_keys(self._TELEMETRY_LATENCIES_KEY, self._TELEMETRY_KEY_DEFAULT_TTL, total_keys, inserted)

    def expire_keys(self, queue_key, key_default_ttl, total_keys, inserted):
        """
        Set expire

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        if total_keys == inserted:
            self._redis_client.expire(queue_key, key_default_ttl)

    def record_bur_time_out(self):
        """record BUR timeouts"""
        pass

    def record_ready_time(self, ready_time):
        """Record ready time."""
        pass


class RedisTelemetryStorageAsync(RedisTelemetryStorageBase):
    """Redis based telemetry async storage class."""

    async def create(redis_client, sdk_metadata):
        """
        Create instance and reset tags

        :param redis_client: Redis client or compliant interface.
        :type redis_client: splitio.storage.adapters.redis.RedisAdapter
        :param sdk_metadata: SDK & Machine information.
        :type sdk_metadata: splitio.client.util.SdkMetadata

        :return: self instance.
        :rtype: splitio.storage.redis.RedisTelemetryStorageAsync
        """
        self = RedisTelemetryStorageAsync()
        await self._reset_config_tags()
        self._redis_client = redis_client
        self._sdk_metadata = sdk_metadata
        self._tel_config = await TelemetryConfigAsync.create()
        self._make_pipe = redis_client.pipeline
        return self

    async def _reset_config_tags(self):
        """Reset all config tags"""
        self._config_tags = []

    async def add_config_tag(self, tag):
        """Record tag string."""
        if len(self._config_tags) < MAX_TAGS:
            self._config_tags.append(tag)

    async def record_config(self, config, extra_config):
        """
        initilize telemetry objects

        :param congif: factory configuration parameters
        :type config: splitio.client.config
        """
        await self._tel_config.record_config(config, extra_config)

    async def record_bur_time_out(self):
        """record BUR timeouts"""
        pass

    async def record_ready_time(self, ready_time):
        """Record ready time."""
        pass

    async def pop_config_tags(self):
        """Get and reset tags."""
        tags = self._config_tags
        await self._reset_config_tags()
        return tags

    async def push_config_stats(self):
        """push config stats to redis."""
        _LOGGER.debug("Adding Config stats to redis key %s" % (self._TELEMETRY_CONFIG_KEY))
        stats = str(self._format_config_stats(await self._tel_config.get_stats(), await self.pop_config_tags()))
        _LOGGER.debug(stats)
        await self._redis_client.hset(self._TELEMETRY_CONFIG_KEY, self._sdk_metadata.sdk_version + '/' + self._sdk_metadata.instance_name + '/' + self._sdk_metadata.instance_ip, stats)

    async def record_exception(self, method):
        """
        record an exception

        :param method: method name
        :type method: string
        """
        _LOGGER.debug("Adding Excepction stats to redis key %s" % (self._TELEMETRY_EXCEPTIONS_KEY))
        _LOGGER.debug(self._sdk_metadata.sdk_version + '/' + self._sdk_metadata.instance_name + '/' + self._sdk_metadata.instance_ip + '/' +
                    method.value)
        pipe = self._make_pipe()
        pipe.hincrby(self._TELEMETRY_EXCEPTIONS_KEY, self._sdk_metadata.sdk_version + '/' + self._sdk_metadata.instance_name + '/' + self._sdk_metadata.instance_ip + '/' +
                    method.value, 1)
        result = await pipe.execute()
        await self.expire_keys(self._TELEMETRY_EXCEPTIONS_KEY, self._TELEMETRY_KEY_DEFAULT_TTL, 1, result[0])

    async def record_active_and_redundant_factories(self, active_factory_count, redundant_factory_count):
        """Record active and redundant factories."""
        await self._tel_config.record_active_and_redundant_factories(active_factory_count, redundant_factory_count)

    async def expire_latency_keys(self, total_keys, inserted):
        """
        Expire lstency keys

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        await self.expire_keys(self._TELEMETRY_LATENCIES_KEY, self._TELEMETRY_KEY_DEFAULT_TTL, total_keys, inserted)

    async def expire_keys(self, queue_key, key_default_ttl, total_keys, inserted):
        """
        Set expire

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        if total_keys == inserted:
            await self._redis_client.expire(queue_key, key_default_ttl)
