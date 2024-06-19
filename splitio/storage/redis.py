"""Redis storage module."""
import json
import logging
import threading

from splitio.models.impressions import Impression
from splitio.models import splits, segments
from splitio.models.telemetry import TelemetryConfig, TelemetryConfigAsync
from splitio.storage import SplitStorage, SegmentStorage, ImpressionStorage, EventStorage, \
    ImpressionPipelinedStorage, TelemetryStorage, FlagSetsFilter
from splitio.storage.adapters.redis import RedisAdapterException
from splitio.storage.adapters.cache_trait import decorate as add_cache, DEFAULT_MAX_AGE
from splitio.storage.adapters.cache_trait import LocalMemoryCache, LocalMemoryCacheAsync
from splitio.util.storage_helper import get_valid_flag_sets, combine_valid_flag_sets

_LOGGER = logging.getLogger(__name__)
MAX_TAGS = 10

class RedisSplitStorageBase(SplitStorage):
    """Redis-based storage base for     s."""

    _FEATURE_FLAG_KEY = 'SPLITIO.split.{feature_flag_name}'
    _FEATURE_FLAG_TILL_KEY = 'SPLITIO.splits.till'
    _TRAFFIC_TYPE_KEY = 'SPLITIO.trafficType.{traffic_type_name}'
    _FLAG_SET_KEY = 'SPLITIO.flagSet.{flag_set}'

    def _get_key(self, feature_flag_name):
        """
        Use the provided feature_flag_name to build the appropriate redis key.

        :param feature_flag_name: Name of the feature flag to interact with in redis.
        :type feature_flag_name: str

        :return: Redis key.
        :rtype: str.
        """
        return self._FEATURE_FLAG_KEY.format(feature_flag_name=feature_flag_name)

    def _get_traffic_type_key(self, traffic_type_name):
        """
        Use the provided traffic type name to build the appropriate redis key.

        :param traffic_type: Name of the traffic type to interact with in redis.
        :type traffic_type_name: str

        :return: Redis key.
        :rtype: str.
        """
        return self._TRAFFIC_TYPE_KEY.format(traffic_type_name=traffic_type_name)

    def _get_flag_set_key(self, flag_set):
        """
        Use the provided flag set to build the appropriate redis key.
        :param flag_set: Name of the flag set to interact with in redis.
        :type flag_set: str
        :return: Redis key.
        :rtype: str.
        """
        return self._FLAG_SET_KEY.format(flag_set=flag_set)

    def get(self, feature_flag_name):  # pylint: disable=method-hidden
        """
        Retrieve a feature flag.

        :param feature_flag_name: Name of the feature to fetch.
        :type feature_flag_name: str

        :return: A feature flag object parsed from redis if the key exists. None otherwise
        :rtype: splitio.models.splits.Split
        """
        pass

    def fetch_many(self, feature_flag_names):
        """
        Retrieve feature flags.

        :param feature_flag_names: Names of the features to fetch.
        :type feature_flag_name: list(str)

        :return: A dict with feature flag objects parsed from redis.
        :rtype: dict(feature_flag_name, splitio.models.splits.Split)
        """
        pass

    def is_valid_traffic_type(self, traffic_type_name):  # pylint: disable=method-hidden
        """
        Return whether the traffic type exists in at least one feature flag in cache.

        :param traffic_type_name: Traffic type to validate.
        :type traffic_type_name: str

        :return: True if the traffic type is valid. False otherwise.
        :rtype: bool
        """
        pass

    def update(self, to_add, to_delete, new_change_number):
        """
        Update feature flag storage.

        :param to_add: List of feature flags to add
        :type to_add: list[splitio.models.splits.Split]
        :param to_delete: List of feature flags to delete
        :type to_delete: list[splitio.models.splits.Split]
        :param new_change_number: New change number.
        :type new_change_number: int
        """
        raise NotImplementedError('Only redis-consumer mode is supported.')

    def get_change_number(self):
        """
        Retrieve latest feature flag change number.

        :rtype: int
        """
        pass

    def get_split_names(self):
        """
        Retrieve a list of all feature flag names.

        :return: List of feature flag names.
        :rtype: list(str)
        """
        pass

    def get_splits_count(self):
        """
        Return feature flags count.

        :rtype: int
        """
        return 0

    def get_all_splits(self):
        """
        Return all the feature flags in cache.
        :return: List of all feature flags in cache.
        :rtype: list(splitio.models.splits.Split)
        """
        pass

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
        raise NotImplementedError('Not supported for redis.')


class RedisSplitStorage(RedisSplitStorageBase):
    """Redis-based storage for feature flags."""

    def __init__(self, redis_client, enable_caching=False, max_age=DEFAULT_MAX_AGE, config_flag_sets=[]):
        """
        Class constructor.

        :param redis_client: Redis client or compliant interface.
        :type redis_client: splitio.storage.adapters.redis.RedisAdapter
        """
        self._redis = redis_client
        self.flag_set_filter = FlagSetsFilter(config_flag_sets)
        self._pipe = self._redis.pipeline
        if enable_caching:
            self.get = add_cache(lambda *p, **_: p[0], max_age)(self.get)
            self.is_valid_traffic_type = add_cache(lambda *p, **_: p[0], max_age)(self.is_valid_traffic_type)  # pylint: disable=line-too-long
            self.fetch_many = add_cache(lambda *p, **_: frozenset(p[0]), max_age)(self.fetch_many)

    def get(self, feature_flag_name):  # pylint: disable=method-hidden
        """
        Retrieve a feature flag.

        :param feature_flag_name: Name of the feature to fetch.
        :type feature_flag_name: str

        :return: A feature flag object parsed from redis if the key exists. None otherwise
        :rtype: splitio.models.splits.Split
        """
        try:
            raw = self._redis.get(self._get_key(feature_flag_name))
            _LOGGER.debug("Fetchting feature flag [%s] from redis" % feature_flag_name)
            _LOGGER.debug(raw)
            return splits.from_raw(json.loads(raw)) if raw is not None else None

        except RedisAdapterException:
            _LOGGER.error('Error fetching feature flag from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    def get_feature_flags_by_sets(self, flag_sets):
        """
        Retrieve feature flags by flag set.
        :param flag_set: Names of the flag set to fetch.
        :type flag_set: str
        :return: Feature flag names that are tagged with the flag set
        :rtype: listt(str)
        """
        try:
            sets_to_fetch = get_valid_flag_sets(flag_sets, self.flag_set_filter)
            if sets_to_fetch == []:
                return []

            keys = [self._get_flag_set_key(flag_set) for flag_set in sets_to_fetch]
            pipe = self._pipe()
            for key in keys:
                pipe.smembers(key)
            result_sets = pipe.execute()
            _LOGGER.debug("Fetchting Feature flags by set [%s] from redis" % (keys))
            _LOGGER.debug(result_sets)
            return list(combine_valid_flag_sets(result_sets))

        except RedisAdapterException:
            _LOGGER.error('Error fetching feature flag from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    def fetch_many(self, feature_flag_names):
        """
        Retrieve feature flags.

        :param feature_flag_names: Names of the features to fetch.
        :type feature_flag_name: list(str)

        :return: A dict with feature flag objects parsed from redis.
        :rtype: dict(feature_flag_name, splitio.models.splits.Split)
        """
        to_return = dict()
        try:
            keys = [self._get_key(feature_flag_name) for feature_flag_name in feature_flag_names]
            raw_feature_flags = self._redis.mget(keys)
            _LOGGER.debug("Fetchting feature flags [%s] from redis" % feature_flag_names)
            _LOGGER.debug(raw_feature_flags)
            for i in range(len(feature_flag_names)):
                feature_flag = None
                try:
                    feature_flag = splits.from_raw(json.loads(raw_feature_flags[i]))
                except (ValueError, TypeError):
                    _LOGGER.error('Could not parse feature flag.')
                    _LOGGER.debug("Raw feature flag that failed parsing attempt: %s", raw_feature_flags[i])
                to_return[feature_flag_names[i]] = feature_flag
        except RedisAdapterException:
            _LOGGER.error('Error fetching feature flags from storage')
            _LOGGER.debug('Error: ', exc_info=True)
        return to_return

    def is_valid_traffic_type(self, traffic_type_name):  # pylint: disable=method-hidden
        """
        Return whether the traffic type exists in at least one feature flag in cache.

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
            _LOGGER.error('Error fetching feature flag from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return False

    def get_change_number(self):
        """
        Retrieve latest feature flag change number.

        :rtype: int
        """
        try:
            stored_value = self._redis.get(self._FEATURE_FLAG_TILL_KEY)
            _LOGGER.debug("Fetching feature flag Change Number from redis: %s" % stored_value)
            return json.loads(stored_value) if stored_value is not None else None

        except RedisAdapterException:
            _LOGGER.error('Error fetching feature flag change number from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    def get_split_names(self):
        """
        Retrieve a list of all feature flag names.

        :return: List of feature flag names.
        :rtype: list(str)
        """
        try:
            keys = self._redis.keys(self._get_key('*'))
            _LOGGER.debug("Fetchting feature flag names from redis: %s" % keys)
            return [key.replace(self._get_key(''), '') for key in keys]

        except RedisAdapterException:
            _LOGGER.error('Error fetching feature flag names from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return []

    def get_all_splits(self):
        """
        Return all the feature flags in cache.
        :return: List of all feature flags in cache.
        :rtype: list(splitio.models.splits.Split)
        """
        keys = self._redis.keys(self._get_key('*'))
        to_return = []
        try:
            _LOGGER.debug("Fetchting all feature flags from redis: %s" % keys)
            raw_feature_flags = self._redis.mget(keys)
            _LOGGER.debug(raw_feature_flags)
            for raw in raw_feature_flags:
                try:
                    to_return.append(splits.from_raw(json.loads(raw)))
                except (ValueError, TypeError):
                    _LOGGER.error('Could not parse feature flag. Skipping')
                    _LOGGER.debug("Raw feature flag that failed parsing attempt: %s", raw)
        except RedisAdapterException:
            _LOGGER.error('Error fetching all feature flags from storage')
            _LOGGER.debug('Error: ', exc_info=True)
        return to_return

class RedisSplitStorageAsync(RedisSplitStorage):
    """Async Redis-based storage for feature flags."""

    def __init__(self, redis_client, enable_caching=False, max_age=DEFAULT_MAX_AGE, config_flag_sets=[]):
        """
        Class constructor.
        """
        self.redis = redis_client
        self._enable_caching = enable_caching
        self.flag_set_filter = FlagSetsFilter(config_flag_sets)
        self._pipe = self.redis.pipeline
        if enable_caching:
            self._feature_flag_cache = LocalMemoryCacheAsync(None, None, max_age)
            self._traffic_type_cache = LocalMemoryCacheAsync(None, None, max_age)


    async def get(self, feature_flag_name):  # pylint: disable=method-hidden
        """
        Retrieve a feature flag.
        :param feature_flag_name: Name of the feature to fetch.
        :type feature_flag_name: str

        :param default_treatment: name of the default treatment to return
        :type default_treatment: str
        return: A feature flag object parsed from redis if the key exists. None otherwise

        :param change_number: change_number
        :rtype: splitio.models.splits.Split
        :type change_number: int
        """
        try:
            raw_feature_flags = None
            if self._enable_caching:
                raw_feature_flags = await self._feature_flag_cache.get_key(feature_flag_name)
            if raw_feature_flags is None:
                raw_feature_flags = await self.redis.get(self._get_key(feature_flag_name))
                if self._enable_caching:
                    await self._feature_flag_cache.add_key(feature_flag_name, raw_feature_flags)
                _LOGGER.debug("Fetchting feature flag [%s] from redis" % feature_flag_name)
                _LOGGER.debug(raw_feature_flags)
            return splits.from_raw(json.loads(raw_feature_flags)) if raw_feature_flags is not None else None

        except RedisAdapterException:
            _LOGGER.error('Error fetching feature flag from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    async def get_feature_flags_by_sets(self, flag_sets):
        """
        Retrieve feature flags by flag set.
        :param flag_set: Names of the flag set to fetch.
        :type flag_set: str
        :return: Feature flag names that are tagged with the flag set
        :rtype: listt(str)
        """
        try:
            sets_to_fetch = get_valid_flag_sets(flag_sets, self.flag_set_filter)
            if sets_to_fetch == []:
                return []

            keys = [self._get_flag_set_key(flag_set) for flag_set in sets_to_fetch]
            pipe = self._pipe()
            [pipe.smembers(key) for key in keys]
            result_sets = await pipe.execute()
            _LOGGER.debug("Fetchting Feature flags by set [%s] from redis" % (keys))
            _LOGGER.debug(result_sets)
            return list(combine_valid_flag_sets(result_sets))

        except RedisAdapterException:
            _LOGGER.error('Error fetching feature flag from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    async def fetch_many(self, feature_flag_names):
        """
        Retrieve feature flags.
        :param feature_flag_names: Names of the features to fetch.
        :type feature_flag_name: list(str)
        :return: A dict with feature flag objects parsed from redis.
        :rtype: dict(feature_flag_name, splitio.models.splits.Split)
        """
        to_return = dict()
        try:
            raw_feature_flags = None
            if self._enable_caching:
                raw_feature_flags = await self._feature_flag_cache.get_key(frozenset(feature_flag_names))
            if raw_feature_flags is None:
                raw_feature_flags = await self.redis.mget([self._get_key(feature_flag_name) for feature_flag_name in feature_flag_names])
                if self._enable_caching:
                    await self._feature_flag_cache.add_key(frozenset(feature_flag_names), raw_feature_flags)
            for i in range(len(feature_flag_names)):
                feature_flag = None
                try:
                    feature_flag = splits.from_raw(json.loads(raw_feature_flags[i]))
                except (ValueError, TypeError):
                    _LOGGER.error('Could not parse feature flag.')
                    _LOGGER.debug("Raw feature flag that failed parsing attempt: %s", raw_feature_flags[i])
                to_return[feature_flag_names[i]] = feature_flag
        except RedisAdapterException:
            _LOGGER.error('Error fetching feature flags from storage')
            _LOGGER.debug('Error: ', exc_info=True)
        return to_return

    async def is_valid_traffic_type(self, traffic_type_name):  # pylint: disable=method-hidden
        """
        Return whether the traffic type exists in at least one feature flag in cache.
        :param traffic_type_name: Traffic type to validate.
        :type traffic_type_name: str
        :return: True if the traffic type is valid. False otherwise.
        :rtype: bool
        """
        try:
            raw_traffic_type = None
            if self._enable_caching:
                raw_traffic_type = await self._traffic_type_cache.get_key(traffic_type_name)
            if raw_traffic_type is None:
                raw_traffic_type = await self.redis.get(self._get_traffic_type_key(traffic_type_name))
                if self._enable_caching:
                    await self._traffic_type_cache.add_key(traffic_type_name, raw_traffic_type)
            count = json.loads(raw_traffic_type) if raw_traffic_type else 0
            return count > 0

        except RedisAdapterException:
            _LOGGER.error('Error fetching traffic type from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return False

    async def get_change_number(self):
        """
        Retrieve latest feature flag change number.
        :rtype: int
        """
        try:
            stored_value = await self.redis.get(self._FEATURE_FLAG_TILL_KEY)
            return json.loads(stored_value) if stored_value is not None else None

        except RedisAdapterException:
            _LOGGER.error('Error fetching feature flag change number from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return None

    async def get_split_names(self):
        """
        Retrieve a list of all feature flag names.
        :return: List of feature flag names.
        :rtype: list(str)
        """
        try:
            keys = await self.redis.keys(self._get_key('*'))
            return [key.replace(self._get_key(''), '') for key in keys]

        except RedisAdapterException:
            _LOGGER.error('Error fetching feature flag names from storage')
            _LOGGER.debug('Error: ', exc_info=True)
            return []

    async def get_all_splits(self):
        """
        Return all the feature flags in cache.
        :return: List of all feature flags in cache.
        :rtype: list(splitio.models.splits.Split)
        """
        keys = await self.redis.keys(self._get_key('*'))
        to_return = []
        try:
            raw_feature_flags = await self.redis.mget(keys)
            for raw in raw_feature_flags:
                try:
                    to_return.append(splits.from_raw(json.loads(raw)))
                except (ValueError, TypeError):
                    _LOGGER.error('Could not parse feature flag. Skipping')
                    _LOGGER.debug("Raw feature flag that failed parsing attempt: %s", raw)
        except RedisAdapterException:
            _LOGGER.error('Error fetching all feature flags from storage')
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
        try:
            res = self._redis.sismember(self._get_key(segment_name), key)
            _LOGGER.debug("Checking Segment [%s] contain key [%s] in redis: %s" % (segment_name, key, res))
            return bool(res)
        except RedisAdapterException:
            _LOGGER.error('Error testing members in segment stored in redis')
            _LOGGER.debug('Error: ', exc_info=True)
            return False

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

    def record_config(self, config, extra_config, total_flag_sets, invalid_flag_sets):
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

    def record_config(self, config, extra_config, total_flag_sets, invalid_flag_sets):
        """
        initilize telemetry objects

        :param congif: factory configuration parameters
        :type config: splitio.client.config
        """
        self._tel_config.record_config(config, extra_config, total_flag_sets, invalid_flag_sets)

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

    @classmethod
    async def create(cls, redis_client, sdk_metadata):
        """
        Create instance and reset tags

        :param redis_client: Redis client or compliant interface.
        :type redis_client: splitio.storage.adapters.redis.RedisAdapter
        :param sdk_metadata: SDK & Machine information.
        :type sdk_metadata: splitio.client.util.SdkMetadata

        :return: self instance.
        :rtype: splitio.storage.redis.RedisTelemetryStorageAsync
        """
        self = cls()
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

    async def record_config(self, config, extra_config, total_flag_sets, invalid_flag_sets):
        """
        initilize telemetry objects

        :param congif: factory configuration parameters
        :type config: splitio.client.config
        """
        await self._tel_config.record_config(config, extra_config, total_flag_sets, invalid_flag_sets)

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
