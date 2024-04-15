import abc
import logging
import json

from splitio.storage.adapters.redis import RedisAdapterException

_LOGGER = logging.getLogger(__name__)
_MTK_QUEUE_KEY = 'SPLITIO.uniquekeys'
_MTK_KEY_DEFAULT_TTL = 3600
_IMP_COUNT_QUEUE_KEY = 'SPLITIO.impressions.count'
_IMP_COUNT_KEY_DEFAULT_TTL = 3600

class ImpressionsSenderAdapter(object, metaclass=abc.ABCMeta):
    """Impressions Sender Adapter interface."""

    @abc.abstractmethod
    def record_unique_keys(self, data):
        """
        No Return value

        """
        pass

class InMemorySenderAdapterBase(ImpressionsSenderAdapter):
    """In Memory Impressions Sender Adapter base class."""

    def record_unique_keys(self, uniques):
        """
        post the unique keys to split back end.

        :param uniques: unique keys disctionary
        :type uniques: Dictionary {'feature_flag1': set(), 'feature_flag2': set(), .. }
        """
        pass

    def _uniques_formatter(self, uniques):
        """
        Format the unique keys dictionary array to a JSON body

        :param uniques: unique keys disctionary
        :type uniques: Dictionary {'feature1_flag': set(), 'feature2_flag': set(), .. }

        :return: unique keys JSON array
        :rtype: json
        """
        return [{'f': feature, 'ks': list(keys)} for feature, keys in uniques.items()]

class InMemorySenderAdapter(InMemorySenderAdapterBase):
    """In Memory Impressions Sender Adapter class."""

    def __init__(self, telemtry_http_client):
        """
        Initialize In memory sender adapter instance

        :param telemtry_http_client: instance of telemetry http api
        :type telemtry_http_client: splitio.api.telemetry.TelemetryAPI
        """
        self._telemtry_http_client = telemtry_http_client

    def record_unique_keys(self, uniques):
        """
        post the unique keys to split back end.

        :param uniques: unique keys disctionary
        :type uniques: Dictionary {'feature_flag1': set(), 'feature_flag2': set(), .. }
        """
        if len(uniques) == 0:
            return

        self._telemtry_http_client.record_unique_keys({'keys': self._uniques_formatter(uniques)})


class InMemorySenderAdapterAsync(InMemorySenderAdapterBase):
    """In Memory Impressions Sender Adapter class."""

    def __init__(self, telemtry_http_client):
        """
        Initialize In memory sender adapter instance

        :param telemtry_http_client: instance of telemetry http api
        :type telemtry_http_client: splitio.api.telemetry.TelemetryAPI
        """
        self._telemtry_http_client = telemtry_http_client

    async def record_unique_keys(self, uniques):
        """
        post the unique keys to split back end.

        :param uniques: unique keys disctionary
        :type uniques: Dictionary {'feature_flag1': set(), 'feature_flag2': set(), .. }
        """
        await self._telemtry_http_client.record_unique_keys({'keys': self._uniques_formatter(uniques)})


class RedisSenderAdapter(ImpressionsSenderAdapter):
    """Redis Impressions Sender Adapter class."""

    def __init__(self, redis_client):
        """
        Initialize Redis sender adapter instance

        :param telemtry_http_client: instance of telemetry http api
        :type telemtry_http_client: splitio.api.telemetry.TelemetryAPI
        """
        self._redis_client = redis_client

    def record_unique_keys(self, uniques):
        """
        post the unique keys to redis.

        :param uniques: unique keys disctionary
        :type uniques: Dictionary {'feature_flag1': set(), 'feature_flag2': set(), .. }
        """
        if len(uniques) == 0:
            return

        bulk_mtks = _uniques_formatter(uniques)
        try:
            inserted = self._redis_client.rpush(_MTK_QUEUE_KEY, *bulk_mtks)
            self._expire_keys(_MTK_QUEUE_KEY, _MTK_KEY_DEFAULT_TTL, inserted, len(bulk_mtks))
            return True

        except RedisAdapterException:
            _LOGGER.error('Something went wrong when trying to add mtks to redis')
            _LOGGER.error('Error: ', exc_info=True)
            return False

    def flush_counters(self, to_send):
        """
        post the impression counters to redis.

        :param to_send: unique keys disctionary
        :type to_send: Dictionary {'feature_flag1': set(), 'feature_flag2': set(), .. }
        """
        if len(to_send) == 0:
            return

        try:
            resulted = 0
            counted = 0
            pipe = self._redis_client.pipeline()
            for pf_count in to_send:
                pipe.hincrby(_IMP_COUNT_QUEUE_KEY, pf_count.feature + "::" + str(pf_count.timeframe), pf_count.count)
                counted += pf_count.count
            resulted = sum(pipe.execute())
            self._expire_keys(_IMP_COUNT_QUEUE_KEY,
                              _IMP_COUNT_KEY_DEFAULT_TTL, resulted, counted)
            return True

        except RedisAdapterException:
            _LOGGER.error('Something went wrong when trying to add counters to redis')
            _LOGGER.error('Error: ', exc_info=True)
            return False

    def _expire_keys(self, queue_key, key_default_ttl, total_keys, inserted):
        """
        Set expire

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        if total_keys == inserted:
            self._redis_client.expire(queue_key, key_default_ttl)


class RedisSenderAdapterAsync(ImpressionsSenderAdapter):
    """In Redis Impressions Sender Adapter async class."""

    def __init__(self, redis_client):
        """
        Initialize Redis sender adapter instance

        :param telemtry_http_client: instance of telemetry http api
        :type telemtry_http_client: splitio.api.telemetry.TelemetryAPI
        """
        self._redis_client = redis_client

    async def record_unique_keys(self, uniques):
        """
        post the unique keys to redis.

        :param uniques: unique keys disctionary
        :type uniques: Dictionary {'feature_flag1': set(), 'feature_flag2': set(), .. }
        """
        bulk_mtks = _uniques_formatter(uniques)
        try:
            inserted = await self._redis_client.rpush(_MTK_QUEUE_KEY, *bulk_mtks)
            await self._expire_keys(_MTK_QUEUE_KEY, _MTK_KEY_DEFAULT_TTL, inserted, len(bulk_mtks))
            return True

        except RedisAdapterException:
            _LOGGER.error('Something went wrong when trying to add mtks to redis')
            _LOGGER.error('Error: ', exc_info=True)
            return False

    async def flush_counters(self, to_send):
        """
        post the impression counters to redis.

        :param to_send: unique keys disctionary
        :type to_send: Dictionary {'feature_flag1': set(), 'feature_flag2': set(), .. }
        """
        try:
            resulted = 0
            counted = 0
            pipe = self._redis_client.pipeline()
            for pf_count in to_send:
                pipe.hincrby(_IMP_COUNT_QUEUE_KEY, pf_count.feature + "::" + str(pf_count.timeframe), pf_count.count)
                counted += pf_count.count
            resulted = sum(await pipe.execute())
            await self._expire_keys(_IMP_COUNT_QUEUE_KEY,
                              _IMP_COUNT_KEY_DEFAULT_TTL, resulted, counted)
            return True

        except RedisAdapterException:
            _LOGGER.error('Something went wrong when trying to add counters to redis')
            _LOGGER.error('Error: ', exc_info=True)
            return False

    async def _expire_keys(self, queue_key, key_default_ttl, total_keys, inserted):
        """
        Set expire

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        if total_keys == inserted:
            await self._redis_client.expire(queue_key, key_default_ttl)


class PluggableSenderAdapter(ImpressionsSenderAdapter):
    """Pluggable Impressions Sender Adapter class."""

    def __init__(self, adapter_client, prefix=None):
        """
        Initialize pluggable sender adapter instance

        :param telemtry_http_client: instance of telemetry http api
        :type telemtry_http_client: splitio.api.telemetry.TelemetryAPI
        """
        self._adapter_client = adapter_client
        self._prefix = ""
        if prefix is not None:
            self._prefix = prefix + "."

    def record_unique_keys(self, uniques):
        """
        post the unique keys to storage.

        :param uniques: unique keys disctionary
        :type uniques: Dictionary {'feature_flag1': set(), 'feature_flag2': set(), .. }
        """
        if len(uniques) == 0:
            return

        bulk_mtks = _uniques_formatter(uniques)
        try:
            inserted = self._adapter_client.push_items(self._prefix + _MTK_QUEUE_KEY, *bulk_mtks)
            self._expire_keys(self._prefix + _MTK_QUEUE_KEY, _MTK_KEY_DEFAULT_TTL, inserted, len(bulk_mtks))
            return True

        except RedisAdapterException:
            _LOGGER.error('Something went wrong when trying to add mtks to storage adapter')
            _LOGGER.error('Error: ', exc_info=True)
            return False

    def flush_counters(self, to_send):
        """
        post the impression counters to storage.

        :param to_send: unique keys disctionary
        :type to_send: Dictionary {'feature_flag1': set(), 'feature_flag2': set(), .. }
        """
        if len(to_send) == 0:
            return
        try:
            resulted = 0
            for pf_count in to_send:
                key = self._prefix + _IMP_COUNT_QUEUE_KEY + "." + pf_count.feature + "::" + str(pf_count.timeframe)
                resulted = self._adapter_client.increment(key, pf_count.count)
                self._expire_keys(key, _IMP_COUNT_KEY_DEFAULT_TTL, resulted, pf_count.count)
            return True

        except RedisAdapterException:
            _LOGGER.error('Something went wrong when trying to add counters to storage adapter')
            _LOGGER.error('Error: ', exc_info=True)
            return False

    def _expire_keys(self, queue_key, key_default_ttl, total_keys, inserted):
        """
        Set expire

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        if total_keys == inserted:
            self._adapter_client.expire(queue_key, key_default_ttl)


class PluggableSenderAdapterAsync(ImpressionsSenderAdapter):
    """Pluggable Impressions Sender Adapter class."""

    def __init__(self, adapter_client, prefix=None):
        """
        Initialize pluggable sender adapter instance

        :param telemtry_http_client: instance of telemetry http api
        :type telemtry_http_client: splitio.api.telemetry.TelemetryAPI
        """
        self._adapter_client = adapter_client
        self._prefix = ""
        if prefix is not None:
            self._prefix = prefix + "."

    async def record_unique_keys(self, uniques):
        """
        post the unique keys to storage.

        :param uniques: unique keys disctionary
        :type uniques: Dictionary {'feature_flag1': set(), 'feature_flag2': set(), .. }
        """
        bulk_mtks = _uniques_formatter(uniques)
        try:
            inserted = await self._adapter_client.push_items(self._prefix + _MTK_QUEUE_KEY, *bulk_mtks)
            await self._expire_keys(self._prefix + _MTK_QUEUE_KEY, _MTK_KEY_DEFAULT_TTL, inserted, len(bulk_mtks))
            return True

        except RedisAdapterException:
            _LOGGER.error('Something went wrong when trying to add mtks to storage adapter')
            _LOGGER.error('Error: ', exc_info=True)
            return False

    async def flush_counters(self, to_send):
        """
        post the impression counters to storage.

        :param to_send: unique keys disctionary
        :type to_send: Dictionary {'feature_flag1': set(), 'feature_flag2': set(), .. }
        """
        try:
            resulted = 0
            for pf_count in to_send:
                key = self._prefix + _IMP_COUNT_QUEUE_KEY + "." + pf_count.feature + "::" + str(pf_count.timeframe)
                resulted = await self._adapter_client.increment(key, pf_count.count)
                await self._expire_keys(key, _IMP_COUNT_KEY_DEFAULT_TTL, resulted, pf_count.count)
            return True

        except RedisAdapterException:
            _LOGGER.error('Something went wrong when trying to add counters to storage adapter')
            _LOGGER.error('Error: ', exc_info=True)
            return False

    async def _expire_keys(self, queue_key, key_default_ttl, total_keys, inserted):
        """
        Set expire

        :param total_keys: length of keys.
        :type total_keys: int
        :param inserted: added keys.
        :type inserted: int
        """
        if total_keys == inserted:
            await self._adapter_client.expire(queue_key, key_default_ttl)

def _uniques_formatter(uniques):
    """
    Format the unique keys dictionary array to a JSON body

    :param uniques: unique keys disctionary
    :type uniques: Dictionary {'feature_flag1': set(), 'feature_flag2': set(), .. }

    :return: unique keys JSON array
    :rtype: json
    """
    return [json.dumps({'f': feature, 'ks': list(keys)}) for feature, keys in uniques.items()]
