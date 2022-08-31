import abc
import logging
import json

from splitio.storage.adapters.redis import RedisAdapterException

_LOGGER = logging.getLogger(__name__)

class ImpressionsSenderAdapter(object, metaclass=abc.ABCMeta):
    """Impressions Sender Adapter interface."""

    @abc.abstractmethod
    def record_unique_keys(self, data):
        """
        No Return value

        """
        pass

class InMemorySenderAdapter(ImpressionsSenderAdapter):
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
        :type uniques: Dictionary {'feature1': set(), 'feature2': set(), .. }
        """
        self._telemtry_http_client.record_unique_keys({'keys': self._uniques_formatter(uniques)})

    def _uniques_formatter(self, uniques):
        """
        Format the unique keys dictionary array to a JSON body

        :param uniques: unique keys disctionary
        :type uniques: Dictionary {'feature1': set(), 'feature2': set(), .. }

        :return: unique keys JSON array
        :rtype: json
        """
        return [{'f': feature, 'ks': list(keys)} for feature, keys in uniques.items()]

class RedisSenderAdapter(ImpressionsSenderAdapter):
    """In Memory Impressions Sender Adapter class."""

    MTK_QUEUE_KEY = 'SPLITIO.uniquekeys'
    MTK_KEY_DEFAULT_TTL = 3600

    def __init__(self, redis_client):
        """
        Initialize In memory sender adapter instance

        :param telemtry_http_client: instance of telemetry http api
        :type telemtry_http_client: splitio.api.telemetry.TelemetryAPI
        """
        self._redis_client = redis_client

    def record_unique_keys(self, uniques):
        """
        post the unique keys to split back end.

        :param uniques: unique keys disctionary
        :type uniques: Dictionary {'feature1': set(), 'feature2': set(), .. }
        """
        bulk_mtks = self._uniques_formatter(uniques)
        try:
            self._redis_client.rpush(self.MTK_QUEUE_KEY, *bulk_mtks)
            self._redis_client.expire(self.MTK_QUEUE_KEY, self.MTK_KEY_DEFAULT_TTL)
            return True
        except RedisAdapterException:
            _LOGGER.error('Something went wrong when trying to add mtks to redis')
            _LOGGER.error('Error: ', exc_info=True)
            return False

    def _uniques_formatter(self, uniques):
        """
        Format the unique keys dictionary array to a JSON body

        :param uniques: unique keys disctionary
        :type uniques: Dictionary {'feature1': set(), 'feature2': set(), .. }

        :return: unique keys JSON array
        :rtype: json
        """
        return [json.dumps({'f': feature, 'ks': list(keys)}) for feature, keys in uniques.items()]
