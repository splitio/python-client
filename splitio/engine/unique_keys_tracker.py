import abc
import threading
import logging
from splitio.engine.filters.bloom_filter import BloomFilter

_LOGGER = logging.getLogger(__name__)

class BaseUniqueKeysTracker(object, metaclass=abc.ABCMeta):
    """Unique Keys Tracker interface."""

    @abc.abstractmethod
    def track(self, key, feature_name):
        """
        Return a boolean flag

        """
        pass

    @abc.abstractmethod
    def start(self):
        """
        No return value

        """
        pass

    @abc.abstractmethod
    def stop(self):
        """
        No return value

        """
        pass

class UniqueKeysTracker(BaseUniqueKeysTracker):
    """Unique Keys Tracker class."""

    def __init__(self, cache_size=30000, max_bulk_size=5000, task_refresh_rate = 24):
        self._cache_size = cache_size
        self._max_bulk_size = max_bulk_size
        self._task_refresh_rate = task_refresh_rate
        self._filter = BloomFilter(cache_size)
        self._lock = threading.RLock()
        self._cache = {}
        # TODO: initialize impressions sender adapter and task referesh rate in next PR

    def track(self, key, feature_name):
        """
        Return a boolean flag

        """
        if self._filter.contains(feature_name+key):
            return False

        with self._lock:
            self._add_or_update(feature_name, key)
            self._filter.add(feature_name+key)

        if len(self._cache[feature_name]) == self._cache_size:
            _LOGGER.warn("MTK Cache size for Split [%s] has reach maximum unique keys [%d], flushing data now.", feature_name, self._cache_size)
#            TODO: Flush the data and reset split cache in next PR
        if self._get_dict_size() >= self._max_bulk_size:
            _LOGGER.info("Bulk MTK cache size has reach maximum, flushing data now.")
#            TODO: Flush the data and reset split cache in next PR

        return True

    def _get_dict_size(self):
        total_size = 0
        for key in self._cache:
            total_size = total_size + len(self._cache[key])
        return total_size

    def _add_or_update(self, feature_name, key):
        if feature_name not in self._cache:
            self._cache[feature_name] = set()
        self._cache[feature_name].add(key)

    def start(self):
        """
        TODO: Add start posting impressions job in next PR

        """

    def stop(self):
        """
        TODO: Add stop posting impressions job in next PR

        """
