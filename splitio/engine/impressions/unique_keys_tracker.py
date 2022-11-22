import abc
import threading
import logging
from splitio.engine.filters import BloomFilter

_LOGGER = logging.getLogger(__name__)

class BaseUniqueKeysTracker(object, metaclass=abc.ABCMeta):
    """Unique Keys Tracker interface."""

    @abc.abstractmethod
    def track(self, key, feature_name):
        """
        Return a boolean flag

        """
        pass

class UniqueKeysTracker(BaseUniqueKeysTracker):
    """Unique Keys Tracker class."""

    def __init__(self, cache_size=30000):
        """
        Initialize unique keys tracker instance

        :param cache_size: The size of the unique keys dictionary
        :type key: int
        """
        self._cache_size = cache_size
        self._filter = BloomFilter(cache_size)
        self._lock = threading.RLock()
        self._cache = {}
        self._queue_full_hook = None
        self._current_cache_size = 0

    def track(self, key, feature_name):
        """
        Return a boolean flag

        :param key: key to be added to MTK list
        :type key: int
        :param feature_name: split name associated with the key
        :type feature_name: str

        :return: True if successful
        :rtype: boolean
        """
        with self._lock:
            if self._filter.contains(feature_name+key):
                return False
            self._add_or_update(feature_name, key)
            self._filter.add(feature_name+key)
            self._current_cache_size = self._current_cache_size + 1

        if self._current_cache_size > self._cache_size:
            _LOGGER.info(
                'Unique Keys queue is full, flushing the current queue now.'
            )
            if self._queue_full_hook is not None and callable(self._queue_full_hook):
                _LOGGER.info('Calling hook.')
                self._queue_full_hook()
        return True

    def _add_or_update(self, feature_name, key):
        """
        Add the feature_name+key to both bloom filter and dictionary.

        :param feature_name: split name associated with the key
        :type feature_name: str
        :param key: key to be added to MTK list
        :type key: int
        """

        with self._lock:
            if feature_name not in self._cache:
                self._cache[feature_name] = set()
            self._cache[feature_name].add(key)

    def set_queue_full_hook(self, hook):
        """
        Set a hook to be called when the queue is full.

        :param h: Hook to be called when the queue is full
        """
        if callable(hook):
            self._queue_full_hook = hook

    def clear_filter(self):
        """
        Delete the filter items

        """
        with self._lock:
            self._filter.clear()

    def get_cache_info_and_pop_all(self):
        with self._lock:
            temp_cach = self._cache
            temp_cache_size = self._current_cache_size
            self._cache = {}
            self._current_cache_size = 0

            return temp_cach, temp_cache_size