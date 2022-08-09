import abc
import threading
import logging
from splitio.engine.filters.bloom_filter import BloomFilter
from splitio.engine.sender_adapters.in_memory_sender_adapter import InMemorySenderAdapter

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
        if self._filter.contains(feature_name+key):
            return False

        with self._lock:
            self._add_or_update(feature_name, key)
            self._filter.add(feature_name+key)

        if self._get_dict_size() > self._cache_size:
            if self._queue_full_hook is not None and callable(self._queue_full_hook):
                self._queue_full_hook()
            _LOGGER.info(
                'Unique Keys queue is full, flushing the current queue now.'
            )
        return True

    def _get_dict_size(self):
        """
        Return the size of unique keys dictionary (number of keys in all features)

        :return: dictionary set() items count
        :rtype: int
        """
        total_size = 0
        for key in self._uniqe_keys_tracker._cache:
            total_size = total_size + len(self._uniqe_keys_tracker._cache[key])
        return total_size

    def _add_or_update(self, feature_name, key):
        """
        Add the feature_name+key to both bloom filter and dictionary.

        :param feature_name: split name associated with the key
        :type feature_name: str
        :param key: key to be added to MTK list
        :type key: int
        """
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
