import threading
import logging
from splitio.engine.filters.bloom_filter import BloomFilter
from splitio.engine.sender_adapters.in_memory_sender_adapter import InMemorySenderAdapter

_LOGGER = logging.getLogger(__name__)

class UniqueKeysSynchronizer(object):
    """Unique Keys Synchronizer class."""

    def __init__(self, uniqe_keys_tracker = None):
        """
        Initialize Unique keys synchronizer instance

        :param uniqe_keys_tracker: instance of uniqe keys tracker
        :type uniqe_keys_tracker: splitio.engine.uniqur_key_tracker.UniqueKeysTracker
        """
        self._uniqe_keys_tracker = uniqe_keys_tracker
        self._lock = threading.RLock()

    def SendAll(self):
        """
        Flush the unique keys dictionary to split back end.
        Limit each post to the max_bulk_size value.

        """
        cache_size = self._uniqe_keys_tracker._get_dict_size()
        if cache_size <= self._max_bulk_size:
            self._uniqe_keys_tracker._impressions_sender_adapter.record_unique_keys(self._uniqe_keys_tracker._cache)
        else:
            for bulk in self._split_cache_to_bulks():
                self._uniqe_keys_tracker._impressions_sender_adapter.record_unique_keys(bulk)

        with self._lock:
            self._uniqe_keys_tracker._cache = {}

    def _split_cache_to_bulks(self):
        """
        Split the current unique keys dictionary into seperate dictionaries,
        each with the size of max_bulk_size. Overflow the last feature set() to new unique keys dictionary.

        :return: array of unique keys dictionaries
        :rtype: [Dict{'feature1': set(), 'feature2': set(), .. }]
        """
        bulks = []
        bulk = {}
        total_size = 0
        for feature in self._uniqe_keys_tracker._cache:
            total_size = total_size + len(self._uniqe_keys_tracker._cache[feature])
            if total_size > self._max_bulk_size:
                bulk[feature] = set()
                cnt = 1
                new_set = set()
                for key in self._uniqe_keys_tracker._cache[feature]:
                    if cnt < (total_size - self._max_bulk_size):
                        bulk[key].add(key)
                    else:
                        new_set.add(key)
                    cnt = cnt + 1
                bulks.append(bulk)
                bulk = {}
                bulk[feature] = new_set
                total_size = 0
            else:
                bulk[feature] = self._uniqe_keys_tracker._cache[feature]
        if total_size != 0:
            bulks.append(bulk)

        return bulks

class ClearFilterSynchronizer(object):
    """Clear filter class."""

    def __init__(self, uniqe_keys_tracker = None):
        """
        Initialize Unique keys synchronizer instance

        :param uniqe_keys_tracker: instance of uniqe keys tracker
        :type uniqe_keys_tracker: splitio.engine.uniqur_key_tracker.UniqueKeysTracker
        """
        self._uniqe_keys_tracker = uniqe_keys_tracker
        self._lock = threading.RLock()

    def clearAll(self):
        """
        Clear the bloom filter cache

        """
        with self._lock:
            self._uniqe_keys_tracker._filter.clear()
