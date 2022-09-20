import abc
import threading

from bloom_filter2 import BloomFilter as BloomFilter2

class BaseFilter(object, metaclass=abc.ABCMeta):
    """Impressions Filter interface."""

    @abc.abstractmethod
    def add(self, data):
        """
        Return a boolean flag

        """
        pass

    @abc.abstractmethod
    def contains(self, data):
        """
        Return a boolean flag

        """
        pass

    @abc.abstractmethod
    def clear(self):
        """
        No return

        """
        pass

class BloomFilter(BaseFilter):
    """Optimized mode strategy."""

    def __init__(self, max_elements=5000, error_rate=0.01):
        """
        Construct a bloom filter instance.

        :param max_element: maximum elements in the filter
        :type string:

        :param error_rate: error rate for the false positives, reduce it will consume more memory
        :type numeric:
        """
        self._max_elements = max_elements
        self._error_rate = error_rate
        self._imps_bloom_filter = BloomFilter2(max_elements=self._max_elements, error_rate=self._error_rate)
        self._lock = threading.RLock()

    def add(self, data):
        """
        Add an item to the bloom filter instance.

        :param data: element to be added
        :type string:

        :return: True if successful
        :rtype: boolean
        """
        with self._lock:
            self._imps_bloom_filter.add(data)
            return data in self._imps_bloom_filter

    def contains(self, data):
        """
        Check if an item exist in the bloom filter instance.

        :param data: element to be checked
        :type string:

        :return: True if exist
        :rtype: boolean
        """
        with self._lock:
            return data in self._imps_bloom_filter

    def clear(self):
        """
        Destroy the current filter instance and create new one.

        """
        with self._lock:
            self._imps_bloom_filter.close()
            self._imps_bloom_filter = BloomFilter2(max_elements=self._max_elements, error_rate=self._error_rate)
