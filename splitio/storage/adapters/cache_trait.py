"""Caching trait module."""

import threading
import time


class ElementExpiredException(Exception):
    """Exception to be thrown when an element requested is present but expired."""

    pass


class ElementNotPresentException(Exception):
    """Exception to be thrown when an element requested is not present."""

    pass


class LocalMemoryCache(object):
    """Key/Value local memory cache. with deprecation."""

    def __init__(self, max_age_seconds=5):
        """Class constructor."""
        self._data = {}
        self._lock = threading.RLock()
        self._max_age_seconds = max_age_seconds

    def set(self, key, value):
        """
        Set a key/value pair.

        :param key: Key used to reference the value.
        :type key: str
        :param value: Value to store.
        :type value: object
        """
        with self._lock:
            self._data[key] = (value, time.time())

    def get(self, key):
        """
        Attempt to get a value based on a key.

        :param key: Key associated with the value.
        :type key: str

        :return: The value associated with the key. None if it doesn't exist.
        :rtype: object
        """
        try:
            value, set_time = self._data[key]
        except KeyError:
            raise ElementNotPresentException('Element %s not present in local storage' % key)

        if (time.time() - set_time) > self._max_age_seconds:
            raise ElementExpiredException('Element %s present but expired' % key)

        return value
