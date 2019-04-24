"""UWSGI Cache Storage adapter module."""

from __future__ import absolute_import, division, print_function, unicode_literals
import time

try:
    #uwsgi is loaded at runtime by uwsgi app.
    import uwsgi
except ImportError:
    def missing_uwsgi_dependencies(*args, **kwargs):  #pylint: disable=unused-argument
        """Only complain for missing deps if they're used."""
        raise NotImplementedError('Missing uWSGI support dependencies.')
    uwsgi = missing_uwsgi_dependencies

# Cache used for locking & signaling keys
_SPLITIO_LOCK_CACHE_NAMESPACE = 'splitio_locks'

# Cache where split definitions are stored
_SPLITIO_SPLITS_CACHE_NAMESPACE = 'splitio_splits'

# Cache where segments are stored
_SPLITIO_SEGMENTS_CACHE_NAMESPACE = 'splitio_segments'

# Cache where impressions are stored
_SPLITIO_IMPRESSIONS_CACHE_NAMESPACE = 'splitio_impressions'

# Cache where metrics are stored
_SPLITIO_METRICS_CACHE_NAMESPACE = 'splitio_metrics'

# Cache where events are stored (1 key with lots of blocks)
_SPLITIO_EVENTS_CACHE_NAMESPACE = 'splitio_events'

# Cache where changeNumbers are stored
_SPLITIO_CHANGE_NUMBERS = 'splitio_changeNumbers'

# Cache with a big block size used for lists
_SPLITIO_MISC_NAMESPACE = 'splitio_misc'


class UWSGILock(object):
    """Context manager to be used for locking a key in the cache."""

    def __init__(self, adapter, key, overwrite_lock_seconds=5):
        """
        Initialize a lock with the key `key` and waits up to `overwrite_lock_seconds` to release.

        :param key: Key to be used.
        :type key: str

        :param overwrite_lock_seconds: How many seconds to wait before force-releasing.
        :type overwrite_lock_seconds: int
        """
        self._key = key
        self._overwrite_lock_seconds = overwrite_lock_seconds
        self._uwsgi = adapter

    def __enter__(self):
        """Loop until the lock is manually released or timeout occurs."""
        initial_time = time.time()
        while True:
            if not self._uwsgi.cache_exists(self._key, _SPLITIO_LOCK_CACHE_NAMESPACE):
                self._uwsgi.cache_set(self._key, str('locked'), 0, _SPLITIO_LOCK_CACHE_NAMESPACE)
                return
            else:
                if time.time() - initial_time > self._overwrite_lock_seconds:
                    return
            time.sleep(0.1)

    def __exit__(self, *args):
        """Remove lock."""
        self._uwsgi.cache_del(self._key, _SPLITIO_LOCK_CACHE_NAMESPACE)


class UWSGICacheEmulator(object):
    """UWSGI mock."""

    def __init__(self):
        """
        UWSGI Cache Emulator for unit tests. Implements uwsgi cache framework interface.

        http://uwsgi-docs.readthedocs.io/en/latest/Caching.html#accessing-the-cache-from-your-applications-using-the-cache-api
        """
        self._cache = dict()

    @staticmethod
    def _check_string_data_type(value):
        if type(value).__name__ == 'str':
            return True
        raise TypeError(
            'The value to add into uWSGI cache must be string and %s given' % type(value).__name__
        )

    def cache_get(self, key, cache_namespace='default'):
        """Get an element from cache."""
        if self.cache_exists(key, cache_namespace):
            return self._cache[cache_namespace][key]
        return None

    def cache_set(self, key, value, expires=0, cache_namespace='default'):  #pylint: disable=unused-argument
        """Set an elemen in the cache."""
        self._check_string_data_type(value)

        if cache_namespace in self._cache:
            self._cache[cache_namespace][key] = value
        else:
            self._cache[cache_namespace] = {key:value}

    def cache_update(self, key, value, expires=0, cache_namespace='default'):
        """Update an element."""
        self.cache_set(key, value, expires, cache_namespace)

    def cache_exists(self, key, cache_namespace='default'):
        """Return whether the element exists."""
        if cache_namespace in self._cache:
            if key in self._cache[cache_namespace]:
                return True
        return False

    def cache_del(self, key, cache_namespace='default'):
        """Delete an item from the cache."""
        if cache_namespace in self._cache:
            self._cache[cache_namespace].pop(key, None)

    def cache_clear(self, cache_namespace='default'):
        """Delete all elements in cache."""
        self._cache.pop(cache_namespace, None)


def get_uwsgi(emulator=False):
    """Return a uwsgi imported module or an emulator to use in unit test."""
    if emulator:
        return UWSGICacheEmulator()

    return uwsgi
