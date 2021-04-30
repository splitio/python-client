"""Caching trait module."""

import threading
import time
from functools import update_wrapper


DEFAULT_MAX_AGE = 5
DEFAULT_MAX_SIZE = 100


class LocalMemoryCache(object):  # pylint: disable=too-many-instance-attributes
    """
    Key/Value local memory cache. with expiration & LRU eviction.

    LRU double-linked-list format:

    {
        'key1'---------------------------------------------------------------
        'key2'------------------------------------                           |
        'key3'------------                        |                          |
    }                     |                       |                          |
                          V                       V                          V
                     || MRU  ||  -previous-> ||   X  || ... -previous-> || LRU  || -previous-> None
    None <---next--- || node ||  <---next--- || node || ... <---next--- || node ||
    """

    class _Node(object):  # pylint: disable=too-few-public-methods
        """Links to previous an next items in the circular list."""

        def __init__(self, key, value, last_update, previous_element, next_element):  # pylint: disable=too-many-arguments
            """Class constructor."""
            self.key = key  # we also keep the key for O(1) access when removing the LRU.
            self.value = value
            self.last_update = last_update
            self.previous = previous_element
            self.next = next_element

        def __str__(self):
            """Return string representation."""
            return '(%s, %s)' % (self.key, self.value)

    def __init__(
            self,
            key_func,
            user_func,
            max_age_seconds=DEFAULT_MAX_AGE,
            max_size=DEFAULT_MAX_SIZE
    ):
        """Class constructor."""
        self._data = {}
        self._lock = threading.Lock()
        self._max_age_seconds = max_age_seconds
        self._max_size = max_size
        self._lru = None
        self._mru = None
        self._key_func = key_func
        self._user_func = user_func

    def get(self, *args, **kwargs):
        """
        Fetch an item from the cache. If it's a miss, call user function to refill.

        :param args: User supplied positional arguments
        :type args: list
        :param kwargs: User supplied keyword arguments
        :type kwargs: dict

        :return: Cached/Fetched object
        :rtype: object
        """
        with self._lock:
            key = self._key_func(*args, **kwargs)
            node = self._data.get(key)
            if node is not None:
                if self._is_expired(node):
                    node.value = self._user_func(*args, **kwargs)
                    node.last_update = time.time()
            else:
                value = self._user_func(*args, **kwargs)
                node = LocalMemoryCache._Node(key, value, time.time(), None, None)
            node = self._bubble_up(node)
            self._data[key] = node
            self._rollover()
            return node.value

    def remove_expired(self):
        """Remove expired elements."""
        with self._lock:
            self._data = {
                key: value for (key, value) in self._data.items()
                if not self._is_expired(value)
            }

    def clear(self):
        """Clear the cache."""
        self._data = {}
        self._lru = None
        self._mru = None

    def _is_expired(self, node):
        """Return whether the data held by the node is expired."""
        return time.time() - self._max_age_seconds > node.last_update

    def _bubble_up(self, node):
        """Send node to the top of the list (mark it as the MRU)."""
        if node is None:
            return None

        # First item, just set lru & mru
        if not self._data:
            self._lru = node
            self._mru = node
            return node

        # MRU, just return it
        if node is self._mru:
            return node

        # LRU, update pointer and end-of-list
        if node is self._lru:
            self._lru = node.next
            self._lru.previous = None

        if node.previous is not None:
            node.previous.next = node.next
        if node.next is not None:
            node.next.previous = node.previous

        node.previous = self._mru
        node.previous.next = node
        node.next = None
        self._mru = node

        return node

    def _rollover(self):
        """Check we're within the size limit. Otherwise drop the LRU."""
        if len(self._data) > self._max_size:
            next_item = self._lru.next
            del self._data[self._lru.key]
            self._lru = next_item
            self._lru.previous = None

    def __str__(self):
        """User friendly representation of cache."""
        nodes = []
        node = self._mru
        while node is not None:
            nodes.append('\t<%s: %s>  -->' % (node.key, node.value))
            node = node.previous
        return '<MRU>\n' + '\n'.join(nodes) + '\n<LRU>'


def decorate(key_func, max_age_seconds=DEFAULT_MAX_AGE, max_size=DEFAULT_MAX_SIZE):
    """
    Decorate a function or method to cache results up  to `max_age_seconds`.

    :param key_func: user specified function to execute over the arguments to determine the key.
    :type key_func: callable
    :param max_age_seconds: Maximum number of seconds during which the cached value is valid.
    :type max_age_seconds: int

    :return: Decorating function wrapper.
    :rtype: callable
    """
    if max_age_seconds < 0:
        raise TypeError('Max cache age cannot be a negative number.')

    if max_size < 0:
        raise TypeError('Max cache size cannot be a negative number.')

    if max_age_seconds == 0 or max_size == 0:
        return lambda function: function  # bypass cache overlay.

    def _decorator(user_function):
        """
        Decorate function to be used with `@` syntax.

        :param user_function: Function to decorate with cacheable results
        :type user_function: callable

        :return: A function that looks exactly the same but with cacheable results.
        :rtype: callable
        """
        _cache = LocalMemoryCache(key_func, user_function, max_age_seconds, max_size)
        # The lambda below IS necessary, otherwise update_wrapper fails since the function
        # is an instance method and has no reference to the __module__ namespace.
        wrapper = lambda *args, **kwargs: _cache.get(*args, **kwargs)  # pylint: disable=unnecessary-lambda
        return update_wrapper(wrapper, user_function)

    return _decorator
