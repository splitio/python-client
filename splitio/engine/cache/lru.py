"""Simple test-and-set LRU Cache."""
import threading


DEFAULT_MAX_SIZE = 5000


class SimpleLruCache(object):  # pylint: disable=too-many-instance-attributes
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

        def __init__(self, key, value, previous_element, next_element):
            """Class constructor."""
            self.key = key  # we also keep the key for O(1) access when removing the LRU.
            self.value = value
            self.previous = previous_element
            self.next = next_element

        def __str__(self):
            """Return string representation."""
            return '(%s, %s)' % (self.key, self.value)

    def __init__(self, max_size=DEFAULT_MAX_SIZE):
        """Class constructor."""
        self._data = {}
        self._lock = threading.Lock()
        self._max_size = max_size
        self._lru = None
        self._mru = None

    def test_and_set(self, key, value):
        """
        Set an item in the cache and return the previous value.

        :param key: object key
        :type args: object
        :param value: object value
        :type kwargs: object

        :return: previous value if any. None otherwise
        :rtype: object
        """
        with self._lock:
            node = self._data.get(key)
            to_return = node.value if node else None
            if node is None:
                node = SimpleLruCache._Node(key, value, None, None)
            node = self._bubble_up(node)
            self._data[key] = node
            self._rollover()
            return to_return

    def clear(self):
        """Clear the cache."""
        self._data = {}
        self._lru = None
        self._mru = None

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
