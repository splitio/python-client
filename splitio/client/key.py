"""A module for Split.io SDK API clients."""
from __future__ import absolute_import, division, print_function, \
    unicode_literals


class Key(object):
    """Key class includes a matching key and bucketing key."""

    def __init__(self, matching_key, bucketing_key):
        """Construct a key object."""
        self._matching_key = matching_key
        self._bucketing_key = bucketing_key

    @property
    def matching_key(self):
        """Return matching key."""
        return self._matching_key

    @property
    def bucketing_key(self):
        """Return bucketing key."""
        return self._bucketing_key
