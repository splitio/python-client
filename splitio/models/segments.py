"""Segment module."""


class Segment(object):
    """Segment object class."""

    def __init__(self, name, keys, change_number):
        """
        Class constructor.

        :param name: Segment name.
        :type name: str

        :param keys: List of keys belonging to the segment.
        :type keys: List
        """
        self._name = name
        self._keys = set(keys)
        self._change_number = change_number

    @property
    def name(self):
        """Return segment name."""
        return self._name

    def contains(self, key):
        """
        Return whether the supplied key belongs to the segment.

        :param key: User key.
        :type key: str

        :return: True if the user is in the segment. False otherwise.
        :rtype: bool
        """
        return key in self._keys

    def update(self, to_add, to_remove):
        """
        Add supplied keys to the segment.

        :param to_add: List of keys to add.
        :type to_add: list
        :param to_remove: List of keys to remove.
        :type to_remove: list
        """
        self._keys = self._keys.union(set(to_add)).difference(to_remove)

    @property
    def keys(self):
        """
        Return the segment keys.

        :return: A set of the segment keys
        :rtype: set
        """
        return self._keys

    @property
    def change_number(self):
        """Return segment change number."""
        return self._change_number

    @change_number.setter
    def change_number(self, new_value):
        """
        Set new change number.

        :param new_value: New change number.
        :type new_value: int
        """
        self._change_number = new_value


def from_raw(raw_segment):
    """
    Parse a new segment from a raw segment_changes response.

    :param raw_segment: Segment parsed from segment changes response.
    :type raw_segment: dict

    :return: New segment model object
    :rtype: splitio.models.segment.Segment
    """
    keys = set(raw_segment['added']).difference(raw_segment['removed'])
    return Segment(raw_segment['name'], keys, raw_segment['till'])
