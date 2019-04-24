"""Split partition module."""

from future.utils import python_2_unicode_compatible


class Partition(object):
    """Partition object class."""

    def __init__(self, treatment, size):
        """
        Class constructor.

        :param treatment: The treatment for the partition
        :type treatment: str
        :param size: A number between 0 a 100
        :type size: float
        """
        if size < 0 or size > 100:
            raise ValueError('size MUST BE between 0 and 100')

        self._treatment = treatment
        self._size = size

    @property
    def treatment(self):
        """Return the treatment associated with this partition."""
        return self._treatment

    @property
    def size(self):
        """Return the percentage owned by this partition."""
        return self._size

    def to_json(self):
        """Return a JSON representation of a partition."""
        return {
            'treatment': self._treatment,
            'size': self._size
        }

    @python_2_unicode_compatible
    def __str__(self):
        """Return string representation of a partition."""
        return '{size}%:{treatment}'.format(size=self._size,
                                            treatment=self._treatment)


def from_raw(raw_partition):
    """
    Build a partition object from a splitChanges partition portion.

    :param raw_partition: JSON snippet of a partition.
    :type raw_partition: dict

    :return: New partition object.
    :rtype: Partition
    """
    return Partition(raw_partition['treatment'], raw_partition['size'])
