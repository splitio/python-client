from __future__ import absolute_import, division, print_function, \
    unicode_literals


def as_int32(value):
    if not -2147483649 <= value <= 2147483648:
        return (value + 2147483648) % 4294967296 - 2147483648
    return value


def legacy_hash(key, seed):
    """
    Generates a hash for a key and a feature seed.
    :param key: The key for which to get the hash
    :type key: str
    :param seed: The feature seed
    :type seed: int
    :return: The hash for the key and seed
    :rtype: int
    """
    h = 0

    for c in map(ord, key):
        h = as_int32(as_int32(31 * as_int32(h)) + c)

    return int(as_int32(h ^ as_int32(seed)))
