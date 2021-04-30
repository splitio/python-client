"""Legacy hash function module."""


def as_int32(value):
    """Handle overflow when working with 32 lower bits of 64 bit ints."""
    if not -2147483649 <= value <= 2147483648:
        return (value + 2147483648) % 4294967296 - 2147483648
    return value


def legacy_hash(key, seed):
    """
    Generate a hash for a key and a feature seed.

    :param key: The key for which to get the hash
    :type key: str
    :param seed: The feature seed
    :type seed: int
    :return: The hash for the key and seed
    :rtype: int
    """
    current_hash = 0

    for char in map(ord, key):
        current_hash = as_int32(as_int32(31 * as_int32(current_hash)) + char)

    return int(as_int32(current_hash ^ as_int32(seed)))
