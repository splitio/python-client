"""MurmurHash3 hash module."""

from __future__ import absolute_import, division, print_function, \
    unicode_literals

from six.moves import range


def murmur32_py(key, seed=0x0):
    """
    Pure python implementation of murmur32 hash.

    :param key: Key to hash
    :type key: str
    :param seed: Seed to use when hashing
    :type seed: int

    :return: hashed value
    :rtype: int

    """
    key = bytearray(key, 'utf-8')

    def fmix(current_hash):
        """Mix has bytes."""
        current_hash ^= current_hash >> 16
        current_hash = (current_hash * 0x85ebca6b) & 0xFFFFFFFF
        current_hash ^= current_hash >> 13
        current_hash = (current_hash * 0xc2b2ae35) & 0xFFFFFFFF
        current_hash ^= current_hash >> 16
        return current_hash

    length = len(key)
    nblocks = int(length/4)

    hash1 = seed & 0xFFFFFFFF

    calc1 = 0xcc9e2d51
    calc2 = 0x1b873593

    # body
    for block_start in range(0, nblocks * 4, 4):
        # ??? big endian?
        key1 = key[block_start + 3] << 24 | \
             key[block_start + 2] << 16 | \
             key[block_start + 1] << 8 | \
             key[block_start + 0]

        key1 = (calc1 * key1) & 0xFFFFFFFF
        key1 = (key1 << 15 | key1 >> 17) & 0xFFFFFFFF  # inlined ROTL32
        key1 = (calc2 * key1) & 0xFFFFFFFF

        hash1 ^= key1
        hash1 = (hash1 << 13 | hash1 >> 19) & 0xFFFFFFFF  # inlined ROTL32
        hash1 = (hash1 * 5 + 0xe6546b64) & 0xFFFFFFFF

    # tail
    tail_index = nblocks * 4
    key1 = 0
    tail_size = length & 3

    if tail_size >= 3:
        key1 ^= key[tail_index + 2] << 16
    if tail_size >= 2:
        key1 ^= key[tail_index + 1] << 8
    if tail_size >= 1:
        key1 ^= key[tail_index + 0]

    if tail_size > 0:
        key1 = (key1 * calc1) & 0xFFFFFFFF
        key1 = (key1 << 15 | key1 >> 17) & 0xFFFFFFFF  # inlined ROTL32
        key1 = (key1 * calc2) & 0xFFFFFFFF
        hash1 ^= key1

    unsigned_val = fmix(hash1 ^ length)
    return unsigned_val
