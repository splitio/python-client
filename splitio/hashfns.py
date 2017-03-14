"""
This module contains hash functions implemented in pure python
as well as the optional import (if installed) of a C compiled murmur hash
function with python bindings.
"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import sys as _sys
if (_sys.version_info > (3, 0)):
    def xrange(a, b, c):
        return range(a, b, c)

    def xencode(x):
        if isinstance(x, bytes) or isinstance(x, bytearray):
            return x
        else:
            return x.encode()
else:
    def xencode(x):
        return x
del _sys


def murmur32_py(key, seed=0x0):
    """
    Pure python implementation of murmur32 hash
    """

    key = bytearray(xencode(key))

    def fmix(h):
        h ^= h >> 16
        h = (h * 0x85ebca6b) & 0xFFFFFFFF
        h ^= h >> 13
        h = (h * 0xc2b2ae35) & 0xFFFFFFFF
        h ^= h >> 16
        return h

    length = len(key)
    nblocks = int(length/4)

    h1 = seed

    c1 = 0xcc9e2d51
    c2 = 0x1b873593

    # body
    for block_start in xrange(0, nblocks * 4, 4):
        # ??? big endian?
        k1 = key[block_start + 3] << 24 | \
             key[block_start + 2] << 16 | \
             key[block_start + 1] << 8 | \
             key[block_start + 0]

        k1 = (c1 * k1) & 0xFFFFFFFF
        k1 = (k1 << 15 | k1 >> 17) & 0xFFFFFFFF  # inlined ROTL32
        k1 = (c2 * k1) & 0xFFFFFFFF

        h1 ^= k1
        h1 = (h1 << 13 | h1 >> 19) & 0xFFFFFFFF  # inlined ROTL32
        h1 = (h1 * 5 + 0xe6546b64) & 0xFFFFFFFF

    # tail
    tail_index = nblocks * 4
    k1 = 0
    tail_size = length & 3

    if tail_size >= 3:
        k1 ^= key[tail_index + 2] << 16
    if tail_size >= 2:
        k1 ^= key[tail_index + 1] << 8
    if tail_size >= 1:
        k1 ^= key[tail_index + 0]

    if tail_size > 0:
        k1 = (k1 * c1) & 0xFFFFFFFF
        k1 = (k1 << 15 | k1 >> 17) & 0xFFFFFFFF  # inlined ROTL32
        k1 = (k1 * c2) & 0xFFFFFFFF
        h1 ^= k1

    unsigned_val = fmix(h1 ^ length)
    if unsigned_val & 0x80000000 == 0:
        return unsigned_val
    else:
        return -((unsigned_val ^ 0xFFFFFFFF) + 1)


try:
    # First attempt to import module with C++ core (faster)
    import mmh3
    _murmur_hash = mmh3.hash
except:
    # Fallback to interpreted python hash algoritm (slower)
    _murmur_hash = murmur32_py


def as_int32(value):
    if not -2147483649 <= value <= 2147483648:
        return (value + 2147483648) % 4294967296 - 2147483648
    return value


def _basic_hash(key, seed):
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


_HASH_ALGORITHMS = {
    'legacy': _basic_hash,
    'murmur': _murmur_hash
}


def get_hash_fn(algo):
    """
    Return appropriate hash function for requested algorithm
    :param algo: Algoritm to use
    :return: Hash function
    :rtype: function
    """
    return _HASH_ALGORITHMS.get(algo, _basic_hash)
