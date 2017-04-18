"""
This module contains hash functions implemented in pure python
as well as the optional import (if installed) of a C compiled murmur hash
function with python bindings.
"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

from splitio.splits import HashAlgorithm
from splitio.hashfns import legacy

try:
    # First attempt to import module with C++ core (faster)
    import mmh3
    from ctypes import c_uint

    def _murmur_hash(key, seed):
        ukey = key.encode('utf8')
        return c_uint(mmh3.hash(ukey, seed)).value
except:
    # Fallback to interpreted python hash algoritm (slower)
    from splitio.hashfns import murmur3py
    _murmur_hash = murmur3py.murmur32_py


_HASH_ALGORITHMS = {
    HashAlgorithm.LEGACY: legacy.legacy_hash,
    HashAlgorithm.MURMUR: _murmur_hash
}


def get_hash_fn(algo):
    """
    Return appropriate hash function for requested algorithm
    :param algo: Algoritm to use
    :return: Hash function
    :rtype: function
    """
    return _HASH_ALGORITHMS.get(algo, legacy.legacy_hash)
