"""
Hash functions module.

This module contains hash functions implemented in pure python
as well as the optional import (if installed) of a C compiled murmur hash
function with python bindings.
"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

from splitio.models.splits import HashAlgorithm
from splitio.engine.hashfns import legacy

try:
    # First attempt to import module with C++ core (faster)
    import mmh3cffi

    def _murmur_hash(key, seed):
        return mmh3cffi.hash_str(key, seed)
except ImportError:
    # Fallback to interpreted python hash algoritm (slower)
    from splitio.engine.hashfns import murmur3py  #pylint: disable=ungrouped-imports
    _murmur_hash = murmur3py.murmur32_py  #pylint: disable=invalid-name


_HASH_ALGORITHMS = {
    HashAlgorithm.LEGACY: legacy.legacy_hash,
    HashAlgorithm.MURMUR: _murmur_hash
}


def get_hash_fn(algo):
    """
    Return appropriate hash function for requested algorithm.

    :param algo: Algoritm to use
    :type algo: int
    :return: Hash function
    :rtype: function
    """
    return _HASH_ALGORITHMS.get(algo, legacy.legacy_hash)
