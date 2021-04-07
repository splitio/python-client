"""MurmurHash3 hash module."""


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


def hash128_x64(key, seed):
    """
    Pure python implementation of murmurhash3-128.

    borrowed from: https://github.com/wc-duck/pymmh3/blob/master/pymmh3.py
    """
    key = bytearray(key, 'utf-8')

    def fmix(k):
        k ^= k >> 33
        k = (k * 0xff51afd7ed558ccd) & 0xFFFFFFFFFFFFFFFF
        k ^= k >> 33
        k = (k * 0xc4ceb9fe1a85ec53) & 0xFFFFFFFFFFFFFFFF
        k ^= k >> 33
        return k

    length = len(key)
    nblocks = int(length / 16)

    h1 = seed
    h2 = seed

    c1 = 0x87c37b91114253d5
    c2 = 0x4cf5ad432745937f

    # body
    for block_start in range(0, nblocks * 8, 8):
        # ??? big endian?
        k1 = key[2 * block_start + 7] << 56 | \
             key[2 * block_start + 6] << 48 | \
             key[2 * block_start + 5] << 40 | \
             key[2 * block_start + 4] << 32 | \
             key[2 * block_start + 3] << 24 | \
             key[2 * block_start + 2] << 16 | \
             key[2 * block_start + 1] << 8 | \
             key[2 * block_start + 0]

        k2 = key[2 * block_start + 15] << 56 | \
            key[2 * block_start + 14] << 48 | \
            key[2 * block_start + 13] << 40 | \
            key[2 * block_start + 12] << 32 | \
            key[2 * block_start + 11] << 24 | \
            key[2 * block_start + 10] << 16 | \
            key[2 * block_start + 9] << 8 | \
            key[2 * block_start + 8]

        k1 = (c1 * k1) & 0xFFFFFFFFFFFFFFFF
        k1 = (k1 << 31 | k1 >> 33) & 0xFFFFFFFFFFFFFFFF  # inlined ROTL64
        k1 = (c2 * k1) & 0xFFFFFFFFFFFFFFFF
        h1 ^= k1

        h1 = (h1 << 27 | h1 >> 37) & 0xFFFFFFFFFFFFFFFF  # inlined ROTL64
        h1 = (h1 + h2) & 0xFFFFFFFFFFFFFFFF
        h1 = (h1 * 5 + 0x52dce729) & 0xFFFFFFFFFFFFFFFF

        k2 = (c2 * k2) & 0xFFFFFFFFFFFFFFFF
        k2 = (k2 << 33 | k2 >> 31) & 0xFFFFFFFFFFFFFFFF  # inlined ROTL64
        k2 = (c1 * k2) & 0xFFFFFFFFFFFFFFFF
        h2 ^= k2

        h2 = (h2 << 31 | h2 >> 33) & 0xFFFFFFFFFFFFFFFF  # inlined ROTL64
        h2 = (h1 + h2) & 0xFFFFFFFFFFFFFFFF
        h2 = (h2 * 5 + 0x38495ab5) & 0xFFFFFFFFFFFFFFFF

    # tail
    tail_index = nblocks * 16
    k1 = 0
    k2 = 0
    tail_size = length & 15

    if tail_size >= 15:
        k2 ^= key[tail_index + 14] << 48
    if tail_size >= 14:
        k2 ^= key[tail_index + 13] << 40
    if tail_size >= 13:
        k2 ^= key[tail_index + 12] << 32
    if tail_size >= 12:
        k2 ^= key[tail_index + 11] << 24
    if tail_size >= 11:
        k2 ^= key[tail_index + 10] << 16
    if tail_size >= 10:
        k2 ^= key[tail_index + 9] << 8
    if tail_size >= 9:
        k2 ^= key[tail_index + 8]

    if tail_size > 8:
        k2 = (k2 * c2) & 0xFFFFFFFFFFFFFFFF
        k2 = (k2 << 33 | k2 >> 31) & 0xFFFFFFFFFFFFFFFF  # inlined ROTL64
        k2 = (k2 * c1) & 0xFFFFFFFFFFFFFFFF
        h2 ^= k2

    if tail_size >= 8:
        k1 ^= key[tail_index + 7] << 56
    if tail_size >= 7:
        k1 ^= key[tail_index + 6] << 48
    if tail_size >= 6:
        k1 ^= key[tail_index + 5] << 40
    if tail_size >= 5:
        k1 ^= key[tail_index + 4] << 32
    if tail_size >= 4:
        k1 ^= key[tail_index + 3] << 24
    if tail_size >= 3:
        k1 ^= key[tail_index + 2] << 16
    if tail_size >= 2:
        k1 ^= key[tail_index + 1] << 8
    if tail_size >= 1:
        k1 ^= key[tail_index + 0]

    if tail_size > 0:
        k1 = (k1 * c1) & 0xFFFFFFFFFFFFFFFF
        k1 = (k1 << 31 | k1 >> 33) & 0xFFFFFFFFFFFFFFFF  # inlined ROTL64
        k1 = (k1 * c2) & 0xFFFFFFFFFFFFFFFF
        h1 ^= k1

    # finalization
    h1 ^= length
    h2 ^= length

    h1 = (h1 + h2) & 0xFFFFFFFFFFFFFFFF
    h2 = (h1 + h2) & 0xFFFFFFFFFFFFFFFF

    h1 = fmix(h1)
    h2 = fmix(h2)

    h1 = (h1 + h2) & 0xFFFFFFFFFFFFFFFF
    h2 = (h1 + h2) & 0xFFFFFFFFFFFFFFFF

    return [h1, h2]
