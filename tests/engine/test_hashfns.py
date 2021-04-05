"""Hash function test module."""
#pylint: disable=no-self-use,protected-access
import io
import json
import os

import pytest
from splitio.engine import hashfns, splitters
from splitio.engine.hashfns.murmur3py import hash128_x64 as murmur3_128_py
from splitio.models import splits


class HashFunctionsTests(object):
    """Hash functions test cases."""

    def test_get_hash_function(self):
        """Test that the correct hash function is returned."""
        assert hashfns.get_hash_fn(splits.HashAlgorithm.LEGACY) == hashfns.legacy.legacy_hash
        assert hashfns.get_hash_fn(splits.HashAlgorithm.MURMUR) == hashfns._murmur_hash

    def test_legacy_hash_ascii_data(self):
        """Test legacy hash function against known results."""
        splitter = splitters.Splitter()
        file_name = os.path.join(os.path.dirname(__file__), 'files', 'sample-data.jsonl')
        with open(file_name, 'r') as flo:
            lines = flo.read().split('\n')

        for line in lines:
            if line is None or line == '':
                continue
            seed, key, hashed, bucket = json.loads(line)
            assert hashfns.legacy.legacy_hash(key, seed) == hashed
            assert splitter.get_bucket(key, seed, splits.HashAlgorithm.LEGACY) == bucket

    def test_murmur_hash_ascii_data(self):
        """Test legacy hash function against known results."""
        splitter = splitters.Splitter()
        file_name = os.path.join(os.path.dirname(__file__), 'files', 'murmur3-sample-data-v2.csv')
        with open(file_name, 'r') as flo:
            lines = flo.read().split('\n')

        for line in lines:
            if line is None or line == '':
                continue
            seed, key, hashed, bucket = line.split(',')
            seed = int(seed)
            bucket = int(bucket)
            hashed = int(hashed)
            assert hashfns._murmur_hash(key, seed) == hashed
            assert splitter.get_bucket(key, seed, splits.HashAlgorithm.MURMUR) == bucket

    def test_murmur_more_ascii_data(self):
        """Test legacy hash function against known results."""
        splitter = splitters.Splitter()
        file_name = os.path.join(os.path.dirname(__file__), 'files', 'murmur3-custom-uuids.csv')
        with open(file_name, 'r') as flo:
            lines = flo.read().split('\n')

        for line in lines:
            if line is None or line == '':
                continue
            seed, key, hashed, bucket = line.split(',')
            seed = int(seed)
            bucket = int(bucket)
            hashed = int(hashed)
            assert hashfns._murmur_hash(key, seed) == hashed
            assert splitter.get_bucket(key, seed, splits.HashAlgorithm.MURMUR) == bucket

    def test_murmur_hash_non_ascii_data(self):
        """Test legacy hash function against known results."""
        splitter = splitters.Splitter()
        file_name = os.path.join(
            os.path.dirname(__file__),
            'files',
            'murmur3-sample-data-non-alpha-numeric-v2.csv'
        )
        with io.open(file_name, 'r', encoding='utf-8') as flo:
            lines = flo.read().split('\n')

        for line in lines:
            if line is None or line == '':
                continue
            seed, key, hashed, bucket = line.split(',')
            seed = int(seed)
            bucket = int(bucket)
            hashed = int(hashed)
            assert hashfns._murmur_hash(key, seed) == hashed
            assert splitter.get_bucket(key, seed, splits.HashAlgorithm.MURMUR) == bucket

    def test_murmur128(self):
        """Test legacy hash function against known results."""
        file_name = os.path.join(os.path.dirname(__file__), 'files', 'murmur128_test_suite.csv')
        with io.open(file_name, 'r', encoding='utf-8') as flo:
            lines = flo.read().split('\n')

        for line in lines:
            if line is None or line == '':
                continue
            key, seed, hashed = line.split(',')
            seed = int(seed)
            hashed = int(hashed)
            assert hashfns.murmur_128(key, seed) == hashed

    def test_murmur128_pure_python(self):
        """Test legacy hash function against known results."""
        file_name = os.path.join(os.path.dirname(__file__), 'files', 'murmur128_test_suite.csv')
        with io.open(file_name, 'r', encoding='utf-8') as flo:
            lines = flo.read().split('\n')

        for line in lines:
            if line is None or line == '':
                continue
            key, seed, hashed = line.split(',')
            seed = int(seed)
            hashed = int(hashed)
            assert murmur3_128_py(key, seed)[0] == hashed
