"""Unit tests for the matchers module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from collections import Counter
from math import sqrt
from json import loads
from os.path import join, dirname
from random import randint
from unittest import TestCase, skip

from splitio.splits import Partition, HashAlgorithm
from splitio.splitters import Splitter
from splitio.treatments import CONTROL
from splitio.hashfns import _basic_hash, _murmur_hash
from splitio.tests.utils import MockUtilsMixin, random_alphanumeric_string
import io

class SplitterGetTreatmentTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_seed = mock.MagicMock()
        self.some_partitions = [mock.MagicMock(), mock.MagicMock()]
        self.splitter = Splitter()
#        self.hash_key_mock = self.patch_object(self.splitter, 'hash_key')
        self.get_bucket_mock = self.patch_object(self.splitter, 'get_bucket')
        self.get_treatment_for_bucket_mock = self.patch_object(self.splitter,
                                                               'get_treatment_for_bucket')

    def test_get_treatment_returns_control_if_partitions_is_none(self):
        """Test that get_treatment returns the control treatment if partitions is None"""
        self.assertEqual(CONTROL, self.splitter.get_treatment(self.some_key, self.some_seed, None, HashAlgorithm.LEGACY))

    def test_get_treatment_returns_control_if_partitions_is_empty(self):
        """Test that get_treatment returns the control treatment if partitions is empty"""
        self.assertEqual(CONTROL, self.splitter.get_treatment(self.some_key, self.some_seed, [], HashAlgorithm.LEGACY))

    def test_get_treatment_returns_only_partition_treatment_if_it_is_100(self):
        """Test that get_treatment returns the only partition treatment if it is 100%"""
        some_partition = mock.MagicMock()
        some_partition.size = 100
        self.assertEqual(some_partition.treatment, self.splitter.get_treatment(self.some_key,
                                                                               self.some_seed,
                                                                               [some_partition],
                                                                               HashAlgorithm.LEGACY))

    def test_get_treatment_calls_get_treatment_for_bucket_if_more_than_1_partition(self):
        """
        Test that get_treatment calls get_treatment_for_bucket if there is more than one
        partition
        """
        self.splitter.get_treatment(self.some_key, self.some_seed, self.some_partitions, HashAlgorithm.LEGACY)
        self.get_treatment_for_bucket_mock.assert_called_once_with(
            self.get_bucket_mock.return_value, self.some_partitions)

    def test_get_treatment_returns_get_treatment_for_bucket_result_if_more_than_1_partition(self):
        """
        Test that get_treatment returns the result of callling get_treatment_for_bucket if there
        is more than one partition
        """
        self.assertEqual(
            self.get_treatment_for_bucket_mock.return_value, self.splitter.get_treatment(
                self.some_key, self.some_seed, self.some_partitions, HashAlgorithm.LEGACY))

    def test_get_treatment_calls_hash_key_if_more_than_1_partition(self):
        """
        Test that get_treatment calls hash_key if there is more than one partition
        """
        self.splitter.get_treatment(self.some_key, self.some_seed, self.some_partitions, HashAlgorithm.LEGACY)
#        self.hash_key_mock.assert_called_once_with(self.some_key, self.some_seed)

    def test_get_treatment_calls_get_bucket_if_more_than_1_partition(self):
        """
        Test that get_treatment calls get_bucket if there is more than one partition
        """
        self.splitter.get_treatment(self.some_key, self.some_seed, self.some_partitions, HashAlgorithm.LEGACY)
#        self.get_bucket_mock.assert_called_once_with(self.hash_key_mock.return_value)


class SplitterGetTreatmentForBucket(TestCase):
    def setUp(self):
        self.some_partitions = [mock.MagicMock(), mock.MagicMock(), mock.MagicMock()]
        self.some_partitions[0].size = 10
        self.some_partitions[1].size = 20
        self.some_partitions[2].size = 30
        self.splitter = Splitter()

    def test_returns_control_if_bucket_is_not_covered(self):
        """
        Tests that get_treatment_for_bucket returns CONTROL if the bucket is over the segments
        covered by the partition
        """
        self.assertEqual(CONTROL, self.splitter.get_treatment_for_bucket(100, self.some_partitions))

    def test_returns_treatment_of_partition_that_has_bucket(self):
        """
        Tests that get_treatment_for_bucket returns the treatment of the partition that covers the
        bucket.
        """
        self.assertEqual(self.some_partitions[0].treatment,
                         self.splitter.get_treatment_for_bucket(1, self.some_partitions))
        self.assertEqual(self.some_partitions[1].treatment,
                         self.splitter.get_treatment_for_bucket(15, self.some_partitions))
        self.assertEqual(self.some_partitions[2].treatment,
                         self.splitter.get_treatment_for_bucket(33, self.some_partitions))


class SplitterHashKeyTests(TestCase):
    def setUp(self):
        self.splitter = Splitter()

    def test_with_sample_data(self):
        """
        Tests basic hash against expected values using alphanumeric values
        """
        with open(join(dirname(__file__), 'sample-data.jsonl')) as f:
            for line in map(loads, f):
                seed, key, hash_, bucket = line
                self.assertEqual(int(hash_), _basic_hash(key, int(seed)))
    @skip
    def test_with_non_alpha_numeric_sample_data(self):
        """
        Tests basic hash against expected values using non alphanumeric values
        """
        with io.open(join(dirname(__file__), 'sample-data-non-alpha-numeric.jsonl'), 'r', encoding='utf-8') as f:
            for line in map(loads, f):
                seed, key, hash_, bucket = line
                self.assertEqual(int(hash_), _basic_hash(key, int(seed)))

    def test_murmur_with_sample_data(self):
        """
        Tests murmur32 hash against expected values using alphanumeric values
        """
        with open(join(dirname(__file__), 'murmur3-sample-data-v2.csv')) as f:
            for line in f:
                seed, key, hash_, bucket = line.split(',')
                self.assertEqual(int(hash_), _murmur_hash(key, int(seed)))

    def test_murmur_with_non_alpha_numeric_sample_data(self):
        """
        Tests murmur32 hash against expected values using non alphanumeric values
        """
        with io.open(join(dirname(__file__), 'murmur3-sample-data-non-alpha-numeric-v2.csv'), 'r', encoding='utf-8') as f:
            for line in f:
                seed, key, hash_, bucket = line.split(',')
                self.assertEqual(int(hash_), _murmur_hash(key, int(seed)))


class SplitterGetBucketUnitTests(TestCase):
    def setUp(self):
        self.splitter = Splitter()

    def test_with_sample_data(self):
        """
        Tests hash_key against expected values using alphanumeric values
        """
        with open(join(dirname(__file__), 'sample-data.jsonl')) as f:
            for line in map(loads, f):
                seed, key, hash_, bucket = line
                self.assertEqual(int(bucket), self.splitter.get_bucket(int(hash_)))

    def test_with_non_alpha_numeric_sample_data(self):
        """
        Tests hash_key against expected values using non alphanumeric values
        """
        with open(join(dirname(__file__), 'sample-data-non-alpha-numeric.jsonl')) as f:
            for line in map(loads, f):
                seed, key, hash_, bucket = line
                self.assertEqual(int(bucket), self.splitter.get_bucket(int(hash_)))


@skip
class SplitterGetTreatmentDistributionTests(TestCase):
    def setUp(self):
        self.splitter = Splitter()

    def test_1_percent_treatments_evenly_distributed(self):
        """Test that get_treatment distributes treatments according to partitions"""
        seed = randint(-2147483649, 2147483648)
        partitions = [Partition(mock.MagicMock(), 1) for _ in range(100)]
        n = 100000
        p = 0.01

        treatments = [self.splitter.get_treatment(random_alphanumeric_string(randint(16, 32)),
                                                  seed, partitions, HashAlgorithm.LEGACY) for _ in range(n)]
        counter = Counter(treatments)

        mean = n * p
        stddev = sqrt(mean * (1 - p))

        count_min = int(mean - 4 * stddev)
        count_max = int(mean + 4 * stddev)

        for count in counter.values():
            self.assertTrue(count_min <= count <= count_max)
