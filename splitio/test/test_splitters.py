"""Unit tests for the matchers module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from string import printable
from random import randint, choice
from sys import maxint
from unittest import TestCase, skip

from splitio.splitters import Splitter
from splitio.treatments import CONTROL
from splitio.test.utils import MockUtilsMixin


class SplitterGetTreatmentTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_seed = mock.MagicMock()
        self.some_partitions = [mock.MagicMock(), mock.MagicMock()]
        self.splitter = Splitter()
        self.hash_key_mock = self.patch_object(self.splitter, 'hash_key')
        self.get_bucket_mock = self.patch_object(self.splitter, 'get_bucket')
        self.get_treatment_for_bucket_mock = self.patch_object(self.splitter,
                                                               'get_treatment_for_bucket')

    def test_get_treatment_returns_control_if_partitions_is_none(self):
        """Test that get_treatment returns the control treatment if partitions is None"""
        self.assertEqual(CONTROL, self.splitter.get_treatment(self.some_key, self.some_seed, None))

    def test_get_treatment_returns_control_if_partitions_is_empty(self):
        """Test that get_treatment returns the control treatment if partitions is empty"""
        self.assertEqual(CONTROL, self.splitter.get_treatment(self.some_key, self.some_seed, []))

    def test_get_treatment_returns_only_partition_treatment_if_it_is_100(self):
        """Test that get_treatment returns the only partition treatment if it is 100%"""
        some_partition = mock.MagicMock()
        some_partition.size = 100
        self.assertEqual(some_partition.treatment, self.splitter.get_treatment(self.some_key,
                                                                               self.some_seed,
                                                                               [some_partition]))

    def test_get_treatment_calls_get_treatment_for_bucket_if_more_than_1_partition(self):
        """
        Test that get_treatment calls get_treatment_for_bucket if there is more than one
        partition
        """
        self.splitter.get_treatment(self.some_key, self.some_seed, self.some_partitions)
        self.get_treatment_for_bucket_mock.assert_called_once_with(
            self.get_bucket_mock.return_value, self.some_partitions)

    def test_get_treatment_returns_get_treatment_for_bucket_result_if_more_than_1_partition(self):
        """
        Test that get_treatment returns the result of callling get_treatment_for_bucket if there
        is more than one partition
        """
        self.assertEqual(
            self.get_treatment_for_bucket_mock.return_value, self.splitter.get_treatment(
                self.some_key, self.some_seed, self.some_partitions))

    def test_get_treatment_calls_hash_key_if_more_than_1_partition(self):
        """
        Test that get_treatment calls hash_key if there is more than one partition
        """
        self.splitter.get_treatment(self.some_key, self.some_seed, self.some_partitions)
        self.hash_key_mock.assert_called_once_with(self.some_key, self.some_seed)

    def test_get_treatment_calls_get_bucket_if_more_than_1_partition(self):
        """
        Test that get_treatment calls get_bucket if there is more than one partition
        """
        self.splitter.get_treatment(self.some_key, self.some_seed, self.some_partitions)
        self.get_bucket_mock.assert_called_once_with(self.hash_key_mock.return_value)


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


@skip
class SplitterHashKeyTests(TestCase):
    def setUp(self):
        self.splitter = Splitter()

    # TODO Write hash unit tests


@skip
class SplitterGetBucketUnitTests(TestCase):
    def setUp(self):
        self.splitter = Splitter()

    # TODO Write get_bucket unit tests