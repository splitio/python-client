"""Unit tests for the transformers module"""
from __future__ import absolute_import, division, print_function, unicode_literals

from builtins import int
from unittest import TestCase

import arrow

from splitio.transformers import (AsNumberTransformMixin, AsDateHourMinuteTimestampTransformMixin,
                                  AsDateTimestampTransformMixin)
from splitio.tests.utils import MockUtilsMixin


class AsNumberTransformMixinTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.transformer = AsNumberTransformMixin()

    def test_transform_key_works_with_small_int(self):
        """Tests that transform_key works with small integers"""
        transformed = self.transformer.transform_key(12345)
        self.assertIsInstance(transformed, int)
        self.assertEqual(12345, transformed)

    def test_transform_key_works_with_large_int(self):
        """Tests that transform_key works with large integers"""
        transformed = self.transformer.transform_key(9223372036854775808)
        self.assertIsInstance(transformed, int)
        self.assertEqual(9223372036854775808, transformed)

    def test_transform_key_works_with_small_str(self):
        """Tests that transform_key works with strings with small integers"""
        transformed = self.transformer.transform_key('12345')
        self.assertIsInstance(transformed, int)
        self.assertEqual(12345, transformed)

    def test_transform_key_works_with_large_str(self):
        """Tests that transform_key works with strings with large integers"""
        transformed = self.transformer.transform_key('9223372036854775808')
        self.assertIsInstance(transformed, int)
        self.assertEqual(9223372036854775808, transformed)

    def test_transform_key_returns_none_with_invalid_number(self):
        """Tests that transform_key returns none with strings with invalid integers"""
        self.assertIsNone(self.transformer.transform_key('foobar'))

    def test_transform_condition_parameter_returns_transform_key_result(self):
        """Tests that transform_condition_parameter returns the result of calling transform_key"""
        transform_key_mock = self.patch_object(self.transformer, '_transform_key')
        self.assertEqual(transform_key_mock.return_value,
                         self.transformer.transform_condition_parameter('foobar'))


class AsDateHourMinuteTimestampTransformMixinTests(TestCase):
    def setUp(self):
        self.transformer = AsDateHourMinuteTimestampTransformMixin()

    def test_transform_key_truncates_second_millisecond(self):
        """Tests that transform_key truncates seconds and milliseconds"""
        value = arrow.get(2016, 5, 1, 16, 35, 28, 19).timestamp

        transformed = self.transformer.transform_key(value)
        self.assertEqual(1462120500000, transformed)

    def test_transform_key_works_on_epoch_lower_limit(self):
        """Tests that transform_key works when supplied with the epoch timestamp lower limit"""
        value = arrow.get(1970, 1, 1, 0, 0, 0, 0).timestamp

        transformed = self.transformer.transform_key(value)
        self.assertEqual(0, transformed)

    def test_transform_key_works_on_epoch_upper_limit(self):
        """Tests that transform_key works when supplied with the epoch timestamp upper limit"""
        value = arrow.get(2038, 1, 19, 3, 14, 8, 0).timestamp

        transformed = self.transformer.transform_key(value)
        self.assertEqual(2147483640000, transformed)

    def test_transform_key_works_under_epoch_lower_limit(self):
        """Tests that transform_key works when supplied with a value under the epoch timestamp
        lower limit"""
        value = arrow.get(1969, 1, 1, 20, 16, 13, 5).timestamp

        transformed = self.transformer.transform_key(value)
        self.assertEqual(-31463040000, transformed)

    def test_transform_key_works_over_epoch_upper_limit(self):
        """Tests that transform_key works when supplied with a value over the epoch timestamp upper
        limit"""
        value = arrow.get(2038, 2, 19, 3, 14, 8, 9).timestamp

        transformed = self.transformer.transform_key(value)
        self.assertEqual(2150162040000, transformed)

    def test_transform_key_returns_none_with_invalid_numer(self):
        """Tests that transform_key returns None when given an invalid number"""
        self.assertIsNone(self.transformer.transform_key('foobar'))

    def test_transform_condition_parameter_truncates_second_millisecond(self):
        """Tests that transform truncates seconds and milliseconds"""
        value = arrow.get(2016, 5, 1, 16, 35, 28, 19).timestamp * 1000

        transformed = self.transformer.transform_condition_parameter(value)
        self.assertEqual(1462120500000, transformed)

    def test_transform_condition_parameter_works_on_epoch_lower_limit(self):
        """Tests that transform_condition_parameter works when supplied with the epoch timestamp
        lower limit"""
        value = arrow.get(1970, 1, 1, 0, 0, 0, 0).timestamp * 1000

        transformed = self.transformer.transform_condition_parameter(value)
        self.assertEqual(0, transformed)

    def test_transform_condition_parameter_works_on_epoch_upper_limit(self):
        """Tests that transform_condition_parameter works when supplied with the epoch timestamp
        upper limit"""
        value = arrow.get(2038, 1, 19, 3, 14, 8, 0).timestamp * 1000

        transformed = self.transformer.transform_condition_parameter(value)
        self.assertEqual(2147483640000, transformed)

    def test_transform_condition_parameter_works_under_epoch_lower_limit(self):
        """Tests that transform_condition_parameter works when supplied with a value under the
        epoch timestamp lower limit"""
        value = arrow.get(1969, 1, 1, 20, 16, 13, 5).timestamp * 1000

        transformed = self.transformer.transform_condition_parameter(value)
        self.assertEqual(-31463040000, transformed)

    def test_transform_condition_parameter_works_over_epoch_upper_limit(self):
        """Tests that transform_condition_parameter works when supplied with a value over the epoch
        timestamp upper limit"""
        value = arrow.get(2038, 2, 19, 3, 14, 8, 9).timestamp * 1000

        transformed = self.transformer.transform_condition_parameter(value)
        self.assertEqual(2150162040000, transformed)

    def test_transform_condition_parameter_returns_none_with_invalid_numer(self):
        """Tests that transform_key returns None when given an invalid number"""
        self.assertIsNone(self.transformer.transform_condition_parameter('foobar'))


class AsDateTimestampTransformMixinTests(TestCase):
    def setUp(self):
        self.transformer = AsDateTimestampTransformMixin()

    def test_transform_key_truncates_second_millisecond(self):
        """Tests that transform_key truncates seconds and milliseconds"""
        value = arrow.get(2016, 5, 1, 16, 35, 28, 19).timestamp

        transformed = self.transformer.transform_key(value)
        self.assertEqual(1462060800000, transformed)

    def test_transform_key_works_on_epoch_lower_limit(self):
        """Tests that transform_key works when supplied with the epoch timestamp lower limit"""
        value = arrow.get(1970, 1, 1, 0, 0, 0, 0).timestamp

        transformed = self.transformer.transform_key(value)
        self.assertEqual(0, transformed)

    def test_transform_key_works_on_epoch_upper_limit(self):
        """Tests that transform_key works when supplied with the epoch timestamp upper limit"""
        value = arrow.get(2038, 1, 19, 3, 14, 8, 0).timestamp

        transformed = self.transformer.transform_key(value)
        self.assertEqual(2147472000000, transformed)

    def test_transform_key_works_under_epoch_lower_limit(self):
        """Tests that transform_key works when supplied with a value under the epoch timestamp
        lower limit"""
        value = arrow.get(1969, 1, 1, 20, 12, 34, 8).timestamp

        transformed = self.transformer.transform_key(value)
        self.assertEqual(-31536000000, transformed)

    def test_transform_key_works_over_epoch_upper_limit(self):
        """Tests that transform_key works when supplied with a value over the epoch timestamp upper
        limit"""
        value = arrow.get(2038, 2, 19, 3, 14, 8, 15).timestamp

        transformed = self.transformer.transform_key(value)
        self.assertEqual(2150150400000, transformed)

    def test_transform_key_returns_none_with_invalid_numer(self):
        """Tests that transform_key returns None when given an invalid number"""
        self.assertIsNone(self.transformer.transform_key('foobar'))

    def test_transform_condition_parameter_truncates_second_millisecond(self):
        """Tests that transform_key truncates seconds and milliseconds"""
        value = arrow.get(2016, 5, 1, 16, 35, 28, 19).timestamp * 1000
        transformed = self.transformer.transform_condition_parameter(value)
        self.assertEqual(1462060800000, transformed)

    def test_transform_condition_parameter_works_on_epoch_lower_limit(self):
        """Tests that transform_key works when supplied with the epoch timestamp lower limit"""
        value = arrow.get(1970, 1, 1, 0, 0, 0, 0).timestamp * 1000
        transformed = self.transformer.transform_condition_parameter(value)
        self.assertEqual(0, transformed)

    def test_transform_condition_parameter_works_on_epoch_upper_limit(self):
        """Tests that transform_key works when supplied with the epoch timestamp upper limit"""
        value = arrow.get(2038, 1, 19, 3, 14, 8, 0).timestamp * 1000
        transformed = self.transformer.transform_condition_parameter(value)
        self.assertEqual(2147472000000, transformed)

    def test_transform_condition_parameter_works_under_epoch_lower_limit(self):
        """Tests that transform_key works when supplied with a value under the epoch timestamp
        lower limit"""
        value = arrow.get(1969, 1, 1, 20, 12, 34, 8).timestamp * 1000
        transformed = self.transformer.transform_condition_parameter(value)
        self.assertEqual(-31536000000, transformed)

    def test_transform_condition_parameter_works_over_epoch_upper_limit(self):
        """Tests that transform_key works when supplied with a value over the epoch timestamp upper
        limit"""
        value = arrow.get(2038, 2, 19, 3, 14, 8, 15).timestamp * 1000
        transformed = self.transformer.transform_condition_parameter(value)
        self.assertEqual(2150150400000, transformed)

    def test_transform_condition_parameter_returns_none_with_invalid_numer(self):
        """Tests that transform_key returns None when given an invalid number"""
        self.assertIsNone(self.transformer.transform_condition_parameter('foobar'))
