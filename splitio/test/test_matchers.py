"""Unit tests for the matchers module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase

from splitio.matchers import (AndCombiner, CombiningMatcher, AllKeysMatcher, NegatableMatcher,
                              AttributeMatcher, BetweenMatcher, DateTimeBetweenMatcher,
                              NumberBetweenMatcher, DataType, EqualToCompareMixin,
                              GreaterOrEqualToCompareMixin, LessThanOrEqualToCompareMixin,
                              CompareMatcher, UserDefinedSegmentMatcher, WhitelistMatcher)
from splitio.test.utils import MockUtilsMixin


class AndCombinerTests(TestCase):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_attributes = mock.MagicMock()
        self.combiner = AndCombiner()

    def test_combine_returns_false_on_none_matchers(self):
        """Tests that combine returns false if matchers is None"""
        self.assertFalse(self.combiner.combine(None, self.some_key, self.some_attributes))

    def test_combine_returns_false_on_empty_matchers(self):
        """Tests that combine returns false if matchers is empty"""
        self.assertFalse(self.combiner.combine([], self.some_key, self.some_attributes))

    def test_combine_calls_match_on_all_matchers(self):
        """Tests that combine calls match on all matchers"""
        matchers = [mock.MagicMock(), mock.MagicMock(), mock.MagicMock()]

        # We set all return values of match to True to avoid short circuiting
        for matcher in matchers:
            matcher.match.return_value = True

        self.combiner.combine(matchers, self.some_key, self.some_attributes)

        for matcher in matchers:
            matcher.match.assert_called_once_with(self.some_key, self.some_attributes)

    def test_combine_short_circuits_check(self):
        """Tests that combine stops checking after the first false"""
        matchers = [mock.MagicMock(), mock.MagicMock(), mock.MagicMock()]

        # We set the second return value of match to False to short-circuit the check
        matchers[0].match.return_value = True
        matchers[1].match.return_value = False

        self.combiner.combine(matchers, self.some_key, self.some_attributes)

        matchers[0].match.assert_called_once_with(self.some_key, self.some_attributes)
        matchers[1].match.assert_called_once_with(self.some_key, self.some_attributes)
        matchers[2].match.assert_not_called()


class CombiningMatcherTests(TestCase):
    def setUp(self):
        self.some_combiner = mock.MagicMock()
        self.some_delegates = [mock.MagicMock(), mock.MagicMock(), mock.MagicMock()]
        self.some_key = mock.MagicMock()
        self.some_attributes = mock.MagicMock()

        self.matcher = CombiningMatcher(self.some_combiner, self.some_delegates)

    def test_match_call_combiner_combine(self):
        """Tests that match calls combine() on the combiner"""
        self.matcher.match(self.some_key, self.some_attributes)

        self.assertEqual(1, self.some_combiner.combine.call_count)
        args, _ = self.some_combiner.combine.call_args

        self.assertListEqual(list(args[0]), list(self.some_delegates))
        self.assertEqual(args[1], self.some_key)
        self.assertEqual(args[2], self.some_attributes)

    def test_match_returns_combiner_combine_result(self):
        """Tests that match returns the result of the combiner combine() method"""
        self.assertEqual(self.some_combiner.combine.return_value,
                         self.matcher.match(self.some_key, self.some_attributes))


class AllKeysMatcherTests(TestCase):
    def setUp(self):
        self.some_key = mock.MagicMock()

        self.matcher = AllKeysMatcher()

    def test_match_returns_true_if_key_is_not_none(self):
        """Tests that match returns True if the key is not None"""
        self.assertTrue(self.matcher.match(self.some_key))

    def test_match_returns_false_if_key_is_none(self):
        """Tests that match returns False if the key is not None"""
        self.assertFalse(self.matcher.match(None))


class NegatableMatcherTests(TestCase):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_delegate = mock.MagicMock()

    def test_match_calls_delegate_match(self):
        """Tests that match calls the delegate match method"""
        matcher = NegatableMatcher(True, self.some_delegate)

        matcher.match(self.some_key)

        self.some_delegate.match.assert_called_once_with(self.some_key)

    def test_if_negate_true_match_negates_result_of_delegate_match(self):
        """Tests that if negate is True, match negates the result of the delegate match"""
        matcher = NegatableMatcher(True, self.some_delegate)

        self.some_delegate.match.return_value = True
        self.assertFalse(matcher.match(self.some_key))

        self.some_delegate.match.return_value = False
        self.assertTrue(matcher.match(self.some_key))

    def test_if_negate_false_match_doesnt_negate_result_of_delegate_match(self):
        """Tests that if negate is False, match doesn't negates the result of the delegate match"""
        matcher = NegatableMatcher(False, self.some_delegate)

        self.some_delegate.match.return_value = True
        self.assertTrue(matcher.match(self.some_key))

        self.some_delegate.match.return_value = False
        self.assertFalse(matcher.match(self.some_key))


class AttributeMatcherTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.negatable_matcher_mock = self.patch('splitio.matchers.NegatableMatcher').return_value
        self.some_attribute = mock.MagicMock()
        self.some_key = mock.MagicMock()

        self.some_attributes = mock.MagicMock()

        self.some_matcher = mock.MagicMock()
        self.some_negate = mock.MagicMock()

        self.matcher = AttributeMatcher(self.some_attribute, self.some_matcher, self.some_negate)

    def test_match_calls_negatable_matcher_match_with_key_if_attribute_is_none(self):
        """
        Tests that match calls the negatable matcher match method with the supplied key if
        attribute is None
        """
        matcher = AttributeMatcher(None, self.some_matcher, self.some_negate)
        matcher.match(self.some_key, self.some_attributes)

        self.negatable_matcher_mock.match.assert_called_once_with(self.some_key)

    def test_match_returns_false_attributes_is_none(self):
        """
        Tests that match returns False if attributes is None
        """
        self.assertFalse(self.matcher.match(self.some_key, None))

    def test_match_returns_false_attribute_value_is_none(self):
        """
        Tests that match returns None if the value of the attribute is None
        """
        self.some_attributes.get.return_value = None

        self.assertFalse(self.matcher.match(self.some_key, self.some_attributes))

    def test_match_calls_negatable_matcher_match_with_attribute_if_attributes_is_not_none(self):
        """
        Tests that match calls the negatable matcher match method with the supplied key if
        attributes is None
        """
        self.matcher.match(self.some_key, self.some_attributes)

        self.negatable_matcher_mock.match.assert_called_once_with(
            self.some_attributes.get.return_value)

    def test_match_returns_result_negatable_matcher_match(self):
        """
        Tests that match returns the result of invoking match on the negatable matcher
        """
        self.assertEqual(self.negatable_matcher_mock.match.return_value,
                         self.matcher.match(self.some_key, self.some_attributes))


class BetweenMatcherForDataTypeTests(TestCase):
    def test_for_data_type_returns_date_time_between_batcher_for_datetime(self):
        """
        Tests that for_data_type returns a DateTimeBetweenMatcher matcher with the
        DataType.DATETIME data type
        """
        matcher = BetweenMatcher.for_data_type(DataType.DATETIME, 1461601825000, 1461609025000)
        self.assertIsInstance(matcher, DateTimeBetweenMatcher)

    def test_start_end_are_properly_transformed_for_datetime_datatype(self):
        """
        Tests that for_data_type returns a DateTimeBetweenMatcher for which the values of start
        and end have been properly transformed.
        """
        matcher = BetweenMatcher.for_data_type(DataType.DATETIME, 1461601825000, 1461609025000)

        self.assertEqual(1461601800000, matcher.start)
        self.assertEqual(1461609000000, matcher.end)

    def test_for_data_type_returns_number_between_batcher_for_number(self):
        """
        Tests that for_data_type returns a NumberBetweenMatcher matcher with the
        DataType.NUMBER data type
        """
        matcher = BetweenMatcher.for_data_type(DataType.NUMBER, 1, 100)
        self.assertIsInstance(matcher, NumberBetweenMatcher)


class BetweenMatcherTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_start = mock.MagicMock()
        self.some_end = mock.MagicMock()
        self.transformed_key = mock.MagicMock()
        self.transformed_key.__le__.return_value = True
        self.transformed_start = mock.MagicMock()
        self.transformed_start.__le__.return_value = True
        self.transformed_end = mock.MagicMock()
        self.some_data_type = mock.MagicMock()

        self.transform_mock = self.patch('splitio.matchers.BetweenMatcher.transform')

        def transform_side_effect(value):
            if value == self.some_key:
                return self.transformed_key
            elif value == self.some_start:
                return self.transformed_start
            elif value == self.some_end:
                return self.transformed_end

            raise ValueError()

        self.transform_mock.side_effect = transform_side_effect

        self.matcher = BetweenMatcher(self.some_start, self.some_end, self.some_data_type)

        self.transform_mock.reset_mock()

    def test_match_calls_transform_on_key(self):
        """Tests that match calls transform on key"""
        self.matcher.match(self.some_key)

        self.transform_mock.assert_called_once_with(self.some_key)

    def test_match_calls_transform_on_key(self):
        """Tests that match returns None if transform returns None"""
        self.transform_mock.side_effect = None
        self.transform_mock.return_value = None

        self.assertIsNone(self.matcher.match(self.some_key))

    def test_match_checks_transformed_key_between_start_and_end(self):
        """Tests that match checks that the transformed key is between the start and end"""
        self.matcher.match(self.some_key)

        self.transformed_start.__le__.assert_called_once_with(self.transformed_key)
        self.transformed_key.__le__.assert_called_once_with(self.transformed_end)

    def test_match_returns_true_if_key_between_start_and_end(self):
        """Tests that match returns True if key is between the start and end"""
        self.assertTrue(self.matcher.match(self.some_key))

    def test_match_returns_false_if_key_less_than_start(self):
        """Tests that match returns False if key is less than start"""
        self.transformed_start.__le__.return_value = False
        self.assertFalse(self.matcher.match(self.some_key))

    def test_match_returns_false_if_key_greater_than_end(self):
        """Tests that match returns False if key is greater than end"""
        self.transformed_key.__le__.return_value = False
        self.assertFalse(self.matcher.match(self.some_key))


class EqualToCompareMixinTests(TestCase):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_key.__eq__.return_value = True
        self.some_value = mock.MagicMock()
        self.mixin = EqualToCompareMixin()

    def test_compare_checks_if_key_is_equal_to_value(self):
        """Tests that compare checks if the key and the value are equal"""
        self.mixin.compare(self.some_key, self.some_value)
        self.some_key.__eq__.assert_called_once_with(self.some_value)

    def test_compare_returns_true_if_key_and_value_are_equal(self):
        """Tests that compare returns True the key and the value are equal"""
        self.assertTrue(self.mixin.compare(self.some_key, self.some_value))

    def test_compare_returns_false_if_key_and_value_are_not_equal(self):
        """Tests that compare returns False if the key and the value are not equal"""
        self.some_key.__eq__.return_value = False
        self.assertFalse(self.mixin.compare(self.some_key, self.some_value))


class GreaterOrEqualToCompareMixinTests(TestCase):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_key.__ge__.return_value = True
        self.some_value = mock.MagicMock()
        self.mixin = GreaterOrEqualToCompareMixin()

    def test_compare_checks_if_key_is_greater_than_or_equal_to_value(self):
        """Tests that compare checks if the key is greater than or equal to value"""
        self.mixin.compare(self.some_key, self.some_value)
        self.some_key.__ge__.assert_called_once_with(self.some_value)

    def test_compare_returns_true_if_key_greater_than_or_equal_to_value(self):
        """Tests that compare returns True if the key is greater than or equal to value"""
        self.assertTrue(self.mixin.compare(self.some_key, self.some_value))

    def test_compare_returns_false_if_key_not_greater_than_or_equal_to_value(self):
        """Tests that compare returns True if the key is not greater than or equal to value"""
        self.some_key.__ge__.return_value = False
        self.assertFalse(self.mixin.compare(self.some_key, self.some_value))


class LessThanOrEqualToCompareMixinTests(TestCase):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_key.__le__.return_value = True
        self.some_value = mock.MagicMock()
        self.mixin = LessThanOrEqualToCompareMixin()

    def test_compare_checks_if_key_is_less_than_or_equal_to_value(self):
        """Tests that compare checks if the key is less than or equal to value"""
        self.mixin.compare(self.some_key, self.some_value)
        self.some_key.__le__.assert_called_once_with(self.some_value)

    def test_compare_returns_true_if_key_less_than_or_equal_to_value(self):
        """Tests that compare returns True if the key is less than or equal to value"""
        self.assertTrue(self.mixin.compare(self.some_key, self.some_value))

    def test_compare_returns_false_if_key_not_less_than_or_equal_to_value(self):
        """Tests that compare returns True if the key is not less than or equal to value"""
        self.some_key.__le__.return_value = False
        self.assertFalse(self.mixin.compare(self.some_key, self.some_value))


class CompareMatcherTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_compare_to = mock.MagicMock()
        self.transformed_key = mock.MagicMock()
        self.transformed_compare_to = mock.MagicMock()
        self.some_data_type = mock.MagicMock()

        self.transform_mock = self.patch('splitio.matchers.CompareMatcher.transform')

        def transform_side_effect(value):
            if value == self.some_key:
                return self.transformed_key
            elif value == self.some_compare_to:
                return self.transformed_compare_to

            raise ValueError()

        self.transform_mock.side_effect = transform_side_effect

        self.matcher = CompareMatcher(self.some_compare_to, self.some_data_type)
        self.compare_mock = self.patch_object(self.matcher, 'compare')

        self.transform_mock.reset_mock()

    def test_match_calls_transform_on_the_key(self):
        """Tests that match calls transform on the supplied key"""
        self.matcher.match(self.some_key)

        self.transform_mock.assert_called_once_with(self.some_key)

    def test_match_calls_returns_none_if_transformed_key_is_none(self):
        """Tests that match returns None if the transformed key is None"""
        self.transform_mock.side_effect = None
        self.transform_mock.return_value = None

        self.assertIsNone(self.matcher.match(self.some_key))

    def test_match_calls_compare_on_transformed_key_and_compare_to(self):
        """Tests that match calls compare with the transformed key and the compare_to value"""
        self.matcher.match(self.some_key)

        self.compare_mock.assert_called_once_with(self.transformed_key,
                                                  self.transformed_compare_to)

    def test_match_returns_compare_result(self):
        """
        Tests that match returns the result of running compare with the transformed key and the
        compare_to value
        """
        self.assertEqual(self.compare_mock.return_value, self.matcher.match(self.some_key))


class UserDefinedSegmentMatcherTests(TestCase):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_segment = mock.MagicMock()
        self.matcher = UserDefinedSegmentMatcher(self.some_segment)

    def test_match_calls_contain_in_segment(self):
        """Tests that match calls contains on the associated segment"""
        self.matcher.match(self.some_key)

        self.some_segment.contains.assert_called_once_with(self.some_key)

    def test_match_returns_result_of_contains(self):
        """Tests that match returns the result of calling contains on the associated segment"""
        self.assertEqual(self.some_segment.contains.return_value,
                         self.matcher.match(self.some_key))


class WhitelistMatcherTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_key = mock.MagicMock()

        self.some_whitelist = mock.MagicMock()
        self.whitelist_frozenset = mock.MagicMock()

        self.frozenset_mock = mock.MagicMock()
        self.frozenset_mock.return_value = self.whitelist_frozenset

        with self.patch_builtin('frozenset', self.frozenset_mock):
            self.matcher = WhitelistMatcher(self.some_whitelist)

    def test_match_calls_in_on_whitelist(self):
        """Tests that match checks if the key is in the (frozen) whitelist"""
        self.matcher.match(self.some_key)

        self.whitelist_frozenset.__contains__.assert_called_once_with(self.some_key)

    def test_match_calls_returns_result_of_checkinf_in(self):
        """
        Tests that match returns the result of checking if the key is in the (frozen) whitelist
        """
        self.assertEqual(self.whitelist_frozenset.__contains__.return_value,
                         self.matcher.match(self.some_key))
