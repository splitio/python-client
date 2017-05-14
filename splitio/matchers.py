from __future__ import absolute_import, division, print_function, \
    unicode_literals

from enum import Enum
from sys import modules

from future.utils import python_2_unicode_compatible
from six import string_types
from splitio.transformers import AsDateHourMinuteTimestampTransformMixin, \
    AsNumberTransformMixin, AsDateTimestampTransformMixin, TransformMixin


DataType = Enum('DataType', 'DATETIME NUMBER')


class AndCombiner(object):
    """Combines the calls to all delegates match() method with a conjunction"""
    def combine(self, matchers, key, attributes):
        """
        Combines the calls to the delegates match() methods to produce a single
        boolean response
        :param matchers: List of delegate matchers
        :type matchers: list
        :param key: Key to match
        :type key: str
        :param attributes: Attributes to match
        :type attributes: dict
        :return: Conjunction of all matchers match() results
        :rtype: bool
        """
        if not matchers:
            return False

        return all(matcher.match(key, attributes) for matcher in matchers)

    @python_2_unicode_compatible
    def __str__(self):
        return 'and'


class CombiningMatcher(object):
    def __init__(self, combiner, delegates):
        """
        Combines the results of multiple delegate matchers using a specific
        combiner to produce a single boolean result
        :param combiner: The combiner to use to generate a single result from
            the individual ones
        :type combiner: AndCombiner
        :param delegates: Delegate matchers
        :type delegates: list
        """
        self._combiner = combiner
        self._delegates = tuple(delegates)

    def match(self, key, attributes=None):
        """
        Tests whether there is a match for the given key and attributes
        :param key: Key to match
        :type key: str
        :param attributes: Attributes to match
        :type attributes: dict
        :return: Whether there is a match for the given key and attributes
        :rtype: bool
        """
        return self._combiner.combine(self._delegates, key, attributes)

    @python_2_unicode_compatible
    def __str__(self):
        return 'if {delegates}'.format(
            delegates=' '.join(
                '{combiner} {matcher}'.format(combiner=self._combiner, matcher=matcher)
                for matcher in self._delegates))


class AllKeysMatcher(object):
    """A matcher that always returns True"""
    def match(self, key):
        """
        Returns True except when the key is None
        :param key: The key to match
        :type key: str
        :return: True except when the key is None
        :rtype: bool
        """
        return key is not None

    @python_2_unicode_compatible
    def __str__(self):
        return 'in segment all'


class NegatableMatcher(object):
    def __init__(self, negate, delegate):
        """
        A matcher that negates the result of a delegate matcher based on the
        negate flag
        :param negate: Whether to negate the result of the delegate matcher
        :type negate: bool
        :param delegate: The delegate matcher
        :type delegate: Matcher
        """
        self._negate = negate
        self._delegate = delegate

    def match(self, key):
        """
        Check of a match for the given key
        :param key: The key to match
        :type key: str
        :return: True if there is a match, False otherwise
        :rtype: bool
        """
        result = self._delegate.match(key)
        return result if not self._negate else not result

    @property
    def negate(self):
        return self._negate

    @property
    def delegate(self):
        return self._delegate

    @python_2_unicode_compatible
    def __str__(self):
        return '{negate}{delegate}'.format(
            negate='not ' if self._negate else '',
            delegate=self._delegate
        )


class AttributeMatcher(object):
    def __init__(self, attribute, matcher, negate):
        """
        A matcher that looks for the value of a specific attribute and passes it
        to the delegate matcher to provide a result.
        :param attribute: Name of the attribute
        :type attribute: str
        :param matcher: The delegate matcher
        :type matcher: Matcher
        :param negate: Whether to negate the result
        :type negate: bool
        """
        self._attribute = attribute
        self._matcher = NegatableMatcher(negate, matcher)

    def match(self, key, attributes=None):
        """
        Matches against the value of an attribute associated with the provided
        key
        :param key: The key to match
        :type key: str
        :param attributes: Dictionary of attributes to match
        :type attributes: dict
        :return: If negate is False, it returns the result of calling the
                 delegate match method on the attribute value associated with
                 the key. If negate is True, it returns the opposite.
        :rtype: bool
        """
        if self._attribute is None:
            return self._matcher.match(key)

        if attributes is None or \
                self._attribute not in attributes or \
                attributes[self._attribute] is None:
            return False

        return self._matcher.match(attributes[self._attribute])

    @python_2_unicode_compatible
    def __str__(self):
        return 'key{attribute} is {matcher}'.format(
            attribute='.{}'.format(self._attribute) if self._attribute is not None else '',
            matcher=self._matcher)


class ForDataTypeMixin(object):
    """
    A mixin to provide a class method called for_data_type to build the
    appropriate matcher for the given data type. The class needs to define a
    dictionary member named MATCHER_FOR_DATA_TYPE that matches constructors with
    data types like so:

    MATCHER_FOR_DATA_TYPE = {
        DataType.DATETIME: 'DateTimeBetweenMatcher',
        DataType.NUMBER: 'NumberBetweenMatcher'
    }

    Then, you can use the following syntax to build the appropriate constructor:

    matcher = BetweenMatcher.for_data_type(DataType.NUMBER, 5, 10)
    """
    @staticmethod
    def get_class(class_name):
        return getattr(modules[__name__], class_name)

    @classmethod
    def for_data_type(cls, data_type, *args, **kwargs):
        """
        Build a matcher appropriate for the supplied data type
        :param data_type: The data type for which to build a matcher
        :type data_type: DataType
        :param args: arguments to be passed to the actual matcher contructor
        :type args: iterable
        :param kwargs: keyword arguments to be passed to the actual matcher
            contructor
        :type kwargs: dict
        :return: A matcher appropriate for the given data type
        :rtype: Matcher
        """
        if data_type is None:
            raise ValueError('Invalid data type')

        return cls.get_class(cls.MATCHER_FOR_DATA_TYPE[data_type])(*args, **kwargs)


class BetweenMatcher(TransformMixin, ForDataTypeMixin):
    MATCHER_FOR_DATA_TYPE = {
        DataType.DATETIME: 'DateTimeBetweenMatcher',
        DataType.NUMBER: 'NumberBetweenMatcher'
    }

    def __init__(self, start, end, data_type):
        """
        A matcher that checks if a (transformed) value is between two other
        values.
        :param start: The start of the interval
        :type start: any
        :param end: The end of the interval
        :type end: any
        :param data_type: The data type for the values
        :type data_type: DataType
        """
        self._data_type = data_type
        self._original_start = start
        self._original_end = end
        self._start = self.transform_condition_parameter(start)
        self._end = self.transform_condition_parameter(end)

    @property
    def start(self):
        return self._start

    @property
    def end(self):
        return self._end

    def match(self, key):
        """
        Returns True if the key (after being transformed by the transform_key()
        method) is between start and end
        :param key: The key to match
        :type key: any
        :return: Whether the transformed key is between start and end
        :rtype: bool
        """
        transformed_key = self.transform_key(key)

        if transformed_key is None:
            return None

        return self._start <= transformed_key <= self._end

    @python_2_unicode_compatible
    def __str__(self):
        return 'between {start} and {end}'.format(
            start=self._start, end=self._end
        )


class DateTimeBetweenMatcher(BetweenMatcher,
                             AsDateHourMinuteTimestampTransformMixin):
    def __init__(self, start, end):
        super(DateTimeBetweenMatcher, self).__init__(
            start, end, DataType.DATETIME
        )


class NumberBetweenMatcher(BetweenMatcher, AsNumberTransformMixin):
    def __init__(self, start, end):
        super(NumberBetweenMatcher, self).__init__(start, end, DataType.NUMBER)


class CompareMixin(object):
    def compare(self, key, value):
        raise NotImplementedError()


class EqualToCompareMixin(CompareMixin):
    def compare(self, key, value):
        return key == value


class GreaterOrEqualToCompareMixin(CompareMixin):
    def compare(self, key, value):
        return key >= value


class LessThanOrEqualToCompareMixin(CompareMixin):
    def compare(self, key, value):
        return key <= value


class CompareMatcher(TransformMixin, CompareMixin):
    def __init__(self, compare_to, data_type):
        """
        A matcher that compares a (transformed) key to a specific value
        :param compare_to: The value to match
        :type compare_to: any
        :param data_type: The data type to use for comparison
        :type data_type: DataType
        """
        self._data_type = data_type
        self._original_compare_to = compare_to
        self._compare_to = self.transform_condition_parameter(compare_to)

    def match(self, key):
        """
        Compares the supplied key with the matcher's value using the compare()
        method
        :param key: The key to match
        :type key: str
        :return: The resulf of calling compare() with the key and the value
        :rtype: bool
        """
        transformed_key = self.transform_key(key)

        if transformed_key is None:
            return None

        return self.compare(transformed_key, self._compare_to)


class EqualToMatcher(CompareMatcher, EqualToCompareMixin, ForDataTypeMixin):
    MATCHER_FOR_DATA_TYPE = {
        DataType.DATETIME: 'DateEqualToMatcher',
        DataType.NUMBER: 'NumberEqualToMatcher'
    }

    @python_2_unicode_compatible
    def __str__(self):
        return '== {compare_to}'.format(compare_to=self._compare_to)


class GreaterThanOrEqualToMatcher(CompareMatcher, GreaterOrEqualToCompareMixin,
                                  ForDataTypeMixin):
    MATCHER_FOR_DATA_TYPE = {
        DataType.DATETIME: 'DateTimeGreaterThanOrEqualToMatcher',
        DataType.NUMBER: 'NumberGreaterThanOrEqualToMatcher'
    }

    @python_2_unicode_compatible
    def __str__(self):
        return '>= {compare_to}'.format(compare_to=self._compare_to)


class LessThanOrEqualToMatcher(CompareMatcher, LessThanOrEqualToCompareMixin,
                               ForDataTypeMixin):
    MATCHER_FOR_DATA_TYPE = {
        DataType.DATETIME: 'DateTimeLessThanOrEqualToMatcher',
        DataType.NUMBER: 'NumberLessThanOrEqualToMatcher'
    }

    @python_2_unicode_compatible
    def __str__(self):
        return '<= {compare_to}'.format(compare_to=self._compare_to)


class DateEqualToMatcher(EqualToMatcher, AsDateTimestampTransformMixin):
    def __init__(self, compare_to):
        super(DateEqualToMatcher, self).__init__(compare_to, DataType.DATETIME)


class NumberEqualToMatcher(EqualToMatcher, AsNumberTransformMixin):
    def __init__(self, compare_to):
        super(NumberEqualToMatcher, self).__init__(compare_to, DataType.NUMBER)


class DateTimeGreaterThanOrEqualToMatcher(GreaterThanOrEqualToMatcher,
                                          AsDateHourMinuteTimestampTransformMixin):
    def __init__(self, compare_to):
        super(DateTimeGreaterThanOrEqualToMatcher, self).__init__(
            compare_to, DataType.DATETIME
        )


class NumberGreaterThanOrEqualToMatcher(GreaterThanOrEqualToMatcher,
                                        AsNumberTransformMixin):
    def __init__(self, compare_to):
        super(NumberGreaterThanOrEqualToMatcher, self).__init__(
            compare_to,
            DataType.NUMBER
        )


class DateTimeLessThanOrEqualToMatcher(LessThanOrEqualToMatcher,
                                       AsDateHourMinuteTimestampTransformMixin):
    def __init__(self, compare_to):
        super(DateTimeLessThanOrEqualToMatcher, self).__init__(
            compare_to,
            DataType.DATETIME
        )


class NumberLessThanOrEqualToMatcher(LessThanOrEqualToMatcher,
                                     AsNumberTransformMixin):
    def __init__(self, compare_to):
        super(NumberLessThanOrEqualToMatcher, self).__init__(
            compare_to,
            DataType.NUMBER
        )


class UserDefinedSegmentMatcher(object):
    def __init__(self, segment):
        """
        A matcher that looks if a key is contained in a segment
        :param segment: The segment to match
        :type segment: Segment
        """
        self._segment = segment

    @property
    def segment(self):
        return self._segment

    def match(self, key):
        """
        Checks if key is contained within the segment by calling contains()
        :param key: The key to match
        :type key: str
        :return: The result of calling contains() on the segment
        :rtype: bool
        """
        return self._segment.contains(key)

    @python_2_unicode_compatible
    def __str__(self):
        return 'in segment {segment_name}'.format(
            segment_name=self._segment.name
        )


class WhitelistMatcher(object):
    def __init__(self, whitelist):
        """
        A matcher that checks if a key is in a whitelist
        :param whitelist: A list of strings of whitelisted keys
        :type whitelist: list
        """
        self._whitelist = frozenset(whitelist)

    def match(self, key):
        """
        Checks if a key is in the whitelist
        :param key: The key to match
        :type key: str
        :return: True if the key is in the whitelist, False otherwise
        :rtype: bool
        """
        return key in self._whitelist

    @python_2_unicode_compatible
    def __str__(self):
        return 'in whitelist [{whitelist}]'.format(
            whitelist=','.join('"{}"'.format(item) for item in self._whitelist)
        )


class StartsWithMatcher(object):
    def __init__(self, whitelist):
        """
        A matcher that checks if a any of the strings in whitelist is a prefix
        of key
        :param whitelist: A list of strings that will be treated as prefixes
        :type whitelist: list
        """
        self._whitelist = frozenset(whitelist)

    def match(self, key):
        """
        Checks if any of the strings in whitelist is a prefix of key
        :param key: The key to match
        :type key: str
        :return: True under the conditiones described above
        :rtype: bool
        """
        return (isinstance(key, string_types) and
                any(key.startswith(s) for s in self._whitelist))

    @python_2_unicode_compatible
    def __str__(self):
        return 'has one of the following prefixes [{whitelist}]'.format(
            whitelist=','.join('"{}"'.format(item) for item in self._whitelist)
        )


class EndsWithMatcher(object):
    def __init__(self, whitelist):
        """
        A matcher that checks if a any of the strings in whitelist is a suffix
        of key
        :param whitelist: A list of strings that will be treated as suffixes
        :type whitelist: list
        """
        self._whitelist = frozenset(whitelist)

    def match(self, key):
        """
        Checks if any of the strings in whitelist is a suffix of key
        :param key: The key to match
        :type key: str
        :return: True under the conditiones described above
        :rtype: bool
        """
        return (isinstance(key, string_types) and
                any(key.endswith(s) for s in self._whitelist))

    @python_2_unicode_compatible
    def __str__(self):
        return 'has one of the following suffixes [{whitelist}]'.format(
            whitelist=','.join('"{}"'.format(item) for item in self._whitelist)
        )


class ContainsStringMatcher(object):
    def __init__(self, whitelist):
        """
        A matcher that checks if a any of the strings in whitelist is a is
        contained in key
        :param whitelist: A list of strings that will be treated as suffixes
        :type whitelist: list
        """
        self._whitelist = frozenset(whitelist)

    def match(self, key):
        """
        Checks if any of the strings in whitelist is a suffix of key
        :param key: The key to match
        :type key: str
        :return: True under the conditiones described above
        :rtype: bool
        """
        return (isinstance(key, string_types) and
                 any(s in key for s in self._whitelist))

    @python_2_unicode_compatible
    def __str__(self):
        return 'contains one of the following string: [{whitelist}]'.format(
            whitelist=','.join('"{}"'.format(item) for item in self._whitelist)
        )


class ContainsAllOfSetMatcher(object):
    def __init__(self, whitelist):
        """
        A matcher that checks if the key, treated as a set, contains all
        the elements in whitelist
        :param whitelist: A list of strings that will be treated as a set
        :type whitelist: list
        """
        self._whitelist = frozenset(whitelist)

    def match(self, key):
        """
        Checks if all the strings in whitelist are in the key when treated as
        a set
        :param key: The key to match
        :type key: str
        :return: True under the conditiones described above
        :rtype: bool
        """
        try:
            setkey = set(key)
            return set(self._whitelist).issubset(setkey)
        except TypeError:
            return False

    @python_2_unicode_compatible
    def __str__(self):
        return 'contains all of the following set: [{whitelist}]'.format(
            whitelist=','.join('"{}"'.format(item) for item in self._whitelist)
        )


class ContainsAnyOfSetMatcher(object):
    def __init__(self, whitelist):
        """
        A matcher that checks if the key, treated as a set, contains any
        the elements in whitelist
        :param whitelist: A list of strings that will be treated as a set
        :type whitelist: list
        """
        self._whitelist = frozenset(whitelist)

    def match(self, key):
        """
        Checks if any of the strings in whitelist are in the key when treated as
        a set
        :param key: The key to match
        :type key: str
        :return: True under the conditiones described above
        :rtype: bool
        """
        try:
            setkey = set(key)
            return set(self._whitelist).intersection(setkey)
        except TypeError:
            return False

    @python_2_unicode_compatible
    def __str__(self):
        return 'contains on of the following se: [{whitelist}]'.format(
            whitelist=','.join('"{}"'.format(item) for item in self._whitelist)
        )


class EqualToSetMatcher(object):
    def __init__(self, whitelist):
        """
        A matcher that checks if the key, treated as a set, is equal to the set
        formed by the elements in whitelist
        :param whitelist: A list of strings that will be treated as a set
        :type whitelist: list
        """
        self._whitelist = frozenset(whitelist)

    def match(self, key):
        """
        checks if the key, treated as a set, is equal to the set formed by the
        elements in whitelist
        :param key: The key to match
        :type key: str
        :return: True under the conditiones described above
        :rtype: bool
        """
        try:
            setkey = set(key)
            return set(self._whitelist) == setkey
        except TypeError:
            return False

    @python_2_unicode_compatible
    def __str__(self):
        return 'equals the following set: [{whitelist}]'.format(
            whitelist=','.join('"{}"'.format(item) for item in self._whitelist)
        )


class PartOfSetMatcher(object):
    def __init__(self, whitelist):
        """
        A matcher that checks if the key, treated as a set, is part of the
        whitelist set
        :param whitelist: A list of strings that will be treated as a set
        :type whitelist: list
        """
        self._whitelist = frozenset(whitelist)

    def match(self, key):
        """
        Checks if the whitelist set contains the 'key' set
        :param key: The key to match
        :type key: str
        :return: True under the conditiones described above
        :rtype: bool
        """
        try:
            setkey = set(key)
            return len(setkey) > 0 and setkey.issubset(set(self._whitelist))
        except TypeError:
            return False

    @python_2_unicode_compatible
    def __str__(self):
        return 'is a subset of the following set: [{whitelist}]'.format(
            whitelist=','.join('"{}"'.format(item) for item in self._whitelist)
        )
