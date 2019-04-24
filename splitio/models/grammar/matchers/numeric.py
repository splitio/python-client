"""Numeric & Date based matchers."""
import numbers

import logging
from future.utils import python_2_unicode_compatible
from six import string_types

from splitio.models.grammar.matchers.base import Matcher
from splitio.models import datatypes


class Sanitizer(object):  # pylint: disable=too-few-public-methods
    """Numeric input sanitizer."""

    _logger = logging.getLogger('InputSanitizer')

    @classmethod
    def ensure_int(cls, data):
        """
        Do a best effort attempt to conver input to a int.

        :param input: user supplied input.
        :type input: mixed.
        """
        if data is None:  # Failed to fetch attribute. no need to convert.
            return None

        # For some reason bool is considered an integral type. We want to avoid True
        # to be converted to 1, and False to 0 on numeric matchers since it can be
        # misleading.
        if isinstance(data, numbers.Integral) and not isinstance(data, bool):
            return data

        if not isinstance(data, string_types):
            cls._logger.error('Cannot convert %s to int. Failing.', type(data))
            return None


        cls._logger.warning(
            'Supplied attribute is of type %s and should have been an int. ',
            type(data)
        )

        try:
            return int(data)
        except ValueError:
            cls._logger.error('Cannot convert %s to int. Failing.', type(data))
            return None


class ZeroSecondDataMatcher(object):  #pylint: disable=too-few-public-methods
    """Mixin to use in matchers that when dealing with datetimes, truncate seconds."""

    data_parsers = {
        'NUMBER': lambda x: x,
        'DATETIME': datatypes.java_ts_truncate_seconds
    }

    input_parsers = {
        'NUMBER': lambda x: x,
        'DATETIME': datatypes.ts_truncate_seconds
    }


class ZeroTimeDataMatcher(object):  #pylint: disable=no-init,too-few-public-methods
    """Mixin to use in matchers that when dealing with datetimes, truncate time."""

    input_parsers = {
        'NUMBER': lambda x: x,
        'DATETIME': datatypes.ts_truncate_time
    }

    data_parsers = {
        'NUMBER': lambda x: x,
        'DATETIME': datatypes.java_ts_truncate_time
    }


class BetweenMatcher(Matcher, ZeroSecondDataMatcher):
    """Matcher that returns true if user input is within a specified range."""

    def _build(self, raw_matcher):
        """
        Build InBetweenMatcher.

        :param raw_matcher: raw matcher as fetched from splitChanges response.
        :type raw_matcher: dict
        """
        self._data_type = raw_matcher['betweenMatcherData']['dataType']
        self._original_lower = raw_matcher['betweenMatcherData']['start']
        self._original_upper = raw_matcher['betweenMatcherData']['end']
        self._lower = self.data_parsers[self._data_type](self._original_lower)
        self._upper = self.data_parsers[self._data_type](self._original_upper)

    def _match(self, key, attributes=None, context=None):
        """
        Evaluate user input against matcher and return whether the match is successful.

        :param key: User key.
        :type key: str.
        :param attributes: Custom user attributes.
        :type attributes: dict.
        :param context: Evaluation context
        :type context: dict

        :returns: Wheter the match is successful.
        :rtype: bool
        """
        matching_data = Sanitizer.ensure_int(self._get_matcher_input(key, attributes))
        if matching_data is None:
            return False
        return self._lower <= self.input_parsers[self._data_type](matching_data) <= self._upper

    @python_2_unicode_compatible
    def __str__(self):
        """Return string Representation."""
        return 'between {start} and {end}'.format(start=self._lower, end=self._upper)

    def _add_matcher_specific_properties_to_json(self):
        """Return BetweenMatcher specific properties."""
        return {
            'betweenMatcherData': {
                'dataType': self._data_type,
                'start': self._original_lower,
                'end': self._original_upper
            }
        }


class EqualToMatcher(Matcher, ZeroTimeDataMatcher):
    """Return true if the provided input is equal to the value stored in the matcher."""

    def _build(self, raw_matcher):
        """
        Build EqualToMatcher.

        :param raw_matcher: raw matcher as fetched from splitChanges response.
        :type raw_matcher: dict
        """
        self._data_type = raw_matcher['unaryNumericMatcherData']['dataType']
        self._original_value = raw_matcher['unaryNumericMatcherData']['value']
        self._value = self.data_parsers[self._data_type](self._original_value)

    def _match(self, key, attributes=None, context=None):
        """
        Evaluate user input against a matcher and return whether the match is successful.

        :param key: User key.
        :type key: str.
        :param attributes: Custom user attributes.
        :type attributes: dict.
        :param context: Evaluation context
        :type context: dict

        :returns: Wheter the match is successful.
        :rtype: bool
        """
        matching_data = Sanitizer.ensure_int(self._get_matcher_input(key, attributes))
        if matching_data is None:
            return False
        return self.input_parsers[self._data_type](matching_data) == self._value

    def _add_matcher_specific_properties_to_json(self):
        """Return EqualTo specific properties."""
        return {
            'unaryNumericMatcherData': {
                'dataType': self._data_type,
                'value': self._original_value,
            }
        }


class GreaterThanOrEqualMatcher(Matcher, ZeroSecondDataMatcher):
    """Return true if the provided input is >= the value stored in the matcher."""

    def _build(self, raw_matcher):
        """
        Build GreaterThanOrEqualMatcher.

        :param raw_matcher: raw matcher as fetched from splitChanges response.
        :type raw_matcher: dict
        """
        self._data_type = raw_matcher['unaryNumericMatcherData']['dataType']
        self._original_value = raw_matcher['unaryNumericMatcherData']['value']
        self._value = self.data_parsers[self._data_type](self._original_value)

    def _match(self, key, attributes=None, context=None):
        """
        Evaluate user input against a matcher and return whether the match is successful.

        :param key: User key.
        :type key: str.
        :param attributes: Custom user attributes.
        :type attributes: dict.
        :param context: Evaluation context
        :type context: dict

        :returns: Wheter the match is successful.
        :rtype: bool
        """
        matching_data = Sanitizer.ensure_int(self._get_matcher_input(key, attributes))
        if matching_data is None:
            return False
        return self.input_parsers[self._data_type](matching_data) >= self._value

    def _add_matcher_specific_properties_to_json(self):
        """Return GreaterThan specific properties."""
        return {
            'unaryNumericMatcherData': {
                'dataType': self._data_type,
                'value': self._original_value,
            }
        }


class LessThanOrEqualMatcher(Matcher, ZeroSecondDataMatcher):
    """Return true if the provided input is <= the value stored in the matcher."""

    def _build(self, raw_matcher):
        """
        Build LessThanOrEqualMatcher.

        :param raw_matcher: raw matcher as fetched from splitChanges response.
        :type raw_matcher: dict
        """
        self._data_type = raw_matcher['unaryNumericMatcherData']['dataType']
        self._original_value = raw_matcher['unaryNumericMatcherData']['value']
        self._value = self.data_parsers[self._data_type](self._original_value)

    def _match(self, key, attributes=None, context=None):
        """
        Evaluate user input against a matcher and return whether the match is successful.

        :param key: User key.
        :type key: str.
        :param attributes: Custom user attributes.
        :type attributes: dict.
        :param context: Evaluation context
        :type context: dict

        :returns: Wheter the match is successful.
        :rtype: bool
        """
        matching_data = Sanitizer.ensure_int(self._get_matcher_input(key, attributes))
        if matching_data is None:
            return False
        return self.input_parsers[self._data_type](matching_data) <= self._value

    def _add_matcher_specific_properties_to_json(self):
        """Return LessThan specific properties."""
        return {
            'unaryNumericMatcherData': {
                'dataType': self._data_type,
                'value': self._original_value,
            }
        }
