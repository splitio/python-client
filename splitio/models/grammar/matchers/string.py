"""String matchers module."""
import logging
import json
import re

from splitio.models.grammar.matchers.base import Matcher


_LOGGER = logging.getLogger(__name__)


class Sanitizer(object):  # pylint: disable=too-few-public-methods
    """Numeric input sanitizer."""

    @classmethod
    def ensure_string(cls, data):
        """
        Do a best effort attempt to conver input to a string.

        :param input: user supplied input.
        :type input: mixed.

        :return: String or None
        :rtype: string
        """
        if data is None:  # Failed to fetch attribute. no need to convert.
            return None

        if isinstance(data, str):
            return data

        _LOGGER.warning(
            'Supplied attribute is of type %s and should have been a string. ',
            type(data)
        )
        try:
            return json.dumps(data)
        except TypeError:
            return None


class WhitelistMatcher(Matcher):
    """Matcher that returns true if the user key is within a whitelist."""

    def _build(self, raw_matcher):
        """
        Build an WhitelistMatcher.

        :param raw_matcher: raw matcher as fetched from splitChanges response.
        :type raw_matcher: dict
        """
        self._whitelist = frozenset(raw_matcher['whitelistMatcherData']['whitelist'])

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
        matching_data = Sanitizer.ensure_string(self._get_matcher_input(key, attributes))
        if matching_data is None:
            return False
        return matching_data in self._whitelist

    def _add_matcher_specific_properties_to_json(self):
        """Return Whitelist specific properties."""
        return {
            'whitelistMatcherData': {
                'whitelist': list(self._whitelist)
            }
        }

    def __str__(self):
        """Return string Representation."""
        return 'in whitelist [{whitelist}]'.format(
            whitelist=','.join('"{}"'.format(item) for item in self._whitelist)
        )


class StartsWithMatcher(Matcher):
    """Matcher that returns true if the key is a prefix of the stored value."""

    def _build(self, raw_matcher):
        """
        Build an StartsWithMatcher.

        :param raw_matcher: raw matcher as fetched from splitChanges response.
        :type raw_matcher: dict
        """
        self._whitelist = frozenset(raw_matcher['whitelistMatcherData']['whitelist'])

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
        matching_data = Sanitizer.ensure_string(self._get_matcher_input(key, attributes))
        if matching_data is None:
            return False
        return (isinstance(key, str) and
                any(matching_data.startswith(s) for s in self._whitelist))

    def _add_matcher_specific_properties_to_json(self):
        """Return StartsWith specific properties."""
        return {
            'whitelistMatcherData': {
                'whitelist': list(self._whitelist)
            }
        }

    def __str__(self):
        """Return string Representation."""
        return 'has one of the following prefixes [{whitelist}]'.format(
            whitelist=','.join('"{}"'.format(item) for item in self._whitelist)
        )


class EndsWithMatcher(Matcher):
    """Matcher that returns true if the key ends with the suffix stored in matcher data."""

    def _build(self, raw_matcher):
        """
        Build an EndsWithMatcher.

        :param raw_matcher: raw matcher as fetched from splitChanges response.
        :type raw_matcher: dict
        """
        self._whitelist = frozenset(raw_matcher['whitelistMatcherData']['whitelist'])

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
        matching_data = Sanitizer.ensure_string(self._get_matcher_input(key, attributes))
        if matching_data is None:
            return False
        return (isinstance(key, str) and
                any(matching_data.endswith(s) for s in self._whitelist))

    def _add_matcher_specific_properties_to_json(self):
        """Return EndsWith specific properties."""
        return {
            'whitelistMatcherData': {
                'whitelist': list(self._whitelist)
            }
        }

    def __str__(self):
        """Return string Representation."""
        return 'has one of the following suffixes [{whitelist}]'.format(
            whitelist=','.join('"{}"'.format(item) for item in self._whitelist)
        )


class ContainsStringMatcher(Matcher):
    """Matcher that returns true if the input key is part of the string in matcher data."""

    def _build(self, raw_matcher):
        """
        Build a ContainsStringMatcher.

        :param raw_matcher: raw matcher as fetched from splitChanges response.
        :type raw_matcher: dict
        """
        self._whitelist = frozenset(raw_matcher['whitelistMatcherData']['whitelist'])

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
        matching_data = Sanitizer.ensure_string(self._get_matcher_input(key, attributes))
        if matching_data is None:
            return False
        return (isinstance(matching_data, str) and
                any(s in matching_data for s in self._whitelist))

    def _add_matcher_specific_properties_to_json(self):
        """Return ContainsString specific properties."""
        return {
            'whitelistMatcherData': {
                'whitelist': list(self._whitelist)
            }
        }

    def __str__(self):
        """Return string Representation."""
        return 'contains one of the following string: [{whitelist}]'.format(
            whitelist=','.join('"{}"'.format(item) for item in self._whitelist)
        )


class RegexMatcher(Matcher):
    """Matcher that returns true if the user input matches the regex stored in the matcher."""

    def _build(self, raw_matcher):
        """
        Build a RegexMatcher.

        :param raw_matcher: raw matcher as fetched from splitChanges response.
        :type raw_matcher: dict
        """
        self._data = raw_matcher['stringMatcherData']
        self._regex = re.compile(self._data)

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
        matching_data = Sanitizer.ensure_string(self._get_matcher_input(key, attributes))
        if matching_data is None:
            return False
        try:
            matches = re.search(self._regex, matching_data)
            return matches is not None
        except TypeError:
            return False

    def _add_matcher_specific_properties_to_json(self):
        """Return Regex specific properties."""
        return {'stringMatcherData': self._data}
