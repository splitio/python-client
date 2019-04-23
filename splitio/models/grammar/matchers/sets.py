"""Set based matchers module."""
from __future__ import absolute_import, division, print_function, \
    unicode_literals
from future.utils import python_2_unicode_compatible

from splitio.models.grammar.matchers.base import Matcher


class ContainsAllOfSetMatcher(Matcher):
    """Matcher that returns true if the user data is a subset of the matcher's data."""

    def _build(self, raw_matcher):
        """
        Build an ContainsAllOfSetMatcher.

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
        matching_data = self._get_matcher_input(key, attributes)
        if matching_data is None:
            return False
        try:
            setkey = set(matching_data)
            return self._whitelist.issubset(setkey)
        except TypeError:
            return False

    def _add_matcher_specific_properties_to_json(self):
        """Return ContainsAllOfSet specific properties."""
        return {
            'whitelistMatcherData': {
                'whitelist': list(list(self._whitelist))
            }
        }

    @python_2_unicode_compatible
    def __str__(self):
        """Return string Representation."""
        return 'contains all of the following set: [{whitelist}]'.format(
            whitelist=','.join('"{}"'.format(item) for item in self._whitelist)
        )


class ContainsAnyOfSetMatcher(Matcher):
    """Matcher that returns true if the intersection of both sets is not empty."""

    def _build(self, raw_matcher):
        """
        Build an ContainsAnyOfSetMatcher.

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
        matching_data = self._get_matcher_input(key, attributes)
        if matching_data is None:
            return False
        try:
            return len(self._whitelist.intersection(set(matching_data))) != 0
        except TypeError:
            return False

    def _add_matcher_specific_properties_to_json(self):
        """Return ContainsAnyOfSet specific properties."""
        return {
            'whitelistMatcherData': {
                'whitelist': list(self._whitelist)
            }
        }

    @python_2_unicode_compatible
    def __str__(self):
        """Return string Representation."""
        return 'contains on of the following se: [{whitelist}]'.format(
            whitelist=','.join('"{}"'.format(item) for item in self._whitelist)
        )


class EqualToSetMatcher(Matcher):
    """Matcher that returns true if the set provided by the user is equal to the matcher's one."""

    def _build(self, raw_matcher):
        """
        Build an EqualToSetMatcher.

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
        matching_data = self._get_matcher_input(key, attributes)
        if matching_data is None:
            return False
        try:
            return self._whitelist == set(matching_data)
        except TypeError:
            return False

    def _add_matcher_specific_properties_to_json(self):
        """Return EqualToSet specific properties."""
        return {
            'whitelistMatcherData': {
                'whitelist': list(self._whitelist)
            }
        }

    @python_2_unicode_compatible
    def __str__(self):
        """Return string Representation."""
        return 'equals the following set: [{whitelist}]'.format(
            whitelist=','.join('"{}"'.format(item) for item in self._whitelist)
        )


class PartOfSetMatcher(Matcher):
    """a."""

    def _build(self, raw_matcher):
        """
        Build an PartOfSetMatcher.

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
        matching_data = self._get_matcher_input(key, attributes)
        if matching_data is None:
            return False
        try:
            setkey = set(matching_data)
            return len(setkey) > 0 and setkey.issubset(set(self._whitelist))
        except TypeError:
            return False

    def _add_matcher_specific_properties_to_json(self):
        """Return PartOfSet specific properties."""
        return {
            'whitelistMatcherData': {
                'whitelist': list(self._whitelist)
            }
        }

    @python_2_unicode_compatible
    def __str__(self):
        """Return string Representation."""
        return 'is a subset of the following set: [{whitelist}]'.format(
            whitelist=','.join('"{}"'.format(item) for item in self._whitelist)
        )
