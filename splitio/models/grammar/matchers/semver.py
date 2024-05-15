"""Semver matcher classes."""
import logging

from splitio.models.grammar.matchers.base import Matcher
from splitio.models.grammar.matchers.string import Sanitizer
from splitio.models.grammar.matchers.utils.utils import build_semver_or_none


_LOGGER = logging.getLogger(__name__)


class EqualToSemverMatcher(Matcher):
    """A matcher for Semver equal to."""

    def _build(self, raw_matcher):
        """
        Build an EqualToSemverMatcher.

        :param raw_matcher: raw matcher as fetched from splitChanges response.
        :type raw_matcher: dict
        """
        self._data = raw_matcher.get('stringMatcherData')
        self._semver = build_semver_or_none(raw_matcher.get('stringMatcherData'))

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
        if self._semver is None:
            _LOGGER.error("stringMatcherData is required for EQUAL_TO_SEMVER matcher type")
            return False

        matching_data = Sanitizer.ensure_string(self._get_matcher_input(key, attributes))
        if matching_data is None:
            return False

        matching_semver = build_semver_or_none(matching_data)
        if matching_semver is None:
            return False

        return self._semver.version == matching_semver.version

    def __str__(self):
        """Return string Representation."""
        return f'equal semver {self._data}'

    def _add_matcher_specific_properties_to_json(self):
        """Add matcher specific properties to base dict before returning it."""
        return {'matcherType': 'EQUAL_TO_SEMVER', 'stringMatcherData': self._data}

class GreaterThanOrEqualToSemverMatcher(Matcher):
    """A matcher for Semver greater than or equal to."""

    def _build(self, raw_matcher):
        """
        Build a GreaterThanOrEqualToSemverMatcher.

        :param raw_matcher: raw matcher as fetched from splitChanges response.
        :type raw_matcher: dict
        """
        self._data = raw_matcher.get('stringMatcherData')
        self._semver = build_semver_or_none(raw_matcher.get('stringMatcherData'))

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
        if self._semver is None:
            _LOGGER.error("stringMatcherData is required for GREATER_THAN_OR_EQUAL_TO_SEMVER matcher type")
            return False

        matching_data = Sanitizer.ensure_string(self._get_matcher_input(key, attributes))
        if matching_data is None:
            return False

        matching_semver = build_semver_or_none(matching_data)
        if matching_semver is None:
            return False

        return matching_semver.compare(self._semver) in [0, 1]

    def __str__(self):
        """Return string Representation."""
        return f'greater than or equal to semver {self._data}'

    def _add_matcher_specific_properties_to_json(self):
        """Add matcher specific properties to base dict before returning it."""
        return {'matcherType': 'GREATER_THAN_OR_EQUAL_TO_SEMVER', 'stringMatcherData': self._data}


class LessThanOrEqualToSemverMatcher(Matcher):
    """A matcher for Semver less than or equal to."""

    def _build(self, raw_matcher):
        """
        Build a LessThanOrEqualToSemverMatcher.

        :param raw_matcher: raw matcher as fetched from splitChanges response.
        :type raw_matcher: dict
        """
        self._data = raw_matcher.get('stringMatcherData')
        self._semver = build_semver_or_none(raw_matcher.get('stringMatcherData'))

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
        if self._semver is None:
            _LOGGER.error("stringMatcherData is required for LESS_THAN_OR_EQUAL_TO_SEMVER matcher type")
            return False

        matching_data = Sanitizer.ensure_string(self._get_matcher_input(key, attributes))
        if matching_data is None:
            return False

        matching_semver = build_semver_or_none(matching_data)
        if matching_semver is None:
            return False

        return matching_semver.compare(self._semver) in [0, -1]

    def __str__(self):
        """Return string Representation."""
        return f'less than or equal to semver {self._data}'

    def _add_matcher_specific_properties_to_json(self):
        """Add matcher specific properties to base dict before returning it."""
        return {'matcherType': 'LESS_THAN_OR_EQUAL_TO_SEMVER', 'stringMatcherData': self._data}


class BetweenSemverMatcher(Matcher):
    """A matcher for Semver between."""

    def _build(self, raw_matcher):
        """
        Build a BetweenSemverMatcher.

        :param raw_matcher: raw matcher as fetched from splitChanges response.
        :type raw_matcher: dict
        """
        self._data = raw_matcher.get('betweenStringMatcherData')
        self._semver_start = build_semver_or_none(self._data['start'])
        self._semver_end = build_semver_or_none(self._data['end'])

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
        if self._semver_start is None or self._semver_end is None:
            _LOGGER.error("betweenStringMatcherData is required for BETWEEN_SEMVER matcher type")
            return False

        matching_data = Sanitizer.ensure_string(self._get_matcher_input(key, attributes))
        if matching_data is None:
            return False

        matching_semver = build_semver_or_none(matching_data)
        if matching_semver is None:
            return False

        return (self._semver_start.compare(matching_semver) in [0, -1]) and (self._semver_end.compare(matching_semver) in [0, 1])

    def __str__(self):
        """Return string Representation."""
        return 'between semver {start} and {end}'.format(start=self._data.get('start'), end=self._data.get('end'))

    def _add_matcher_specific_properties_to_json(self):
        """Add matcher specific properties to base dict before returning it."""
        return {'matcherType': 'BETWEEN_SEMVER', 'betweenStringMatcherData': self._data}


class InListSemverMatcher(Matcher):
    """A matcher for Semver in list."""

    def _build(self, raw_matcher):
        """
        Build a InListSemverMatcher.

        :param raw_matcher: raw matcher as fetched from splitChanges response.
        :type raw_matcher: dict
        """
        self._data = raw_matcher['whitelistMatcherData']['whitelist']
        semver_list = [build_semver_or_none(item) for item in self._data if item]
        self._semver_list = frozenset([item.version for item in semver_list if item])

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
        if self._semver_list is None:
            _LOGGER.error("whitelistMatcherData is required for IN_LIST_SEMVER matcher type")
            return False

        matching_data = Sanitizer.ensure_string(self._get_matcher_input(key, attributes))
        if matching_data is None:
            return False

        matching_semver = build_semver_or_none(matching_data)
        if matching_semver is None:
            return False

        return matching_semver.version in self._semver_list

    def __str__(self):
        """Return string Representation."""
        return 'in list semver {data}'.format(data=self._data)

    def _add_matcher_specific_properties_to_json(self):
        """Add matcher specific properties to base dict before returning it."""
        return {'matcherType': 'IN_LIST_SEMVER', 'whitelistMatcherData': {'whitelist': self._data}}
