"""Miscelaneous matchers that don't fall into other categories."""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import json
from future.utils import python_2_unicode_compatible
from six import string_types

from splitio.models.grammar.matchers.base import Matcher


class DependencyMatcher(Matcher):
    """Matcher that returns true if the user's key secondary evaluation result matches."""

    def _build(self, raw_matcher):
        """
        Build an DependencyMatcher.

        :param raw_matcher: raw matcher as fetched from splitChanges response.
        :type raw_matcher: dict
        """
        self._split_name = raw_matcher['dependencyMatcherData']['split']
        self._treatments = raw_matcher['dependencyMatcherData']['treatments']

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
        evaluator = context.get('evaluator')
        assert evaluator is not None

        bucketing_key = context.get('bucketing_key')

        result = evaluator.evaluate_treatment(self._split_name, key, bucketing_key, attributes)
        return result['treatment'] in self._treatments

    def _add_matcher_specific_properties_to_json(self):
        """Return Dependency specific properties."""
        return {
            'dependencyMatcherData': {
                'split': self._split_name,
                'treatments': self._treatments
            }
        }


class BooleanMatcher(Matcher):
    """Matcher that returns true if the user submited value is similar to the stored boolean."""

    def _build(self, raw_matcher):
        """
        Build an BooleanMatcher.

        :param raw_matcher: raw matcher as fetched from splitChanges response.
        :type raw_matcher: dict
        """
        self._data = raw_matcher['booleanMatcherData']

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
        if isinstance(matching_data, bool):
            decoded = matching_data
        elif isinstance(matching_data, string_types):
            try:
                decoded = json.loads(matching_data.lower())
                if not isinstance(decoded, bool):
                    return False
            except ValueError:
                return False
        else:
            return False

        return decoded == self._data

    def _add_matcher_specific_properties_to_json(self):
        """Return Boolean specific properties."""
        return {'booleanMatcherData': self._data}
