"""Abstract matcher module."""
import abc

from splitio.client.key import Key


class Matcher(object, metaclass=abc.ABCMeta):
    """Matcher abstract class."""

    def __init__(self, raw_matcher):
        """
        Initialize generic data and call matcher-specific parser.

        :param raw_matcher: raw matcher as read from splitChanges response.
        :type raw_matcher: dict

        :returns: A concrete matcher object.
        :rtype: Matcher
        """
        self._negate = raw_matcher['negate']
        self._matcher_type = raw_matcher['matcherType']
        key_selector = raw_matcher.get('keySelector')
        if key_selector is not None and 'attribute' in key_selector:
            self._attribute_name = raw_matcher['keySelector']['attribute']
        else:
            self._attribute_name = None
        self._build(raw_matcher)

    def _get_matcher_input(self, key, attributes=None):
        """
        Examine split, attributes & key, and return the appropriate matching input.

        :param key: User-submitted key
        :type key: str | Key
        :param attributes: User-submitted attributes
        :type attributes: dict

        :returns: data to use when matching
        :rtype: str | set | int | bool
        """
        if self._attribute_name is not None:
            if attributes is not None and attributes.get(self._attribute_name) is not None:
                return attributes[self._attribute_name]
            return None

        if isinstance(key, Key):
            return key.matching_key

        return key

    @abc.abstractmethod
    def _build(self, raw_matcher):
        """
        Build the final matcher according to matcher specific data.

        :param raw_matcher: raw matcher as read from splitChanges response.
        :type raw_matcher: dict
        """
        pass

    @abc.abstractmethod
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
        pass

    def evaluate(self, key, attributes=None, context=None):
        """
        Perform the actual evaluation taking into account possible matcher negation.

        :param key: User key.
        :type key: str.
        :param attributes: Custom user attributes.
        :type attributes: dict.
        :param context: Evaluation context
        :type context: dict
        """
        return self._negate ^ self._match(key, attributes, context)

    @abc.abstractmethod
    def _add_matcher_specific_properties_to_json(self):
        """
        Add matcher specific properties to base dict before returning it.

        :return: Dictionary with matcher specific prooperties.
        :rtype: dict
        """
        pass

    def to_json(self):
        """
        Reconstruct the original JSON representation of the matcher.

        :return: JSON representation of a matcher.
        :rtype: dict
        """
        base = {
            "keySelector": {'attribute': self._attribute_name} if self._attribute_name else None,
            "matcherType": self._matcher_type,
            "negate": self._negate,
            "userDefinedSegmentMatcherData": None,
            "whitelistMatcherData": None,
            "unaryNumericMatcherData": None,
            "betweenMatcherData": None,
            "dependencyMatcherData": None,
            "booleanMatcherData": None,
            "stringMatcherData": None,
        }
        base.update(self._add_matcher_specific_properties_to_json())
        return base
