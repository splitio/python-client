"""Semver matcher classes."""
import logging

from splitio.models.grammar.matchers.base import Matcher
from splitio.models.grammar.matchers.string import Sanitizer

_LOGGER = logging.getLogger(__name__)

class Semver(object):
    """Semver class."""

    _METADATA_DELIMITER = "+"
    _PRE_RELEASE_DELIMITER = "-"
    _VALUE_DELIMITER = "."

    def __init__(self, version):
        """
        Class Initializer

        :param version: raw version as read from splitChanges response.
        :type version: str
        """
        self._major = 0
        self._minor = 0
        self._patch = 0
        self._pre_release = []
        self._is_stable = False
        self.version = ""
        self._metadata = ""
        self._parse(version)

    @classmethod
    def build(cls, version):
        try:
            self = cls(version)
        except RuntimeError as e:
            _LOGGER.error("Failed to parse Semver data, incorrect data type:  %s", e)
            return None

        return self

    def _parse(self, version):
        """
        Parse the string in self.version to update the other internal variables
        """
        without_metadata = self.remove_metadata_if_exists(version)

        index = without_metadata.find(self._PRE_RELEASE_DELIMITER)
        if index == -1:
            self._is_stable = True
        else:
            pre_release_data = without_metadata[index+1:]
            if pre_release_data == "":
                raise RuntimeError("Pre-release is empty despite delimeter exists: " + version)

            without_metadata = without_metadata[:index]
            self._pre_release = pre_release_data.split(self._VALUE_DELIMITER)

        self.set_major_minor_and_patch(without_metadata)

    def remove_metadata_if_exists(self, version):
        """
        Check if there is any metadata characters in self.version.

        :returns: The semver string without the metadata
        :rtype: str
        """
        index = version.find(self._METADATA_DELIMITER)
        if index == -1:
            return version

        self._metadata = version[index+1:]
        if self._metadata == "":
            raise RuntimeError("Metadata is empty despite delimeter exists: " + version)

        return  version[:index]

    def set_major_minor_and_patch(self, version):
        """
        Set the major, minor and patch internal variables based on string passed.

        :param version: raw version containing major.minor.patch numbers.
        :type version: str
        """

        parts = version.split(self._VALUE_DELIMITER)
        if len(parts) != 3 or not (parts[0].isnumeric() and parts[1].isnumeric() and parts[2].isnumeric()):
            raise RuntimeError("Unable to convert to Semver, incorrect format: " + version)

        self._major = int(parts[0])
        self._minor = int(parts[1])
        self._patch = int(parts[2])

        self.version = "{major}{DELIMITER}{minor}{DELIMITER}{patch}".format(major = self._major, DELIMITER = self._VALUE_DELIMITER,
                    minor = self._minor, patch = self._patch)
        self.version += "{DELIMITER}{pre_release}".format(DELIMITER=self._PRE_RELEASE_DELIMITER,
                    pre_release = '.'.join(self._pre_release)) if len(self._pre_release) > 0 else ""
        self.version += "{DELIMITER}{metadata}".format(DELIMITER=self._METADATA_DELIMITER, metadata = self._metadata) if self._metadata != "" else ""

    def compare(self, to_compare):
        """
        Compare the current Semver object to a given Semver object, return:
            0: if self == passed
            1: if self > passed
            -1: if self < passed

        :param to_compare: a Semver object
        :type to_compare: splitio.models.grammar.matchers.semver.Semver

        :returns: integer based on comparison
        :rtype: int
        """
        if self.version == to_compare.version:
            return 0

        # Compare major, minor, and patch versions numerically
        result = self._compare_vars(self._major, to_compare._major)
        if result != 0:
            return result

        result = self._compare_vars(self._minor, to_compare._minor)
        if result != 0:
            return result

        result = self._compare_vars(self._patch, to_compare._patch)
        if result != 0:
            return result

        if not self._is_stable and to_compare._is_stable:
            return -1
        elif self._is_stable and not to_compare._is_stable:
            return 1

        # Compare pre-release versions lexically
        min_length = min(len(self._pre_release), len(to_compare._pre_release))
        for i in range(min_length):
            if self._pre_release[i] == to_compare._pre_release[i]:
                continue

            if self._pre_release[i].isnumeric() and to_compare._pre_release[i].isnumeric():
                 return self._compare_vars(int(self._pre_release[i]), int(to_compare._pre_release[i]))

            return self._compare_vars(self._pre_release[i], to_compare._pre_release[i])

        # Compare lengths of pre-release versions
        return self._compare_vars(len(self._pre_release), len(to_compare._pre_release))

    def _compare_vars(self, var1, var2):
        """
        Compare 2 variables and return int as follows:
            0: if var1 == var2
            1: if var1 > var2
            -1: if var1 < var2

        :param var1: any object accept ==, < or > operators
        :type var1: str/int
        :param var2: any object accept ==, < or > operators
        :type var2: str/int

        :returns: integer based on comparison
        :rtype: int
        """
        if var1 == var2:
            return 0
        if var1 > var2:
            return 1
        return -1

class EqualToSemverMatcher(Matcher):
    """A matcher for Semver equal to."""

    def _build(self, raw_matcher):
        """
        Build an EqualToSemverMatcher.

        :param raw_matcher: raw matcher as fetched from splitChanges response.
        :type raw_matcher: dict
        """
        self._data = raw_matcher.get('stringMatcherData')
        self._semver = Semver.build(self._data)

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
        if self._data is None or self._semver is None:
            _LOGGER.error("stringMatcherData is required for EQUAL_TO_SEMVER matcher type")
            return False

        matching_data = Sanitizer.ensure_string(self._get_matcher_input(key, attributes))
        if matching_data is None:
            return False

        matching_semver = Semver.build(matching_data)
        if matching_semver is None:
            return False

        return self._semver.version == matching_semver.version

    def __str__(self):
        """Return string Representation."""
        return 'equal semver {data}'.format(data=self._data)

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
        self._semver = Semver.build(self._data)

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
        if self._data is None or self._semver is None:
            _LOGGER.error("stringMatcherData is required for GREATER_THAN_OR_EQUAL_TO_SEMVER matcher type")
            return False

        matching_data = Sanitizer.ensure_string(self._get_matcher_input(key, attributes))
        if matching_data is None:
            return False

        matching_semver = Semver.build(matching_data)
        if matching_semver is None:
            return False

        return matching_semver.compare(self._semver) in [0, 1]

    def __str__(self):
        """Return string Representation."""
        return 'greater than or equal to semver {data}'.format(data=self._data)

    def _add_matcher_specific_properties_to_json(self):
        """Add matcher specific properties to base dict before returning it."""
        return {'matcherType': 'GREATER_THAN_OR_EQUAL_TO_SEMVER', 'stringMatcherData': self._data}
