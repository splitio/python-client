"""Utils module."""

import logging

_LOGGER = logging.getLogger(__name__)

M_DELIMITER = "+"
P_DELIMITER = "-"
V_DELIMITER = "."
    

def compare(var1, var2):
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


def build_semver_or_none(version):
    try:
        return Semver(version)
    except (RuntimeError, ValueError):
        _LOGGER.error("Invalid semver version: %s", version)
        return None


class Semver(object):
    """Semver class."""

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
        self._version = ""
        self._metadata = ""
        self._parse(version)

    def _parse(self, version):
        """
        Parse the string in self.version to update the other internal variables
        """
        without_metadata = self._extract_metadata(version)
        index = without_metadata.find(P_DELIMITER)
        if index == -1:
            self._is_stable = True
        else:
            pre_release_data = without_metadata[index+1:]
            if pre_release_data == "":
                raise RuntimeError("Pre-release is empty despite delimiter exists: " + version)

            without_metadata = without_metadata[:index]
            for pre_digit in pre_release_data.split(V_DELIMITER):
                if pre_digit.isnumeric():
                    pre_digit = str(int(pre_digit))
                self._pre_release.append(pre_digit)

        self._set_components(without_metadata)

    def _extract_metadata(self, version):
        """
        Check if there is any metadata characters in self.version.

        :returns: The semver string without the metadata
        :rtype: str
        """
        index = version.find(M_DELIMITER)
        if index == -1:
            return version

        self._metadata = version[index+1:]
        if self._metadata == "":
            raise RuntimeError("Metadata is empty despite delimiter exists: " + version)

        return version[:index]

    def _set_components(self, version):
        """
        Set the major, minor and patch internal variables based on string passed.

        :param version: raw version containing major.minor.patch numbers.
        :type version: str
        """

        parts = version.split(V_DELIMITER)
        if len(parts) != 3:
            raise RuntimeError("Unable to convert to Semver, incorrect format: " + version)
        try:
            self._major, self._minor, self._patch = int(parts[0]), int(parts[1]), int(parts[2])
            self._version = f"{self._major}{V_DELIMITER}{self._minor}{V_DELIMITER}{self._patch}"
            self._version += f"{P_DELIMITER + V_DELIMITER.join(self._pre_release) if len(self._pre_release) > 0 else ''}"
            self._version += f"{M_DELIMITER + self._metadata if self._metadata else ''}"
        except Exception:
            raise RuntimeError("Unable to convert to Semver, incorrect format: " + version)

    @property
    def version(self):
        return self._version

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
        result = compare(self._major, to_compare._major)
        if result != 0:
            return result

        result = compare(self._minor, to_compare._minor)
        if result != 0:
            return result

        result = compare(self._patch, to_compare._patch)
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
                return compare(int(self._pre_release[i]), int(to_compare._pre_release[i]))

            return compare(self._pre_release[i], to_compare._pre_release[i])

        # Compare lengths of pre-release versions
        return compare(len(self._pre_release), len(to_compare._pre_release))
