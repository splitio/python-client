"""Semver matcher classes."""
import abc

class Semver(object, metaclass=abc.ABCMeta):
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
        self._old_version = version
        self._parse()

    def _parse(self):
        """
        Parse the string in self._old_version to update the other internal variables
        """
        without_metadata = self.remove_metadata_if_exists()

        index = without_metadata.find(self._PRE_RELEASE_DELIMITER)
        if index == -1:
            self._is_stable = True
        else:
            pre_release_data = without_metadata[index+1:]
            without_metadata = without_metadata[:index]
            self._pre_release = pre_release_data.split(self._VALUE_DELIMITER)

        self.set_major_minor_and_patch(without_metadata)

    def remove_metadata_if_exists(self):
        """
        Check if there is any metadata characters in self._old_version.

        :returns: The semver string without the metadata
        :rtype: str
        """
        index = self._old_version.find(self._METADATA_DELIMITER)
        if index == -1:
            return self._old_version

        return  self._old_version[:index]

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
        if self._old_version == to_compare._old_version:
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
