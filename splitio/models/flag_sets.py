"""Flagsets classes."""
import threading

class FlagSetsFilter(object):
    """Config Flagsets Filter storage."""

    def __init__(self, flag_sets=[]):
        """Constructor."""
        self.flag_sets = set(flag_sets)
        self.should_filter = any(flag_sets)

    def set_exist(self, flag_set):
        """
        Check if a flagset exist in flagset filter

        :param flag_set: set name
        :type flag_set: str

        :rtype: bool
        """
        if not self.should_filter:
            return True
        if not isinstance(flag_set, str) or flag_set == '':
            return False

        return any(self.flag_sets.intersection(set([flag_set])))

    def intersect(self, flag_sets):
        """
        Check if a set exist in config flagset filter

        :param flag_set: set of flagsets
        :type flag_set: set

        :rtype: bool
        """
        if not self.should_filter:
            return True
        if not isinstance(flag_sets, set) or len(flag_sets) == 0:
            return False
        return any(self.flag_sets.intersection(flag_sets))


class FlagSets(object):
    """InMemory Flagsets storage."""

    def __init__(self, flag_sets=[]):
        """Constructor."""
        self._lock = threading.RLock()
        self.sets_feature_flag_map = {}
        for flag_set in flag_sets:
            self.sets_feature_flag_map[flag_set] = set()

    def flag_set_exist(self, flag_set):
        """
        Check if a flagset exist in stored flagset

        :param flag_set: set name
        :type flag_set: str

        :rtype: bool
        """
        with self._lock:
            return flag_set in self.sets_feature_flag_map.keys()

    def get_flag_set(self, flag_set):
        """
        fetch feature flags stored in a flag set

        :param flag_set: set name
        :type flag_set: str

        :rtype: list(str)
        """
        with self._lock:
            if self.flag_set_exist(flag_set):
                return self.sets_feature_flag_map[flag_set]

    def add_flag_set(self, flag_set):
        """
        Add new flag set to storage

        :param flag_set: set name
        :type flag_set: str
        """
        with self._lock:
            if not self.flag_set_exist(flag_set):
                self.sets_feature_flag_map[flag_set] = set()

    def remove_flag_set(self, flag_set):
        """
        Remove existing flag set from storage

        :param flag_set: set name
        :type flag_set: str
        """
        with self._lock:
            if self.flag_set_exist(flag_set):
                del self.sets_feature_flag_map[flag_set]

    def add_feature_flag_to_flag_set(self, flag_set, feature_flag):
        """
        Add a feature flag to existing flag set

        :param flag_set: set name
        :type flag_set: str
        :param feature_flag: feature flag name
        :type feature_flag: str
        """
        with self._lock:
            if self.flag_set_exist(flag_set):
                self.sets_feature_flag_map[flag_set].add(feature_flag)

    def remove_feature_flag_to_flag_set(self, flag_set, feature_flag):
        """
        Remove a feature flag from existing flag set

        :param flag_set: set name
        :type flag_set: str
        :param feature_flag: feature flag name
        :type feature_flag: str
        """
        with self._lock:
            if self.flag_set_exist(flag_set):
                self.sets_feature_flag_map[flag_set].remove(feature_flag)
