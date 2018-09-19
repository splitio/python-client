"""A module for Split.io Managers"""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging

from splitio.redis_support import RedisSplitCache
from splitio.splits import SplitView
from splitio.utils import bytes_to_string
from . import input_validator


class SplitManager(object):
    def __init__(self):
        """Basic interface of a SplitManager. Specific implementations need to override the
        splits, split and split_names method.
        """
        self._logger = logging.getLogger(self.__class__.__name__)

    def split_names(self):
        """Get the name of fetched splits.
        :return: A list of str
        :rtype: list
        """
        raise NotImplementedError()

    def splits(self):  # pragma: no cover
        """Get the fetched splits. Subclasses need to override this method.
        :return: A List of SplitView.
        :rtype: list
        """
        raise NotImplementedError()

    def split(self, feature_name):  # pragma: no cover
        """Get the splitView of feature_name. Subclasses need to override this method.
        :return: The SplitView instance.
        :rtype: SplitView
        """
        raise NotImplementedError()


class RedisSplitManager(SplitManager):
    def __init__(self, redis_broker):
        """A SplitManager implementation that uses Redis as its backend.
        :param redis: A redis client
        :type redis: StrctRedis"""
        super(RedisSplitManager, self).__init__()

        split_fetcher = redis_broker.get_split_fetcher()
        split_cache = split_fetcher._split_cache

        self._split_cache = split_cache
        self._split_fetcher = split_fetcher

    def split_names(self):
        """Get the name of fetched splits.
        :return: A list of str
        :rtype: list
        """
        splits = self._split_cache.get_splits_keys()
        split_names = []
        for split_name in splits:
            split_name = bytes_to_string(split_name)
            split_names.append(split_name.replace
                               (RedisSplitCache._KEY_TEMPLATE.format(suffix=''), ''))

        return split_names

    def splits(self):  # pragma: no cover
        """Get the fetched splits. Subclasses need to override this method.
        :return: A List of SplitView.
        :rtype: list
        """
        splits = self._split_fetcher.fetch_all()
        change_number = self._split_cache.get_change_number()

        split_views = []

        for split in splits:
            treatments = []
            if hasattr(split, 'conditions'):
                for condition in split.conditions:
                    for partition in condition.partitions:
                        treatments.append(partition.treatment)
                split_views.append(SplitView(name=split.name, traffic_type=split.traffic_type_name,
                                             killed=split.killed, treatments=list(set(treatments)),
                                             change_number=change_number))

        return split_views

    def split(self, feature_name):  # pragma: no cover
        """Get the splitView of feature_name. Subclasses need to override this method.
        :return: The SplitView instance.
        :rtype: SplitView
        """
        feature_name = input_validator.validate_manager_feature_name(feature_name)

        if feature_name is None:
            return None

        split = self._split_fetcher.fetch(feature_name)

        if split is None:
            return None

        change_number = self._split_cache.get_change_number()

        treatments = []

        for condition in split.conditions:
            for partition in condition.partitions:
                treatments.append(partition.treatment)

        # Using sets to avoid duplicate entries
        split_view = SplitView(name=split.name, traffic_type=split.traffic_type_name,
                               killed=split.killed, treatments=list(set(treatments)),
                               change_number=change_number)
        return split_view


class UWSGISplitManager(SplitManager):
    def __init__(self, broker):
        """A SplitManager implementation that uses uWSGI as its backend.
        :param uwsgi: A uwsgi module
        :type uwsgi: module"""
        super(UWSGISplitManager, self).__init__()

        split_fetcher = broker.get_split_fetcher()
        split_cache = split_fetcher._split_cache

        self._split_cache = split_cache
        self._split_fetcher = split_fetcher

    def split_names(self):
        """Get the name of fetched splits.
        :return: A list of str
        :rtype: list
        """
        splits = self._split_cache.get_splits_keys()
        split_names = []
        for split_name in splits:
            split_name = bytes_to_string(split_name)
            split_names.append(split_name)

        return split_names

    def splits(self):  # pragma: no cover
        """Get the fetched splits. Subclasses need to override this method.
        :return: A List of SplitView.
        :rtype: list
        """
        splits = self._split_fetcher.fetch_all()

        split_views = []

        for split in splits:
            treatments = []
            if hasattr(split, 'conditions'):
                for condition in split.conditions:
                    for partition in condition.partitions:
                        treatments.append(partition.treatment)
                split_views.append(SplitView(name=split.name, traffic_type=split.traffic_type_name,
                                   killed=split.killed, treatments=list(set(treatments)),
                                   change_number=split.change_number))

        return split_views

    def split(self, feature_name):  # pragma: no cover
        """Get the splitView of feature_name. Subclasses need to override this method.
        :return: The SplitView instance.
        :rtype: SplitView
        """
        feature_name = input_validator.validate_manager_feature_name(feature_name)

        if feature_name is None:
            return None

        split = self._split_fetcher.fetch(feature_name)

        if split is None:
            return None

        treatments = []

        for condition in split.conditions:
            for partition in condition.partitions:
                treatments.append(partition.treatment)

        # Using sets on treatments to avoid duplicate entries
        split_view = SplitView(name=split.name, traffic_type=split.traffic_type_name,
                               killed=split.killed, treatments=list(set(treatments)),
                               change_number=split.change_number)
        return split_view


class SelfRefreshingSplitManager(SplitManager):

    def __init__(self, broker):
        """A SplitManager implementation that uses in-memory as its backend.
        :param redis: A SplitFetcher instance
        :type redis: SelfRefreshingSplitFetcher"""
        super(SelfRefreshingSplitManager, self).__init__()

        self._split_fetcher = broker.get_split_fetcher()

    def split_names(self):
        """Get the name of fetched splits.
        :return: A list of str
        :rtype: list
        """
        splits = self._split_fetcher.fetch_all()
        split_names = []
        for split in splits:
            split_names.append(split.name)

        return split_names

    def splits(self):  # pragma: no cover
        """Get the fetched splits.
        :return: A List of SplitView.
        :rtype: list
        """
        change_number = self._split_fetcher.change_number
        splits = self._split_fetcher.fetch_all()

        split_views = []

        for split in splits:
            treatments = []
            for condition in split.conditions:
                for partition in condition.partitions:
                    treatments.append(partition.treatment)
            split_views.append(SplitView(name=split.name, traffic_type=split.traffic_type_name,
                               killed=split.killed, treatments=list(set(treatments)),
                               change_number=change_number))

        return split_views

    def split(self, feature_name):
        """Get the splitView of feature_name. Subclasses need to override this method.
        :return: The SplitView instance.
        :rtype: SplitView
        """
        feature_name = input_validator.validate_manager_feature_name(feature_name)

        if feature_name is None:
            return None

        split = self._split_fetcher.fetch(feature_name)

        if split is None:
            return None

        change_number = self._split_fetcher.change_number

        treatments = []

        for condition in split.conditions:
            for partition in condition.partitions:
                treatments.append(partition.treatment)

        # Using sets to avoid duplicate entries
        split_view = SplitView(name=split.name, traffic_type=split.traffic_type_name,
                               killed=split.killed, treatments=list(set(treatments)),
                               change_number=change_number)
        return split_view


class LocalhostSplitManager(SplitManager):
    def __init__(self, split_fetcher):
        """
        Basic interface of a SplitManager. Specific implementations need to
        override the splits, split and split_names method.
        """
        super(LocalhostSplitManager, self).__init__()
        self._split_fetcher = split_fetcher

    def split_names(self):
        """
        Get the name of fetched splits.
        :return: A list of str
        :rtype: list
        """
        splits = self._split_fetcher.fetch_all()
        split_names = []
        for split in splits:
            split_names.append(split.name)

        return split_names

    def splits(self):  # pragma: no cover
        """Get the fetched splits.
        :return: A List of SplitView.
        :rtype: list
        """
        change_number = -1
        splits = self._split_fetcher.fetch_all()

        split_views = []

        for split in splits:
            treatments = [split.default_treatment]
            split_views.append(SplitView(name=split.name, traffic_type=None, killed=split.killed,
                               treatments=list(set(treatments)), change_number=change_number))

        return split_views

    def split(self, feature_name):
        """
        Get the splitView of feature_name. Subclasses need to override this
        method.
        :return: The SplitView instance.
        :rtype: SplitView
        """
        split = self._split_fetcher.fetch(feature_name)
        if split is None:
            return None

        change_number = -1
        treatments = [split.default_treatment]

        # Using sets to avoid duplicate entries
        split_view = SplitView(name=split.name, traffic_type=None, killed=split.killed,
                               treatments=list(set(treatments)), change_number=change_number)
        return split_view
