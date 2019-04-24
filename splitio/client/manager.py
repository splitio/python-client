"""A module for Split.io Managers."""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging

from . import input_validator


class SplitManager(object):
    """Split Manager. Gives insights on data cached by splits."""

    def __init__(self, factory):
        """
        Class constructor.

        :param factory: Factory containing all storage references.
        :type factory: splitio.client.factory.SplitFactory
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._factory = factory
        self._storage = factory._get_storage('splits')

    def split_names(self):
        """
        Get the name of fetched splits.

        :return: A list of str
        :rtype: list
        """
        if self._factory.destroyed:
            self._logger.error("Client has already been destroyed - no calls possible.")
            return []

        return self._storage.get_split_names()

    def splits(self):
        """
        Get the fetched splits. Subclasses need to override this method.

        :return: A List of SplitView.
        :rtype: list()
        """
        if self._factory.destroyed:
            self._logger.error("Client has already been destroyed - no calls possible.")
            return []

        return [split.to_split_view() for split in self._storage.get_all_splits()]

    def split(self, feature_name):
        """
        Get the splitView of feature_name. Subclasses need to override this method.

        :param feature_name: Name of the feture to retrieve.
        :type feature_name: str

        :return: The SplitView instance.
        :rtype: splitio.models.splits.SplitView
        """
        if self._factory.destroyed:
            self._logger.error("Client has already been destroyed - no calls possible.")
            return []

        feature_name = input_validator.validate_manager_feature_name(feature_name)
        if feature_name is None:
            return None

        split = self._storage.get(feature_name)
        return split.to_split_view() if split is not None else None
