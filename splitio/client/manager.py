"""A module for Split.io Managers."""
import logging

from . import input_validator


_LOGGER = logging.getLogger(__name__)


class SplitManager(object):
    """Split Manager. Gives insights on data cached by splits."""

    def __init__(self, factory):
        """
        Class constructor.

        :param factory: Factory containing all storage references.
        :type factory: splitio.client.factory.SplitFactory
        """
        self._factory = factory
        self._storage = factory._get_storage('splits')  # pylint: disable=protected-access
        self._telemetry_init_producer = factory._telemetry_init_producer

    def split_names(self):
        """
        Get the name of fetched splits.

        :return: A list of str
        :rtype: list
        """
        if self._factory.destroyed:
            _LOGGER.error("Client has already been destroyed - no calls possible.")
            return []

        if self._factory._waiting_fork():
            _LOGGER.error("Client is not ready - no calls possible")
            return []

        if not self._factory.ready:
            self._telemetry_init_producer.record_not_ready_usage()
            _LOGGER.warning(
                "split_names: The SDK is not ready, results may be incorrect. "
                "Make sure to wait for SDK readiness before using this method"
            )

        return self._storage.get_split_names()

    def splits(self):
        """
        Get the fetched splits. Subclasses need to override this method.

        :return: A List of SplitView.
        :rtype: list()
        """
        if self._factory.destroyed:
            _LOGGER.error("Client has already been destroyed - no calls possible.")
            return []

        if self._factory._waiting_fork():
            _LOGGER.error("Client is not ready - no calls possible")
            return []

        if not self._factory.ready:
            self._telemetry_init_producer.record_not_ready_usage()
            _LOGGER.warning(
                "splits: The SDK is not ready, results may be incorrect. "
                "Make sure to wait for SDK readiness before using this method"
            )

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
            _LOGGER.error("Client has already been destroyed - no calls possible.")
            return None

        if self._factory._waiting_fork():
            _LOGGER.error("Client is not ready - no calls possible")
            return None

        feature_flag = input_validator.validate_manager_feature_flag_name(
            feature_name,
            self._factory.ready,
            self._storage
        )

        if not self._factory.ready:
            self._telemetry_init_producer.record_not_ready_usage()
            _LOGGER.warning(
                "split: The SDK is not ready, results may be incorrect. "
                "Make sure to wait for SDK readiness before using this method"
            )

        return feature_flag.to_split_view() if feature_flag is not None else None

class SplitManagerAsync(object):
    """Split Manager. Gives insights on data cached by splits."""

    def __init__(self, factory):
        """
        Class constructor.

        :param factory: Factory containing all storage references.
        :type factory: splitio.client.factory.SplitFactory
        """
        self._factory = factory
        self._storage = factory._get_storage('splits')  # pylint: disable=protected-access
        self._telemetry_init_producer = factory._telemetry_init_producer

    async def split_names(self):
        """
        Get the name of fetched splits.

        :return: A list of str
        :rtype: list
        """
        if self._factory.destroyed:
            _LOGGER.error("Client has already been destroyed - no calls possible.")
            return []

        if self._factory._waiting_fork():
            _LOGGER.error("Client is not ready - no calls possible")
            return []

        if not self._factory.ready:
            await self._telemetry_init_producer.record_not_ready_usage()
            _LOGGER.warning(
                "split_names: The SDK is not ready, results may be incorrect. "
                "Make sure to wait for SDK readiness before using this method"
            )

        return await self._storage.get_split_names()

    async def splits(self):
        """
        Get the fetched splits. Subclasses need to override this method.

        :return: A List of SplitView.
        :rtype: list()
        """
        if self._factory.destroyed:
            _LOGGER.error("Client has already been destroyed - no calls possible.")
            return []

        if self._factory._waiting_fork():
            _LOGGER.error("Client is not ready - no calls possible")
            return []

        if not self._factory.ready:
            await self._telemetry_init_producer.record_not_ready_usage()
            _LOGGER.warning(
                "splits: The SDK is not ready, results may be incorrect. "
                "Make sure to wait for SDK readiness before using this method"
            )

        return [split.to_split_view() for split in await self._storage.get_all_splits()]

    async def split(self, feature_name):
        """
        Get the splitView of feature_name. Subclasses need to override this method.

        :param feature_name: Name of the feture to retrieve.
        :type feature_name: str

        :return: The SplitView instance.
        :rtype: splitio.models.splits.SplitView
        """
        if self._factory.destroyed:
            _LOGGER.error("Client has already been destroyed - no calls possible.")
            return None

        if self._factory._waiting_fork():
            _LOGGER.error("Client is not ready - no calls possible")
            return None

        feature_flag = await input_validator.validate_manager_feature_flag_name_async(
            feature_name,
            self._factory.ready,
            self._storage
        )

        if not self._factory.ready:
            await self._telemetry_init_producer.record_not_ready_usage()
            _LOGGER.warning(
                "split: The SDK is not ready, results may be incorrect. "
                "Make sure to wait for SDK readiness before using this method"
            )

        return feature_flag.to_split_view() if feature_flag is not None else None
