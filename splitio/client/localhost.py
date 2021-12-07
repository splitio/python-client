"""Localhost client mocked components."""
import logging
import re

from splitio.storage import ImpressionStorage, EventStorage

_LEGACY_COMMENT_LINE_RE = re.compile(r'^#.*$')
_LEGACY_DEFINITION_LINE_RE = re.compile(r'^(?<![^#])(?P<feature>[\w_-]+)\s+(?P<treatment>[\w_-]+)$')


_LOGGER = logging.getLogger(__name__)


class LocalhostImpressionsStorage(ImpressionStorage):
    """Impression storage that doesn't cache anything."""

    def put(self, *_, **__):  # pylint: disable=arguments-differ
        """Accept any arguments and do nothing."""
        pass

    def pop_many(self, *_, **__):  # pylint: disable=arguments-differ
        """Accept any arguments and do nothing."""
        pass

    def clear(self, *_, **__):  # pylint: disable=arguments-differ
        """Accept any arguments and do nothing."""
        pass


class LocalhostEventsStorage(EventStorage):
    """Impression storage that doesn't cache anything."""

    def put(self, *_, **__):  # pylint: disable=arguments-differ
        """Accept any arguments and do nothing."""
        pass

    def pop_many(self, *_, **__):  # pylint: disable=arguments-differ
        """Accept any arguments and do nothing."""
        pass

    def clear(self, *_, **__):  # pylint: disable=arguments-differ
        """Accept any arguments and do nothing."""
        pass
