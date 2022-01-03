"""Localhost client mocked components."""

import itertools
import logging
import re

from future.utils import raise_from
import yaml

from splitio.models import splits
from splitio.storage import ImpressionStorage, EventStorage
from splitio.tasks import BaseSynchronizationTask
from splitio.tasks.util import asynctask

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
