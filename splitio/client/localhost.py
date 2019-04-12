"""Localhost client mocked components."""

import logging
import re
from splitio.models.splits import from_raw
from splitio.storage import ImpressionStorage, EventStorage, TelemetryStorage
from splitio.tasks import BaseSynchronizationTask
from splitio.tasks.util import asynctask

_COMMENT_LINE_RE = re.compile('^#.*$')
_DEFINITION_LINE_RE = re.compile('^(?<![^#])(?P<feature>[\w_-]+)\s+(?P<treatment>[\w_-]+)$')


_LOGGER = logging.getLogger(__name__)


class LocalhostImpressionsStorage(ImpressionStorage):
    """Impression storage that doesn't cache anything."""

    def put(self, *_, **__):  #pylint: disable=arguments-differ
        """Accept any arguments and do nothing."""
        pass

    def pop_many(self, *_, **__):  #pylint: disable=arguments-differ
        """Accept any arguments and do nothing."""
        pass


class LocalhostEventsStorage(EventStorage):
    """Impression storage that doesn't cache anything."""

    def put(self, *_, **__):  #pylint: disable=arguments-differ
        """Accept any arguments and do nothing."""
        pass

    def pop_many(self, *_, **__):  #pylint: disable=arguments-differ
        """Accept any arguments and do nothing."""
        pass


class LocalhostTelemetryStorage(TelemetryStorage):
    """Impression storage that doesn't cache anything."""

    def inc_latency(self, *_, **__):  #pylint: disable=arguments-differ
        """Accept any arguments and do nothing."""
        pass

    def inc_counter(self, *_, **__):  #pylint: disable=arguments-differ
        """Accept any arguments and do nothing."""
        pass

    def put_gauge(self, *_, **__):  #pylint: disable=arguments-differ
        """Accept any arguments and do nothing."""
        pass

    def pop_latencies(self, *_, **__):  #pylint: disable=arguments-differ
        """Accept any arguments and do nothing."""
        pass

    def pop_counters(self, *_, **__):  #pylint: disable=arguments-differ
        """Accept any arguments and do nothing."""
        pass

    def pop_gauges(self, *_, **__):  #pylint: disable=arguments-differ
        """Accept any arguments and do nothing."""
        pass


class LocalhostSplitSynchronizationTask(BaseSynchronizationTask):
    """Split synchronization task that periodically checks the file and updated the splits."""

    def __init__(self, filename, storage, ready_event):
        """
        Class constructor.

        :param filename: File to parse splits from.
        :type filename: str
        :param storage: Split storage
        :type storage: splitio.storage.SplitStorage
        :param ready_event: Eevent to set when sync is done.
        :type ready_event: threading.Event
        """
        self._filename = filename
        self._ready_event = ready_event
        self._storage = storage
        self._task = asynctask.AsyncTask(self._update_splits, 5, self._on_start)

    def _on_start(self):
        """Sync splits and set event if successful."""
        self._update_splits()
        self._ready_event.set()

    @staticmethod
    def _make_all_keys_based_split(split_name, treatment):
        """
        Make a split with a single all_keys matcher.

        :param split_name: Name of the split.
        :type split_name: str.
        """
        return from_raw({
            'changeNumber': 123,
            'trafficTypeName': 'user',
            'name': split_name,
            'trafficAllocation': 100,
            'trafficAllocationSeed': 123456,
            'seed': 321654,
            'status': 'ACTIVE',
            'killed': False,
            'defaultTreatment': treatment,
            'algo': 2,
            'conditions': [
               {
                    'partitions': [
                        {'treatment': treatment, 'size': 100}
                    ],
                    'contitionType': 'WHITELIST',
                    'label': 'some_other_label',
                    'matcherGroup': {
                        'matchers': [
                            {
                                'matcherType': 'ALL_KEYS',
                                'negate': False,
                            }
                        ],
                        'combiner': 'AND'
                    }
                }
            ]
        })

    @classmethod
    def _read_splits_from_file(cls, filename):
        """
        Parse a splits file and return a populated storage.

        :param filename: Path of the file containing mocked splits & treatments.
        :type filename: str.

        :return: Storage populataed with splits ready to be evaluated.
        :rtype: InMemorySplitStorage
        """
        splits = {}
        try:
            with open(filename, 'r') as flo:
                for line in flo:
                    if line.strip() == '':
                        continue

                    comment_match = _COMMENT_LINE_RE.match(line)
                    if comment_match:
                        continue

                    definition_match = _DEFINITION_LINE_RE.match(line)
                    if definition_match:
                        splits[definition_match.group('feature')] = cls._make_all_keys_based_split(
                            definition_match.group('feature'),
                            definition_match.group('treatment')
                        )
                        continue

                    _LOGGER.warning(
                        'Invalid line on localhost environment split '
                        'definition. Line = %s',
                        line
                    )
            return splits
        except IOError as e:
            raise ValueError("Error parsing split file")
            # TODO: ver raise from!
#            raise_from(ValueError(
#                'There was a problem with '
#                'the splits definition file "{}"'.format(filename)),
#                e
#            )


    def _update_splits(self):
        """Update splits in storage."""
        _LOGGER.info('Synchronizing splits now.')
        splits = self._read_splits_from_file(self._filename)
        to_delete = [name for name in self._storage.get_split_names() if name not in splits.keys()]
        for split in splits.values():
            self._storage.put(split)

        for split in to_delete:
            self._storage.remove(split)


    def is_running(self):
        """Return whether the task is running."""
        return self._task.running

    def start(self):
        """Start split synchronization."""
        self._task.start()

    def stop(self, stop_event):
        """
        Stop task.

        :param stop_event: Event top set when the task finishes.
        :type stop_event: threading.Event.
        """
        self._task.stop(stop_event)


