"""Localhost client mocked components."""

import itertools
import logging
import re

from future.utils import raise_from
import yaml

from splitio.models import splits
from splitio.storage import ImpressionStorage, EventStorage, TelemetryStorage
from splitio.tasks import BaseSynchronizationTask
from splitio.tasks.util import asynctask

_LEGACY_COMMENT_LINE_RE = re.compile(r'^#.*$')
_LEGACY_DEFINITION_LINE_RE = re.compile(r'^(?<![^#])(?P<feature>[\w_-]+)\s+(?P<treatment>[\w_-]+)$')


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

    def __init__(self, filename, storage, period, ready_event):
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
        self._period = period
        self._task = asynctask.AsyncTask(self._update_splits, period, self._on_start)

    def _on_start(self):
        """Sync splits and set event if successful."""
        self._update_splits()
        self._ready_event.set()

    @staticmethod
    def _make_split(split_name, conditions, configs=None):
        """
        Make a split with a single all_keys matcher.

        :param split_name: Name of the split.
        :type split_name: str.
        """
        return splits.from_raw({
            'changeNumber': 123,
            'trafficTypeName': 'user',
            'name': split_name,
            'trafficAllocation': 100,
            'trafficAllocationSeed': 123456,
            'seed': 321654,
            'status': 'ACTIVE',
            'killed': False,
            'defaultTreatment': 'control',
            'algo': 2,
            'conditions': conditions,
            'configurations': configs
        })

    @staticmethod
    def _make_all_keys_condition(treatment):
        return {
            'partitions': [
                {'treatment': treatment, 'size': 100}
            ],
            'conditionType': 'WHITELIST',
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

    @staticmethod
    def _make_whitelist_condition(whitelist, treatment):
        return {
            'partitions': [
                {'treatment': treatment, 'size': 100}
            ],
            'conditionType': 'WHITELIST',
            'label': 'some_other_label',
            'matcherGroup': {
                'matchers': [
                    {
                        'matcherType': 'WHITELIST',
                        'negate': False,
                        'whitelistMatcherData': {
                            'whitelist': whitelist
                        }
                    }
                ],
                'combiner': 'AND'
            }
        }

    @classmethod
    def _read_splits_from_legacy_file(cls, filename):
        """
        Parse a splits file and return a populated storage.

        :param filename: Path of the file containing mocked splits & treatments.
        :type filename: str.

        :return: Storage populataed with splits ready to be evaluated.
        :rtype: InMemorySplitStorage
        """
        to_return = {}
        try:
            with open(filename, 'r') as flo:
                for line in flo:
                    if line.strip() == '' or _LEGACY_COMMENT_LINE_RE.match(line):
                        continue

                    definition_match = _LEGACY_DEFINITION_LINE_RE.match(line)
                    if not definition_match:
                        _LOGGER.warning(
                            'Invalid line on localhost environment split '
                            'definition. Line = %s',
                            line
                        )
                        continue

                    cond = cls._make_all_keys_condition(definition_match.group('treatment'))
                    splt = cls._make_split(definition_match.group('feature'), [cond])
                    to_return[splt.name] = splt
            return to_return

        except IOError as exc:
            raise_from(
                ValueError("Error parsing file %s. Make sure it's readable." % filename),
                exc
            )

    @classmethod
    def _read_splits_from_yaml_file(cls, filename):
        """
        Parse a splits file and return a populated storage.

        :param filename: Path of the file containing mocked splits & treatments.
        :type filename: str.

        :return: Storage populataed with splits ready to be evaluated.
        :rtype: InMemorySplitStorage
        """
        try:
            with open(filename, 'r') as flo:
                parsed = yaml.load(flo.read(), Loader=yaml.FullLoader)

            grouped_by_feature_name = itertools.groupby(
                sorted(parsed, key=lambda i: next(iter(i.keys()))),
                lambda i: next(iter(i.keys())))

            to_return = {}
            for (split_name, statements) in grouped_by_feature_name:
                configs = {}
                whitelist = []
                all_keys = []
                for statement in statements:
                    data = next(iter(statement.values()))  # grab the first (and only) value.
                    if 'keys' in data:
                        keys = data['keys'] if isinstance(data['keys'], list) else [data['keys']]
                        whitelist.append(cls._make_whitelist_condition(keys, data['treatment']))
                    else:
                        all_keys.append(cls._make_all_keys_condition(data['treatment']))
                    if 'config' in data:
                        configs[data['treatment']] = data['config']
                to_return[split_name] = cls._make_split(split_name, whitelist + all_keys, configs)
            return to_return

        except IOError as exc:
            raise_from(
                ValueError("Error parsing file %s. Make sure it's readable." % filename),
                exc
            )

    def _update_splits(self):
        """Update splits in storage."""
        _LOGGER.info('Synchronizing splits now.')
        if self._filename.lower().endswith(('.yaml', '.yml')):
            fetched = self._read_splits_from_yaml_file(self._filename)
        else:
            fetched = self._read_splits_from_legacy_file(self._filename)
        to_delete = [name for name in self._storage.get_split_names() if name not in fetched.keys()]
        for split in fetched.values():
            self._storage.put(split)

        for split in to_delete:
            self._storage.remove(split)

    def is_running(self):
        """Return whether the task is running."""
        return self._task.running

    def start(self):
        """Start split synchronization."""
        self._task.start()

    def stop(self, event=None):
        """
        Stop task.

        :param stop_event: Event top set when the task finishes.
        :type stop_event: threading.Event.
        """
        self._task.stop(event)
