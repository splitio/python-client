"""Splits synchronization logic."""
import logging
import re
import itertools

from future.utils import raise_from
import yaml

from splitio.api import APIException
from splitio.models import splits


_LEGACY_COMMENT_LINE_RE = re.compile(r'^#.*$')
_LEGACY_DEFINITION_LINE_RE = re.compile(r'^(?<![^#])(?P<feature>[\w_-]+)\s+(?P<treatment>[\w_-]+)$')


_LOGGER = logging.getLogger(__name__)


class SplitSynchronizer(object):
    """Split changes synchronizer."""

    def __init__(self, split_api, split_storage):
        """
        Class constructor.

        :param split_api: Split API Client.
        :type split_api: splitio.api.splits.SplitsAPI

        :param split_storage: Split Storage.
        :type split_storage: splitio.storage.InMemorySplitStorage
        """
        self._api = split_api
        self._split_storage = split_storage

    def synchronize_splits(self, till=None):
        """
        Hit endpoint, update storage and return True if sync is complete.

        :param till: Passed till from Streaming.
        :type till: int
        """
        while True:
            change_number = self._split_storage.get_change_number()
            if change_number is None:
                change_number = -1
            if till is not None and till < change_number:
                # the passed till is less than change_number, no need to perform updates
                return

            try:
                split_changes = self._api.fetch_splits(change_number)
            except APIException as exc:
                _LOGGER.error('Exception raised while fetching splits')
                _LOGGER.debug('Exception information: ', exc_info=True)
                raise exc

            for split in split_changes.get('splits', []):
                if split['status'] == splits.Status.ACTIVE.value:
                    self._split_storage.put(splits.from_raw(split))
                else:
                    self._split_storage.remove(split['name'])

            self._split_storage.set_change_number(split_changes['till'])
            if split_changes['till'] == split_changes['since'] \
               and (till is None or split_changes['till'] >= till):
                return

    def kill_split(self, split_name, default_treatment, change_number):
        """
        Local kill for split.

        :param split_name: name of the split to perform kill
        :type split_name: str
        :param default_treatment: name of the default treatment to return
        :type default_treatment: str
        :param change_number: change_number
        :type change_number: int
        """
        self._split_storage.kill_locally(split_name, default_treatment, change_number)


class LocalSplitSynchronizer(object):
    """Localhost mode split synchronizer."""

    def __init__(self, filename, split_storage):
        """
        Class constructor.

        :param filename: File to parse splits from.
        :type filename: str
        :param split_storage: Split Storage.
        :type split_storage: splitio.storage.InMemorySplitStorage
        """
        self._filename = filename
        self._split_storage = split_storage

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

    def synchronize_splits(self, till=None):  # pylint:disable=unused-argument
        """Update splits in storage."""
        _LOGGER.info('Synchronizing splits now.')
        if self._filename.lower().endswith(('.yaml', '.yml')):
            fetched = self._read_splits_from_yaml_file(self._filename)
        else:
            fetched = self._read_splits_from_legacy_file(self._filename)
        to_delete = [name for name in self._split_storage.get_split_names()
                     if name not in fetched.keys()]
        for split in fetched.values():
            self._split_storage.put(split)

        for split in to_delete:
            self._split_storage.remove(split)
