"""Splits synchronization logic."""
import logging
import re
import itertools
import yaml
import time
import json
from enum import Enum

from splitio.api import APIException, APIUriException
from splitio.api.commons import FetchOptions
from splitio.client.input_validator import validate_flag_sets
from splitio.models import splits
from splitio.util.backoff import Backoff
from splitio.util.time import get_current_epoch_time_ms
from splitio.util.storage_helper import update_feature_flag_storage, update_feature_flag_storage_async
from splitio.sync import util
from splitio.optional.loaders import asyncio, aiofiles

_LEGACY_COMMENT_LINE_RE = re.compile(r'^#.*$')
_LEGACY_DEFINITION_LINE_RE = re.compile(r'^(?<![^#])(?P<feature>[\w_-]+)\s+(?P<treatment>[\w_-]+)$')


_LOGGER = logging.getLogger(__name__)


_ON_DEMAND_FETCH_BACKOFF_BASE = 10  # backoff base starting at 10 seconds
_ON_DEMAND_FETCH_BACKOFF_MAX_WAIT = 30  # don't sleep for more than 30 seconds
_ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES = 10


class SplitSynchronizerBase(object):
    """Feature Flag changes synchronizer."""

    def __init__(self, feature_flag_api, feature_flag_storage):
        """
        Class constructor.

        :param feature_flag_api: Feature Flag API Client.
        :type feature_flag_api: splitio.api.splits.SplitsAPI

        :param feature_flag_storage: Feature Flag Storage.
        :type feature_flag_storage: splitio.storage.InMemorySplitStorage
        """
        self._api = feature_flag_api
        self._feature_flag_storage = feature_flag_storage
        self._backoff = Backoff(
                                _ON_DEMAND_FETCH_BACKOFF_BASE,
                                _ON_DEMAND_FETCH_BACKOFF_MAX_WAIT)

    @property
    def feature_flag_storage(self):
        """Return Feature_flag storage object"""
        return self._feature_flag_storage

    def _get_config_sets(self):
        """
        Get all filter flag sets cnverrted to string, if no filter flagsets exist return None
        :return: string with flagsets
        :rtype: str
        """
        if self._feature_flag_storage.flag_set_filter.flag_sets == set({}):
            return None

        return ','.join(self._feature_flag_storage.flag_set_filter.sorted_flag_sets)

class SplitSynchronizer(SplitSynchronizerBase):
    """Feature Flag changes synchronizer."""

    def __init__(self, feature_flag_api, feature_flag_storage):
        """
        Class constructor.

        :param feature_flag_api: Feature Flag API Client.
        :type feature_flag_api: splitio.api.splits.SplitsAPI

        :param feature_flag_storage: Feature Flag Storage.
        :type feature_flag_storage: splitio.storage.InMemorySplitStorage
        """
        SplitSynchronizerBase.__init__(self, feature_flag_api, feature_flag_storage)

    def _fetch_until(self, fetch_options, till=None):
        """
        Hit endpoint, update storage and return when since==till.

        :param fetch_options Fetch options for getting feature flag definitions.
        :type fetch_options splitio.api.FetchOptions

        :param till: Passed till from Streaming.
        :type till: int

        :return: last change number
        :rtype: int
        """
        segment_list = set()
        while True:  # Fetch until since==till
            change_number = self._feature_flag_storage.get_change_number()
            if change_number is None:
                change_number = -1
            if till is not None and till < change_number:
                # the passed till is less than change_number, no need to perform updates
                return change_number, segment_list

            try:
                feature_flag_changes = self._api.fetch_splits(change_number, fetch_options)
            except APIException as exc:
                if exc._status_code is not None and exc._status_code == 414:
                    _LOGGER.error('Exception caught: the amount of flag sets provided are big causing uri length error.')
                    _LOGGER.debug('Exception information: ', exc_info=True)
                    raise APIUriException("URI is too long due to FlagSets count", exc._status_code)

                _LOGGER.error('Exception raised while fetching feature flags')
                _LOGGER.debug('Exception information: ', exc_info=True)
                raise exc
            fetched_feature_flags = [(splits.from_raw(feature_flag)) for feature_flag in feature_flag_changes.get('splits', [])]
            segment_list = update_feature_flag_storage(self._feature_flag_storage, fetched_feature_flags, feature_flag_changes['till'])
            if feature_flag_changes['till'] == feature_flag_changes['since']:
                return feature_flag_changes['till'], segment_list

            fetched_feature_flags = [(splits.from_raw(feature_flag)) for feature_flag in feature_flag_changes.get('splits', [])]
            segment_list = update_feature_flag_storage(self._feature_flag_storage, fetched_feature_flags, feature_flag_changes['till'])
            if feature_flag_changes['till'] == feature_flag_changes['since']:
                return feature_flag_changes['till'], segment_list

    def _attempt_feature_flag_sync(self, fetch_options, till=None):
        """
        Hit endpoint, update storage and return True if sync is complete.

        :param fetch_options Fetch options for getting feature flag definitions.
        :type fetch_options splitio.api.FetchOptions

        :param till: Passed till from Streaming.
        :type till: int

        :return: Flags to check if it should perform bypass or operation ended
        :rtype: bool, int, int
        """
        self._backoff.reset()
        final_segment_list = set()
        remaining_attempts = _ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES
        while True:
            remaining_attempts -= 1
            change_number, segment_list = self._fetch_until(fetch_options, till)
            final_segment_list.update(segment_list)
            if till is None or till <= change_number:
                return True, remaining_attempts, change_number, final_segment_list

            elif remaining_attempts <= 0:
                return False, remaining_attempts, change_number, final_segment_list

            how_long = self._backoff.get()
            time.sleep(how_long)

    def _get_config_sets(self):
        """
        Get all filter flag sets cnverrted to string, if no filter flagsets exist return None

        :return: string with flagsets
        :rtype: str
        """
        if self._feature_flag_storage.flag_set_filter.flag_sets == set({}):
            return None

        return ','.join(self._feature_flag_storage.flag_set_filter.sorted_flag_sets)

    def synchronize_splits(self, till=None):
        """
        Hit endpoint, update storage and return True if sync is complete.

        :param till: Passed till from Streaming.
        :type till: int
        """
        final_segment_list = set()
        fetch_options = FetchOptions(True, sets=self._get_config_sets())  # Set Cache-Control to no-cache
        successful_sync, remaining_attempts, change_number, segment_list = self._attempt_feature_flag_sync(fetch_options,
                                                                                      till)
        final_segment_list.update(segment_list)
        attempts = _ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES - remaining_attempts
        if successful_sync:  # succedeed sync
            _LOGGER.debug('Refresh completed in %d attempts.', attempts)
            return final_segment_list

        with_cdn_bypass = FetchOptions(True, change_number, sets=self._get_config_sets())  # Set flag for bypassing CDN
        without_cdn_successful_sync, remaining_attempts, change_number, segment_list = self._attempt_feature_flag_sync(with_cdn_bypass, till)
        final_segment_list.update(segment_list)
        without_cdn_attempts = _ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES - remaining_attempts
        if without_cdn_successful_sync:
            _LOGGER.debug('Refresh completed bypassing the CDN in %d attempts.',
                          without_cdn_attempts)
            return final_segment_list
        else:
            _LOGGER.debug('No changes fetched after %d attempts with CDN bypassed.',
                          without_cdn_attempts)

    def kill_split(self, feature_flag_name, default_treatment, change_number):
        """
        Local kill for feature flag.

        :param feature_flag_name: name of the feature flag to perform kill
        :type feature_flag_name: str
        :param default_treatment: name of the default treatment to return
        :type default_treatment: str
        :param change_number: change_number
        :type change_number: int
        """
        self._feature_flag_storage.kill_locally(feature_flag_name, default_treatment, change_number)

class SplitSynchronizerAsync(SplitSynchronizerBase):
    """Feature Flag changes synchronizer async."""

    def __init__(self, feature_flag_api, feature_flag_storage):
        """
        Class constructor.

        :param feature_flag_api: Feature Flag API Client.
        :type feature_flag_api: splitio.api.splits.SplitsAPI

        :param feature_flag_storage: Feature Flag Storage.
        :type feature_flag_storage: splitio.storage.InMemorySplitStorage
        """
        SplitSynchronizerBase.__init__(self, feature_flag_api, feature_flag_storage)

    async def _fetch_until(self, fetch_options, till=None):
        """
        Hit endpoint, update storage and return when since==till.

        :param fetch_options Fetch options for getting feature flag definitions.
        :type fetch_options splitio.api.FetchOptions

        :param till: Passed till from Streaming.
        :type till: int

        :return: last change number
        :rtype: int
        """
        segment_list = set()
        while True:  # Fetch until since==till
            change_number = await self._feature_flag_storage.get_change_number()
            if change_number is None:
                change_number = -1
            if till is not None and till < change_number:
                # the passed till is less than change_number, no need to perform updates
                return change_number, segment_list

            try:
                feature_flag_changes = await self._api.fetch_splits(change_number, fetch_options)
            except APIException as exc:
                if exc._status_code is not None and exc._status_code == 414:
                    _LOGGER.error('Exception caught: the amount of flag sets provided are big causing uri length error.')
                    _LOGGER.debug('Exception information: ', exc_info=True)
                    raise APIUriException("URI is too long due to FlagSets count", exc._status_code)

                _LOGGER.error('Exception raised while fetching feature flags')
                _LOGGER.debug('Exception information: ', exc_info=True)
                raise exc

            fetched_feature_flags = [(splits.from_raw(feature_flag)) for feature_flag in feature_flag_changes.get('splits', [])]
            segment_list = await update_feature_flag_storage_async(self._feature_flag_storage, fetched_feature_flags, feature_flag_changes['till'])
            if feature_flag_changes['till'] == feature_flag_changes['since']:
                return feature_flag_changes['till'], segment_list

    async def _attempt_feature_flag_sync(self, fetch_options, till=None):
        """
        Hit endpoint, update storage and return True if sync is complete.

        :param fetch_options Fetch options for getting feature flag definitions.
        :type fetch_options splitio.api.FetchOptions

        :param till: Passed till from Streaming.
        :type till: int

        :return: Flags to check if it should perform bypass or operation ended
        :rtype: bool, int, int
        """
        self._backoff.reset()
        final_segment_list = set()
        remaining_attempts = _ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES
        while True:
            remaining_attempts -= 1
            change_number, segment_list = await self._fetch_until(fetch_options, till)
            final_segment_list.update(segment_list)
            if till is None or till <= change_number:
                return True, remaining_attempts, change_number, final_segment_list

            elif remaining_attempts <= 0:
                return False, remaining_attempts, change_number, final_segment_list

            how_long = self._backoff.get()
            await asyncio.sleep(how_long)

    async def synchronize_splits(self, till=None):
        """
        Hit endpoint, update storage and return True if sync is complete.

        :param till: Passed till from Streaming.
        :type till: int
        """
        final_segment_list = set()
        fetch_options = FetchOptions(True, sets=self._get_config_sets())  # Set Cache-Control to no-cache
        successful_sync, remaining_attempts, change_number, segment_list = await self._attempt_feature_flag_sync(fetch_options,
                                                                                      till)
        final_segment_list.update(segment_list)
        attempts = _ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES - remaining_attempts
        if successful_sync:  # succedeed sync
            _LOGGER.debug('Refresh completed in %d attempts.', attempts)
            return final_segment_list

        with_cdn_bypass = FetchOptions(True, change_number, sets=self._get_config_sets())  # Set flag for bypassing CDN
        without_cdn_successful_sync, remaining_attempts, change_number, segment_list = await self._attempt_feature_flag_sync(with_cdn_bypass, till)
        final_segment_list.update(segment_list)
        without_cdn_attempts = _ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES - remaining_attempts
        if without_cdn_successful_sync:
            _LOGGER.debug('Refresh completed bypassing the CDN in %d attempts.',
                          without_cdn_attempts)
            return final_segment_list

        else:
            _LOGGER.debug('No changes fetched after %d attempts with CDN bypassed.',
                          without_cdn_attempts)

    async def kill_split(self, feature_flag_name, default_treatment, change_number):
        """
        Local kill for feature flag.

        :param feature_flag_name: name of the feature flag to perform kill
        :type feature_flag_name: str
        :param default_treatment: name of the default treatment to return
        :type default_treatment: str
        :param change_number: change_number
        :type change_number: int
        """
        await self._feature_flag_storage.kill_locally(feature_flag_name, default_treatment, change_number)


class LocalhostMode(Enum):
    """types for localhost modes"""
    LEGACY = 0
    YAML = 1
    JSON = 2

class LocalSplitSynchronizerBase(object):
    """Localhost mode feature_flag base synchronizer."""

    _DEFAULT_FEATURE_FLAG_TILL = -1

    @staticmethod
    def _make_feature_flag(feature_flag_name, conditions, configs=None):
        """
        Make a Feature flag with a single all_keys matcher.

        :param feature_flag_name: Name of the feature flag.
        :type feature_flag_name: str.
        """
        return splits.from_raw({
            'changeNumber': 123,
            'trafficTypeName': 'user',
            'name': feature_flag_name,
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

    def _sanitize_feature_flag(self, parsed):
        """
        implement Sanitization if neded.

        :param parsed: feature flags, till and since elements dict
        :type parsed: Dict

        :return: sanitized structure dict
        :rtype: Dict
        """
        parsed = self._sanitize_json_elements(parsed)
        parsed['splits'] = self._sanitize_feature_flag_elements(parsed['splits'])

        return parsed

    def _sanitize_json_elements(self, parsed):
        """
        Sanitize all json elements.

        :param parsed: feature flags, till and since elements dict
        :type parsed: Dict

        :return: sanitized structure dict
        :rtype: Dict
        """
        if 'splits' not in parsed:
            parsed['splits'] = []
        if 'till' not in parsed or parsed['till'] is None or parsed['till'] < -1:
            parsed['till'] = -1
        if 'since' not in parsed or parsed['since'] is None or parsed['since'] < -1 or parsed['since'] > parsed['till']:
            parsed['since'] = parsed['till']

        return parsed

    def _sanitize_feature_flag_elements(self, parsed_feature_flags):
        """
        Sanitize all feature flags elements.

        :param parsed_feature_flags: feature flags array
        :type parsed_feature_flags: [Dict]

        :return: sanitized structure dict
        :rtype: [Dict]
        """
        sanitized_feature_flags = []
        for feature_flag in parsed_feature_flags:
            if 'name' not in feature_flag or feature_flag['name'].strip() == '':
                _LOGGER.warning("A feature flag in json file does not have (Name) or property is empty, skipping.")
                continue
            for element in [('trafficTypeName', 'user', None, None, None, None),
                            ('trafficAllocation', 100, 0, 100,  None, None),
                            ('trafficAllocationSeed', int(get_current_epoch_time_ms() / 1000), None, None, None, [0]),
                            ('seed', int(get_current_epoch_time_ms() / 1000), None, None, None, [0]),
                            ('status', splits.Status.ACTIVE.value, None, None, [e.value for e in splits.Status], None),
                            ('killed', False, None, None, None, None),
                            ('defaultTreatment', 'control', None, None, None, ['', ' ']),
                            ('changeNumber', 0, 0, None, None, None),
                            ('algo', 2, 2, 2, None, None)]:
                feature_flag = util._sanitize_object_element(feature_flag, 'split', element[0], element[1], lower_value=element[2], upper_value=element[3], in_list=element[4], not_in_list=element[5])
            feature_flag = self._sanitize_condition(feature_flag)
            if 'sets' not in feature_flag:
                feature_flag['sets'] = []
            feature_flag['sets'] = validate_flag_sets(feature_flag['sets'], 'Localhost Validator')
            sanitized_feature_flags.append(feature_flag)
        return sanitized_feature_flags

    def _sanitize_condition(self, feature_flag):
        """
        Sanitize feature flag and ensure a condition type ROLLOUT and matcher exist with ALL_KEYS elements.

        :param feature_flag: feature flag dict object
        :type feature_flag: Dict

        :return: sanitized feature flag
        :rtype: Dict
        """
        found_all_keys_matcher = False
        feature_flag['conditions'] = feature_flag.get('conditions', [])
        if len(feature_flag['conditions']) > 0:
            last_condition = feature_flag['conditions'][-1]
            if 'conditionType' in last_condition:
                if last_condition['conditionType'] == 'ROLLOUT':
                    if 'matcherGroup' in last_condition:
                        if 'matchers' in last_condition['matcherGroup']:
                            for matcher in last_condition['matcherGroup']['matchers']:
                                if matcher['matcherType'] == 'ALL_KEYS':
                                    found_all_keys_matcher = True
                                    break

        if not found_all_keys_matcher:
            _LOGGER.debug("Missing default rule condition for feature flag: %s, adding default rule with 100%% off treatment", feature_flag['name'])
            feature_flag['conditions'].append(
            {
                "conditionType": "ROLLOUT",
                "matcherGroup": {
                "combiner": "AND",
                "matchers": [{
                    "keySelector": { "trafficType": "user", "attribute": None },
                    "matcherType": "ALL_KEYS",
                    "negate": False,
                    "userDefinedSegmentMatcherData": None,
                    "whitelistMatcherData": None,
                    "unaryNumericMatcherData": None,
                    "betweenMatcherData": None,
                    "booleanMatcherData": None,
                    "dependencyMatcherData": None,
                    "stringMatcherData": None
                    }]
                },
                "partitions": [
                    { "treatment": "on", "size": 0 },
                    { "treatment": "off", "size": 100 }
                ],
            "label": "default rule"
        })

        return feature_flag

    @classmethod
    def _convert_yaml_to_feature_flag(cls, parsed):
        grouped_by_feature_name = itertools.groupby(
            sorted(parsed, key=lambda i: next(iter(i.keys()))),
            lambda i: next(iter(i.keys())))
        to_return = {}
        for (feature_flag_name, statements) in grouped_by_feature_name:
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
            to_return[feature_flag_name] = cls._make_feature_flag(feature_flag_name, whitelist + all_keys, configs)
        return to_return


class LocalSplitSynchronizer(LocalSplitSynchronizerBase):
    """Localhost mode feature_flag synchronizer."""

    def __init__(self, filename, feature_flag_storage, localhost_mode=LocalhostMode.LEGACY):
        """
        Class constructor.

        :param filename: File to parse feature flags from.
        :type filename: str
        :param feature_flag_storage: Feature flag Storage.
        :type feature_flag_storage: splitio.storage.InMemorySplitStorage
        :param localhost_mode: mode for localhost either JSON, YAML or LEGACY.
        :type localhost_mode: splitio.sync.split.LocalhostMode
        """
        self._filename = filename
        self._feature_flag_storage = feature_flag_storage
        self._localhost_mode = localhost_mode
        self._current_json_sha = "-1"

    @classmethod
    def _read_feature_flags_from_legacy_file(cls, filename):
        """
        Parse a feature flags file and return a populated storage.

        :param filename: Path of the file containing mocked feature flags & treatments.
        :type filename: str.

        :return: Storage populataed with feature flags ready to be evaluated.
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
                            'Invalid line on localhost environment feature flag '
                            'definition. Line = %s',
                            line
                        )
                        continue

                    cond = cls._make_all_keys_condition(definition_match.group('treatment'))
                    splt = cls._make_feature_flag(definition_match.group('feature'), [cond])
                    to_return[splt.name] = splt
            return to_return

        except IOError as exc:
            raise ValueError("Error parsing file %s. Make sure it's readable." % filename) from exc

    @classmethod
    def _read_feature_flags_from_yaml_file(cls, filename):
        """
        Parse a feature flags file and return a populated storage.

        :param filename: Path of the file containing mocked feature flags & treatments.
        :type filename: str.

        :return: Storage populated with feature flags ready to be evaluated.
        :rtype: InMemorySplitStorage
        """
        try:
            with open(filename, 'r') as flo:
                parsed = yaml.load(flo.read(), Loader=yaml.FullLoader)

            return cls._convert_yaml_to_feature_flag(parsed)
        except IOError as exc:
            raise ValueError("Error parsing file %s. Make sure it's readable." % filename) from exc

    def synchronize_splits(self, till=None):  # pylint:disable=unused-argument
        """Update feature flags in storage."""
        _LOGGER.info('Synchronizing feature flags now.')
        try:
            return self._synchronize_json() if self._localhost_mode == LocalhostMode.JSON else self._synchronize_legacy()
        except Exception as exc:
            _LOGGER.debug('Exception: ', exc_info=True)
            raise APIException("Error fetching feature flags information") from exc

    def _synchronize_legacy(self):
        """
        Update feature flags in storage for legacy mode.

        :return: empty array for compatibility with json mode
        :rtype: []
        """

        if self._filename.lower().endswith(('.yaml', '.yml')):
            fetched = self._read_feature_flags_from_yaml_file(self._filename)
        else:
            fetched = self._read_feature_flags_from_legacy_file(self._filename)
        to_delete = [name for name in self._feature_flag_storage.get_split_names()
                     if name not in fetched.keys()]
        to_add = [feature_flag for feature_flag in fetched.values()]
        self._feature_flag_storage.update(to_add, to_delete, 0)
        return []

    def _synchronize_json(self):
        """
        Update feature flags in storage for json mode.

        :return: segment names string array
        :rtype: [str]
        """
        try:
            fetched, till = self._read_feature_flags_from_json_file(self._filename)
            segment_list = set()
            fecthed_sha = util._get_sha(json.dumps(fetched))
            if fecthed_sha == self._current_json_sha:
                return []

            self._current_json_sha = fecthed_sha
            if self._feature_flag_storage.get_change_number() > till and till != self._DEFAULT_FEATURE_FLAG_TILL:
                return []

            fetched_feature_flags = [splits.from_raw(feature_flag) for feature_flag in fetched]
            segment_list = update_feature_flag_storage(self._feature_flag_storage, fetched_feature_flags, till)
            return segment_list

        except Exception as exc:
            _LOGGER.debug('Exception: ', exc_info=True)
            raise ValueError("Error reading feature flags from json.") from exc

    def _read_feature_flags_from_json_file(self, filename):
        """
        Parse a feature flags file and return a populated storage.

        :param filename: Path of the file containing feature flags
        :type filename: str.

        :return: Tuple: sanitized feature flag structure dict and till
        :rtype: Tuple(Dict, int)
        """
        try:
            with open(filename, 'r') as flo:
                parsed = json.load(flo)
            santitized = self._sanitize_feature_flag(parsed)
            return santitized['splits'], santitized['till']
        except Exception as exc:
            _LOGGER.debug('Exception: ', exc_info=True)
            raise ValueError("Error parsing file %s. Make sure it's readable." % filename) from exc


class LocalSplitSynchronizerAsync(LocalSplitSynchronizerBase):
    """Localhost mode async feature_flag synchronizer."""

    def __init__(self, filename, feature_flag_storage, localhost_mode=LocalhostMode.LEGACY):
        """
        Class constructor.

        :param filename: File to parse feature flags from.
        :type filename: str
        :param feature_flag_storage: Feature flag Storage.
        :type feature_flag_storage: splitio.storage.InMemorySplitStorage
        :param localhost_mode: mode for localhost either JSON, YAML or LEGACY.
        :type localhost_mode: splitio.sync.split.LocalhostMode
        """
        self._filename = filename
        self._feature_flag_storage = feature_flag_storage
        self._localhost_mode = localhost_mode
        self._current_json_sha = "-1"

    @classmethod
    async def _read_feature_flags_from_legacy_file(cls, filename):
        """
        Parse a feature flags file and return a populated storage.

        :param filename: Path of the file containing mocked feature flags & treatments.
        :type filename: str.

        :return: Storage populataed with feature flags ready to be evaluated.
        :rtype: InMemorySplitStorage
        """
        to_return = {}
        try:
            async with aiofiles.open(filename, 'r') as flo:
                for line in await flo.read():
                    if line.strip() == '' or _LEGACY_COMMENT_LINE_RE.match(line):
                        continue

                    definition_match = _LEGACY_DEFINITION_LINE_RE.match(line)
                    if not definition_match:
                        _LOGGER.warning(
                            'Invalid line on localhost environment feature flag '
                            'definition. Line = %s',
                            line
                        )
                        continue

                    cond = cls._make_all_keys_condition(definition_match.group('treatment'))
                    splt = cls._make_feature_flag(definition_match.group('feature'), [cond])
                    to_return[splt.name] = splt
            return to_return

        except IOError as exc:
            raise ValueError("Error parsing file %s. Make sure it's readable." % filename) from exc

    @classmethod
    async def _read_feature_flags_from_yaml_file(cls, filename):
        """
        Parse a feature flags file and return a populated storage.

        :param filename: Path of the file containing mocked feature flags & treatments.
        :type filename: str.

        :return: Storage populated with feature flags ready to be evaluated.
        :rtype: InMemorySplitStorage
        """
        try:
            async with aiofiles.open(filename, 'r') as flo:
                parsed = yaml.load(await flo.read(), Loader=yaml.FullLoader)

            return cls._convert_yaml_to_feature_flag(parsed)
        except IOError as exc:
            raise ValueError("Error parsing file %s. Make sure it's readable." % filename) from exc

    async def synchronize_splits(self, till=None):  # pylint:disable=unused-argument
        """Update feature flags in storage."""
        _LOGGER.info('Synchronizing feature flags now.')
        try:
            return await self._synchronize_json() if self._localhost_mode == LocalhostMode.JSON else await self._synchronize_legacy()
        except Exception as exc:
            _LOGGER.debug('Exception: ', exc_info=True)
            raise APIException("Error fetching feature flags information") from exc

    async def _synchronize_legacy(self):
        """
        Update feature flags in storage for legacy mode.

        :return: empty array for compatibility with json mode
        :rtype: []
        """

        if self._filename.lower().endswith(('.yaml', '.yml')):
            fetched = await self._read_feature_flags_from_yaml_file(self._filename)
        else:
            fetched = await self._read_feature_flags_from_legacy_file(self._filename)
        to_delete = [name for name in await self._feature_flag_storage.get_split_names()
                     if name not in fetched.keys()]
        to_add = [feature_flag for feature_flag in fetched.values()]
        await self._feature_flag_storage.update(to_add, to_delete, 0)

        return []

    async def _synchronize_json(self):
        """
        Update feature flags in storage for json mode.

        :return: segment names string array
        :rtype: [str]
        """
        try:
            fetched, till = await self._read_feature_flags_from_json_file(self._filename)
            segment_list = set()
            fecthed_sha = util._get_sha(json.dumps(fetched))
            if fecthed_sha == self._current_json_sha:
                return []

            self._current_json_sha = fecthed_sha
            if await self._feature_flag_storage.get_change_number() > till and till != self._DEFAULT_FEATURE_FLAG_TILL:
                return []

            fetched_feature_flags = [splits.from_raw(feature_flag) for feature_flag in fetched]
            segment_list = await update_feature_flag_storage_async(self._feature_flag_storage, fetched_feature_flags, till)
            return segment_list

        except Exception as exc:
            _LOGGER.debug('Exception: ', exc_info=True)
            raise ValueError("Error reading feature flags from json.") from exc

    async def _read_feature_flags_from_json_file(self, filename):
        """
        Parse a feature flags file and return a populated storage.

        :param filename: Path of the file containing feature flags
        :type filename: str.

        :return: Tuple: sanitized feature flag structure dict and till
        :rtype: Tuple(Dict, int)
        """
        try:
            async with aiofiles.open(filename, 'r') as flo:
                parsed = json.loads(await flo.read())
            santitized = self._sanitize_feature_flag(parsed)
            return santitized['splits'], santitized['till']
        except Exception as exc:
            _LOGGER.debug('Exception: ', exc_info=True)
            raise ValueError("Error parsing file %s. Make sure it's readable." % filename) from exc
