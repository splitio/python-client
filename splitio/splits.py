"""This module contains everything related to splits"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import logging

from builtins import dict
from enum import Enum
from json import load
from requests.exceptions import HTTPError
from threading import Thread, Timer, RLock
from collections import namedtuple

from future.utils import python_2_unicode_compatible

from splitio.matchers import CombiningMatcher, AndCombiner, AllKeysMatcher, \
    UserDefinedSegmentMatcher, WhitelistMatcher, EqualToMatcher, \
    GreaterThanOrEqualToMatcher, LessThanOrEqualToMatcher, BetweenMatcher, \
    AttributeMatcher, DataType, StartsWithMatcher, EndsWithMatcher, \
    ContainsStringMatcher, ContainsAllOfSetMatcher, ContainsAnyOfSetMatcher, \
    EqualToSetMatcher, PartOfSetMatcher

SplitView = namedtuple(
    'SplitView',
    ['name', 'traffic_type', 'killed', 'treatments', 'change_number']
)


class Status(Enum):
    """Split status"""
    ACTIVE = "ACTIVE"
    ARCHIVED = "ARCHIVED"


class HashAlgorithm(Enum):
    """
    Hash algorithm names
    """
    LEGACY = 1
    MURMUR = 2


class ConditionType(Enum):
    """
    Split possible condition types
    """
    WHITELIST = 'WHITELIST'
    ROLLOUT = 'ROLLOUT'


class Split(object):
    def __init__(self, name, seed, killed, default_treatment, traffic_type_name,
                 status, change_number, conditions=None, algo=None,
                 traffic_allocation=None, traffic_allocation_seed=None):
        """
        A class that represents a split. It associates a feature name with a set
        of matchers (responsible of telling which condition to use) and
        conditions (which determines which treatment to use)
        :param name: Name of the feature
        :type name: unicode
        :param seed: Seed
        :type seed: int
        :param killed: Whether the split is killed or not
        :type killed: bool
        :param default_treatment: Default treatment for the split
        :type default_treatment: str
        :param conditions: Set of conditions to test
        :type conditions: list
        """
        self._name = name
        self._seed = seed
        self._killed = killed
        self._default_treatment = default_treatment
        self._traffic_type_name = traffic_type_name
        self._status = status
        self._change_number = change_number
        self._conditions = conditions if conditions is not None else []

        if traffic_allocation >= 0 and traffic_allocation <= 100:
            self._traffic_allocation = traffic_allocation
        else:
            self._traffic_allocation = 100

        self._traffic_allocation_seed = traffic_allocation_seed
        try:
            self._algo = HashAlgorithm(algo)
        except ValueError:
            self._algo = HashAlgorithm.LEGACY

    @property
    def name(self):
        return self._name

    @property
    def seed(self):
        return self._seed

    @property
    def algo(self):
        return self._algo

    @property
    def killed(self):
        return self._killed

    @property
    def default_treatment(self):
        return self._default_treatment

    @property
    def traffic_type_name(self):
        return self._traffic_type_name

    @property
    def status(self):
        return self._status

    @property
    def change_number(self):
        return self._change_number

    @property
    def conditions(self):
        return self._conditions

    @property
    def traffic_allocation(self):
        return self._traffic_allocation

    @property
    def traffic_allocation_seed(self):
        return self._traffic_allocation_seed

    @python_2_unicode_compatible
    def __str__(self):
        return 'name: {name}, seed: {seed}, killed: {killed}, ' \
               'default treatment: {default_treatment}, ' \
               'conditions: {conditions}'.format(
                   name=self._name, seed=self._seed, killed=self._killed,
                   default_treatment=self._default_treatment,
                   conditions=','.join(map(str, self._conditions))
               )


class AllKeysSplit(Split):
    def __init__(self, name, treatment):
        """
        A split implementation that matches everything to a single treatment.
        :param name: Name of the feature
        :type name: str
        :param treatment: The treatment for the feature
        :type treatment: str
        """
        super(AllKeysSplit, self).__init__(
            name, None, False, treatment, None, None, None,
            [Condition(AttributeMatcher(None, AllKeysMatcher(), False),
                       [Partition(treatment, 100)],
                       None)])


class Condition(object):
    def __init__(self, matcher, partitions, label,
                 condition_type=ConditionType.WHITELIST):
        """
        A class that represents a split condition. It associates a matcher with
        a set of partitions.
        :param matcher: A combining matcher
        :type matcher: CombiningMatcher
        :param partitions: A list of partitions
        :type partitions: list
        """
        self._matcher = matcher
        self._partitions = tuple(partitions)
        self._label = label
        self._confition_type = condition_type

    @property
    def matcher(self):
        return self._matcher

    @property
    def partitions(self):
        return self._partitions

    @property
    def label(self):
        return self._label

    @property
    def condition_type(self):
        return self._confition_type

    @python_2_unicode_compatible
    def __str__(self):
        return '{matcher} then split {partitions}'.format(
            matcher=self._matcher, partitions=','.join(
                '{size}:{treatment}'.format(size=partition.size,
                                            treatment=partition.treatment)
                for partition in self._partitions))


class Partition(object):
    def __init__(self, treatment, size):
        """
        A class that represents a partition of a split condition
        :param treatment: The treatment for the partition
        :type treatment: str
        :param size: A number between 0 a 100
        :type size: float
        """
        if size < 0 or size > 100:
            raise ValueError('size MUST BE between 0 and 100')

        self._treatment = treatment
        self._size = size

    @property
    def treatment(self):
        return self._treatment

    @property
    def size(self):
        return self._size

    @python_2_unicode_compatible
    def __str__(self):
        return '{size}%:{treatment}'.format(size=self._size,
                                            treatment=self._treatment)


class SplitFetcher(object):  # pragma: no cover
    def __init__(self):
        """
        The interface for a SplitFetcher.
        It provides access to Split implementations.
        """
        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def change_number(self):
        return None

    def fetch(self, feature):
        """
        Fetches the split for a given feature
        :param feature: The name of the feature
        :type feature: str
        :return: A split associated with the feature
        :rtype: Split
        """
        return None

    def fetch_all(self):
        """
        Feches all splits
        :return: All the know splits so far
        :rtype: list
        """
        return None


class InMemorySplitFetcher(SplitFetcher):
    def __init__(self, splits=None):
        """
        A basic implementation of a split fetcher. It's responsible for
        providing access to the client to the Split representations.
        :param splits: An optional dictionary of feature to split entries
        :type splits: dict
        """
        super(InMemorySplitFetcher, self).__init__()
        self._splits = splits if splits is not None else dict()

    @property
    def change_number(self):
        return -1

    def fetch(self, feature):
        """
        Fetches the split for a given feature
        :param feature: The name of the feature
        :type feature: str
        :return: A split associated with the feature
        :rtype: Split
        """
        return self._splits.get(feature)

    def fetch_all(self):
        """
        Feches all splits
        :return: All the know splits so far
        :rtype: list
        """
        return list(self._splits.values())


class JSONFileSplitFetcher(InMemorySplitFetcher):
    def __init__(self, file_name, split_parser, splits=None):
        """
        A split fetcher that gets the split information from a file with the
        JSON response of a call
        to the splitChanges resource.
        :param file_name: Name of the file with the splitChanges response
        :type file_name: str
        :param split_parser: The parser used to parse the responses
        :type split_parser: SplitParser
        :param splits: An optional dictionary of feature to split entries
        :type splits: dict
        """
        super(JSONFileSplitFetcher, self).__init__(splits=splits)

        self._split_parser = split_parser
        with open(file_name) as f:
            self._json = load(f)

            for split_change in self._json['splits']:
                parsed_split = self._split_parser.parse(split_change)
                self._splits[parsed_split.name] = parsed_split


class SelfRefreshingSplitFetcher(InMemorySplitFetcher):
    def __init__(self, split_change_fetcher, split_parser, interval=30,
                 greedy=True, change_number=-1, splits=None):
        """
        A SplitFetcher implementation that refreshes itself periodically.
        :param split_change_fetcher: The split change fetcher used to fetch
            changes
        :type split_change_fetcher: SplitChangeFetcher
        :param split_parser: The split parser
        :type split_parser: SplitParser
        :param interval: An integer or callable that'll define the refreshing
            interval
        :type interval: int
        :param greedy: Request all changes until they are exhausted
        :type greedy: bool
        :param change_number: An integer with the initial value for the "since"
            API argument
        :type change_number: int
        :param splits: An optional dictionary of feature to split entries
        :type splits: dict
        """
        super(SelfRefreshingSplitFetcher, self).__init__(splits=splits)

        self._split_change_fetcher = split_change_fetcher
        self._split_parser = split_parser
        self._interval = interval
        self._greedy = greedy
        self._change_number = change_number
        self._stopped = True
        self._rlock = RLock()

    @property
    def stopped(self):
        """
        :return: Whether the refresh process has been stopped
        :rtype: bool
        """
        return self._stopped

    @stopped.setter
    def stopped(self, stopped):
        """
        :param stopped: Whether to stop the refreshing process
        :type stopped: bool
        """
        self._stopped = stopped

    @property
    def change_number(self):
        return self._change_number

    def start(self, delayed_update=False):
        """Starts the self-refreshing processes of the splits
        :param delayed_update: Whether to delay the update until the interval
            has passed
        :type delayed_update: bool
        """
        if not self._stopped:
            return

        self._stopped = False

        if delayed_update:
            self._timer_start()
        else:
            self._timer_refresh()

    def _update_splits_from_change_fetcher_response(self, response,
                                                    block_until_ready=False):
        """
        Updates the splits from the response of the split_change_fetcher
        :param response: A JSON with the response of
            split_change_fetcher.fetch()
        :type response: dict
        :param block_until_ready: Whether to block until all data is available
        :param block_until_ready: bool
        """
        added_features = []
        removed_features = []

        for split_change in response['splits']:
            if Status(split_change['status']) != Status.ACTIVE:
                self._splits.pop(split_change['name'], None)
                removed_features.append(split_change['name'])
                continue

            parsed_split = self._split_parser.parse(
                split_change, block_until_ready=block_until_ready
            )
            if parsed_split is None:
                self._logger.warning(
                    'We could not parse the split definition for %s. '
                    'Removing split to be safe.', split_change['name'])
                self._splits.pop(split_change['name'], None)
                removed_features.append(split_change['name'])
                continue

            added_features.append(split_change['name'])
            self._splits[split_change['name']] = parsed_split

        if len(added_features) > 0:
            self._logger.info('Updated features: %s', added_features)

        if len(removed_features) > 0:
            self._logger.info('Deleted features: %s', removed_features)

    def refresh_splits(self, block_until_ready=False):
        """The actual split fetcher refresh process.
        :param block_until_ready: Whether to block until all data is available
        :param block_until_ready: bool
        """
        change_number_before = self._change_number

        try:
            with self._rlock:
                while True:
                    response = self._split_change_fetcher.fetch(
                        self._change_number)

                    if self._change_number >= response['till']:
                        return

                    if 'splits' in response and len(response['splits']) > 0:
                        self._update_splits_from_change_fetcher_response(
                            response, block_until_ready=block_until_ready)
                        self._change_number = response['till']

                    if not self._greedy:
                        return
        except:
            self._logger.info('Exception caught refreshing splits')
            self._stopped = True
        finally:
            self._logger.info('split fetch before: %s, after: %s',
                              change_number_before,
                              self._change_number)

    def _timer_start(self):
        try:
            if hasattr(self._interval, '__call__'):
                interval = self._interval()
            else:
                interval = self._interval

            timer = Timer(interval, self._timer_refresh)
            timer.daemon = True
            timer.start()
        except:
            self._logger.exception('Exception caught refreshing timer')
            self._stopped = True

    def _timer_refresh(self):
        """
        Responsible for setting the periodic calls to _refresh_splits using a
        Timer thread
        """
        if self._stopped:
            return

        try:
            thread = Thread(target=self.refresh_splits)
            thread.daemon = True
            thread.start()
        except:
            self._logger.exception(
                'Exception caught starting splits update thread'
            )

        self._timer_start()


class SplitChangeFetcher(object):
    def __init__(self):
        """Fetches changes in splits since a reference point."""
        self._logger = logging.getLogger(self.__class__.__name__)

    def fetch_from_backend(self, since):
        """
        Fetches changes in splits since a reference point.
        :param since: An integer that indicates that we want the changes that
        occurred AFTER this last change number. A value less than zero implies
        that the client is requesting information for the first time.
        :type since: int
        :return: A dictionary with the changes for splits
        :rtype: dict
        """
        raise NotImplementedError()

    def build_empty_response(self, since):
        """
        Builds an "empty" split change response. Used in case of exceptions or
        other unforseen problems.
        :param since: "till" value of the last split change.
        :type since: int
        :return: A dictionary with an empty (.e.g. no change) response
        :rtype: dict
        """
        return {
            'since': since,
            'till': since,
            'splits': []
        }

    def fetch(self, since):
        """
        Fetches changes in splits since a reference point.

        This method never raises exceptions or returns None.

        The list of "splits" in the returned value contains at most one split
        per name. If multiple changes occurred between the requested and last
        change numbers, only the latest change is returned.

        If no changes occurred, we return an empty list of changes with the same
        change number as the one requested.

        If the client is asking for split changes for the first time, only the
        active partitions  are returned.

        :param since: An integer that indicates that we want the changes that
            occurred AFTER this last change number. A value less than zero
            implies that the client is requesting information for the first
            time.
        :type since: int
        :return: A dictionary with the changes for splits
        :rtype: dict
        """
        try:
            split_change = self.fetch_from_backend(since)
        except:
            self._logger.exception('Exception caught fetching split changes')
            split_change = self.build_empty_response(since)

        return split_change


class ApiSplitChangeFetcher(SplitChangeFetcher):
    def __init__(self, api):
        """
        A SplitChangeFetcher implementation that retrieves the changes from
        Split.io's RESTful SDK API.
        :param api: The API client to use
        :type api: SdkApi
        """
        super(ApiSplitChangeFetcher, self).__init__()
        self._api = api

    def fetch_from_backend(self, since):
        try:
            return self._api.split_changes(since)
        except HTTPError as e:
            # We handle HTTP error here to allow for status code metrics once
            # they're implemented
            raise e


class SplitParser(object):
    def __init__(self, segment_fetcher):
        """
        A parser for the response of the splitChanges.
        :param segment_fetcher: The segment fetcher to use with user segment
            conditions
        :type segment_fetcher: SegmentFetcher
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._segment_fetcher = segment_fetcher

    def parse(self, split, block_until_ready=False):
        """
        Parse a "split" item of the response of the splitChanges endpoint.

        If the split is archived, this method returns None/ This method will
        never raise an exception. If theres a problem with the process, it'll
        return None.
        :param split: A dictionary with a parsed JSON of a split item
        :type split: dict
        :param block_until_ready: Whether to block until all data is available
        :type block_until_ready: bool
        :return: A parsed split
        :rtype: Split
        """
        try:
            return self._parse(split, block_until_ready=block_until_ready)
        except:
            self._logger.exception('Exception caught parsing split')
            return None

    def _parse(self, split, block_until_ready=False):
        """
        Parse a "split" item of the response of the splitChanges endpoint.
        :param split: A dictionary with a parsed JSON of a split item
        :type split: dict
        :param block_until_ready: Whether to block until all data is available
        :type block_until_ready: bool
        :return: A parsed split
        :rtype: Split
        """
        if Status[split['status']] != Status.ACTIVE:
            return None

        partial_split = self._parse_split(
            split, block_until_ready=block_until_ready
        )
        self._parse_conditions(
            partial_split, split, block_until_ready=block_until_ready
        )

        return partial_split

    def _parse_split(self, split, block_until_ready=False):
        """Parse split properties.
        :param split: A dictionary with a parsed JSON of a split item
        :type split: dict
        :param block_until_ready: Whether to block until all data is available
        :type block_until_ready: bool
        :return: A partial parsed split
        :rtype: Split
        """
        return Split(
            split['name'],
            split['seed'],
            split['killed'],
            split['defaultTreatment'],
            split['trafficTypeName'],
            split['status'],
            split['changeNumber'],
            algo=split.get('algo'),
            traffic_allocation=split.get('trafficAllocation'),
            traffic_allocation_seed=split.get('trafficAllocationSeed')
        )

    def _parse_conditions(self, partial_split, split, block_until_ready=False):
        """Parse split conditions
        :param partial_split: The partially parsed split
        :param partial_split: Split
        :param split: A dictionary with a parsed JSON of a split item
        :type split: dict
        :param block_until_ready: Whether to block until all data is available
        :type block_until_ready: bool
        :return:
        """
        for condition in split['conditions']:
            parsed_partitions = [
                Partition(partition['treatment'], partition['size'])
                for partition in condition['partitions']
            ]
            combining_matcher = self._parse_matcher_group(
                partial_split, condition['matcherGroup'],
                block_until_ready=block_until_ready
            )
            label = None
            if 'label' in condition:
                label = condition['label']

            try:
                condition_type = ConditionType(condition.get('conditionType'))
            except:
                condition_type = ConditionType.WHITELIST

            partial_split.conditions.append(
                Condition(
                    combining_matcher,
                    parsed_partitions,
                    label,
                    condition_type
                )
            )

    def _parse_matcher_group(self, partial_split, matcher_group,
                             block_until_ready=False):
        """
        Parses a matcher group
        :param partial_split: The partially parsed split
        :param partial_split: Split
        :param matcher_group: A list of dictionaries with the JSON
            representation of a matcher
        :type matcher_group: list
        :param block_until_ready: Whether to block until all data is available
        :type block_until_ready: bool
        :return: A combining matcher
        :rtype: CombiningMatcher
        """
        if ('matchers' not in matcher_group or
           len(matcher_group['matchers']) == 0):
            raise ValueError('Missing or empty matchers')

        delegates = [self._parse_matcher(partial_split, matcher,
                                         block_until_ready=block_until_ready)
                     for matcher in matcher_group['matchers']]
        combiner = self._parse_combiner(matcher_group['combiner'])

        return CombiningMatcher(combiner, delegates)

    def _get_matcher_attribute(self, attribute, matcher):
        """
        Validates the presence of an attribute on a matcher dictionarry and
        returns its value.
        :param attribute: The name of the attribute
        :type attribute: str
        :param matcher: A dictionary with the JSON representation of a matcher
        :type matcher: dict
        :return: The value of matcher[attribute]
        :rtype: object
        """
        if attribute not in matcher or matcher[attribute] is None:
            raise ValueError(
                'Null or missing matcher attribute {}'.format(attribute)
            )

        return matcher[attribute]

    def _get_matcher_data_data_type(self, matcher_data):
        """
        Gets the data type for a matcher data dictionary
        :param matcher_data: A dictionary with the JSON representation of a
            matcher data
        :type matcher_data: dict
        :return: The data type associated with the matcher data
        :rtype: DataType
        """
        try:
            return DataType[matcher_data.get('dataType', None)]
        except KeyError:
            raise ValueError('Invalid data type value: {}'.format(
                matcher_data.get('dataType', None)
            ))

    def _parse_combiner(self, combiner, *args, **kwargs):
        """
        Parses a combiner
        :param combiner: The identifier of a combiner
        :type combiner: str
        :return: The combiner associated with the identifier
        :rtype: AndCombiner
        """
        if combiner == 'AND':
            return AndCombiner()

        raise ValueError('Invalid combiner type: {}'.format(combiner))

    def _parse_matcher_all_keys(self, partial_split, matcher, *args, **kwargs):
        """
        Parses an ALL_KEYS matcher
        :param partial_split: The partially parsed split
        :param partial_split: Split
        :param matcher: A dictionary with the JSON representation of an
            ALL_KEYS matcher
        :type matcher: dict
        :return: The parsed matcher
        :rtype: AllKeysMatcher
        """
        delegate = AllKeysMatcher()
        return delegate

    def _parse_matcher_in_segment(self, partial_split, matcher,
                                  block_until_ready=False, *args, **kwargs):
        """
        Parses an IN_SEGMENT matcher
        :param partial_split: The partially parsed split
        :param partial_split: Split
        :param matcher: A dictionary with the JSON representation of an
            IN_SEGMENT matcher
        :type matcher: dict
        :return: The parsed matcher
        :rtype: UserDefinedSegmentMatcher
        """
        matcher_data = self._get_matcher_attribute(
            'userDefinedSegmentMatcherData', matcher
        )
        segment = self._segment_fetcher.fetch(
            matcher_data['segmentName'], block_until_ready=block_until_ready
        )
        delegate = UserDefinedSegmentMatcher(segment)
        return delegate

    def _parse_matcher_whitelist(self, partial_split, matcher, *args, **kwargs):
        """
        Parses a WHITELIST matcher
        :param partial_split: The partially parsed split
        :param partial_split: Split
        :param matcher: A dictionary with the JSON representation of a
            WHITELIST matcher
        :type matcher: dict
        :return: The parsed matcher
        :rtype: WhitelistMatcher
        """
        matcher_data = self._get_matcher_attribute(
            'whitelistMatcherData',
            matcher
        )
        delegate = WhitelistMatcher(matcher_data['whitelist'])
        return delegate

    def _parse_matcher_equal_to(self, partial_split, matcher, *args, **kwargs):
        """
        Parses an EQUAL_TO matcher
        :param partial_split: The partially parsed split
        :param partial_split: Split
        :param matcher: A dictionary with the JSON representation of an EQUAL_TO
            matcher
        :type matcher: dict
        :return: The parsed matcher (dependent on data type)
        :rtype: EqualToMatcher
        """
        matcher_data = self._get_matcher_attribute(
            'unaryNumericMatcherData', matcher
        )
        data_type = self._get_matcher_data_data_type(matcher_data)

        delegate = EqualToMatcher.for_data_type(
            data_type, matcher_data.get('value', None)
        )
        return delegate

    def _parse_matcher_greater_than_or_equal_to(self, partial_split, matcher,
                                                *args, **kwargs):
        """
        Parses a GREATER_THAN_OR_EQUAL_TO matcher
        :param partial_split: The partially parsed split
        :param partial_split: Split
        :param matcher: A dictionary with the JSON representation of a
            GREATER_THAN_OR_EQUAL_TO
                        matcher
        :type matcher: dict
        :return: The parsed matcher (dependent on data type)
        :rtype: GreaterThanOrEqualToMatcher
        """
        matcher_data = self._get_matcher_attribute(
            'unaryNumericMatcherData', matcher
        )
        data_type = self._get_matcher_data_data_type(matcher_data)

        delegate = GreaterThanOrEqualToMatcher.for_data_type(
            data_type, matcher_data.get('value', None)
        )
        return delegate

    def _parse_matcher_less_than_or_equal_to(self, partial_split, matcher,
                                             *args, **kwargs):
        """
        Parses a LESS_THAN_OR_EQUAL_TO matcher
        :param partial_split: The partially parsed split
        :param partial_split: Split
        :param matcher: A dictionary with the JSON representation of an
            LESS_THAN_OR_EQUAL_TO matcher
        :type matcher: dict
        :return: The parsed matcher (dependent on data type)
        :rtype: LessThanOrEqualToMatcher
        """
        matcher_data = self._get_matcher_attribute(
            'unaryNumericMatcherData', matcher
        )
        data_type = self._get_matcher_data_data_type(matcher_data)

        delegate = LessThanOrEqualToMatcher.for_data_type(data_type,
                                                          matcher_data['value'])
        return delegate

    def _parse_matcher_starts_with(self, partial_split, matcher, *args,
                                   **kwargs):
        """
        Parses a STARTS_WITH matcher
        :param partial_split: The partially parsed split
        :param partial_split: Split
        :param matcher: A dictionary with the JSON representation of a
            STARTS_WITH matcher
        :type matcher: dict
        :return: The parsed matcher (dependent on data type)
        :rtype: BetweenMatcher
        """
        matcher_data = self._get_matcher_attribute(
            'whitelistMatcherData',
            matcher
        )
        delegate = StartsWithMatcher(matcher_data['whitelist'])
        return delegate

    def _parse_matcher_ends_with(self, partial_split, matcher, *args,
                                 **kwargs):
        """
        Parses a ENDS_WITH matcher
        :param partial_split: The partially parsed split
        :param partial_split: Split
        :param matcher: A dictionary with the JSON representation of a
            ENDS_WITH matcher
        :type matcher: dict
        :return: The parsed matcher (dependent on data type)
        :rtype: BetweenMatcher
        """
        matcher_data = self._get_matcher_attribute(
            'whitelistMatcherData',
            matcher
        )
        delegate = EndsWithMatcher(matcher_data['whitelist'])
        return delegate

    def _parse_matcher_contains_string(self, partial_split, matcher, *args,
                                       **kwargs):
        """
        Parses a CONTAINS_STRING matcher
        :param partial_split: The partially parsed split
        :param partial_split: Split
        :param matcher: A dictionary with the JSON representation of a
            CONTAINS_STRING matcher
        :type matcher: dict
        :return: The parsed matcher (dependent on data type)
        :rtype: BetweenMatcher
        """
        matcher_data = self._get_matcher_attribute(
            'whitelistMatcherData',
            matcher
        )
        delegate = ContainsStringMatcher(matcher_data['whitelist'])
        return delegate

    def _parse_matcher_contains_all_of_set(self, partial_split, matcher, *args,
                                           **kwargs):
        """
        Parses a CONTAINS_ALL_OF_SET matcher
        :param partial_split: The partially parsed split
        :param partial_split: Split
        :param matcher: A dictionary with the JSON representation of a
            CONTAINS_ALL_OF_SET matcher
        :type matcher: dict
        :return: The parsed matcher (dependent on data type)
        :rtype: BetweenMatcher
        """
        matcher_data = self._get_matcher_attribute(
            'whitelistMatcherData',
            matcher
        )
        delegate = ContainsAllOfSetMatcher(matcher_data['whitelist'])
        return delegate

    def _parse_matcher_contains_any_of_set(self, partial_split, matcher, *args,
                                           **kwargs):
        """
        Parses a CONTAINS_ANY_OF_SET matcher
        :param partial_split: The partially parsed split
        :param partial_split: Split
        :param matcher: A dictionary with the JSON representation of a
            CONTAINS_ANY_OF_SET matcher
        :type matcher: dict
        :return: The parsed matcher (dependent on data type)
        :rtype: BetweenMatcher
        """
        matcher_data = self._get_matcher_attribute(
            'whitelistMatcherData',
            matcher
        )
        delegate = ContainsAnyOfSetMatcher(matcher_data['whitelist'])
        return delegate

    def _parse_matcher_equal_to_set(self, partial_split, matcher, *args,
                                    **kwargs):
        """
        Parses a EQUAL_TO_SET matcher
        :param partial_split: The partially parsed split
        :param partial_split: Split
        :param matcher: A dictionary with the JSON representation of a
            EQUAL_TO_SET matcher
        :type matcher: dict
        :return: The parsed matcher (dependent on data type)
        :rtype: BetweenMatcher
        """
        matcher_data = self._get_matcher_attribute(
            'whitelistMatcherData',
            matcher
        )
        delegate = EqualToSetMatcher(matcher_data['whitelist'])
        return delegate

    def _parse_matcher_part_of_set(self, partial_split, matcher, *args,
                                   **kwargs):
        """
        Parses a PART_OF_SET matcher
        :param partial_split: The partially parsed split
        :param partial_split: Split
        :param matcher: A dictionary with the JSON representation of a
            PART_OF_SET matcher
        :type matcher: dict
        :return: The parsed matcher (dependent on data type)
        :rtype: BetweenMatcher
        """
        matcher_data = self._get_matcher_attribute(
            'whitelistMatcherData',
            matcher
        )
        delegate = PartOfSetMatcher(matcher_data['whitelist'])
        return delegate

    def _parse_matcher_between(self, partial_split, matcher, *args, **kwargs):
        """
        Parses a BETWEEN matcher
        :param partial_split: The partially parsed split
        :param partial_split: Split
        :param matcher: A dictionary with the JSON representation of an BETWEEN
            matcher
        :type matcher: dict
        :return: The parsed matcher (dependent on data type)
        :rtype: BetweenMatcher
        """
        matcher_data = self._get_matcher_attribute(
            'betweenMatcherData', matcher
        )
        data_type = self._get_matcher_data_data_type(matcher_data)

        delegate = BetweenMatcher.for_data_type(data_type,
                                                matcher_data.get('start', None),
                                                matcher_data.get('end', None))
        return delegate

    def _parse_matcher(self, partial_split, matcher, block_until_ready=False):
        """
        Parses a matcher
        :param partial_split: The partially parsed split
        :param partial_split: Split
        :param matcher: A dictionary with the JSON representation of a matcher
        :type matcher: dict
        :param block_until_ready: Whether to block until all data is available
        :type block_until_ready: bool
        :return: A parsed attribute matcher (with a delegate dependent on type)
        :rtype: AttributeMatcher
        """
        if 'matcherType' not in matcher or matcher['matcherType'] is None:
            raise ValueError('Missing matcher type value')

        matcher_type = matcher['matcherType']

        try:
            matcher_parse_method = getattr(
                self, '_parse_matcher_{}'.format(matcher_type.strip().lower()))
            delegate = matcher_parse_method(partial_split, matcher,
                                            block_until_ready=block_until_ready)
        except AttributeError:
            raise ValueError('Invalid matcher type: {}'.format(matcher_type))

        if delegate is None:
            raise ValueError(
                'Unable to create matcher for matcher type: {}'
                .format(matcher_type)
            )

        attribute = None
        if 'keySelector' in matcher and matcher['keySelector'] and \
                'attribute' in matcher['keySelector']:
            attribute = matcher['keySelector']['attribute']

        return AttributeMatcher(
            attribute, delegate, matcher.get('negate', False)
        )


class CacheBasedSplitFetcher(SplitFetcher):
    def __init__(self, split_cache):
        """
        A cache based SplitFetcher implementation
        :param split_cache: The split cache
        :type split_cache: SplitCache
        """
        super(CacheBasedSplitFetcher, self).__init__()

        self._split_cache = split_cache

    def fetch(self, feature):
        return self._split_cache.get_split(feature)

    def fetch_all(self):
        """
        Feches all splits
        :return: All the know splits so far
        :rtype: list
        """
        return self._split_cache.get_splits()

    @property
    def change_number(self):
        return self._split_cache.get_change_number()
