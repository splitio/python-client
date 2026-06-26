"""Splits module."""
from enum import Enum
from collections import namedtuple
import logging

from splitio_commons.models import MatcherNotFoundException, _DEFAULT_CONDITIONS_TEMPLATE, Status, HashAlgorithm, Prerequisites
from splitio_commons.models.grammar import condition
from splitio_commons.models.definitions import Definition, Prerequisites, Status, from_raw_prerequisites

_LOGGER = logging.getLogger(__name__)

SplitView = namedtuple(
    'SplitView',
    ['name', 'traffic_type', 'killed', 'treatments', 'change_number', 'configs', 'default_treatment', 'sets', 'impressions_disabled', 'prerequisites']
)

class Split(Definition):  # pylint: disable=too-many-instance-attributes
    """Split model object."""

    def __init__(  # pylint: disable=too-many-arguments
            self,
            name,
            seed,
            killed,
            default_treatment,
            traffic_type_name,
            status,
            change_number,
            conditions=None,
            algo=None,
            traffic_allocation=None,
            traffic_allocation_seed=None,
            configurations=None,
            sets=None,
            impressions_disabled=None,
            prerequisites = None
    ):
        """
        Class constructor.

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
        :param algo: Hash algorithm to use when splitting.
        :type algo: HashAlgorithm
        :param traffic_allocation: Percentage of traffic to consider.
        :type traffic_allocation: int
        :pram traffic_allocation_seed: Seed used to hash traffic allocation.
        :type traffic_allocation_seed: int
        :pram sets: list of flag sets
        :type sets: list
        :pram impressions_disabled: track impressions flag
        :type impressions_disabled: boolean
        :pram prerequisites: prerequisites
        :type prerequisites: List of Preqreuisites
        """
        Definition.__init__(self, name, seed, killed, default_treatment, traffic_type_name, status,
            change_number, conditions, algo, traffic_allocation, traffic_allocation_seed, configurations,
            sets, impressions_disabled, prerequisites)

    def to_split_view(self):
        """
        Return a SplitView for the manager.

        :return: A portion of the split useful for inspecting by the user.
        :rtype: SplitView
        """
        return SplitView(
            self.name,
            self.traffic_type_name,
            self.killed,
            list(set(part.treatment for cond in self.conditions for part in cond.partitions)),
            self.change_number,
            self._configurations if self._configurations is not None else {},
            self._default_treatment,
            list(self._sets) if self._sets is not None else [],
            self._impressions_disabled,
            self._prerequisites
        )

def from_raw(raw_definition):
    """
    Parse a definition from a JSON portion of definitionChanges.

    :param raw_definition: JSON object extracted from a definitionChange's definition array (definitionChanges response)
    :type raw_definition: dict

    :return: A parsed definition object capable of performing evaluations.
    :rtype: definition
    """
    try:
        conditions = [condition.from_raw(c) for c in raw_definition['conditions']] 
    except MatcherNotFoundException as e:
        _LOGGER.error(str(e))
        _LOGGER.debug("Using default conditions template for feature flag: %s", raw_definition['name'])
        conditions = [condition.from_raw(_DEFAULT_CONDITIONS_TEMPLATE)]
    return Split(
        raw_definition['name'],
        raw_definition['seed'],
        raw_definition['killed'],
        raw_definition['defaultTreatment'],
        raw_definition['trafficTypeName'],
        raw_definition['status'],
        raw_definition['changeNumber'],
        conditions,
        raw_definition.get('algo'),
        traffic_allocation=raw_definition.get('trafficAllocation'),
        traffic_allocation_seed=raw_definition.get('trafficAllocationSeed'),
        configurations=raw_definition.get('configurations'),
        sets=set(raw_definition.get('sets')) if raw_definition.get('sets') is not None else [],
        impressions_disabled=raw_definition.get('impressionsDisabled') if raw_definition.get('impressionsDisabled') is not None else False,
        prerequisites=from_raw_prerequisites(raw_definition.get('prerequisites')) if raw_definition.get('prerequisites') is not None else []
    )