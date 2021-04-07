"""Splits module."""
from enum import Enum
from collections import namedtuple

from splitio.models.grammar import condition


SplitView = namedtuple(
    'SplitView',
    ['name', 'traffic_type', 'killed', 'treatments', 'change_number', 'configs']
)


class Status(Enum):
    """Split status."""

    ACTIVE = "ACTIVE"
    ARCHIVED = "ARCHIVED"


class HashAlgorithm(Enum):
    """Hash algorithm names."""

    LEGACY = 1
    MURMUR = 2


class Split(object):  # pylint: disable=too-many-instance-attributes
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
            configurations=None
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
        """
        self._name = name
        self._seed = seed
        self._killed = killed
        self._default_treatment = default_treatment
        self._traffic_type_name = traffic_type_name
        try:
            self._status = Status(status)
        except ValueError:
            self._status = Status.ARCHIVED

        self._change_number = change_number
        self._conditions = conditions if conditions is not None else []

        if traffic_allocation is None:
            self._traffic_allocation = 100
        elif traffic_allocation >= 0 and traffic_allocation <= 100:
            self._traffic_allocation = traffic_allocation
        else:
            self._traffic_allocation = 100

        self._traffic_allocation_seed = traffic_allocation_seed
        try:
            self._algo = HashAlgorithm(algo)
        except ValueError:
            self._algo = HashAlgorithm.LEGACY

        self._configurations = configurations

    @property
    def name(self):
        """Return name."""
        return self._name

    @property
    def seed(self):
        """Return seed."""
        return self._seed

    @property
    def algo(self):
        """Return hash algorithm."""
        return self._algo

    @property
    def killed(self):
        """Return whether the split has been killed."""
        return self._killed

    @property
    def default_treatment(self):
        """Return the default treatment."""
        return self._default_treatment

    @property
    def traffic_type_name(self):
        """Return the traffic type of the split."""
        return self._traffic_type_name

    @property
    def status(self):
        """Return the status of the split."""
        return self._status

    @property
    def change_number(self):
        """Return the change number of the split."""
        return self._change_number

    @property
    def conditions(self):
        """Return the condition list of the split."""
        return self._conditions

    @property
    def traffic_allocation(self):
        """Return the traffic allocation percentage of the split."""
        return self._traffic_allocation

    @property
    def traffic_allocation_seed(self):
        """Return the traffic allocation seed of the split."""
        return self._traffic_allocation_seed

    def get_configurations_for(self, treatment):
        """Return the mapping of treatments to configurations."""
        return self._configurations.get(treatment) if self._configurations else None

    def get_segment_names(self):
        """
        Return a list of segment names referenced in all matchers from this split.

        :return: List of segment names.
        :rtype: list(string)
        """
        return [name for cond in self.conditions for name in cond.get_segment_names()]

    def to_json(self):
        """Return a JSON representation of this split."""
        return {
            'changeNumber': self.change_number,
            'trafficTypeName': self.traffic_type_name,
            'name': self.name,
            'trafficAllocation': self.traffic_allocation,
            'trafficAllocationSeed': self.traffic_allocation_seed,
            'seed': self.seed,
            'status': self.status.value,
            'killed': self.killed,
            'defaultTreatment': self.default_treatment,
            'algo': self.algo.value,
            'conditions': [c.to_json() for c in self.conditions],
            'configurations': self._configurations
        }

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
            self._configurations if self._configurations is not None else {}
        )

    def local_kill(self, default_treatment, change_number):
        """
        Perform split kill.

        :param default_treatment: name of the default treatment to return
        :type default_treatment: str
        :param change_number: change_number
        :type change_number: int
        """
        self._default_treatment = default_treatment
        self._change_number = change_number
        self._killed = True

    def __str__(self):
        """Return string representation."""
        return 'name: {name}, seed: {seed}, killed: {killed}, ' \
               'default treatment: {default_treatment}, ' \
               'conditions: {conditions}'.format(
                   name=self._name, seed=self._seed, killed=self._killed,
                   default_treatment=self._default_treatment,
                   conditions=','.join(map(str, self._conditions))
               )


def from_raw(raw_split):
    """
    Parse a split from a JSON portion of splitChanges.

    :param raw_split: JSON object extracted from a splitChange's split array (splitChanges response)
    :type raw_split: dict

    :return: A parsed Split object capable of performing evaluations.
    :rtype: Split
    """
    return Split(
        raw_split['name'],
        raw_split['seed'],
        raw_split['killed'],
        raw_split['defaultTreatment'],
        raw_split['trafficTypeName'],
        raw_split['status'],
        raw_split['changeNumber'],
        [condition.from_raw(c) for c in raw_split['conditions']],
        raw_split.get('algo'),
        traffic_allocation=raw_split.get('trafficAllocation'),
        traffic_allocation_seed=raw_split.get('trafficAllocationSeed'),
        configurations=raw_split.get('configurations')
    )
