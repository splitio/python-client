"""Split conditions module."""

from enum import Enum
from future.utils import python_2_unicode_compatible
import six

from splitio.models.grammar import matchers
from splitio.models.grammar import partitions

_MATCHER_COMBINERS = {
    'AND': lambda ms, k, a, c: all(m.evaluate(k, a, c) for m in ms)
}


class ConditionType(Enum):
    """Split possible condition types."""

    WHITELIST = 'WHITELIST'
    ROLLOUT = 'ROLLOUT'


class Condition(object):
    """Condition object class."""

    def __init__(  #pylint: disable=too-many-arguments
            self,
            matcher_list,
            combiner, parts, label,
            condition_type=ConditionType.WHITELIST
    ):
        """
        Class constructor.

        :param matcher: A combining matcher
        :type matcher: CombiningMatcher
        :param parts: A list of partitions
        :type parts: list
        """
        self._matchers = matcher_list
        self._combiner = combiner
        self._partitions = tuple(parts)
        self._label = label
        self._condition_type = condition_type

    @property
    def matchers(self):
        """Return the list of matchers associated to the condition."""
        return self._matchers

    @property
    def partitions(self):
        """Return the list of partitions associated with the condition."""
        return self._partitions

    @property
    def label(self):
        """Return the label of this condition."""
        return self._label

    @property
    def condition_type(self):
        """Return the condition type."""
        return self._condition_type

    def matches(self, key, attributes=None, context=None):
        """
        Check whether the condition matches against user submitted input.

        :param key: User key
        :type key: splitio.client.key.Key
        :param attributes: User custom attributes.
        :type attributes: dict
        :param context: Evaluation context
        :type context: dict
        """
        return self._combiner(self._matchers, key, attributes, context)

    def get_segment_names(self):
        """
        Fetch segment names for all IN_SEGMENT matchers.

        :return: List of segment names
        :rtype: list(str)
        """
        return [
            matcher._segment_name for matcher in self.matchers  #pylint: disable=protected-access
            if isinstance(matcher, matchers.UserDefinedSegmentMatcher)
        ]

    @python_2_unicode_compatible
    def __str__(self):
        """Return the string representation of the condition."""
        return '{matcher} then split {parts}'.format(
            matcher=self._matchers, parts=','.join(
                '{size}:{treatment}'.format(size=partition.size,
                                            treatment=partition.treatment)
                for partition in self._partitions))

    def to_json(self):
        """Return the JSON representation of this condition."""
        return {
            'conditionType': self._condition_type.name,
            'label': self._label,
            'matcherGroup': {
                'combiner': next(
                    (k, v) for k, v in six.iteritems(_MATCHER_COMBINERS) if v == self._combiner
                )[0],
                'matchers': [m.to_json() for m in self.matchers]
            },
            'partitions': [p.to_json() for p in self.partitions]
        }


def from_raw(raw_condition):
    """
    Parse a condition from a JSON portion of splitChanges.

    :param raw_condition: JSON object extracted from a split's conditions array.
    :type raw_condition: dict

    :return: A condition object.
    :rtype: Condition
    """
    parsed_partitions = [
        partitions.from_raw(raw_partition)
        for raw_partition in raw_condition['partitions']
    ]

    matcher_objects = [matchers.from_raw(x) for x in raw_condition['matcherGroup']['matchers']]
    combiner = _MATCHER_COMBINERS[raw_condition['matcherGroup']['combiner']]
    label = raw_condition.get('label')

    condition_type = ConditionType(raw_condition.get('conditionType', ConditionType.WHITELIST))

    return Condition(matcher_objects, combiner, parsed_partitions, label, condition_type)
