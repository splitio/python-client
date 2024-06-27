"""Split evaluator module."""
import logging
from collections import namedtuple

from splitio.models.impressions import Label
from splitio.models.grammar.condition import ConditionType
from splitio.models.grammar.matchers.misc import DependencyMatcher
from splitio.models.grammar.matchers.keys import UserDefinedSegmentMatcher
from splitio.optional.loaders import asyncio

CONTROL = 'control'
EvaluationContext = namedtuple('EvaluationContext', ['flags', 'segment_memberships'])

_LOGGER = logging.getLogger(__name__)


class Evaluator(object):  # pylint: disable=too-few-public-methods
    """Split Evaluator class."""

    def __init__(self, splitter):
        """
        Construct a Evaluator instance.

        :param splitter: partition object.
        :type splitter: splitio.engine.splitters.Splitters
        """
        self._splitter = splitter

    def eval_many_with_context(self, key, bucketing, features, attrs, ctx):
        """
        ...
        """
        # we can do a linear evaluation here, since all the dependencies are already fetched
        return {
            name: self.eval_with_context(key, bucketing, name, attrs, ctx)
            for name in features
        }

    def eval_with_context(self, key, bucketing, feature_name, attrs, ctx):
        """
        ...
        """
        label = ''
        _treatment = CONTROL
        _change_number = -1

        feature = ctx.flags.get(feature_name)
        if not feature:
            _LOGGER.warning('Unknown or invalid feature: %s', feature)
            label = Label.SPLIT_NOT_FOUND
        else:
            _change_number = feature.change_number
            if feature.killed:
                label = Label.KILLED
                _treatment = feature.default_treatment
            else:
                treatment, label = self._treatment_for_flag(feature, key, bucketing, attrs, ctx)
                if treatment is None:
                    label = Label.NO_CONDITION_MATCHED
                    _treatment = feature.default_treatment
                else:
                    _treatment = treatment

        return {
            'treatment': _treatment,
            'configurations': feature.get_configurations_for(_treatment) if feature else None,
            'impression': {
                'label': label,
                'change_number': _change_number
            }
        }

    def _treatment_for_flag(self, flag, key, bucketing, attributes, ctx):
        """
        ...
        """
        bucketing = bucketing if bucketing is not None else key
        rollout = False
        for condition in flag.conditions:
            if not rollout and condition.condition_type == ConditionType.ROLLOUT:
                if flag.traffic_allocation < 100:
                    bucket = self._splitter.get_bucket(bucketing, flag.traffic_allocation_seed, flag.algo)
                    if bucket > flag.traffic_allocation:
                        return flag.default_treatment, Label.NOT_IN_SPLIT

                rollout = True

            if condition.matches(key, attributes, {
                'evaluator': self,
                'bucketing_key': bucketing,
                'ec': ctx,
                }):

                return self._splitter.get_treatment(bucketing, flag.seed, condition.partitions, flag.algo), condition.label

        return flag.default_treatment, Label.NO_CONDITION_MATCHED

class EvaluationDataFactory:

    def __init__(self, split_storage, segment_storage):
        self._flag_storage = split_storage
        self._segment_storage = segment_storage

    def context_for(self, key, feature_names):
        """
        Recursively iterate & fetch all data required to evaluate these flags.
        :type features: list
        :type bucketing_key: str
        :type attributes: dict

        :rtype: EvaluationContext
        """
        pending = set(feature_names)
        splits = {}
        pending_memberships = set()
        while pending:
            fetched = self._flag_storage.fetch_many(list(pending))
            features = filter_missing(fetched)
            splits.update(features)
            pending = set()
            for feature in features.values():
                cf, cs = get_dependencies(feature)
                pending.update(filter(lambda f: f not in splits, cf))
                pending_memberships.update(cs)

        return EvaluationContext(splits, {
            segment: self._segment_storage.segment_contains(segment, key)
            for segment in pending_memberships
        })


class AsyncEvaluationDataFactory:

    def __init__(self, split_storage, segment_storage):
        self._flag_storage = split_storage
        self._segment_storage = segment_storage

    async def context_for(self, key, feature_names):
        """
        Recursively iterate & fetch all data required to evaluate these flags.
        :type features: list
        :type bucketing_key: str
        :type attributes: dict

        :rtype: EvaluationContext
        """
        pending = set(feature_names)
        splits = {}
        pending_memberships = set()
        while pending:
            fetched = await self._flag_storage.fetch_many(list(pending))
            features = filter_missing(fetched)
            splits.update(features)
            pending = set()
            for feature in features.values():
                cf, cs = get_dependencies(feature)
                pending.update(filter(lambda f: f not in splits, cf))
                pending_memberships.update(cs)

        segment_names = list(pending_memberships)
        segment_memberships = await asyncio.gather(*[
            self._segment_storage.segment_contains(segment, key)
            for segment in segment_names
        ])

        return EvaluationContext(splits, dict(zip(segment_names, segment_memberships)))


def get_dependencies(feature):
    """
    :rtype: tuple(list, list)
    """
    feature_names = []
    segment_names = []
    for condition in feature.conditions:
        for matcher in condition.matchers:
            if isinstance(matcher,UserDefinedSegmentMatcher):
                segment_names.append(matcher._segment_name)
            elif isinstance(matcher, DependencyMatcher):
                feature_names.append(matcher._split_name)

    return feature_names, segment_names

def filter_missing(features):
    return {k: v for (k, v) in features.items() if v is not None}
