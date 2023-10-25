"""Split evaluator module."""
import logging
from collections import namedtuple

from splitio.models.impressions import Label
from splitio.models.grammar import matchers
from splitio.models.grammar.condition import ConditionType
from splitio.models.grammar.matchers.misc import DependencyMatcher
from splitio.engine import FeatureNotFoundException

CONTROL = 'control'
EvaluationDataContext = namedtuple('EvaluationDataContext', ['feature_flag', 'evaluation_contexts'])

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

    def _evaluate_treatment(self, feature_flag, matching_key, bucketing_key, evaluation_contexts):
        """
        Evaluate the user submitted data against a feature and return the resulting treatment.

        :param feature_flag: Split object
        :type feature_flag: splitio.models.splits.Split|None

        :param matching_key: The matching_key for which to get the treatment
        :type matching_key: str

        :param bucketing_key: The bucketing_key for which to get the treatment
        :type bucketing_key: str

        :param evaluation_contexts: array of condition matchers for passed feature_flag
        :type bucketing_key: Dict

        :return: The treatment for the key and feature flag
        :rtype: object
        """
        label = ''
        _treatment = CONTROL
        _change_number = -1

        if feature_flag is None:
            _LOGGER.warning('Unknown or invalid feature: %s', feature_flag.name)
            label = Label.SPLIT_NOT_FOUND
        else:
            _change_number = feature_flag.change_number
            if feature_flag.killed:
                label = Label.KILLED
                _treatment = feature_flag.default_treatment
            else:
                treatment, label = self._get_treatment_for_feature_flag(
                    feature_flag,
                    matching_key,
                    bucketing_key,
                    evaluation_contexts
                )
                if treatment is None:
                    label = Label.NO_CONDITION_MATCHED
                    _treatment = feature_flag.default_treatment
                else:
                    _treatment = treatment

        return {
            'treatment': _treatment,
            'configurations': feature_flag.get_configurations_for(_treatment) if feature_flag else None,
            'impression': {
                'label': label,
                'change_number': _change_number
            }
        }

    def evaluate_feature(self, feature_flag, matching_key, bucketing_key, evaluation_contexts):
        """
        Evaluate the user submitted data against a feature and return the resulting treatment.

        :param feature_flag: Split object
        :type feature_flag: splitio.models.splits.Split|None

        :param matching_key: The matching_key for which to get the treatment
        :type matching_key: str

        :param bucketing_key: The bucketing_key for which to get the treatment
        :type bucketing_key: str

        :param evaluation_contexts: array of condition matchers for passed feature_flag
        :type bucketing_key: Dict

        :return: The treatment for the key and split
        :rtype: object
        """
        # Calling evaluation
        evaluation = self._evaluate_treatment(feature_flag, matching_key,
                                              bucketing_key, evaluation_contexts)

        return evaluation

    def evaluate_features(self, feature_flags, matching_key, bucketing_key, evaluation_contexts):
        """
        Evaluate the user submitted data against multiple features and return the resulting
        treatment.

        :param feature_flags: array of Split objects
        :type feature_flags: [splitio.models.splits.Split|None]

        :param matching_key: The matching_key for which to get the treatment
        :type matching_key: str

        :param bucketing_key: The bucketing_key for which to get the treatment
        :type bucketing_key: str

        :param evaluation_contexts: array of condition matchers for passed feature_flag
        :type bucketing_key: Dict

        :return: The treatments for the key and feature flags
        :rtype: object
        """
        return {
            feature_flag.name: self._evaluate_treatment(feature_flag, matching_key,
                                              bucketing_key, evaluation_contexts[feature_flag.name])
            for (feature_flag) in feature_flags
        }

    def _get_treatment_for_feature_flag(self, feature_flag, matching_key, bucketing_key, evaluation_contexts):
        """
        Evaluate the feature considering the conditions.

        If there is a match, it will return the condition and the label.
        Otherwise, it will return (None, None)

        :param feature_flag: The feature flag for which to get the treatment
        :type feature_flag: Split

        :param matching_key: The key for which to get the treatment
        :type key: str

        :param bucketing_key: The key for which to get the treatment
        :type key: str

        :param evaluation_contexts: array of condition matchers for passed feature_flag
        :type bucketing_key: Dict

        :return: The resulting treatment and label
        :rtype: tuple
        """
        if bucketing_key is None:
            bucketing_key = matching_key

        for evaluation_context, condition in evaluation_contexts:
            if evaluation_context:
                return self._splitter.get_treatment(
                    bucketing_key,
                    feature_flag.seed,
                    condition.partitions,
                    feature_flag.algo
                ), condition.label

        # No condition matches
        return None, None

class EvaluationDataCollector(object):
    """Split Evaluator data collector class."""

    def __init__(self, feature_flag_storage, segment_storage, splitter, evaluator):
        """
        Construct a Evaluator instance.

        :param feature_flag_storage: Feature flag storage object.
        :type feature_flag_storage: splitio.storage.SplitStorage
        :param segment_storage: Segment storage object.
        :type splitter: splitio.storage.SegmentStorage
        :param splitter: partition object.
        :type splitter: splitio.engine.splitters.Splitters
        :param evaluator: Evaluator object
        :type evaluator: splitio.engine.evaluator.Evaluator
        """
        self._feature_flag_storage = feature_flag_storage
        self._segment_storage = segment_storage
        self._splitter = splitter
        self._evaluator = evaluator
        self.feature_flag = None

    def build_evaluation_context(self, feature_flag_names, bucketing_key, matching_key, method, attributes=None):
        evaluation_contexts = {}
        fetched_feature_flags = self._feature_flag_storage.fetch_many(feature_flag_names)
        feature_flags = []
        missing = []
        for feature_flag_name in feature_flag_names:
            try:
                if fetched_feature_flags[feature_flag_name] is None:
                    raise FeatureNotFoundException(feature_flag_name)

                evaluation_data_context = self.get_evaluation_contexts(fetched_feature_flags[feature_flag_name], bucketing_key, matching_key, attributes)
                evaluation_contexts[feature_flag_name] = evaluation_data_context.evaluation_contexts
                feature_flags.append(evaluation_data_context.feature_flag)
            except FeatureNotFoundException:
                _LOGGER.warning(
                    "%s: you passed \"%s\" that does not exist in this environment, "
                    "please double check what Feature flags exist in the Split user interface.",
                    'get_' + method.value,
                    feature_flag_name
                )
                missing.append(feature_flag_name)
        return feature_flags, missing, evaluation_contexts

    def get_evaluation_contexts(self, feature_flag, bucketing_key, matching_key, attributes=None):
        """
        Calculate and store all condition matchers for given feature flag.
        If there are dependent Feature Flag(s), the function will do recursive calls until all matchers are resolved.

        :param feature_flag: Feature flag Split objects
        :type feature_flag: splitio.models.splits.Split
        :param bucketing_key: Bucketing key for which to get the treatment
        :type bucketing_key: str
        :param matching_key: Matching key for which to get the treatment
        :type matching_key: str
        :return: dictionary representing all matchers for each current feature flag
        :type: dict
        """
        segment_matchers = self._get_segment_matchers(feature_flag, matching_key)
        return EvaluationDataContext(feature_flag, self._get_evaluation_contexts(feature_flag, bucketing_key, matching_key, segment_matchers, attributes))

    def _get_evaluation_contexts(self, feature_flag, bucketing_key, matching_key, segment_matchers, attributes=None):
        """
        Calculate and store all condition matchers for given feature flag.
        If there are dependent Feature Flag(s), the function will do recursive calls until all matchers are resolved.

        :param feature_flag: Feature flag Split objects
        :type feature_flag: splitio.models.splits.Split
        :param bucketing_key: Bucketing key for which to get the treatment
        :type bucketing_key: str
        :param matching_key: Matching key for which to get the treatment
        :type matching_key: str
        :param segment_matchers: Segment matchers for the feature flag
        :type segment_matchers: dict
        :return: dictionary representing all matchers for each current feature flag
        :type: dict
        """
        roll_out = False
        context = {
            'segment_matchers': segment_matchers,
            'evaluator': self._evaluator,
            'bucketing_key': bucketing_key
        }
        evaluation_contexts = []
        for condition in feature_flag.conditions:
            if (not roll_out and
                    condition.condition_type == ConditionType.ROLLOUT):
                if feature_flag.traffic_allocation < 100:
                    bucket = self._splitter.get_bucket(
                        bucketing_key,
                        feature_flag.traffic_allocation_seed,
                        feature_flag.algo
                    )
                    if bucket > feature_flag.traffic_allocation:
                        return feature_flag.default_treatment, Label.NOT_IN_SPLIT
                roll_out = True
            dependent_feature_flags = []
            for matcher in condition.matchers:
                if isinstance(matcher, DependencyMatcher):
                    dependent_feature_flag = self._feature_flag_storage.get(matcher.to_json()['dependencyMatcherData']['split'])
                    depenedent_segment_matchers = self._get_segment_matchers(dependent_feature_flag, matching_key)
                    dependent_feature_flags.append((dependent_feature_flag,
                                                   self._get_evaluation_contexts(dependent_feature_flag, bucketing_key, matching_key, depenedent_segment_matchers, attributes)))
            context['dependent_splits'] = dependent_feature_flags
            evaluation_contexts.append((condition.matches(
                matching_key,
                attributes=attributes,
                context=context
            ), condition))

        return evaluation_contexts

    def _get_segment_matchers(self, feature_flag, matching_key):
        """
        Get all segments matchers for given feature flag.
        If there are dependent Feature Flag(s), the function will do recursive calls until all matchers are resolved.

        :param feature_flag: Feature flag Split objects
        :type feature_flag: splitio.models.splits.Split
        :param matching_key: Matching key for which to get the treatment
        :type matching_key: str
        :return: Segment matchers for the feature flag
        :type: dict
        """
        segment_matchers = {}
        for segment in self._get_segment_names(feature_flag):
            for condition in feature_flag.conditions:
                for matcher in condition.matchers:
                    if isinstance(matcher, matchers.UserDefinedSegmentMatcher):
                        segment_matchers[segment] = self._segment_storage.segment_contains(segment, matching_key)
        return segment_matchers

    def _get_segment_names(self, feature_flag):
        """
        Fetch segment names for all IN_SEGMENT matchers.

        :return: List of segment names
        :rtype: list(str)
        """
        segment_names = []
        if feature_flag is None:
            return []
        for condition in feature_flag.conditions:
            matcher_list = condition.matchers
            for matcher in matcher_list:
                if isinstance(matcher, matchers.UserDefinedSegmentMatcher):
                    segment_names.append(matcher._segment_name)

        return segment_names

    async def build_evaluation_context_async(self, feature_flag_names, bucketing_key, matching_key, method, attributes=None):
        evaluation_contexts = {}
        fetched_feature_flags = await self._feature_flag_storage.fetch_many(feature_flag_names)
        feature_flags = []
        missing = []
        for feature_flag_name in feature_flag_names:
            try:
                if fetched_feature_flags[feature_flag_name] is None:
                    raise FeatureNotFoundException(feature_flag_name)

                evaluation_data_context = await self.get_evaluation_contexts_async(fetched_feature_flags[feature_flag_name], bucketing_key, matching_key, attributes)
                evaluation_contexts[feature_flag_name] = evaluation_data_context.evaluation_contexts
                feature_flags.append(evaluation_data_context.feature_flag)
            except FeatureNotFoundException:
                _LOGGER.warning(
                    "%s: you passed \"%s\" that does not exist in this environment, "
                    "please double check what Feature flags exist in the Split user interface.",
                    'get_' + method.value,
                    feature_flag_name
                )
                missing.append(feature_flag_name)
        return feature_flags, missing, evaluation_contexts

    async def get_evaluation_contexts_async(self, feature_flag, bucketing_key, matching_key, attributes=None):
        """
        Calculate and store all condition matchers for given feature flag.
        If there are dependent Feature Flag(s), the function will do recursive calls until all matchers are resolved.

        :param feature_flag: Feature flag Split objects
        :type feature_flag: splitio.models.splits.Split
        :param bucketing_key: Bucketing key for which to get the treatment
        :type bucketing_key: str
        :param matching_key: Matching key for which to get the treatment
        :type matching_key: str
        :return: dictionary representing all matchers for each current feature flag
        :type: dict
        """
        segment_matchers = await self._get_segment_matchers_async(feature_flag, matching_key)
        return EvaluationDataContext(feature_flag, await self._get_evaluation_contexts_async(feature_flag, bucketing_key, matching_key, segment_matchers, attributes))

    async def _get_evaluation_contexts_async(self, feature_flag, bucketing_key, matching_key, segment_matchers, attributes=None):
        """
        Calculate and store all condition matchers for given feature flag for async calls
        If there are dependent Feature Flag(s), the function will do recursive calls until all matchers are resolved.

        :param feature_flag: Feature flag Split objects
        :type feature_flag: splitio.models.splits.Split
        :param bucketing_key: Bucketing key for which to get the treatment
        :type bucketing_key: str
        :param matching_key: Matching key for which to get the treatment
        :type matching_key: str
        :param segment_matchers: Segment matchers for the feature flag
        :type segment_matchers: dict
        :return: dictionary representing all matchers for each current feature flag
        :type: dict
        """
        roll_out = False
        context = {
            'segment_matchers': segment_matchers,
            'evaluator': self._evaluator,
            'bucketing_key': bucketing_key,
        }
        evaluation_contexts = []
        for condition in feature_flag.conditions:
            if (not roll_out and
                    condition.condition_type == ConditionType.ROLLOUT):
                if feature_flag.traffic_allocation < 100:
                    bucket = self._splitter.get_bucket(
                        bucketing_key,
                        feature_flag.traffic_allocation_seed,
                        feature_flag.algo
                    )
                    if bucket > feature_flag.traffic_allocation:
                        return feature_flag.default_treatment, Label.NOT_IN_SPLIT
                roll_out = True
            dependent_feature_flags = []
            for matcher in condition.matchers:
                if isinstance(matcher, DependencyMatcher):
                    dependent_feature_flag = await self._feature_flag_storage.get(matcher.to_json()['dependencyMatcherData']['split'])
                    depenedent_segment_matchers = await self._get_segment_matchers_async(dependent_feature_flag, matching_key)
                    dependent_feature_flags.append((dependent_feature_flag,
                                                   await self._get_evaluation_contexts_async(dependent_feature_flag, bucketing_key, matching_key, depenedent_segment_matchers, attributes)))
            context['dependent_splits'] = dependent_feature_flags
            evaluation_contexts.append((condition.matches(
                matching_key,
                attributes=attributes,
                context=context
            ), condition))

        return evaluation_contexts

    async def _get_segment_matchers_async(self, feature_flag, matching_key):
        """
        Get all segments matchers for given feature flag for async calls
        If there are dependent Feature Flag(s), the function will do recursive calls until all matchers are resolved.

        :param feature_flag: Feature flag Split objects
        :type feature_flag: splitio.models.splits.Split
        :param matching_key: Matching key for which to get the treatment
        :type matching_key: str
        :return: Segment matchers for the feature flag
        :type: dict
        """
        segment_matchers = {}
        for segment in self._get_segment_names(feature_flag):
            for condition in feature_flag.conditions:
                for matcher in condition.matchers:
                    if isinstance(matcher, matchers.UserDefinedSegmentMatcher):
                        segment_matchers[segment] = await self._segment_storage.segment_contains(segment, matching_key)
        return segment_matchers
