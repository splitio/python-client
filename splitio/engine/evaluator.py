"""Split evaluator module."""
import logging
from splitio.models.grammar.condition import ConditionType
from splitio.models.impressions import Label


CONTROL = 'control'


_LOGGER = logging.getLogger(__name__)


class Evaluator(object):  # pylint: disable=too-few-public-methods
    """Split Evaluator class."""

    def __init__(self, feature_flag_storage, segment_storage, splitter):
        """
        Construct a Evaluator instance.

        :param feature_flag_storage: feature_flag storage.
        :type feature_flag_storage: splitio.storage.SplitStorage

        :param segment_storage: Segment storage.
        :type segment_storage: splitio.storage.SegmentStorage
        """
        self._feature_flag_storage = feature_flag_storage
        self._segment_storage = segment_storage
        self._splitter = splitter

    def _evaluate_treatment(self, feature_flag_name, matching_key, bucketing_key, attributes, feature_flag):
        """
        Evaluate the user submitted data against a feature and return the resulting treatment.

        :param feature_flag_name: The feature flag for which to get the treatment
        :type feature:  str

        :param matching_key: The matching_key for which to get the treatment
        :type matching_key: str

        :param bucketing_key: The bucketing_key for which to get the treatment
        :type bucketing_key: str

        :param attributes: An optional dictionary of attributes
        :type attributes: dict

        :param feature_flag: Split object
        :type attributes: splitio.models.splits.Split|None

        :return: The treatment for the key and feature flag
        :rtype: object
        """
        label = ''
        _treatment = CONTROL
        _change_number = -1

        if feature_flag is None:
            _LOGGER.warning('Unknown or invalid feature: %s', feature_flag_name)
            label = Label.SPLIT_NOT_FOUND
        else:
            _change_number = feature_flag.change_number
            if feature_flag.killed:
                label = Label.KILLED
                _treatment = feature_flag.default_treatment
            else:
                treatment, label = self._get_treatment_for_split(
                    feature_flag,
                    matching_key,
                    bucketing_key,
                    attributes
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

    def evaluate_feature(self, feature_flag_name, matching_key, bucketing_key, attributes=None):
        """
        Evaluate the user submitted data against a feature and return the resulting treatment.

        :param feature_flag_name: The feature flag for which to get the treatment
        :type feature:  str

        :param matching_key: The matching_key for which to get the treatment
        :type matching_key: str

        :param bucketing_key: The bucketing_key for which to get the treatment
        :type bucketing_key: str

        :param attributes: An optional dictionary of attributes
        :type attributes: dict

        :return: The treatment for the key and split
        :rtype: object
        """
        # Fetching Split definition
        feature_flag = self._feature_flag_storage.get(feature_flag_name)

        # Calling evaluation
        evaluation = self._evaluate_treatment(feature_flag_name, matching_key,
                                              bucketing_key, attributes, feature_flag)

        return evaluation

    def evaluate_features(self, feature_flag_names, matching_key, bucketing_key, attributes=None):
        """
        Evaluate the user submitted data against multiple features and return the resulting
        treatment.

        :param feature_flag_names: The feature flags for which to get the treatments
        :type feature:  list(str)

        :param matching_key: The matching_key for which to get the treatment
        :type matching_key: str

        :param bucketing_key: The bucketing_key for which to get the treatment
        :type bucketing_key: str

        :param attributes: An optional dictionary of attributes
        :type attributes: dict

        :return: The treatments for the key and feature flags
        :rtype: object
        """
        return {
            feature_flag_name: self._evaluate_treatment(feature_flag_name, matching_key,
                                              bucketing_key, attributes, feature_flag)
            for (feature_flag_name, feature_flag) in self._feature_flag_storage.fetch_many(feature_flag_names).items()
        }

    def _get_treatment_for_split(self, feature_flag, matching_key, bucketing_key, attributes=None):
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

        :param attributes: An optional dictionary of attributes
        :type attributes: dict

        :return: The resulting treatment and label
        :rtype: tuple
        """
        if bucketing_key is None:
            bucketing_key = matching_key

        roll_out = False

        context = {
            'segment_storage': self._segment_storage,
            'evaluator': self,
            'bucketing_key': bucketing_key
        }

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

            condition_matches = condition.matches(
                matching_key,
                attributes=attributes,
                context=context
            )

            if condition_matches:
                return self._splitter.get_treatment(
                    bucketing_key,
                    feature_flag.seed,
                    condition.partitions,
                    feature_flag.algo
                ), condition.label

        # No condition matches
        return None, None
