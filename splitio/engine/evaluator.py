"""Split evaluator module."""
import logging
from splitio.models.impressions import Label


CONTROL = 'control'


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

    def _evaluate_treatment(self, feature_flag, matching_key, bucketing_key, condition_matchers):
        """
        Evaluate the user submitted data against a feature and return the resulting treatment.

        :param feature_flag: Split object
        :type feature_flag: splitio.models.splits.Split|None

        :param matching_key: The matching_key for which to get the treatment
        :type matching_key: str

        :param bucketing_key: The bucketing_key for which to get the treatment
        :type bucketing_key: str

        :param condition_matchers: array of condition matchers for passed feature_flag
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
                    condition_matchers
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

    def evaluate_feature(self, feature_flag, matching_key, bucketing_key, condition_matchers):
        """
        Evaluate the user submitted data against a feature and return the resulting treatment.

        :param feature_flag: Split object
        :type feature_flag: splitio.models.splits.Split|None

        :param matching_key: The matching_key for which to get the treatment
        :type matching_key: str

        :param bucketing_key: The bucketing_key for which to get the treatment
        :type bucketing_key: str

        :param condition_matchers: array of condition matchers for passed feature_flag
        :type bucketing_key: Dict

        :return: The treatment for the key and split
        :rtype: object
        """
        # Calling evaluation
        evaluation = self._evaluate_treatment(feature_flag, matching_key,
                                              bucketing_key, condition_matchers)

        return evaluation

    def evaluate_features(self, feature_flags, matching_key, bucketing_key, condition_matchers):
        """
        Evaluate the user submitted data against multiple features and return the resulting
        treatment.

        :param feature_flags: array of Split objects
        :type feature_flags: [splitio.models.splits.Split|None]

        :param matching_key: The matching_key for which to get the treatment
        :type matching_key: str

        :param bucketing_key: The bucketing_key for which to get the treatment
        :type bucketing_key: str

        :param condition_matchers: array of condition matchers for passed feature_flag
        :type bucketing_key: Dict

        :return: The treatments for the key and feature flags
        :rtype: object
        """
        return {
            feature_flag.name: self._evaluate_treatment(feature_flag, matching_key,
                                              bucketing_key, condition_matchers[feature_flag.name])
            for (feature_flag) in feature_flags
        }

    def _get_treatment_for_feature_flag(self, feature_flag, matching_key, bucketing_key, condition_matchers):
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

        :param condition_matchers: array of condition matchers for passed feature_flag
        :type bucketing_key: Dict

        :return: The resulting treatment and label
        :rtype: tuple
        """
        if bucketing_key is None:
            bucketing_key = matching_key

        for condition_matcher, condition in condition_matchers:
            if condition_matcher:
                return self._splitter.get_treatment(
                    bucketing_key,
                    feature_flag.seed,
                    condition.partitions,
                    feature_flag.algo
                ), condition.label

        # No condition matches
        return None, None
