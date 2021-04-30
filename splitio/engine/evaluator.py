"""Split evaluator module."""
import logging
from splitio.models.grammar.condition import ConditionType
from splitio.models.impressions import Label


CONTROL = 'control'


_LOGGER = logging.getLogger(__name__)


class Evaluator(object):  # pylint: disable=too-few-public-methods
    """Split Evaluator class."""

    def __init__(self, split_storage, segment_storage, splitter):
        """
        Construct a Evaluator instance.

        :param split_storage: Split storage.
        :type split_storage: splitio.storage.SplitStorage

        :param split_storage: Storage storage.
        :type split_storage: splitio.storage.SegmentStorage
        """
        self._split_storage = split_storage
        self._segment_storage = segment_storage
        self._splitter = splitter

    def _evaluate_treatment(self, feature, matching_key, bucketing_key, attributes, split):
        """
        Evaluate the user submitted data against a feature and return the resulting treatment.

        :param feature: The feature for which to get the treatment
        :type feature:  str

        :param matching_key: The matching_key for which to get the treatment
        :type matching_key: str

        :param bucketing_key: The bucketing_key for which to get the treatment
        :type bucketing_key: str

        :param attributes: An optional dictionary of attributes
        :type attributes: dict

        :param split: Split object
        :type attributes: splitio.models.splits.Split|None

        :return: The treatment for the key and split
        :rtype: object
        """
        label = ''
        _treatment = CONTROL
        _change_number = -1

        if split is None:
            _LOGGER.warning('Unknown or invalid feature: %s', feature)
            label = Label.SPLIT_NOT_FOUND
        else:
            _change_number = split.change_number
            if split.killed:
                label = Label.KILLED
                _treatment = split.default_treatment
            else:
                treatment, label = self._get_treatment_for_split(
                    split,
                    matching_key,
                    bucketing_key,
                    attributes
                )
                if treatment is None:
                    label = Label.NO_CONDITION_MATCHED
                    _treatment = split.default_treatment
                else:
                    _treatment = treatment

        return {
            'treatment': _treatment,
            'configurations': split.get_configurations_for(_treatment) if split else None,
            'impression': {
                'label': label,
                'change_number': _change_number
            }
        }

    def evaluate_feature(self, feature, matching_key, bucketing_key, attributes=None):
        """
        Evaluate the user submitted data against a feature and return the resulting treatment.

        :param feature: The feature for which to get the treatment
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
        split = self._split_storage.get(feature)

        # Calling evaluation
        evaluation = self._evaluate_treatment(feature, matching_key,
                                              bucketing_key, attributes, split)

        return evaluation

    def evaluate_features(self, features, matching_key, bucketing_key, attributes=None):
        """
        Evaluate the user submitted data against multiple features and return the resulting
        treatment.

        :param features: The features for which to get the treatments
        :type feature:  list(str)

        :param matching_key: The matching_key for which to get the treatment
        :type matching_key: str

        :param bucketing_key: The bucketing_key for which to get the treatment
        :type bucketing_key: str

        :param attributes: An optional dictionary of attributes
        :type attributes: dict

        :return: The treatments for the key and splits
        :rtype: object
        """
        return {
            feature: self._evaluate_treatment(feature, matching_key,
                                              bucketing_key, attributes, split)
            for (feature, split) in self._split_storage.fetch_many(features).items()
        }

    def _get_treatment_for_split(self, split, matching_key, bucketing_key, attributes=None):
        """
        Evaluate the feature considering the conditions.

        If there is a match, it will return the condition and the label.
        Otherwise, it will return (None, None)

        :param split: The split for which to get the treatment
        :type split: Split

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

        for condition in split.conditions:
            if (not roll_out and
                    condition.condition_type == ConditionType.ROLLOUT):
                if split.traffic_allocation < 100:
                    bucket = self._splitter.get_bucket(
                        bucketing_key,
                        split.traffic_allocation_seed,
                        split.algo
                    )
                    if bucket > split.traffic_allocation:
                        return split.default_treatment, Label.NOT_IN_SPLIT
                roll_out = True

            condition_matches = condition.matches(
                matching_key,
                attributes=attributes,
                context=context
            )

            if condition_matches:
                return self._splitter.get_treatment(
                    bucketing_key,
                    split.seed,
                    condition.partitions,
                    split.algo
                ), condition.label

        # No condition matches
        return None, None
