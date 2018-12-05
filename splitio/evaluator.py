import logging
from splitio.splits import ConditionType
from splitio.impressions import Label
from splitio.splitters import Splitter
from splitio.key import Key
from splitio.treatments import CONTROL
from . import input_validator


class Evaluator(object):

    def __init__(self, broker):
        """
        Construct a Evaluator instance.

        :param broker: Broker that accepts/retrieves splits, segments, events, metrics & impressions
        :type broker: BaseBroker

        :rtype: Evaluator
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._splitter = Splitter()
        self._broker = broker

    def evaluate_treatment(self, feature, matching_key, bucketing_key, attributes=None):
        """
        Evaluates the user submitted data against a feature and return the resulting treatment.

        :param feature: The feature for which to get the treatment
        :type string: feature

        :param matching_key: The matching_key for which to get the treatment
        :type string: str

        :param bucketing_key: The bucketing_key for which to get the treatment
        :type string: str

        :param attributes: An optional dictionary of attributes
        :type attributes: dict

        :return: The treatment for the key and split
        :rtype: object
        """
        label = ''
        _treatment = CONTROL
        _change_number = -1

        # Fetching Split definition
        split = self._broker.fetch_feature(feature)

        if split is None:
            self._logger.warning('Unknown or invalid feature: %s', feature)
            label = Label.SPLIT_NOT_FOUND
            _treatment = CONTROL
        else:
            _change_number = split.change_number
            if split.killed:
                label = Label.KILLED
                _treatment = split.default_treatment
            else:
                treatment, label = self.get_treatment_for_split(
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
            'impression': {
                'label': label,
                'change_number': _change_number
            }
        }

    def get_treatment_for_split(self, split, matching_key, bucketing_key, attributes=None):
        """
        Evaluates the feature considering the conditions. If there is a match, it will return
        the condition and the label. Otherwise, it will return None, None

        :param split: The split for which to get the treatment
        :type split: Split

        :param matching_key: The key for which to get the treatment
        :type key: str

        :param bucketing_key: The key for which to get the treatment
        :type key: str

        :param attributes: An optional dictionary of attributes
        :type attributes: dict

        :return: The treatment for the key and split
        :rtype: object
        """
        if bucketing_key is None:
            bucketing_key = matching_key

        roll_out = False
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

            condition_matches = condition.matcher.match(
                Key(matching_key, bucketing_key),
                attributes=attributes,
                client=self
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

    def get_treatment(self, key, feature, attributes=None):
        """
        Evaluate a feature and return the appropriate traetment.
         Will not generate impressions nor metrics
         :param key: user key
        :type key: mixed
         :param feature: feature name
        :type feature: str
         :param attributes: (Optional) attributes associated with the user key
        :type attributes: dict
        """
        matching_key, bucketing_key = input_validator.validate_key(key)
        feature = input_validator.validate_feature_name(feature)
        if (matching_key is None and bucketing_key is None) or feature is None:
            return CONTROL
        try:
            # Fetching Split definition
            split = self._broker.fetch_feature(feature)
            if split is None:
                self._logger.warning(
                    'Unknown or invalid dependent feature: %s',
                    feature
                )
                return CONTROL
            if split.killed:
                return split.default_treatment
            treatment, _ = self.get_treatment_for_split(
                split,
                matching_key,
                bucketing_key,
                attributes
            )
            if treatment is None:
                return split.default_treatment
            return treatment
        except Exception:  # pylint: disable=broad-except
            self._logger.exception(
                'Exception caught retrieving dependent feature. Returning CONTROL'
            )
            return CONTROL
