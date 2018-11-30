"""A module for Split.io SDK API clients."""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import logging
import time
from splitio.treatments import CONTROL
from splitio.splitters import Splitter
from splitio.impressions import Impression, Label, ImpressionListenerException
from splitio.metrics import SDK_GET_TREATMENT
from splitio.splits import ConditionType
from splitio.events import Event
from . import input_validator
from splitio.key import Key


class Client(object):
    """Client class that uses a broker for storage."""

    def __init__(self, broker, labels_enabled=True, impression_listener=None):
        """
        Construct a Client instance.

        :param broker: Broker that accepts/retrieves splits, segments, events, metrics & impressions
        :type broker: BaseBroker

        :param labels_enabled: Whether to store labels on impressions
        :type labels_enabled: bool

        :param impression_listener: impression listener implementation
        :type impression_listener: ImpressionListener

        :rtype: Client
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._splitter = Splitter()
        self._broker = broker
        self._labels_enabled = labels_enabled
        self._destroyed = False
        self._impression_listener = impression_listener

    def destroy(self):
        """
        Disable the split-client and free all allocated resources.

        Only applicable when using in-memory operation mode.
        """
        self._destroyed = True
        self._broker.destroy()

    def _send_impression_to_listener(self, impression, attributes):
        '''
        Sends impression result to custom listener.

        :param impression: Generated impression
        :type impression: Impression

        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        '''
        if self._impression_listener is not None:
            try:
                self._impression_listener.log_impression(impression, attributes)
            except ImpressionListenerException as e:
                self._logger.exception(e)

    def get_treatment(self, key, feature, attributes=None):
        """
        Get the treatment for a feature and key, with an optional dictionary of attributes.

        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.

        :param key: The key for which to get the treatment
        :type key: str
        :param feature: The name of the feature for which to get the treatment
        :type feature: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: The treatment for the key and feature
        :rtype: str
        """
        if self._destroyed:
            self._logger.warning("Client has already been destroyed, returning CONTROL")
            return CONTROL

        start = int(round(time.time() * 1000))

        matching_key, bucketing_key = input_validator.validate_key(key)
        feature = input_validator.validate_feature_name(feature)

        if (matching_key is None and bucketing_key is None) or feature is None:
            impression = self._build_impression(matching_key, feature, CONTROL, Label.EXCEPTION,
                                                0, bucketing_key, start)
            self._record_stats(impression, start, SDK_GET_TREATMENT)
            return CONTROL

        try:
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

            impression = self._build_impression(matching_key, feature, _treatment, label,
                                                _change_number, bucketing_key, start)
            self._record_stats(impression, start, SDK_GET_TREATMENT)

            self._send_impression_to_listener(impression, attributes)

            return _treatment
        except Exception:  # pylint: disable=broad-except
            self._logger.exception('Exception caught getting treatment for feature')

            try:
                impression = self._build_impression(
                    matching_key,
                    feature,
                    CONTROL,
                    Label.EXCEPTION,
                    self._broker.get_change_number(), bucketing_key, start
                )
                self._record_stats(impression, start, SDK_GET_TREATMENT)

                self._send_impression_to_listener(impression, attributes)
            except Exception:  # pylint: disable=broad-except
                self._logger.exception(
                    'Exception reporting impression into get_treatment exception block'
                )
            return CONTROL

    def get_treatments(self, key, features, attributes=None):
        """
        Get the treatments for a list of features considering a key, with an optional dictionary of
        attributes. This method never raises an exception. If there's a problem, the appropriate
        log message will be generated and the method will return the CONTROL treatment.
        :param key: The key for which to get the treatment
        :type key: str
        :param features: Array of the names of the features for which to get the treatment
        :type feature: list
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: Dictionary with the result of all the features provided
        :rtype: dict
        """
        if self._destroyed:
            self._logger.warning("Client has already been destroyed, returning None")
            return None

        features = input_validator.validate_features_get_treatments(features)

        if features is None:
            return None

        return {feature: self.get_treatment(key, feature, attributes) for feature in features}

    def _build_impression(
            self, matching_key, feature_name, treatment, label,
            change_number, bucketing_key, imp_time
    ):
        """
        Build an impression.
        """
        if not self._labels_enabled:
            label = None

        return Impression(
            matching_key=matching_key, feature_name=feature_name,
            treatment=treatment, label=label, change_number=change_number,
            bucketing_key=bucketing_key, time=imp_time
        )

    def _record_stats(self, impression, start, operation):
        """
        Record impression and metrics.

        :param impression: Generated impression
        :type impression: Impression

        :param start: timestamp when get_treatment was called
        :type start: int

        :param operation: operation performed.
        :type operation: str
        """
        try:
            end = int(round(time.time() * 1000))
            self._broker.log_impression(impression)
            self._broker.log_operation_time(operation, end - start)
        except Exception:  # pylint: disable=broad-except
            self._logger.exception('Exception caught recording impressions and metrics')

    def _get_treatment_for_split(self, split, matching_key, bucketing_key, attributes=None):
        """
        Evaluate the user submitted data againt a feature and return the resulting treatment.

        This method might raise exceptions and should never be used directly.
        :param split: The split for which to get the treatment
        :type split: Split

        :param key: The key for which to get the treatment
        :type key: str

        :param attributes: An optional dictionary of attributes
        :type attributes: dict

        :return: The treatment for the key and split
        :rtype: str
        """
        if bucketing_key is None:
            bucketing_key = matching_key

        matcher_client = MatcherClient(self._broker, self._splitter, self._logger)

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
                client=matcher_client
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

    def track(self, key, traffic_type, event_type, value=None):
        """
        Track an event.

        :param key: user key associated to the event
        :type key: str

        :param traffic_type: traffic type name
        :type traffic_type: str

        :param event_type: event type name
        :type event_type: str

        :param value: (Optional) value associated to the event
        :type value: Number

        :rtype: bool
        """
        key = input_validator.validate_track_key(key)
        event_type = input_validator.validate_event_type(event_type)
        traffic_type = input_validator.validate_traffic_type(traffic_type)
        value = input_validator.validate_value(value)

        if key is None or event_type is None or traffic_type is None or value is False:
            return False

        event = Event(
            key=key,
            trafficTypeName=traffic_type,
            eventTypeId=event_type,
            value=value,
            timestamp=int(time.time()*1000)
        )
        return self._broker.get_events_log().log_event(event)


class MatcherClient(Client):
    """
    Client to be used by matchers such as "Dependency Matcher".

    TODO: Refactor This!
    """

    def __init__(self, broker, splitter, logger):  # pylint: disable=super-init-not-called
        """
        Construct a MatcherClient instance.

        :param broker: Broker where splits & segments will be fetched.
        :type broker: BaseBroker

        :param splitter: splitter
        :type splitter: Splitter

        :param logger: logger object
        :type logger: logging.Logger
        """
        self._broker = broker
        self._splitter = splitter
        self._logger = logger

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

            treatment, _ = self._get_treatment_for_split(
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
