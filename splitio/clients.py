"""A module for Split.io SDK API clients."""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import logging
import time
import re
from numbers import Number
from six import string_types
from splitio.treatments import CONTROL
from splitio.splitters import Splitter
from splitio.impressions import Impression, Label
from splitio.metrics import SDK_GET_TREATMENT
from splitio.splits import ConditionType
from splitio.events import Event

class Key(object):
    """Key class includes a matching key and bucketing key."""

    def __init__(self, matching_key, bucketing_key):
        """Construct a key object."""
        self._matching_key = matching_key
        self._bucketing_key = bucketing_key

    @property
    def matching_key(self):
        """Return matching key."""
        return self._matching_key

    @property
    def bucketing_key(self):
        """Return bucketing key."""
        return self._bucketing_key


class Client(object):
    """Client class that uses a broker for storage."""

    def __init__(self, broker, labels_enabled=True):
        """
        Construct a Client instance.

        :param broker: Broker that accepts/retrieves splits, segments, events, metrics & impressions
        :type broker: BaseBroker

        :param labels_enabled: Whether to store labels on impressions
        :type labels_enabled: bool

        :rtype: Client
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._splitter = Splitter()
        self._broker = broker
        self._labels_enabled = labels_enabled
        self._destroyed = False

    def _get_keys(self, key):
        """
        Parse received key.

        :param key: user submitted key
        :type key: mixed

        :rtype: tuple(string,string)
        """
        if isinstance(key, Key):
            matching_key = key.matching_key
            bucketing_key = key.bucketing_key
        else:
            if isinstance(key, string_types):
                matching_key = key
            elif isinstance(key, Number):
                self._logger.warning("Key received as Number. Converting to string")
                matching_key = str(key)
            else:
                # If the key is not a string, int or Key,
                # set keys to None in order to return CONTROL
                return None, None
            bucketing_key = None
        return matching_key, bucketing_key

    def destroy(self):
        """
        Disable the split-client and free all allocated resources.

        Only applicable when using in-memory operation mode.
        """
        self._destroyed = True
        self._broker.destroy()

    def _validate_input(self, key, feature, start, attributes=None):
        """
        Validate the user-supplied arguments. Return True if valid, False otherwhise.

        :param key: user key
        :type key: mixed

        :param feature: feature name
        :type feature: str

        :param attributes: custom user data
        :type attributes: dict

        :rtype: tuple
        """
        if feature is None:
            self._logger.error("Neither Key or FeatureName can be None")
            return None, None

        if not isinstance(feature, string_types):
            self._logger.error("feature name must be a string")
            return None, None

        if key is None:
            self._logger.error("Neither Key or FeatureName can be None")
            impression = self._build_impression("", feature, CONTROL, Label.EXCEPTION,
                                                0, None, start)
            self._record_stats(impression, start, SDK_GET_TREATMENT)
            return None, None


        matching_key, bucketing_key = self._get_keys(key)
        if matching_key is None and bucketing_key is None:
            self._logger.error(
                "getTreatment: Key should be an object with bucketingKey and matchingKey"
                "or a string."
            )
            return None, None

        return matching_key, bucketing_key



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

        matching_key, bucketing_key = self._validate_input(key, feature, start, attributes)
        if matching_key is None and bucketing_key is None:
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
            return _treatment
        except Exception: #pylint: disable=broad-except
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
            except Exception: #pylint: disable=broad-except
                self._logger.exception(
                    'Exception reporting impression into get_treatment exception block'
                )

            return CONTROL

    def _build_impression(
            self, matching_key, feature_name, treatment, label,
            change_number, bucketing_key, imp_time
    ):
        """
        Build an impression.

        TODO: REFACTOR THIS!
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
        :type operation: string
        """
        try:
            end = int(round(time.time() * 1000))
            self._broker.log_impression(impression)
            self._broker.log_operation_time(operation, end - start)
        except Exception: #pylint: disable=broad-except
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
                    if bucket >= split.traffic_allocation:
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
        :type key: string

        :param traffic_type: traffic type name
        :type traffic_type: string

        :param event_type: event type name
        :type event_type: string

        :param value: (Optional) value associated to the event
        :type value: Number

        :rtype: bool
        """
        if key is None:
            self._logger.error("Key cannot be None")
            return False

        if isinstance(key, Number):
            self._logger.warning("Key must be string. Number supplied. Converting")
            key = str(key)

        if not isinstance(key, string_types):
            self._logger.error("Incorrect type of key supplied. Must be string")
            return False

        if event_type is None:
            self._logger.warning("event_type cannot be None")
            return False

        if not isinstance(event_type, string_types):
            self._logger.error("event_name must be string")
            return False

        if not re.match(r'[a-zA-Z0-9][-_\.a-zA-Z0-9]{0,62}', event_type):
            self._logger.error(
                'event_type must match the regular expression "'
                r'[a-zA-Z0-9][-_\.a-zA-Z0-9]{0,62}"'
            )
            return False

        if traffic_type is None or not isinstance(traffic_type, string_types) or traffic_type == '':
            self._logger.error("traffic_type must be a non-empty string")
            return False

        if value is not None and not isinstance(value, Number):
            self._logger.error("value must be None or must be a number")
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

    def __init__(self, broker, splitter, logger): #pylint: disable=super-init-not-called
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
        if key is None or feature is None:
            return CONTROL

        matching_key, bucketing_key = self._get_keys(key)
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
        except Exception: #pylint: disable=broad-except
            self._logger.exception(
                'Exception caught retrieving dependent feature. Returning CONTROL'
            )
            return CONTROL
