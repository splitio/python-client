"""A module for Split.io SDK API clients."""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import logging
import time
from splitio.treatments import CONTROL
from splitio.splitters import Splitter
from splitio.impressions import Impression, Label, ImpressionListenerException
from splitio.metrics import SDK_GET_TREATMENT, SDK_GET_TREATMENTS
from splitio.events import Event
from . import input_validator
from splitio.evaluator import Evaluator


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
        self._evaluator = Evaluator(broker)

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
                self._logger.error(e)

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
            self._logger.error("Client has already been destroyed - no calls possible")
            return CONTROL

        start = int(round(time.time() * 1000))

        matching_key, bucketing_key = input_validator.validate_key(key, 'get_treatment')
        feature = input_validator.validate_feature_name(feature)

        if (matching_key is None and bucketing_key is None) or feature is None or\
           input_validator.validate_attributes(attributes, 'get_treatment') is False:
            return CONTROL

        try:
            result = self._evaluator.evaluate_treatment(
                feature,
                matching_key,
                bucketing_key,
                attributes
            )

            impression = self._build_impression(matching_key,
                                                feature,
                                                result['treatment'],
                                                result['impression']['label'],
                                                result['impression']['change_number'],
                                                bucketing_key,
                                                start)

            self._record_stats(impression, start, SDK_GET_TREATMENT)

            self._send_impression_to_listener(impression, attributes)

            return result['treatment']
        except Exception:
            self._logger.error('Error getting treatment for feature')

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
                self._logger.error('Error reporting impression into get_treatment exception block')
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
            self._logger.error("Client has already been destroyed - no calls possible")
            return input_validator.generate_control_treatments(features)

        start = int(round(time.time() * 1000))

        matching_key, bucketing_key = input_validator.validate_key(key, 'get_treatments')
        if matching_key is None and bucketing_key is None:
            return input_validator.generate_control_treatments(features)

        if input_validator.validate_attributes(attributes, 'get_treatment') is False:
            return input_validator.generate_control_treatments(features)

        features = input_validator.validate_features_get_treatments(features)
        if features is None:
            return {}

        bulk_impressions = []
        treatments = {}

        for feature in features:
            try:
                treatment = self._evaluator.evaluate_treatment(
                    feature,
                    matching_key,
                    bucketing_key,
                    attributes
                )

                impression = self._build_impression(matching_key,
                                                    feature,
                                                    treatment['treatment'],
                                                    treatment['impression']['label'],
                                                    treatment['impression']['change_number'],
                                                    bucketing_key,
                                                    start)

                bulk_impressions.append(impression)
                treatments[feature] = treatment['treatment']

            except Exception:
                self._logger.error('get_treatments: An exception occured when evaluating '
                                   'feature ' + feature + ' returning CONTROL.')
                treatments[feature] = CONTROL
                continue

            # Register impressions
            try:
                if len(bulk_impressions) > 0:
                    self._record_stats(bulk_impressions, start, SDK_GET_TREATMENTS)

                    for impression in bulk_impressions:
                        self._send_impression_to_listener(impression, attributes)
            except Exception:
                self._logger.error('get_treatments: An exception when trying to store '
                                   'impressions.')

        return treatments

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

    def _record_stats(self, impressions, start, operation):
        """
        Record impressions and metrics.

        :param impressions: Generated impressions
        :type impressions: list||Impression

        :param start: timestamp when get_treatment or get_treatments was called
        :type start: int

        :param operation: operation performed.
        :type operation: str
        """
        try:
            end = int(round(time.time() * 1000))
            if operation == SDK_GET_TREATMENT:
                self._broker.log_impressions([impressions])
            else:
                self._broker.log_impressions(impressions)
            self._broker.log_operation_time(operation, end - start)
        except Exception:
            self._logger.error('Error recording impressions and metrics')

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
        if self._destroyed:
            self._logger.error("Client has already been destroyed - no calls possible")
            return False

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
