"""A module for Split.io SDK API clients."""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import logging
import time
import six
from splitio.engine.evaluator import Evaluator, CONTROL
from splitio.engine.splitters import Splitter
from splitio.models.impressions import Impression, Label
from splitio.models.events import Event, EventWrapper
from splitio.models.telemetry import get_latency_bucket_index
from splitio.client import input_validator
from splitio.client.listener import ImpressionListenerException


class Client(object):  # pylint: disable=too-many-instance-attributes
    """Entry point for the split sdk."""

    _METRIC_GET_TREATMENT = 'sdk.getTreatment'
    _METRIC_GET_TREATMENTS = 'sdk.getTreatments'
    _METRIC_GET_TREATMENT_WITH_CONFIG = 'sdk.getTreatmentWithConfig'
    _METRIC_GET_TREATMENTS_WITH_CONFIG = 'sdk.getTreatmentsWithConfig'

    def __init__(self, factory, labels_enabled=True, impression_listener=None):
        """
        Construct a Client instance.

        :param factory: Split factory (client & manager container)
        :type factory: splitio.client.factory.SplitFactory

        :param labels_enabled: Whether to store labels on impressions
        :type labels_enabled: bool

        :param impression_listener: impression listener implementation
        :type impression_listener: ImpressionListener

        :rtype: Client
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._factory = factory
        self._labels_enabled = labels_enabled
        self._impression_listener = impression_listener

        self._splitter = Splitter()
        self._split_storage = factory._get_storage('splits')  # pylint: disable=protected-access
        self._segment_storage = factory._get_storage('segments')  # pylint: disable=protected-access
        self._impressions_storage = factory._get_storage('impressions')  # pylint: disable=protected-access
        self._events_storage = factory._get_storage('events')  # pylint: disable=protected-access
        self._telemetry_storage = factory._get_storage('telemetry')  # pylint: disable=protected-access
        self._evaluator = Evaluator(self._split_storage, self._segment_storage, self._splitter)

    def destroy(self):
        """
        Destroy the underlying factory.

        Only applicable when using in-memory operation mode.
        """
        self._factory.destroy()

    @property
    def ready(self):
        """Return whether the SDK initialization has finished."""
        return self._factory.ready

    @property
    def destroyed(self):
        """Return whether the factory holding this client has been destroyed."""
        return self._factory.destroyed

    def _send_impression_to_listener(self, impression, attributes):
        """
        Send impression result to custom listener.

        :param impression: Generated impression
        :type impression: Impression

        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        """
        if self._impression_listener is not None:
            try:
                self._impression_listener.log_impression(impression, attributes)
            except ImpressionListenerException:
                self._logger.error(
                    'An exception was raised while calling user-custom impression listener'
                )
                self._logger.debug('Error', exc_info=True)

    def _evaluate_if_ready(self, matching_key, bucketing_key, feature, attributes=None):
        if not self.ready:
            return {
                'treatment': CONTROL,
                'configurations': None,
                'impression': {
                    'label': Label.NOT_READY,
                    'change_number': None
                }
            }

        return self._evaluator.evaluate_feature(
            feature,
            matching_key,
            bucketing_key,
            attributes
        )

    def _make_evaluation(self, key, feature, attributes, method_name, metric_name):
        try:
            if self.destroyed:
                self._logger.error("Client has already been destroyed - no calls possible")
                return CONTROL, None

            start = int(round(time.time() * 1000))

            matching_key, bucketing_key = input_validator.validate_key(key, method_name)
            feature = input_validator.validate_feature_name(
                feature,
                self.ready,
                self._factory._get_storage('splits'),  # pylint: disable=protected-access
                method_name
            )

            if (matching_key is None and bucketing_key is None) \
                    or feature is None \
                    or not input_validator.validate_attributes(attributes, method_name):
                return CONTROL, None

            result = self._evaluate_if_ready(matching_key, bucketing_key, feature, attributes)

            impression = self._build_impression(
                matching_key,
                feature,
                result['treatment'],
                result['impression']['label'],
                result['impression']['change_number'],
                bucketing_key,
                start
            )

            self._record_stats([impression], start, metric_name)
            self._send_impression_to_listener(impression, attributes)
            return result['treatment'], result['configurations']
        except Exception:  # pylint: disable=broad-except
            self._logger.error('Error getting treatment for feature')
            self._logger.debug('Error: ', exc_info=True)
            try:
                impression = self._build_impression(
                    matching_key,
                    feature,
                    CONTROL,
                    Label.EXCEPTION,
                    self._split_storage.get_change_number(),
                    bucketing_key,
                    start
                )
                self._record_stats([impression], start, metric_name)
                self._send_impression_to_listener(impression, attributes)
            except Exception:  # pylint: disable=broad-except
                self._logger.error('Error reporting impression into get_treatment exception block')
                self._logger.debug('Error: ', exc_info=True)
            return CONTROL, None

    def _make_evaluations(self, key, features, attributes, method_name, metric_name):
        if self.destroyed:
            self._logger.error("Client has already been destroyed - no calls possible")
            return input_validator.generate_control_treatments(features, method_name)

        start = int(round(time.time() * 1000))

        matching_key, bucketing_key = input_validator.validate_key(key, method_name)
        if matching_key is None and bucketing_key is None:
            return input_validator.generate_control_treatments(features, method_name)

        if input_validator.validate_attributes(attributes, method_name) is False:
            return input_validator.generate_control_treatments(features, method_name)

        features, missing = input_validator.validate_features_get_treatments(
            method_name,
            features,
            self.ready,
            self._factory._get_storage('splits')  # pylint: disable=protected-access
        )
        if features is None:
            return {}

        bulk_impressions = []
        treatments = {name: (CONTROL, None) for name in missing}

        try:
            evaluations = self._evaluate_features_if_ready(matching_key, bucketing_key,
                                                           list(features), attributes)

            for feature in features:
                try:
                    result = evaluations[feature]
                    impression = self._build_impression(matching_key,
                                                        feature,
                                                        result['treatment'],
                                                        result['impression']['label'],
                                                        result['impression']['change_number'],
                                                        bucketing_key,
                                                        start)

                    bulk_impressions.append(impression)
                    treatments[feature] = (result['treatment'], result['configurations'])

                except Exception:  # pylint: disable=broad-except
                    self._logger.error('%s: An exception occured when evaluating '
                                       'feature %s returning CONTROL.' % (method_name, feature))
                    treatments[feature] = CONTROL, None
                    self._logger.debug('Error: ', exc_info=True)
                    continue

            # Register impressions
            try:
                if bulk_impressions:
                    self._record_stats(bulk_impressions, start, self._METRIC_GET_TREATMENTS)
                    for impression in bulk_impressions:
                        self._send_impression_to_listener(impression, attributes)
            except Exception:  # pylint: disable=broad-except
                self._logger.error('%s: An exception when trying to store '
                                   'impressions.' % method_name)
                self._logger.debug('Error: ', exc_info=True)

            return treatments
        except Exception:  # pylint: disable=broad-except
            self._logger.error('Error getting treatment for features')
            self._logger.debug('Error: ', exc_info=True)
        return input_validator.generate_control_treatments(list(features), method_name)

    def _evaluate_features_if_ready(self, matching_key, bucketing_key, features, attributes=None):
        if not self.ready:
            return {
                feature: {
                    'treatment': CONTROL,
                    'configurations': None,
                    'impression': {'label': Label.NOT_READY, 'change_number': None}
                }
                for feature in features
            }

        return self._evaluator.evaluate_features(
            features,
            matching_key,
            bucketing_key,
            attributes
        )

    def get_treatment_with_config(self, key, feature, attributes=None):
        """
        Get the treatment and config for a feature and key, with optional dictionary of attributes.

        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.

        :param key: The key for which to get the treatment
        :type key: str
        :param feature: The name of the feature for which to get the treatment
        :type feature: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: The treatment for the key and feature
        :rtype: tuple(str, str)
        """
        return self._make_evaluation(key, feature, attributes, 'get_treatment_with_config',
                                     self._METRIC_GET_TREATMENT_WITH_CONFIG)

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
        treatment, _ = self._make_evaluation(key, feature, attributes, 'get_treatment',
                                             self._METRIC_GET_TREATMENT)
        return treatment

    def get_treatments_with_config(self, key, features, attributes=None):
        """
        Evaluate multiple features and return a dict with feature -> (treatment, config).

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
        return self._make_evaluations(key, features, attributes, 'get_treatments_with_config',
                                      self._METRIC_GET_TREATMENTS_WITH_CONFIG)

    def get_treatments(self, key, features, attributes=None):
        """
        Evaluate multiple features and return a dictionary with all the feature/treatments.

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
        with_config = self._make_evaluations(key, features, attributes, 'get_treatments',
                                             self._METRIC_GET_TREATMENTS)
        return {feature: result[0] for (feature, result) in six.iteritems(with_config)}

    def _build_impression(  # pylint: disable=too-many-arguments
            self,
            matching_key,
            feature_name,
            treatment,
            label,
            change_number,
            bucketing_key,
            imp_time
    ):
        """Build an impression."""
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
            self._impressions_storage.put(impressions)
            self._telemetry_storage.inc_latency(operation, get_latency_bucket_index(end - start))
        except Exception:  # pylint: disable=broad-except
            self._logger.error('Error recording impressions and metrics')
            self._logger.debug('Error: ', exc_info=True)

    def track(self, key, traffic_type, event_type, value=None, properties=None, timestamp=None): # pylint: disable=too-many-arguments
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
        :param properties: (Optional) properties associated to the event
        :type properties: dict
        :param timestamp: (Optional) Represents the time in milliseconds since epoch that the event
        occurred
        :type properties: int

        :return: Whether the event was created or not.
        :rtype: bool
        """
        if self.destroyed:
            self._logger.error("Client has already been destroyed - no calls possible")
            return False

        key = input_validator.validate_track_key(key)
        event_type = input_validator.validate_event_type(event_type)
        should_validate_existance = self.ready and self._factory._apikey != 'localhost'  # pylint: disable=protected-access
        traffic_type = input_validator.validate_traffic_type(
            traffic_type,
            should_validate_existance,
            self._factory._get_storage('splits'),  # pylint: disable=protected-access
        )

        value = input_validator.validate_value(value)
        valid, properties, size = input_validator.valid_properties(properties)
        timestamp = input_validator.validate_timestamp(timestamp)

        # pylint: disable=too-many-boolean-expressions
        if key is None or event_type is None or traffic_type is None or value is False \
           or valid is False or timestamp is False:
            return False

        if timestamp is None:
            timestamp = int(time.time()*1000)

        event = Event(
            key=key,
            traffic_type_name=traffic_type,
            event_type_id=event_type,
            value=value,
            timestamp=timestamp,
            properties=properties,
        )
        return self._events_storage.put([EventWrapper(
            event=event,
            size=size,
        )])
