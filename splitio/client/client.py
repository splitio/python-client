"""A module for Split.io SDK API clients."""
import logging

from splitio.engine.evaluator import Evaluator, CONTROL
from splitio.engine.splitters import Splitter
from splitio.models.impressions import Impression, Label
from splitio.models.events import Event, EventWrapper
from splitio.models.telemetry import get_latency_bucket_index, MethodExceptionsAndLatencies
from splitio.client import input_validator
from splitio.util.time import get_current_epoch_time_ms, utctime_ms

_LOGGER = logging.getLogger(__name__)


class Client(object):  # pylint: disable=too-many-instance-attributes
    """Entry point for the split sdk."""

    def __init__(self, factory, recorder, labels_enabled=True):
        """
        Construct a Client instance.

        :param factory: Split factory (client & manager container)
        :type factory: splitio.client.factory.SplitFactory

        :param labels_enabled: Whether to store labels on impressions
        :type labels_enabled: bool

        :param recorder: recorder instance
        :type recorder: splitio.recorder.StatsRecorder

        :rtype: Client
        """
        self._factory = factory
        self._labels_enabled = labels_enabled
        self._recorder = recorder
        self._splitter = Splitter()
        self._split_storage = factory._get_storage('splits')  # pylint: disable=protected-access
        self._segment_storage = factory._get_storage('segments')  # pylint: disable=protected-access
        self._events_storage = factory._get_storage('events')  # pylint: disable=protected-access
        self._evaluator = Evaluator(self._split_storage, self._segment_storage, self._splitter)
        self._telemetry_evaluation_producer = self._factory._telemetry_evaluation_producer
        self._telemetry_init_producer = self._factory._telemetry_init_producer

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

    def _evaluate_if_ready(self, matching_key, bucketing_key, feature, attributes=None):
        if not self.ready:
            self._telemetry_init_producer.record_not_ready_usage()
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
                _LOGGER.error("Client has already been destroyed - no calls possible")
                return CONTROL, None
            if self._factory._waiting_fork():
                _LOGGER.error("Client is not ready - no calls possible")
                return CONTROL, None

            start = get_current_epoch_time_ms()

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
                utctime_ms(),
            )
            self._record_stats([(impression, attributes)], start, metric_name, method_name)
            return result['treatment'], result['configurations']
        except Exception as e:  # pylint: disable=broad-except
            _LOGGER.error('Error getting treatment for feature flag')
            _LOGGER.error(str(e))
            _LOGGER.debug('Error: ', exc_info=True)
            self._telemetry_evaluation_producer.record_exception(metric_name)
            try:
                impression = self._build_impression(
                    matching_key,
                    feature,
                    CONTROL,
                    Label.EXCEPTION,
                    self._split_storage.get_change_number(),
                    bucketing_key,
                    utctime_ms(),
                )
                self._record_stats([(impression, attributes)], start, metric_name)
            except Exception:  # pylint: disable=broad-except
                _LOGGER.error('Error reporting impression into get_treatment exception block')
                _LOGGER.debug('Error: ', exc_info=True)
            return CONTROL, None

    def _make_evaluations(self, key, features, attributes, method_name, metric_name):
        if self.destroyed:
            _LOGGER.error("Client has already been destroyed - no calls possible")
            return input_validator.generate_control_treatments(features, method_name)
        if self._factory._waiting_fork():
            _LOGGER.error("Client is not ready - no calls possible")
            return input_validator.generate_control_treatments(features, method_name)

        start = get_current_epoch_time_ms()

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
                                                        utctime_ms())

                    bulk_impressions.append(impression)
                    treatments[feature] = (result['treatment'], result['configurations'])

                except Exception:  # pylint: disable=broad-except
                    _LOGGER.error('%s: An exception occured when evaluating '
                                  'feature flag %s returning CONTROL.' % (method_name, feature))
                    treatments[feature] = CONTROL, None
                    _LOGGER.debug('Error: ', exc_info=True)
                    continue

            # Register impressions
            try:
                if bulk_impressions:
                    self._record_stats(
                        [(i, attributes) for i in bulk_impressions],
                        start,
                        metric_name,
                        method_name
                    )
            except Exception:  # pylint: disable=broad-except
                _LOGGER.error('%s: An exception when trying to store '
                              'impressions.' % method_name)
                _LOGGER.debug('Error: ', exc_info=True)
                self._telemetry_evaluation_producer.record_exception(metric_name)

            return treatments
        except Exception:  # pylint: disable=broad-except
            self._telemetry_evaluation_producer.record_exception(metric_name)
            _LOGGER.error('Error getting treatment for feature flagss')
            _LOGGER.debug('Error: ', exc_info=True)
        return input_validator.generate_control_treatments(list(features), method_name)

    def _evaluate_features_if_ready(self, matching_key, bucketing_key, features, attributes=None):
        if not self.ready:
            self._telemetry_init_producer.record_not_ready_usage()
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
        Get the treatment and config for a feature flag and key, with optional dictionary of attributes.

        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.

        :param key: The key for which to get the treatment
        :type key: str
        :param feature: The name of the feature flag for which to get the treatment
        :type feature: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: The treatment for the key and feature flag
        :rtype: tuple(str, str)
        """
        return self._make_evaluation(key, feature, attributes, 'get_treatment_with_config',
                                     MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG)

    def get_treatment(self, key, feature, attributes=None):
        """
        Get the treatment for a feature flag and key, with an optional dictionary of attributes.

        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.

        :param key: The key for which to get the treatment
        :type key: str
        :param feature: The name of the feature flag for which to get the treatment
        :type feature: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: The treatment for the key and feature flag
        :rtype: str
        """
        treatment, _ = self._make_evaluation(key, feature, attributes, 'get_treatment',
                                             MethodExceptionsAndLatencies.TREATMENT)
        return treatment

    def get_treatments_with_config(self, key, features, attributes=None):
        """
        Evaluate multiple feature flags and return a dict with feature flag -> (treatment, config).

        Get the treatments for a list of feature flags considering a key, with an optional dictionary of
        attributes. This method never raises an exception. If there's a problem, the appropriate
        log message will be generated and the method will return the CONTROL treatment.
        :param key: The key for which to get the treatment
        :type key: str
        :param features: Array of the names of the feature flags for which to get the treatment
        :type feature: list
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: Dictionary with the result of all the feature flags provided
        :rtype: dict
        """
        return self._make_evaluations(key, features, attributes, 'get_treatments_with_config',
                                      MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG)

    def get_treatments(self, key, features, attributes=None):
        """
        Evaluate multiple feature flags and return a dictionary with all the feature flag/treatments.

        Get the treatments for a list of feature flags considering a key, with an optional dictionary of
        attributes. This method never raises an exception. If there's a problem, the appropriate
        log message will be generated and the method will return the CONTROL treatment.
        :param key: The key for which to get the treatment
        :type key: str
        :param features: Array of the names of the feature flags for which to get the treatment
        :type feature: list
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: Dictionary with the result of all the feature flags provided
        :rtype: dict
        """
        with_config = self._make_evaluations(key, features, attributes, 'get_treatments',
                                             MethodExceptionsAndLatencies.TREATMENTS)
        return {feature: result[0] for (feature, result) in with_config.items()}

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

    def _record_stats(self, impressions, start, operation, method_name=None):
        """
        Record impressions.

        :param impressions: Generated impressions
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]

        :param start: timestamp when get_treatment or get_treatments was called
        :type start: int

        :param operation: operation performed.
        :type operation: str
        """
        end = get_current_epoch_time_ms()
        self._recorder.record_treatment_stats(impressions, get_latency_bucket_index(end - start),
                                              operation, method_name)

    def track(self, key, traffic_type, event_type, value=None, properties=None):
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

        :return: Whether the event was created or not.
        :rtype: bool
        """
        if self.destroyed:
            _LOGGER.error("Client has already been destroyed - no calls possible")
            return False
        if self._factory._waiting_fork():
            _LOGGER.error("Client is not ready - no calls possible")
            return False
        if not self.ready:
            _LOGGER.warning("track: the SDK is not ready, results may be incorrect. Make sure to wait for SDK readiness before using this method")
            self._telemetry_init_producer.record_not_ready_usage()

        start = get_current_epoch_time_ms()
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

        if key is None or event_type is None or traffic_type is None or value is False \
           or valid is False:
            return False

        event = Event(
            key=key,
            traffic_type_name=traffic_type,
            event_type_id=event_type,
            value=value,
            timestamp=utctime_ms(),
            properties=properties,
        )

        try:
            return_flag = self._recorder.record_track_stats([EventWrapper(
                event=event,
                size=size,
            )], get_latency_bucket_index(get_current_epoch_time_ms() - start))
            return return_flag
        except Exception:  # pylint: disable=broad-except
            self._telemetry_evaluation_producer.record_exception(MethodExceptionsAndLatencies.TRACK)
            _LOGGER.error('Error processing track event')
            _LOGGER.debug('Error: ', exc_info=True)
            return False
