"""A module for Split.io SDK API clients."""
import logging

from splitio.engine.evaluator import Evaluator, CONTROL, EvaluationDataFactory, AsyncEvaluationDataFactory
from splitio.engine.splitters import Splitter
from splitio.models.impressions import Impression, Label
from splitio.models.events import Event, EventWrapper
from splitio.models.telemetry import get_latency_bucket_index, MethodExceptionsAndLatencies
from splitio.client import input_validator
from splitio.util.time import get_current_epoch_time_ms, utctime_ms


_LOGGER = logging.getLogger(__name__)


class ClientBase(object):  # pylint: disable=too-many-instance-attributes
    """Entry point for the split sdk."""

    _FAILED_EVAL_RESULT = {
        'treatment': CONTROL,
        'configurations': None,
        'impression': {
            'label': Label.EXCEPTION,
            'change_number': None,
        }
    }

    _NON_READY_EVAL_RESULT = {
        'treatment': CONTROL,
        'configurations': None,
        'impression': {
            'label': Label.NOT_READY,
            'change_number': None
        }
    }

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
        self._feature_flag_storage = factory._get_storage('splits')  # pylint: disable=protected-access
        self._segment_storage = factory._get_storage('segments')  # pylint: disable=protected-access
        self._events_storage = factory._get_storage('events')  # pylint: disable=protected-access
        self._evaluator = Evaluator(self._splitter)
        self._telemetry_evaluation_producer = self._factory._telemetry_evaluation_producer
        self._telemetry_init_producer = self._factory._telemetry_init_producer

    @property
    def ready(self):
        """Return whether the SDK initialization has finished."""
        return self._factory.ready

    @property
    def destroyed(self):
        """Return whether the factory holding this client has been destroyed."""
        return self._factory.destroyed

    def _client_is_usable(self):
        if self.destroyed:
            _LOGGER.error("Client has already been destroyed - no calls possible")
            return False

        if self._factory._waiting_fork():
            _LOGGER.error("Client is not ready - no calls possible")
            return False

        return True

    @staticmethod
    def _validate_treatment_input(key, feature, attributes, method):
        """Perform all static validations on user supplied input."""
        matching_key, bucketing_key = input_validator.validate_key(key, 'get_' + method.value)
        if not matching_key:
            raise _InvalidInputError()

        feature = input_validator.validate_feature_flag_name(feature, 'get_' + method.value)
        if not feature:
            raise _InvalidInputError()

        if not input_validator.validate_attributes(attributes, 'get_' + method.value):
            raise _InvalidInputError()

        return matching_key, bucketing_key, feature, attributes

    @staticmethod
    def _validate_treatments_input(key, features, attributes, method):
        """Perform all static validations on user supplied input."""
        matching_key, bucketing_key = input_validator.validate_key(key, 'get_' + method.value)
        if not matching_key:
            raise _InvalidInputError()

        features = input_validator.validate_feature_flags_get_treatments('get_' + method.value, features)
        if not features:
            raise _InvalidInputError()

        if not input_validator.validate_attributes(attributes, method):
            raise _InvalidInputError()

        return matching_key, bucketing_key, features, attributes


    def _build_impression(self, key, bucketing, feature, result):
        """Build an impression based on evaluation data & it's result."""
        return Impression(
                matching_key=key,
                feature_name=feature,
                treatment=result['treatment'],
                label=result['impression']['label'] if self._labels_enabled else None,
                change_number=result['impression']['change_number'],
                bucketing_key=bucketing,
                time=utctime_ms())

    def _build_impressions(self, key, bucketing, results):
        """Build an impression based on evaluation data & it's result."""
        return [
            self._build_impression(key, bucketing, feature, result)
            for feature, result in results.items()
        ]

    def _validate_track(self, key, traffic_type, event_type, value=None, properties=None):
        """
        Validate track call parameters

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

        :return: validation, event created and its properties size.
        :rtype: tuple(bool, splitio.models.events.Event, int)
        """
        if self.destroyed:
            _LOGGER.error("Client has already been destroyed - no calls possible")
            return False, None, None

        if self._factory._waiting_fork():
            _LOGGER.error("Client is not ready - no calls possible")
            return False, None, None

        key = input_validator.validate_track_key(key)
        event_type = input_validator.validate_event_type(event_type)
        value = input_validator.validate_value(value)
        valid, properties, size = input_validator.valid_properties(properties)

        if key is None or event_type is None or traffic_type is None or value is False \
           or valid is False:
            return False, None, None

        event = Event(
            key=key,
            traffic_type_name=traffic_type,
            event_type_id=event_type,
            value=value,
            timestamp=utctime_ms(),
            properties=properties,
        )

        return True, event, size


class Client(ClientBase):  # pylint: disable=too-many-instance-attributes
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
        ClientBase.__init__(self, factory, recorder, labels_enabled)
        self._context_factory = EvaluationDataFactory(factory._get_storage('splits'), factory._get_storage('segments'))

    def destroy(self):
        """
        Destroy the underlying factory.

        Only applicable when using in-memory operation mode.
        """
        self._factory.destroy()

    def get_treatment(self, key, feature_flag_name, attributes=None):
        """
        Get the treatment for a feature flag and key, with an optional dictionary of attributes.

        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.

        :param key: The key for which to get the treatment
        :type key: str
        :param feature_flag_name: The name of the feature flag for which to get the treatment
        :type feature_flag_name: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: The treatment for the key and feature flag
        :rtype: str
        """
        try:
            treatment, _ = self._get_treatment(MethodExceptionsAndLatencies.TREATMENT, key, feature_flag_name, attributes)
            return treatment

        except:
            _LOGGER.error('get_treatment failed')
            return CONTROL

    def get_treatment_with_config(self, key, feature_flag_name, attributes=None):
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
        try:
            return self._get_treatment(MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG, key, feature_flag_name, attributes)

        except Exception:
            _LOGGER.error('get_treatment_with_config failed')
            return CONTROL, None

    def _get_treatment(self, method, key, feature, attributes=None):
        """
        Validate key, feature flag name and object, and get the treatment and config with an optional dictionary of attributes.

        :param key: The key for which to get the treatment
        :type key: str
        :param feature_flag_name: The name of the feature flag for which to get the treatment
        :type feature_flag_name: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :param method: The method calling this function
        :type method: splitio.models.telemetry.MethodExceptionsAndLatencies
        :return: The treatment and config for the key and feature flag
        :rtype: dict
        """
        if not self._client_is_usable(): # not destroyed & not waiting for a fork
            return CONTROL, None

        start = get_current_epoch_time_ms()
        if not self.ready:
            _LOGGER.error("Client is not ready - no calls possible")
            self._telemetry_init_producer.record_not_ready_usage()

        try:
            key, bucketing, feature, attributes = self._validate_treatment_input(key, feature, attributes, method)
        except _InvalidInputError:
            return CONTROL, None

        result = self._NON_READY_EVAL_RESULT
        if self.ready:
            try:
                ctx = self._context_factory.context_for(key, [feature])
                input_validator.validate_feature_flag_names({feature: ctx.flags.get(feature)}, 'get_' + method.value)
                result = self._evaluator.eval_with_context(key, bucketing, feature, attributes, ctx)
            except RuntimeError as e:
                _LOGGER.error('Error getting treatment for feature flag')
                _LOGGER.debug('Error: ', exc_info=True)
                self._telemetry_evaluation_producer.record_exception(method)
                result = self._FAILED_EVAL_RESULT

        if result['impression']['label'] != Label.SPLIT_NOT_FOUND:
            impression = self._build_impression(key, bucketing, feature, result)
            self._record_stats([(impression, attributes)], start, method)

        return result['treatment'], result['configurations']

    def get_treatments(self, key, feature_flag_names, attributes=None):
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
        try:
            with_config = self._get_treatments(key, feature_flag_names, MethodExceptionsAndLatencies.TREATMENTS, attributes)
            return {feature_flag: result[0] for (feature_flag, result) in with_config.items()}

        except Exception:
            return {feature: CONTROL for feature in feature_flag_names}

    def get_treatments_with_config(self, key, feature_flag_names, attributes=None):
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
        try:
            return self._get_treatments(key, feature_flag_names, MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG, attributes)

        except Exception:
            return {feature: (CONTROL, None) for feature in feature_flag_names}

    def get_treatments_by_flag_set(self, key, flag_set, attributes=None):
            """
            Get treatments for feature flags that contain given flag set.
            This method never raises an exception. If there's a problem, the appropriate log message
            will be generated and the method will return the CONTROL treatment.
            :param key: The key for which to get the treatment
            :type key: str
            :param flag_set: flag set
            :type flag_sets: str
            :param attributes: An optional dictionary of attributes
            :type attributes: dict
            :return: Dictionary with the result of all the feature flags provided
            :rtype: dict
            """
            return self._get_treatments_by_flag_sets( key, [flag_set], MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET, attributes)

    def get_treatments_by_flag_sets(self, key, flag_sets, attributes=None):
        """
        Get treatments for feature flags that contain given flag sets.
        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.
        :param key: The key for which to get the treatment
        :type key: str
        :param flag_sets: list of flag sets
        :type flag_sets: list
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: Dictionary with the result of all the feature flags provided
        :rtype: dict
        """
        return self._get_treatments_by_flag_sets( key, flag_sets, MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS, attributes)

    def get_treatments_with_config_by_flag_set(self, key, flag_set, attributes=None):
        """
        Get treatments for feature flags that contain given flag set.
        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.
        :param key: The key for which to get the treatment
        :type key: str
        :param flag_set: flag set
        :type flag_sets: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: Dictionary with the result of all the feature flags provided
        :rtype: dict
        """
        return self._get_treatments_by_flag_sets( key, [flag_set], MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET, attributes)

    def get_treatments_with_config_by_flag_sets(self, key, flag_sets, attributes=None):
        """
        Get treatments for feature flags that contain given flag set.
        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.
        :param key: The key for which to get the treatment
        :type key: str
        :param flag_set: flag set
        :type flag_sets: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: Dictionary with the result of all the feature flags provided
        :rtype: dict
        """
        return self._get_treatments_by_flag_sets( key, flag_sets, MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS, attributes)

    def _get_treatments_by_flag_sets(self, key, flag_sets, method, attributes=None):
        """
        Get treatments for feature flags that contain given flag sets.
        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.
        :param key: The key for which to get the treatment
        :type key: str
        :param flag_sets: list of flag sets
        :type flag_sets: list
        :param method: Treatment by flag set method flavor
        :type method: splitio.models.telemetry.MethodExceptionsAndLatencies
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: Dictionary with the result of all the feature flags provided
        :rtype: dict
        """
        feature_flags_names = self._get_feature_flag_names_by_flag_sets(flag_sets, 'get_' + method.value)
        if feature_flags_names == []:
            _LOGGER.warning("%s: No valid Flag set or no feature flags found for evaluating treatments", 'get_' + method.value)
            return {}

        if 'config' in method.value:
            return self._get_treatments(key, feature_flags_names, method, attributes)

        with_config = self._get_treatments(key, feature_flags_names, method, attributes)
        return {feature_flag: result[0] for (feature_flag, result) in with_config.items()}

    def get_treatments_by_flag_set(self, key, flag_set, attributes=None):
        """
        Get treatments for feature flags that contain given flag set.

        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.

        :param key: The key for which to get the treatment
        :type key: str
        :param flag_set: flag set
        :type flag_sets: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict

        :return: Dictionary with the result of all the feature flags provided
        :rtype: dict
        """
        return self._get_treatments_by_flag_sets( key, [flag_set], MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET, attributes)

    def get_treatments_by_flag_sets(self, key, flag_sets, attributes=None):
        """
        Get treatments for feature flags that contain given flag sets.

        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.

        :param key: The key for which to get the treatment
        :type key: str
        :param flag_sets: list of flag sets
        :type flag_sets: list
        :param attributes: An optional dictionary of attributes
        :type attributes: dict

        :return: Dictionary with the result of all the feature flags provided
        :rtype: dict
        """
        return self._get_treatments_by_flag_sets( key, flag_sets, MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS, attributes)

    def get_treatments_with_config_by_flag_set(self, key, flag_set, attributes=None):
        """
        Get treatments for feature flags that contain given flag set.

        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.

        :param key: The key for which to get the treatment
        :type key: str
        :param flag_set: flag set
        :type flag_sets: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict

        :return: Dictionary with the result of all the feature flags provided
        :rtype: dict
        """
        return self._get_treatments_by_flag_sets( key, [flag_set], MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET, attributes)

    def get_treatments_with_config_by_flag_sets(self, key, flag_sets, attributes=None):
        """
        Get treatments for feature flags that contain given flag set.

        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.

        :param key: The key for which to get the treatment
        :type key: str
        :param flag_set: flag set
        :type flag_sets: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict

        :return: Dictionary with the result of all the feature flags provided
        :rtype: dict
        """
        return self._get_treatments_by_flag_sets( key, flag_sets, MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS, attributes)

    def _get_feature_flag_names_by_flag_sets(self, flag_sets, method_name):
        """
        Sanitize given flag sets and return list of feature flag names associated with them
        :param flag_sets: list of flag sets
        :type flag_sets: list

        :return: list of feature flag names
        :rtype: list
        """
        sanitized_flag_sets = input_validator.validate_flag_sets(flag_sets, method_name)
        feature_flags_by_set = self._feature_flag_storage.get_feature_flags_by_sets(sanitized_flag_sets)
        if feature_flags_by_set is None:
            _LOGGER.warning("Fetching feature flags for flag set %s encountered an error, skipping this flag set." % (flag_sets))
            return []

        return feature_flags_by_set

    def _get_treatments(self, key, features, method, attributes=None):
        """
        Validate key, feature flag names and objects, and get the treatments and configs with an optional dictionary of attributes.

        :param key: The key for which to get the treatment
        :type key: str
        :param feature_flag_names: Array of feature flag names for which to get the treatments
        :type feature_flag_names: list(str)
        :param method: The method calling this function
        :type method: splitio.models.telemetry.MethodExceptionsAndLatencies
        :param attributes: An optional dictionary of attributes
        :type attributes: dict

        :return: The treatments and configs for the key and feature flags
        :rtype: dict
        """
        start = get_current_epoch_time_ms()
        if not self._client_is_usable():
            return input_validator.generate_control_treatments(features)

        if not self.ready:
            _LOGGER.error("Client is not ready - no calls possible")
            self._telemetry_init_producer.record_not_ready_usage()

        try:
            key, bucketing, features, attributes = self._validate_treatments_input(key, features, attributes, method)
        except _InvalidInputError:
            return input_validator.generate_control_treatments(features)

        results = {n: self._NON_READY_EVAL_RESULT for n in features}
        if self.ready:
            try:
                ctx = self._context_factory.context_for(key, features)
                input_validator.validate_feature_flag_names({feature: ctx.flags.get(feature) for feature in features}, 'get_' + method.value)
                results = self._evaluator.eval_many_with_context(key, bucketing, features, attributes, ctx)
            except RuntimeError as e:
                _LOGGER.error('Error getting treatment for feature flag')
                _LOGGER.debug('Error: ', exc_info=True)
                self._telemetry_evaluation_producer.record_exception(method)
                results = {n: self._FAILED_EVAL_RESULT for n in features}

        imp_attrs = [
            (i, attributes) for i in self._build_impressions(key, bucketing, results)
            if i.label != Label.SPLIT_NOT_FOUND
        ]
        self._record_stats(imp_attrs, start, method)

        return {
            feature: (results[feature]['treatment'], results[feature]['configurations'])
            for feature in results
        }

    def _record_stats(self, impressions, start, operation):
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
                                              operation, 'get_' + operation.value)

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
        if not self.ready:
            _LOGGER.warning("track: the SDK is not ready, results may be incorrect. Make sure to wait for SDK readiness before using this method")
            self._telemetry_init_producer.record_not_ready_usage()

        start = get_current_epoch_time_ms()
        should_validate_existance = self.ready and self._factory._sdk_key != 'localhost'  # pylint: disable=protected-access
        traffic_type = input_validator.validate_traffic_type(
            traffic_type,
            should_validate_existance,
            self._factory._get_storage('splits'),  # pylint: disable=protected-access
        )
        is_valid, event, size = self._validate_track(key, traffic_type, event_type, value, properties)
        if not is_valid:
            return False

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


class ClientAsync(ClientBase):  # pylint: disable=too-many-instance-attributes
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
        ClientBase.__init__(self, factory, recorder, labels_enabled)
        self._context_factory = AsyncEvaluationDataFactory(factory._get_storage('splits'), factory._get_storage('segments'))

    async def destroy(self):
        """
        Destroy the underlying factory.

        Only applicable when using in-memory operation mode.
        """
        await self._factory.destroy()

    async def get_treatment(self, key, feature_flag_name, attributes=None):
        """
        Get the treatment for a feature and key, with an optional dictionary of attributes, for async calls

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
        try:
            treatment, _ = await self._get_treatment(MethodExceptionsAndLatencies.TREATMENT, key, feature_flag_name, attributes)
            return treatment

        except:
            _LOGGER.error('get_treatment failed')
            return CONTROL

    async def get_treatment_with_config(self, key, feature_flag_name, attributes=None):
        """
        Get the treatment for a feature and key, with an optional dictionary of attributes, for async calls

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
        try:
            return await self._get_treatment(MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG, key, feature_flag_name, attributes)

        except Exception:
            _LOGGER.error('get_treatment_with_config failed')
            return CONTROL, None

    async def _get_treatment(self, method, key, feature, attributes=None):
        """
        Validate key, feature flag name and object, and get the treatment and config with an optional dictionary of attributes, for async calls

        :param key: The key for which to get the treatment
        :type key: str
        :param feature_flag_name: The name of the feature flag for which to get the treatment
        :type feature_flag_name: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :param method: The method calling this function
        :type method: splitio.models.telemetry.MethodExceptionsAndLatencies
        :return: The treatment and config for the key and feature flag
        :rtype: dict
        """
        if not self._client_is_usable(): # not destroyed & not waiting for a fork
            return CONTROL, None

        start = get_current_epoch_time_ms()
        if not self.ready:
            _LOGGER.error("Client is not ready - no calls possible")
            await self._telemetry_init_producer.record_not_ready_usage()

        try:
            key, bucketing, feature, attributes = self._validate_treatment_input(key, feature, attributes, method)
        except _InvalidInputError:
            return CONTROL, None

        result = self._NON_READY_EVAL_RESULT
        if self.ready:
            try:
                ctx = await self._context_factory.context_for(key, [feature])
                input_validator.validate_feature_flag_names({feature: ctx.flags.get(feature)}, 'get_' + method.value)
                result = self._evaluator.eval_with_context(key, bucketing, feature, attributes, ctx)
            except Exception as e: # toto narrow this
                _LOGGER.error('Error getting treatment for feature flag')
                _LOGGER.debug('Error: ', exc_info=True)
                await self._telemetry_evaluation_producer.record_exception(method)
                result = self._FAILED_EVAL_RESULT

        if result['impression']['label'] != Label.SPLIT_NOT_FOUND:
            impression = self._build_impression(key, bucketing, feature, result)
            await self._record_stats([(impression, attributes)], start, method)
        return result['treatment'], result['configurations']

    async def get_treatments(self, key, feature_flag_names, attributes=None):
        """
        Evaluate multiple feature flags and return a dictionary with all the feature flag/treatments, for async calls

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
        try:
            with_config = await self._get_treatments(key, feature_flag_names, MethodExceptionsAndLatencies.TREATMENTS, attributes)
            return {feature_flag: result[0] for (feature_flag, result) in with_config.items()}

        except Exception:
            return {feature: CONTROL for feature in feature_flag_names}

    async def get_treatments_with_config(self, key, feature_flag_names, attributes=None):
        """
        Evaluate multiple feature flags and return a dict with feature flag -> (treatment, config), for async calls

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
        try:
            return await self._get_treatments(key, feature_flag_names, MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG, attributes)

        except Exception:
            _LOGGER.error("AA", exc_info=True)
            return {feature: (CONTROL, None) for feature in feature_flag_names}

    async def get_treatments_by_flag_set(self, key, flag_set, attributes=None):
            """
            Get treatments for feature flags that contain given flag set.
            This method never raises an exception. If there's a problem, the appropriate log message
            will be generated and the method will return the CONTROL treatment.
            :param key: The key for which to get the treatment
            :type key: str
            :param flag_set: flag set
            :type flag_sets: str
            :param attributes: An optional dictionary of attributes
            :type attributes: dict
            :return: Dictionary with the result of all the feature flags provided
            :rtype: dict
            """
            return await self._get_treatments_by_flag_sets( key, [flag_set], MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET, attributes)

    async def get_treatments_by_flag_sets(self, key, flag_sets, attributes=None):
        """
        Get treatments for feature flags that contain given flag sets.
        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.
        :param key: The key for which to get the treatment
        :type key: str
        :param flag_sets: list of flag sets
        :type flag_sets: list
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: Dictionary with the result of all the feature flags provided
        :rtype: dict
        """
        return await self._get_treatments_by_flag_sets( key, flag_sets, MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS, attributes)

    async def get_treatments_with_config_by_flag_set(self, key, flag_set, attributes=None):
        """
        Get treatments for feature flags that contain given flag set.
        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.
        :param key: The key for which to get the treatment
        :type key: str
        :param flag_set: flag set
        :type flag_sets: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: Dictionary with the result of all the feature flags provided
        :rtype: dict
        """
        return await self._get_treatments_by_flag_sets( key, [flag_set], MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET, attributes)

    async def get_treatments_with_config_by_flag_sets(self, key, flag_sets, attributes=None):
        """
        Get treatments for feature flags that contain given flag set.
        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.
        :param key: The key for which to get the treatment
        :type key: str
        :param flag_set: flag set
        :type flag_sets: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: Dictionary with the result of all the feature flags provided
        :rtype: dict
        """
        return await self._get_treatments_by_flag_sets( key, flag_sets, MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS, attributes)

    async def _get_treatments_by_flag_sets(self, key, flag_sets, method, attributes=None):
        """
        Get treatments for feature flags that contain given flag sets.
        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.
        :param key: The key for which to get the treatment
        :type key: str
        :param flag_sets: list of flag sets
        :type flag_sets: list
        :param method: Treatment by flag set method flavor
        :type method: splitio.models.telemetry.MethodExceptionsAndLatencies
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: Dictionary with the result of all the feature flags provided
        :rtype: dict
        """
        feature_flags_names = await self._get_feature_flag_names_by_flag_sets(flag_sets, 'get_' + method.value)
        if feature_flags_names == []:
            _LOGGER.warning("%s: No valid Flag set or no feature flags found for evaluating treatments", 'get_' + method.value)
            return {}

        if 'config' in method.value:
            return await self._get_treatments(key, feature_flags_names, method, attributes)

        with_config = await self._get_treatments(key, feature_flags_names, method, attributes)
        return {feature_flag: result[0] for (feature_flag, result) in with_config.items()}

    async def _get_feature_flag_names_by_flag_sets(self, flag_sets, method_name):
        """
        Sanitize given flag sets and return list of feature flag names associated with them
        :param flag_sets: list of flag sets
        :type flag_sets: list
        :return: list of feature flag names
        :rtype: list
        """
        sanitized_flag_sets = input_validator.validate_flag_sets(flag_sets, method_name)
        feature_flags_by_set = await self._feature_flag_storage.get_feature_flags_by_sets(sanitized_flag_sets)
        if feature_flags_by_set is None:
            _LOGGER.warning("Fetching feature flags for flag set %s encountered an error, skipping this flag set." % (flag_sets))
            return []

        return feature_flags_by_set

    async def _get_treatments(self, key, features, method, attributes=None):
        """
        Validate key, feature flag names and objects, and get the treatments and configs with an optional dictionary of attributes, for async calls

        :param key: The key for which to get the treatment
        :type key: str
        :param feature_flag_names: Array of feature flag names for which to get the treatments
        :type feature_flag_names: list(str)
        :param method: The method calling this function
        :type method: splitio.models.telemetry.MethodExceptionsAndLatencies
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: The treatments and configs for the key and feature flags
        :rtype: dict
        """
        start = get_current_epoch_time_ms()
        if not self._client_is_usable():
            return input_validator.generate_control_treatments(features)

        if not self.ready:
            _LOGGER.error("Client is not ready - no calls possible")
            await self._telemetry_init_producer.record_not_ready_usage()

        try:
            key, bucketing, features, attributes = self._validate_treatments_input(key, features, attributes, method)
        except _InvalidInputError:
            return input_validator.generate_control_treatments(features)

        results = {n: self._NON_READY_EVAL_RESULT for n in features}
        if self.ready:
            try:
                ctx = await self._context_factory.context_for(key, features)
                input_validator.validate_feature_flag_names({feature: ctx.flags.get(feature) for feature in features}, 'get_' + method.value)
                results = self._evaluator.eval_many_with_context(key, bucketing, features, attributes, ctx)
            except Exception as e: # toto narrow this
                _LOGGER.error('Error getting treatment for feature flag')
                _LOGGER.debug('Error: ', exc_info=True)
                await self._telemetry_evaluation_producer.record_exception(method)
                results = {n: self._FAILED_EVAL_RESULT for n in features}

        imp_attrs = [
            (i, attributes) for i in self._build_impressions(key, bucketing, results)
            if i.label != Label.SPLIT_NOT_FOUND
        ]
        await self._record_stats(imp_attrs, start, method)

        return {
            feature: (res['treatment'], res['configurations'])
            for feature, res in results.items()
        }

    async def _record_stats(self, impressions, start, operation):
        """
        Record impressions for async calls

        :param impressions: Generated impressions
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]

        :param start: timestamp when get_treatment or get_treatments was called
        :type start: int

        :param operation: operation performed.
        :type operation: str
        """
        end = get_current_epoch_time_ms()
        await self._recorder.record_treatment_stats(impressions, get_latency_bucket_index(end - start),
                                              operation, 'get_' + operation.value)

    async def track(self, key, traffic_type, event_type, value=None, properties=None):
        """
        Track an event for async calls

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
        if not self.ready:
            _LOGGER.warning("track: the SDK is not ready, results may be incorrect. Make sure to wait for SDK readiness before using this method")
            await self._telemetry_init_producer.record_not_ready_usage()

        start = get_current_epoch_time_ms()
        should_validate_existance = self.ready and self._factory._sdk_key != 'localhost'  # pylint: disable=protected-access
        traffic_type = await input_validator.validate_traffic_type_async(
            traffic_type,
            should_validate_existance,
            self._factory._get_storage('splits'),  # pylint: disable=protected-access
        )
        is_valid, event, size = self._validate_track(key, traffic_type, event_type, value, properties)
        if not is_valid:
            return False

        try:
            return_flag = await self._recorder.record_track_stats([EventWrapper(
                event=event,
                size=size,
            )], get_latency_bucket_index(get_current_epoch_time_ms() - start))
            return return_flag

        except Exception:  # pylint: disable=broad-except
            await self._telemetry_evaluation_producer.record_exception(MethodExceptionsAndLatencies.TRACK)
            _LOGGER.error('Error processing track event')
            _LOGGER.debug('Error: ', exc_info=True)
            return False


class _InvalidInputError(Exception):
    pass
