"""A module for Split.io SDK API clients."""
import logging

from splitio.engine.evaluator import Evaluator, CONTROL
from splitio.engine.splitters import Splitter
from splitio.models.impressions import Impression, Label
from splitio.models.events import Event, EventWrapper
from splitio.models.telemetry import get_latency_bucket_index, MethodExceptionsAndLatencies
from splitio.models.grammar import matchers
from splitio.models.grammar.condition import ConditionType
from splitio.models.grammar.matchers.misc import DependencyMatcher
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
        self._evaluator = Evaluator(self._splitter)
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

    def _evaluate_if_ready(self, matching_key, bucketing_key, feature_flag_name, feature_flag, condition_matchers):
        if not self.ready:
            return {
                'treatment': CONTROL,
                'configurations': None,
                'impression': {
                    'label': Label.NOT_READY,
                    'change_number': None
                }
            }
        if feature_flag is None:
            _LOGGER.warning('Unknown or invalid feature: %s', feature_flag_name)

        if bucketing_key is None:
            bucketing_key = matching_key

        return self._evaluator.evaluate_feature(
            feature_flag,
            matching_key,
            bucketing_key,
            condition_matchers
        )

    def _make_evaluation(self, key, feature_flag_name, attributes, method, feature_flag, condition_matchers, storage_change_number):
        """
        Evaluate treatment for given feature flag

        :param key: The key for which to get the treatment
        :type key: str
        :param feature_flag_name: The name of the feature flag for which to get the treatment
        :type feature_flag_name: str
        :param method: The method calling this function
        :type method: splitio.models.telemetry.MethodExceptionsAndLatencies
        :param feature_flag: Feature flag Split object
        :type feature_flag: splitio.models.splits.Split
        :param condition_matchers: A dictionary representing all matchers for the current feature flag
        :type condition_matchers: dict
        :param storage_change_number: the change number for the Feature flag storage.
        :type storage_change_number: int
        :return: The treatment and config for the key and feature flag, impressions created, start time and exception flag
        :rtype: tuple(str, str, splitio.models.impressions.Impression, int, bool)
        """
        try:
            start = get_current_epoch_time_ms()
            matching_key, bucketing_key = input_validator.validate_key(key, method.value)
            if (matching_key is None and bucketing_key is None) \
                    or feature_flag_name is None \
                    or not input_validator.validate_attributes(attributes, method.value):
                return CONTROL, None, None, None, False

            result = self._evaluate_if_ready(matching_key, bucketing_key, feature_flag_name, feature_flag, condition_matchers)

            impression = self._build_impression(
                matching_key,
                feature_flag_name,
                result['treatment'],
                result['impression']['label'],
                result['impression']['change_number'],
                bucketing_key,
                utctime_ms(),
            )
            return result['treatment'], result['configurations'], impression, start, False
        except Exception as e:  # pylint: disable=broad-except
            _LOGGER.error('Error getting treatment for feature flag')
            _LOGGER.error(str(e))
            _LOGGER.debug('Error: ', exc_info=True)
            try:
                impression = self._build_impression(
                    matching_key,
                    feature_flag_name,
                    CONTROL,
                    Label.EXCEPTION,
                    storage_change_number,
                    bucketing_key,
                    utctime_ms(),
                )
                return CONTROL, None, impression, start, True
            except Exception:  # pylint: disable=broad-except
                _LOGGER.error('Error reporting impression into get_treatment exception block')
                _LOGGER.debug('Error: ', exc_info=True)
            return CONTROL, None, None, None, False

    def _make_evaluations(self, key, feature_flag_names, feature_flags, condition_matchers, attributes, method):
        """
        Evaluate treatments for given feature flags

        :param key: The key for which to get the treatment
        :type key: str
        :param feature_flag_names: Array of feature flag names for which to get the treatment
        :type feature_flag_names: list(str)
        :param feature_flags: Array of feature flags Split objects
        :type feature_flag: list(splitio.models.splits.Split)
        :param condition_matchers: dictionary representing all matchers for each current feature flag
        :type condition_matchers: dict
        :param storage_change_number: the change number for the Feature flag storage.
        :type storage_change_number: int
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :param method: The method calling this function
        :type method: splitio.models.telemetry.MethodExceptionsAndLatencies
        :return: The treatments and configs for the key and feature flags, impressions created, start time and exception flag
        :rtype: tuple(dict, splitio.models.impressions.Impression, int, bool)
        """
        start = get_current_epoch_time_ms()

        matching_key, bucketing_key = input_validator.validate_key(key, method.value)
        if input_validator.validate_attributes(attributes, method.value) is False:
            return input_validator.generate_control_treatments(feature_flags, method.value), None, None, False

        treatments = {}
        bulk_impressions = []
        try:
            evaluations = self._evaluate_features_if_ready(matching_key, bucketing_key,
                                                           list(feature_flag_names), feature_flags, condition_matchers)
            exception_flag = False
            for feature_flag_name in feature_flag_names:
                try:
                    result = evaluations[feature_flag_name]
                    impression = self._build_impression(matching_key,
                                                        feature_flag_name,
                                                        result['treatment'],
                                                        result['impression']['label'],
                                                        result['impression']['change_number'],
                                                        bucketing_key,
                                                        utctime_ms())

                    bulk_impressions.append(impression)
                    treatments[feature_flag_name] = (result['treatment'], result['configurations'])

                except Exception:  # pylint: disable=broad-except
                    _LOGGER.error('%s: An exception occured when evaluating '
                                  'feature flag %s returning CONTROL.' % (method.value, feature_flag_name))
                    treatments[feature_flag_name] = CONTROL, None
                    _LOGGER.debug('Error: ', exc_info=True)
                    exception_flag = True
                    continue

            return treatments, bulk_impressions, start, exception_flag
        except Exception:  # pylint: disable=broad-except
            _LOGGER.error('Error getting treatment for feature flags')
            _LOGGER.debug('Error: ', exc_info=True)
        return input_validator.generate_control_treatments(list(feature_flag_names), method.value), None, start, True

    def _evaluate_features_if_ready(self, matching_key, bucketing_key, feature_flag_names, feature_flags, condition_matchers):
        """
        Evaluate treatments for given feature flags

        :param matching_key: Matching key for which to get the treatment
        :type matching_key: str
        :param bucketing_key: Bucketing key for which to get the treatment
        :type bucketing_key: str
        :param feature_flag_names: Array of feature flag names for which to get the treatment
        :type feature_flag_names: list(str)
        :param feature_flags: Array of feature flags Split objects
        :type feature_flag: list(splitio.models.splits.Split)
        :param condition_matchers: dictionary representing all matchers for each current feature flag
        :type condition_matchers: dict
        :return: The treatments, configs and impressions generated for the key and feature flags
        :rtype: dict
        """
        if not self.ready:
            return {
                feature_flag_name: {
                    'treatment': CONTROL,
                    'configurations': None,
                    'impression': {'label': Label.NOT_READY, 'change_number': None}
                }
                for feature_flag_name in feature_flag_names
            }
        return self._evaluator.evaluate_features(
            feature_flags,
            matching_key,
            bucketing_key,
            condition_matchers
        )

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
        return self._get_treatment(key, feature_flag_name, MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG, attributes)

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
        treatment, _ = self._get_treatment(key, feature_flag_name, MethodExceptionsAndLatencies.TREATMENT, attributes)
        return treatment

    def _get_treatment(self, key, feature_flag_name, method, attributes=None):
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
        if self.destroyed:
            _LOGGER.error("Client has already been destroyed - no calls possible")
            return CONTROL, None
        if self._factory._waiting_fork():
            _LOGGER.error("Client is not ready - no calls possible")
            return CONTROL, None
        if not self.ready:
            self._telemetry_init_producer.record_not_ready_usage()

        feature_flag =  self._split_storage.get(feature_flag_name)
        if input_validator.validate_feature_flag_name(
            feature_flag_name,
            self.ready,
            feature_flag,
            method) == None:
            return CONTROL, None

        matching_key, bucketing_key = input_validator.validate_key(key, method.value)
        if bucketing_key is None:
            bucketing_key = matching_key

        segment_matchers = self._get_segment_matchers(feature_flag, matching_key)
        condition_matchers = self._get_condition_matchers(feature_flag, bucketing_key, matching_key, segment_matchers, attributes)
        treatment, config, impression, start, exception_flag = self._make_evaluation(key, feature_flag_name, attributes, method,
                                             feature_flag, condition_matchers, self._split_storage.get_change_number())
        if impression is not None:
            self._record_stats([(impression, attributes)], start, method)

        if exception_flag:
            self._telemetry_evaluation_producer.record_exception(method)

        return treatment, config

    def _get_condition_matchers(self, feature_flag, bucketing_key, matching_key, segment_matchers, attributes=None):
        """
        Calculate and store all condition matchers for given feature flag.
        If there are dependent Feature Flag(s), the function will do recursive calls until all matchers are resolved.

        :param feature_flag: Feature flag Split objects
        :type feature_flag: splitio.models.splits.Split
        :param bucketing_key: Bucketing key for which to get the treatment
        :type bucketing_key: str
        :param matching_key: Matching key for which to get the treatment
        :type matching_key: str
        :param segment_matchers: Segment matchers for the feature flag
        :type segment_matchers: dict
        :return: dictionary representing all matchers for each current feature flag
        :type: dict
        """
        roll_out = False
        context = {
            'segment_matchers': segment_matchers,
            'evaluator': self._evaluator,
            'bucketing_key': bucketing_key,
        }
        condition_matchers = []
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
            dependent_feature_flags = []
            for matcher in condition.matchers:
                if isinstance(matcher, DependencyMatcher):
                    dependent_feature_flag = self._split_storage.get(matcher.to_json()['dependencyMatcherData']['split'])
                    depenedent_segment_matchers = self._get_segment_matchers(dependent_feature_flag, matching_key)
                    dependent_feature_flags.append((dependent_feature_flag,
                                                   self._get_condition_matchers(dependent_feature_flag, bucketing_key, matching_key, depenedent_segment_matchers, attributes)))
            context['dependent_splits'] = dependent_feature_flags
            condition_matchers.append((condition.matches(
                matching_key,
                attributes=attributes,
                context=context
            ), condition))

        return condition_matchers

    def _get_segment_matchers(self, feature_flag, matching_key):
        """
        Get all segments matchers for given feature flag.
        If there are dependent Feature Flag(s), the function will do recursive calls until all matchers are resolved.

        :param feature_flag: Feature flag Split objects
        :type feature_flag: splitio.models.splits.Split
        :param matching_key: Matching key for which to get the treatment
        :type matching_key: str
        :return: Segment matchers for the feature flag
        :type: dict
        """
        segment_matchers = {}
        for segment in self._get_segment_names(feature_flag):
            for condition in feature_flag.conditions:
                for matcher in condition.matchers:
                    if isinstance(matcher, matchers.UserDefinedSegmentMatcher):
                        segment_matchers[segment] = self._segment_storage.segment_contains(segment, matching_key)
        return segment_matchers

    def _get_segment_names(self, feature_flag):
        """
        Fetch segment names for all IN_SEGMENT matchers.

        :return: List of segment names
        :rtype: list(str)
        """
        segment_names = []
        if feature_flag is None:
            return []
        for condition in feature_flag.conditions:
            matcher_list = condition.matchers
            for matcher in matcher_list:
                if isinstance(matcher, matchers.UserDefinedSegmentMatcher):
                    segment_names.append(matcher._segment_name)

        return segment_names

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
        return self._get_treatments(key, feature_flag_names, MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG, attributes)

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
        with_config = self._get_treatments(key, feature_flag_names, MethodExceptionsAndLatencies.TREATMENTS, attributes)
        return {feature_flag: result[0] for (feature_flag, result) in with_config.items()}

    def _get_treatments(self, key, feature_flag_names, method, attributes=None):
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
        if self.destroyed:
            _LOGGER.error("Client has already been destroyed - no calls possible")
            return input_validator.generate_control_treatments(feature_flag_names, method.value)
        if self._factory._waiting_fork():
            _LOGGER.error("Client is not ready - no calls possible")
            return input_validator.generate_control_treatments(feature_flag_names, method.value)

        if not self.ready:
            _LOGGER.error("Client is not ready - no calls possible")
            self._telemetry_init_producer.record_not_ready_usage()

        feature_flags, missing = input_validator.validate_feature_flags_get_treatments(
            method.value,
            feature_flag_names,
            self._factory._get_storage('splits')  # pylint: disable=protected-access
        )
        if feature_flags is None:
            return input_validator.generate_control_treatments(feature_flag_names, method.value)

        missing_treatments = {name: (CONTROL, None) for name in missing}

        matching_key, bucketing_key = input_validator.validate_key(key, method.value)
        if matching_key is None and bucketing_key is None:
            return input_validator.generate_control_treatments(feature_flag_names, method.value)

        if bucketing_key is None:
            bucketing_key = matching_key

        condition_matchers = {}
        for feature_flag in feature_flags:
            segment_matchers = self._get_segment_matchers(feature_flag, matching_key)
            condition_matchers[feature_flag.name] = self._get_condition_matchers(feature_flag, bucketing_key, matching_key, segment_matchers, attributes)

        with_config, impressions, start, exception_flag = self._make_evaluations(key, feature_flag_names, feature_flags, condition_matchers, attributes, method)

        try:
            if impressions:
                self._record_stats(
                    [(i, attributes) for i in impressions],
                    start,
                    method
                )
        except Exception:  # pylint: disable=broad-except
            _LOGGER.error('%s: An exception when trying to store '
                            'impressions.' % method.value)
            _LOGGER.debug('Error: ', exc_info=True)
            self._telemetry_evaluation_producer.record_exception(method)

        if exception_flag:
            self._telemetry_evaluation_producer.record_exception(method)

        with_config.update(missing_treatments)
        return with_config

    async def _get_condition_matchers_async(self, feature_flag, bucketing_key, matching_key, segment_matchers, attributes=None):
        """
        Calculate and store all condition matchers for given feature flag for async calls
        If there are dependent Feature Flag(s), the function will do recursive calls until all matchers are resolved.

        :param feature_flag: Feature flag Split objects
        :type feature_flag: splitio.models.splits.Split
        :param bucketing_key: Bucketing key for which to get the treatment
        :type bucketing_key: str
        :param matching_key: Matching key for which to get the treatment
        :type matching_key: str
        :param segment_matchers: Segment matchers for the feature flag
        :type segment_matchers: dict
        :return: dictionary representing all matchers for each current feature flag
        :type: dict
        """
        roll_out = False
        context = {
            'segment_matchers': segment_matchers,
            'evaluator': self._evaluator,
            'bucketing_key': bucketing_key,
        }
        condition_matchers = []
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
            dependent_splits = []
            for matcher in condition.matchers:
                if isinstance(matcher, DependencyMatcher):
                    dependent_split = await self._split_storage.get(matcher.to_json()['dependencyMatcherData']['split'])
                    depenedent_segment_matchers = await self._get_segment_matchers_async(dependent_split, matching_key)
                    dependent_splits.append((dependent_split,
                                                   await self._get_condition_matchers_async(dependent_split, bucketing_key, matching_key, depenedent_segment_matchers, attributes)))
            context['dependent_splits'] = dependent_splits
            condition_matchers.append((condition.matches(
                matching_key,
                attributes=attributes,
                context=context
            ), condition))

        return condition_matchers

    async def _get_segment_matchers_async(self, feature_flag, matching_key):
        """
        Get all segments matchers for given feature flag for async calls
        If there are dependent Feature Flag(s), the function will do recursive calls until all matchers are resolved.

        :param feature_flag: Feature flag Split objects
        :type feature_flag: splitio.models.splits.Split
        :param matching_key: Matching key for which to get the treatment
        :type matching_key: str
        :return: Segment matchers for the feature flag
        :type: dict
        """
        segment_matchers = {}
        for segment in self._get_segment_names(feature_flag):
            for condition in feature_flag.conditions:
                for matcher in condition.matchers:
                    if isinstance(matcher, matchers.UserDefinedSegmentMatcher):
                        segment_matchers[segment] = await self._segment_storage.segment_contains(segment, matching_key)
        return segment_matchers

    async def get_treatment_async(self, key, feature_flag_name, attributes=None):
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
        treatment, _ = await self._get_treatment_async(key, feature_flag_name, MethodExceptionsAndLatencies.TREATMENT, attributes)
        return treatment

    async def get_treatment_with_config_async(self, key, feature_flag_name, attributes=None):
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
        return await self._get_treatment_async(key, feature_flag_name, MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG, attributes)

    async def _get_treatment_async(self, key, feature_flag_name, method, attributes=None):
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
        if self.destroyed:
            _LOGGER.error("Client has already been destroyed - no calls possible")
            return CONTROL, None
        if self._factory._waiting_fork():
            _LOGGER.error("Client is not ready - no calls possible")
            return CONTROL, None
        if not self.ready:
            await self._telemetry_init_producer.record_not_ready_usage()

        feature_flag =  await self._split_storage.get(feature_flag_name)
        if input_validator.validate_feature_flag_name(
            feature_flag_name,
            self.ready,
            feature_flag,
            method) == None:
            return CONTROL, None

        matching_key, bucketing_key = input_validator.validate_key(key, method.value)
        if bucketing_key is None:
            bucketing_key = matching_key

        segment_matchers = await self._get_segment_matchers_async(feature_flag, matching_key)
        condition_matchers = await self._get_condition_matchers_async(feature_flag, bucketing_key, matching_key, segment_matchers, attributes)
        treatment, config, impression, start, exception_flag = self._make_evaluation(key, feature_flag_name, attributes, method,
                                             feature_flag, condition_matchers, await self._split_storage.get_change_number())
        if impression is not None:
            await self._record_stats_async([(impression, attributes)], start, method)

        if exception_flag:
            await self._telemetry_evaluation_producer.record_exception(method)

        return treatment, config

    async def get_treatments_async(self, key, feature_flag_names, attributes=None):
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
        with_config = await self._get_treatments_async(key, feature_flag_names, MethodExceptionsAndLatencies.TREATMENTS, attributes)
        return {feature_flag: result[0] for (feature_flag, result) in with_config.items()}

    async def get_treatments_with_config_async(self, key, feature_flag_names, attributes=None):
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
        return await self._get_treatments_async(key, feature_flag_names, MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG, attributes)

    async def _get_treatments_async(self, key, feature_flag_names, method, attributes=None):
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
        if self.destroyed:
            _LOGGER.error("Client has already been destroyed - no calls possible")
            return input_validator.generate_control_treatments(feature_flag_names, method.value)
        if self._factory._waiting_fork():
            _LOGGER.error("Client is not ready - no calls possible")
            return input_validator.generate_control_treatments(feature_flag_names, method.value)

        if not self.ready:
            _LOGGER.error("Client is not ready - no calls possible")
            await self._telemetry_init_producer.record_not_ready_usage()

        feature_flags, missing = await input_validator.validate_feature_flags_get_treatments_async(
            method.value,
            feature_flag_names,
            self._factory._get_storage('splits')  # pylint: disable=protected-access
        )
        if feature_flags is None:
            return input_validator.generate_control_treatments(feature_flag_names, method.value)

        missing_treatments = {name: (CONTROL, None) for name in missing}

        matching_key, bucketing_key = input_validator.validate_key(key, method.value)
        if matching_key is None and bucketing_key is None:
            return input_validator.generate_control_treatments(feature_flag_names, method.value)

        if bucketing_key is None:
            bucketing_key = matching_key

        condition_matchers = {}
        for feature_flag in feature_flags:
            segment_matchers = await self._get_segment_matchers_async(feature_flag, matching_key)
            condition_matchers[feature_flag.name] = await self._get_condition_matchers_async(feature_flag, bucketing_key, matching_key, segment_matchers, attributes)

        with_config, impressions, start, exception_flag = self._make_evaluations(key, feature_flag_names, feature_flags, condition_matchers, attributes, method)

        try:
            if impressions:
                await self._record_stats_async(
                    [(i, attributes) for i in impressions],
                    start,
                    method
                )
        except Exception:  # pylint: disable=broad-except
            _LOGGER.error('%s: An exception when trying to store '
                            'impressions.' % method.value)
            _LOGGER.debug('Error: ', exc_info=True)
            await self._telemetry_evaluation_producer.record_exception(method)

        if exception_flag:
            await self._telemetry_evaluation_producer.record_exception(method)

        with_config.update(missing_treatments)
        return with_config

    def _build_impression(  # pylint: disable=too-many-arguments
            self,
            matching_key,
            feature_flag_name,
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
            matching_key=matching_key, feature_name=feature_flag_name,
            treatment=treatment, label=label, change_number=change_number,
            bucketing_key=bucketing_key, time=imp_time
        )

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
                                              operation, operation.value)

    async def _record_stats_async(self, impressions, start, operation):
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
                                              operation, operation.value)

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

    async def track_async(self, key, traffic_type, event_type, value=None, properties=None):
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