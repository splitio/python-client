"""Input validation module."""
from numbers import Number
import logging
import re
import math

from splitio.api import APIException
from splitio.api.commons import FetchOptions
from splitio.client.key import Key
from splitio.engine.evaluator import CONTROL


_LOGGER = logging.getLogger(__name__)
MAX_LENGTH = 250
EVENT_TYPE_PATTERN = r'^[a-zA-Z0-9][-_.:a-zA-Z0-9]{0,79}$'
MAX_PROPERTIES_LENGTH_BYTES = 32768


def _check_not_null(value, name, operation):
    """
    Check if value is null.

    :param key: value to be checked
    :type key: str
    :param name: name to inform the error
    :type feature: str
    :param operation: operation to inform the error
    :type operation: str
    :return: The result of validation
    :rtype: True|False
    """
    if value is None:
        _LOGGER.error('%s: you passed a null %s, %s must be a non-empty string.',
                      operation, name, name)
        return False
    return True


def _check_is_string(value, name, operation):
    """
    Check if value is not string.

    :param key: value to be checked
    :type key: str
    :param name: name to inform the error
    :type feature: str
    :param operation: operation to inform the error
    :type operation: str
    :return: The result of validation
    :rtype: True|False
    """
    if isinstance(value, str) is False:
        _LOGGER.error(
            '%s: you passed an invalid %s, %s must be a non-empty string.',
            operation, name, name
        )
        return False
    return True


def _check_string_not_empty(value, name, operation):
    """
    Check if value is an empty string.

    :param key: value to be checked
    :type key: str
    :param name: name to inform the error
    :type feature: str
    :param operation: operation to inform the error
    :type operation: str
    :return: The result of validation
    :rtype: True|False
    """
    if value.strip() == "":
        _LOGGER.error('%s: you passed an empty %s, %s must be a non-empty string.',
                      operation, name, name)
        return False
    return True


def _check_string_matches(value, operation, pattern):
    """
    Check if value is adhere to a regular expression passed.

    :param key: value to be checked
    :type key: str
    :param operation: operation to inform the error
    :type operation: str
    :param pattern: pattern that needs to adhere
    :type pattern: str
    :return: The result of validation
    :rtype: True|False
    """
    if not re.match(pattern, value):
        _LOGGER.error(
            '%s: you passed %s, event_type must ' +
            'adhere to the regular expression %s. ' +
            'This means an event name must be alphanumeric, cannot be more ' +
            'than 80 characters long, and can only include a dash, underscore, ' +
            'period, or colon as separators of alphanumeric characters.',
            operation, value, pattern
        )
        return False
    return True


def _check_can_convert(value, name, operation):
    """
    Check if is a valid convertion.

    :param key: value to be checked
    :type key: bool|number|array|
    :param name: name to inform the error
    :type feature: str
    :param operation: operation to inform the error
    :type operation: str
    :return: The result of validation
    :rtype: None|string
    """
    if isinstance(value, str):
        return value
    else:
        # check whether if isnan and isinf are really necessary
        if isinstance(value, bool) or (not isinstance(value, Number)) or math.isnan(value) \
           or math.isinf(value):
            _LOGGER.error('%s: you passed an invalid %s, %s must be a non-empty string.',
                          operation, name, name)
            return None
    _LOGGER.warning('%s: %s %s is not of type string, converting.',
                    operation, name, value)
    return str(value)


def _check_valid_length(value, name, operation):
    """
    Check value's length.

    :param key: value to be checked
    :type key: str
    :param name: name to inform the error
    :type feature: str
    :param operation: operation to inform the error
    :type operation: str
    :return: The result of validation
    :rtype: True|False
    """
    if len(value) > MAX_LENGTH:
        _LOGGER.error('%s: %s too long - must be %s characters or less.',
                      operation, name, MAX_LENGTH)
        return False
    return True


def _check_valid_object_key(key, name, operation):
    """
    Check if object key is valid for get_treatment/s when is sent as Key Object.

    :param key: key to be checked
    :type key: str
    :param name: name to be checked
    :type name: str
    :param operation: user operation
    :type operation: str
    :return: The result of validation
    :rtype: str|None
    """
    if key is None:
        _LOGGER.error(
            '%s: you passed a null %s, %s must be a non-empty string.',
            operation, name, name)
        return None
    if isinstance(key, str):
        if not _check_string_not_empty(key, name, operation):
            return None
    key_str = _check_can_convert(key, name, operation)
    if key_str is None or not _check_valid_length(key_str, name, operation):
        return None
    return key_str


def _remove_empty_spaces(value, operation):
    """
    Check if an string has whitespaces.

    :param value: value to be checked
    :type value: str
    :param operation: user operation
    :type operation: str
    :return: The result of trimming
    :rtype: str
    """
    strip_value = value.strip()
    if value != strip_value:
        _LOGGER.warning("%s: feature_name '%s' has extra whitespace, trimming.", operation, value)
    return strip_value


def validate_key(key, method_name):
    """
    Validate Key parameter for get_treatment/s.

    If the matching or bucketing key is invalid, will return None.

    :param key: user key
    :type key: mixed
    :param operation: user operation
    :type operation: str
    :return: The tuple key
    :rtype: (matching_key,bucketing_key)
    """
    matching_key_result = None
    bucketing_key_result = None
    if key is None:
        _LOGGER.error('%s: you passed a null key, key must be a non-empty string.', method_name)
        return None, None

    if isinstance(key, Key):
        matching_key_result = _check_valid_object_key(key.matching_key, 'matching_key', method_name)
        if matching_key_result is None:
            return None, None
        bucketing_key_result = _check_valid_object_key(key.bucketing_key, 'bucketing_key',
                                                       method_name)
        if bucketing_key_result is None:
            return None, None
    else:
        key_str = _check_can_convert(key, 'key', method_name)
        if key_str is not None and \
           _check_string_not_empty(key_str, 'key', method_name) and \
           _check_valid_length(key_str, 'key', method_name):
            matching_key_result = key_str
    return matching_key_result, bucketing_key_result


def validate_feature_name(feature_name, should_validate_existance, split_storage, method_name):
    """
    Check if feature_name is valid for get_treatment.

    :param feature_name: feature_name to be checked
    :type feature_name: str
    :return: feature_name
    :rtype: str|None
    """
    if (not _check_not_null(feature_name, 'feature_name', method_name)) or \
       (not _check_is_string(feature_name, 'feature_name', method_name)) or \
       (not _check_string_not_empty(feature_name, 'feature_name', method_name)):
        return None

    if should_validate_existance and split_storage.get(feature_name) is None:
        _LOGGER.warning(
            "%s: you passed \"%s\" that does not exist in this environment, "
            "please double check what Splits exist in the web console.",
            method_name,
            feature_name
        )
        return None

    return _remove_empty_spaces(feature_name, method_name)


def validate_track_key(key):
    """
    Check if key is valid for track.

    :param key: key to be checked
    :type key: str
    :return: key
    :rtype: str|None
    """
    if not _check_not_null(key, 'key', 'track'):
        return None
    key_str = _check_can_convert(key, 'key', 'track')
    if key_str is None or \
       (not _check_string_not_empty(key_str, 'key', 'track')) or \
       (not _check_valid_length(key_str, 'key', 'track')):
        return None
    return key_str


def validate_traffic_type(traffic_type, should_validate_existance, split_storage):
    """
    Check if traffic_type is valid for track.

    :param traffic_type: traffic_type to be checked
    :type traffic_type: str
    :param should_validate_existance: Whether to check for existante in the split storage.
    :type should_validate_existance: bool
    :param split_storage: Split storage.
    :param split_storage: splitio.storages.SplitStorage
    :return: traffic_type
    :rtype: str|None
    """
    if (not _check_not_null(traffic_type, 'traffic_type', 'track')) or \
       (not _check_is_string(traffic_type, 'traffic_type', 'track')) or \
       (not _check_string_not_empty(traffic_type, 'traffic_type', 'track')):
        return None
    if not traffic_type.islower():
        _LOGGER.warning('track: %s should be all lowercase - converting string to lowercase.',
                        traffic_type)
        traffic_type = traffic_type.lower()

    if should_validate_existance and not split_storage.is_valid_traffic_type(traffic_type):
        _LOGGER.warning(
            'track: Traffic Type %s does not have any corresponding Splits in this environment, '
            'make sure you\'re tracking your events to a valid traffic type defined '
            'in the Split console.',
            traffic_type
        )

    return traffic_type


def validate_event_type(event_type):
    """
    Check if event_type is valid for track.

    :param event_type: event_type to be checked
    :type event_type: str
    :return: event_type
    :rtype: str|None
    """
    if (not _check_not_null(event_type, 'event_type', 'track')) or \
       (not _check_is_string(event_type, 'event_type', 'track')) or \
       (not _check_string_not_empty(event_type, 'event_type', 'track')) or \
       (not _check_string_matches(event_type, 'track', EVENT_TYPE_PATTERN)):
        return None
    return event_type


def validate_value(value):
    """
    Check if value is valid for track.

    :param value: value to be checked
    :type value: number
    :return: value
    :rtype: number|None
    """
    if value is None:
        return None
    if (not isinstance(value, Number)) or isinstance(value, bool):
        _LOGGER.error('track: value must be a number.')
        return False
    return value


def validate_manager_feature_name(feature_name, should_validate_existance, split_storage):
    """
    Check if feature_name is valid for track.

    :param feature_name: feature_name to be checked
    :type feature_name: str
    :return: feature_name
    :rtype: str|None
    """
    if (not _check_not_null(feature_name, 'feature_name', 'split')) or \
       (not _check_is_string(feature_name, 'feature_name', 'split')) or \
       (not _check_string_not_empty(feature_name, 'feature_name', 'split')):
        return None

    if should_validate_existance and split_storage.get(feature_name) is None:
        _LOGGER.warning(
            "split: you passed \"%s\" that does not exist in this environment, "
            "please double check what Splits exist in the web console.",
            feature_name
        )
        return None

    return feature_name


def validate_features_get_treatments(  # pylint: disable=invalid-name
    method_name,
    features,
    should_validate_existance=False,
    split_storage=None
):
    """
    Check if features is valid for get_treatments.

    :param features: array of features
    :type features: list
    :return: filtered_features
    :rtype: tuple
    """
    if features is None or not isinstance(features, list):
        _LOGGER.error("%s: feature_names must be a non-empty array.", method_name)
        return None, None
    if not features:
        _LOGGER.error("%s: feature_names must be a non-empty array.", method_name)
        return None, None
    filtered_features = set(
        _remove_empty_spaces(feature, method_name) for feature in features
        if feature is not None and
        _check_is_string(feature, 'feature_name', method_name) and
        _check_string_not_empty(feature, 'feature_name', method_name)
    )
    if not filtered_features:
        _LOGGER.error("%s: feature_names must be a non-empty array.", method_name)
        return None, None

    if not should_validate_existance:
        return filtered_features, []

    valid_missing_features = set(f for f in filtered_features if split_storage.get(f) is None)
    for missing_feature in valid_missing_features:
        _LOGGER.warning(
            "%s: you passed \"%s\" that does not exist in this environment, "
            "please double check what Splits exist in the web console.",
            method_name,
            missing_feature
        )
    return filtered_features - valid_missing_features, valid_missing_features


def generate_control_treatments(features, method_name):
    """
    Generate valid features to control.

    :param features: array of features
    :type features: list
    :return: dict
    :rtype: dict|None
    """
    return {feature: (CONTROL, None) for feature in validate_features_get_treatments(method_name, features)[0]}


def validate_attributes(attributes, method_name):
    """
    Check if attributes is valid.

    :param attributes: dict
    :type attributes: dict
    :param operation: user operation
    :type operation: str
    :return: bool
    :rtype: True|False
    """
    if attributes is None:
        return True
    if not isinstance(attributes, dict):
        _LOGGER.error('%s: attributes must be of type dictionary.', method_name)
        return False
    return True


class _ApiLogFilter(logging.Filter):  # pylint: disable=too-few-public-methods
    def filter(self, record):
        return record.name not in ('SegmentsAPI', 'HttpClient')


def validate_apikey_type(segment_api):
    """
    Try to guess if the apikey is of browser type and let the user know.

    :param segment_api: Segments API client.
    :type segment_api: splitio.api.segments.SegmentsAPI
    """
    api_messages_filter = _ApiLogFilter()
    _logger = logging.getLogger('splitio.api.segments')
    try:
        _logger.addFilter(api_messages_filter)  # pylint: disable=protected-access
        segment_api.fetch_segment('__SOME_INVALID_SEGMENT__', -1, FetchOptions())
    except APIException as exc:
        if exc.status_code == 403:
            _LOGGER.error('factory instantiation: you passed a browser type '
                          + 'api_key, please grab an api key from the Split '
                          + 'console that is of type sdk')
            return False
    finally:
        _logger.removeFilter(api_messages_filter)  # pylint: disable=protected-access

    # True doesn't mean that the APIKEY is right, only that it's not of type "browser"
    return True


def validate_factory_instantiation(apikey):
    """
    Check if the factory if being instantiated with the appropriate arguments.

    :param apikey: str
    :type apikey: str
    :return: bool
    :rtype: True|False
    """
    if apikey == 'localhost':
        return True
    if (not _check_not_null(apikey, 'apikey', 'factory_instantiation')) or \
       (not _check_is_string(apikey, 'apikey', 'factory_instantiation')) or \
       (not _check_string_not_empty(apikey, 'apikey', 'factory_instantiation')):
        return False
    return True


def valid_properties(properties):
    """
    Check if properties is a valid dict and returns the properties
    that will be sent to the track method, avoiding unexpected types.

    :param properties: dict
    :type properties: dict
    :return: tuple
    :rtype: (bool,dict,int)
    """
    size = 1024  # We assume 1kb events without properties (750 bytes avg measured)

    if properties is None:
        return True, None, size
    if not isinstance(properties, dict):
        _LOGGER.error('track: properties must be of type dictionary.')
        return False, None, 0

    valid_properties = dict()

    for property, element in properties.items():
        if not isinstance(property, str):  # Exclude property if is not string
            continue

        valid_properties[property] = None
        size += len(property)

        if element is None:
            continue

        if not isinstance(element, str) and not isinstance(element, Number) \
           and not isinstance(element, bool):
            _LOGGER.warning('Property %s is of invalid type. Setting value to None', element)
            element = None

        valid_properties[property] = element

        if isinstance(element, str):
            size += len(element)

        if size > MAX_PROPERTIES_LENGTH_BYTES:
            _LOGGER.error(
                'The maximum size allowed for the properties is 32768 bytes. ' +
                'Current one is ' + str(size) + ' bytes. Event not queued'
            )
            return False, None, size

    if len(valid_properties.keys()) > 300:
        _LOGGER.warning('Event has more than 300 properties. Some of them will be trimmed' +
                        ' when processed')
    return True, valid_properties if len(valid_properties) else None, size
