"""Input validation module."""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

from numbers import Number
import logging
import re
import math

import six

from splitio.api import APIException
from splitio.client.key import Key
from splitio.client.util import get_calls
from splitio.engine.evaluator import CONTROL


_LOGGER = logging.getLogger(__name__)
MAX_LENGTH = 250
EVENT_TYPE_PATTERN = r'^[a-zA-Z0-9][-_.:a-zA-Z0-9]{0,79}$'


def _get_first_split_sdk_call():
    """
    Get the method name of the original call on the SplitClient methods.

    :return: Name of the method called by the user.
    :rtype: str
    """
    unknown_method = 'unknown-method'
    try:
        calls = get_calls(['Client', 'SplitManager'])
        if calls:
            return calls[-1]
        return unknown_method
    except Exception:  #pylint: disable=broad-except
        return unknown_method

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
    if isinstance(value, six.string_types) is False:
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
    if isinstance(value, six.string_types):
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
    if isinstance(key, six.string_types):
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


def validate_key(key):
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
    operation = _get_first_split_sdk_call()
    matching_key_result = None
    bucketing_key_result = None
    if key is None:
        _LOGGER.error('%s: you passed a null key, key must be a non-empty string.', operation)
        return None, None

    if isinstance(key, Key):
        matching_key_result = _check_valid_object_key(key.matching_key, 'matching_key', operation)
        if matching_key_result is None:
            return None, None
        bucketing_key_result = _check_valid_object_key(key.bucketing_key, 'bucketing_key',
                                                       operation)
        if bucketing_key_result is None:
            return None, None
    else:
        key_str = _check_can_convert(key, 'key', operation)
        if key_str is not None and \
           _check_string_not_empty(key_str, 'key', operation) and \
           _check_valid_length(key_str, 'key', operation):
            matching_key_result = key_str
    return matching_key_result, bucketing_key_result


def validate_feature_name(feature_name):
    """
    Check if feature_name is valid for get_treatment.

    :param feature_name: feature_name to be checked
    :type feature_name: str
    :return: feature_name
    :rtype: str|None
    """
    operation = _get_first_split_sdk_call()
    if (not _check_not_null(feature_name, 'feature_name', operation)) or \
       (not _check_is_string(feature_name, 'feature_name', operation)) or \
       (not _check_string_not_empty(feature_name, 'feature_name', operation)):
        return None
    return _remove_empty_spaces(feature_name, operation)


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


def validate_traffic_type(traffic_type):
    """
    Check if traffic_type is valid for track.

    :param traffic_type: traffic_type to be checked
    :type traffic_type: str
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


def validate_manager_feature_name(feature_name):
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
    return feature_name


def validate_features_get_treatments(features):  #pylint: disable=invalid-name
    """
    Check if features is valid for get_treatments.

    :param features: array of features
    :type features: list
    :return: filtered_features
    :rtype: list|None
    """
    operation = _get_first_split_sdk_call()
    if features is None or not isinstance(features, list):
        _LOGGER.error("%s: feature_names must be a non-empty array.", operation)
        return None
    if not features:
        _LOGGER.error("%s: feature_names must be a non-empty array.", operation)
        return []
    filtered_features = set(
        _remove_empty_spaces(feature, operation) for feature in features
        if feature is not None and
        _check_is_string(feature, 'feature_name', operation) and
        _check_string_not_empty(feature, 'feature_name', operation)
    )
    if not filtered_features:
        _LOGGER.error("%s: feature_names must be a non-empty array.", operation)
        return None
    return filtered_features


def generate_control_treatments(features):
    """
    Generate valid features to control.

    :param features: array of features
    :type features: list
    :return: dict
    :rtype: dict|None
    """
    return {feature: (CONTROL, None) for feature in validate_features_get_treatments(features)}


def validate_attributes(attributes):
    """
    Check if attributes is valid.

    :param attributes: dict
    :type attributes: dict
    :param operation: user operation
    :type operation: str
    :return: bool
    :rtype: True|False
    """
    operation = _get_first_split_sdk_call()
    if attributes is None:
        return True
    if not isinstance(attributes, dict):
        _LOGGER.error('%s: attributes must be of type dictionary.', operation)
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
    try:
        segment_api._logger.addFilter(api_messages_filter)  #pylint: disable=protected-access
        segment_api.fetch_segment('__SOME_INVALID_SEGMENT__', -1)
    except APIException as exc:
        if exc.status_code == 403:
            _LOGGER.error('factory instantiation: you passed a browser type '
                          + 'api_key, please grab an api key from the Split '
                          + 'console that is of type sdk')
            return False
    finally:
        segment_api._logger.removeFilter(api_messages_filter)  #pylint: disable=protected-access

    # True doesn't mean that the APIKEY is right, only that it's not of type "browser"
    return True


def validate_factory_instantiation(apikey):
    """
    Check if the factory if being instantiated with the appropriate arguments.

    :param apikey: str
    :type apikey: str
    :param config: dict
    :type config: dict
    :param segment_api: Segment API client
    :type segment_api: splitio.api.segments.SegmentsAPI
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
