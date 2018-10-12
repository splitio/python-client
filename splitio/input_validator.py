from __future__ import absolute_import, division, print_function, \
    unicode_literals

from numbers import Number
import logging
import six
import re
from splitio.key import Key

_LOGGER = logging.getLogger(__name__)


def _check_not_null(value, name, operation):
    """
    Checks if value is null

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
        _LOGGER.error('{}: {} cannot be None.'.format(operation, name))
        return False
    return True


def _check_is_string(value, name, operation):
    """
    Checks if value is not string

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
        _LOGGER.error('{}: {} has to be of type string.'.format(
                      operation, name))
        return False
    return True


def _check_string_not_empty(value, name, operation):
    """
    Checks if value is an empty string

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
        _LOGGER.error('{}: {} must not be empty.'.format(operation, name))
        return False
    return True


def _check_string_matches(value, name, operation, pattern):
    """
    Checks if value is adhere to a regular expression passed

    :param key: value to be checked
    :type key: str
    :param name: name to inform the error
    :type feature: str
    :param operation: operation to inform the error
    :type operation: str
    :param pattern: pattern that needs to adhere
    :type pattern: str
    :return: The result of validation
    :rtype: True|False
    """
    if not re.match(pattern, value):
        _LOGGER.error('{}: {} must adhere to the regular expression {}.'
                      .format(operation, name, pattern))
        return False
    return True


def _check_can_convert(value, name, operation, message):
    """
    Checks if is a valid convertion.

    :param key: value to be checked
    :type key: bool|number|array|
    :param name: name to inform the error
    :type feature: str
    :param operation: operation to inform the error
    :type operation: str
    :param message: message to inform the error
    :type message: str
    :return: The result of validation
    :rtype: True|False
    """
    if isinstance(value, six.string_types):
        return True
    else:
        if isinstance(value, bool) or (not isinstance(value, Number)):
            _LOGGER.error('{}: {} {}'.format(operation, name, message))
            return False
    _LOGGER.warning('{}: {} {} is not of type string, converting.'
                    .format(operation, name, value))
    return True


def _check_valid_matching_key(matching_key):
    """
    Checks if matching_key is valid for get_treatment when is
    sent as Key Object

    :param matching_key: matching_key to be checked
    :type matching_key: str
    :return: The result of validation
    :rtype: True|False
    """
    if matching_key is None:
        _LOGGER.error('get_treatment: Key should be an object with bucketingKey and '
                      'matchingKey with valid string properties.')
        return False
    if isinstance(matching_key, six.string_types):
        if not _check_string_not_empty(matching_key, 'matching_key', 'get_treatment'):
            return False
    else:
        if not _check_can_convert(matching_key, 'matching_key', 'get_treatment',
                                  'has to be of type string.'):
            return False
    return True


def _check_valid_bucketing_key(bucketing_key):
    """
    Checks if bucketing_key is valid for get_treatment when is
    sent as Key Object

    :param bucketing_key: bucketing_key to be checked
    :type bucketing_key: str
    :return: The result of validation
    :rtype: True|False
    """
    if bucketing_key is None:
        _LOGGER.warning('get_treatment: Key object should have bucketingKey set.')
        return None
    if not _check_can_convert(bucketing_key, 'bucketing_key', 'get_treatment',
                              'has to be of type string.'):
        return False
    return str(bucketing_key)


def validate_key(key):
    """
    Validate Key parameter for get_treatment, if is invalid at some point
    the bucketing_key or matching_key it will return None

    :param key: user key
    :type key: mixed
    :return: The tuple key
    :rtype: (matching_key,bucketing_key)
    """
    matching_key_result = None
    bucketing_key_result = None
    if not _check_not_null(key, 'key', 'get_treatment'):
        return None, None
    if isinstance(key, Key):
        if _check_valid_matching_key(key.matching_key):
            matching_key_result = str(key.matching_key)
        else:
            return None, None
        bucketing_key_result = _check_valid_bucketing_key(key.bucketing_key)
        if bucketing_key_result is False:
            return None, None
        return matching_key_result, bucketing_key_result
    else:
        if _check_can_convert(key, 'key', 'get_treatment',
                              'has to be of type string or object Key.'):
            matching_key_result = str(key)
    return matching_key_result, bucketing_key_result


def validate_feature_name(feature_name):
    """
    Checks if feature_name is valid for get_treatment

    :param feature_name: feature_name to be checked
    :type feature_name: str
    :return: feature_name
    :rtype: str|None
    """
    if (not _check_not_null(feature_name, 'feature_name', 'get_treatment')) or \
       (not _check_is_string(feature_name, 'feature_name', 'get_treatment')):
        return None
    return feature_name


def validate_track_key(key):
    """
    Checks if key is valid for track

    :param key: key to be checked
    :type key: str
    :return: key
    :rtype: str|None
    """
    if (not _check_not_null(key, 'key', 'track')) or \
       (not _check_can_convert(key, 'key', 'track', 'has to be of type string.')):
        return None
    return str(key)


def validate_traffic_type(traffic_type):
    """
    Checks if traffic_type is valid for track

    :param traffic_type: traffic_type to be checked
    :type traffic_type: str
    :return: traffic_type
    :rtype: str|None
    """
    if (not _check_not_null(traffic_type, 'traffic_type', 'track')) or \
       (not _check_is_string(traffic_type, 'traffic_type', 'track')) or \
       (not _check_string_not_empty(traffic_type, 'traffic_type', 'track')):
        return None
    return traffic_type


def validate_event_type(event_type):
    """
    Checks if event_type is valid for track

    :param event_type: event_type to be checked
    :type event_type: str
    :return: event_type
    :rtype: str|None
    """
    if (not _check_not_null(event_type, 'event_type', 'track')) or \
       (not _check_is_string(event_type, 'event_type', 'track')) or \
       (not _check_string_matches(event_type, 'event_type', 'track',
                                  r'[a-zA-Z0-9][-_\.a-zA-Z0-9]{0,62}')):
        return None
    return event_type


def validate_value(value):
    """
    Checks if value is valid for track

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
    Checks if feature_name is valid for track

    :param feature_name: feature_name to be checked
    :type feature_name: str
    :return: feature_name
    :rtype: str|None
    """
    if (not _check_not_null(feature_name, 'feature_name', 'split')) or \
       (not _check_is_string(feature_name, 'feature_name', 'split')):
        return None
    return feature_name


def validate_features_get_treatments(features):
    """
    Checks if features is valid for get_treatments

    :param features: array of features
    :type features: list
    :return: filtered_features
    :rtype: list|None
    """
    if not _check_not_null(features, 'features', 'get_treatments'):
        return None
    if not isinstance(features, list):
        _LOGGER.error('get_treatments: features must be a list.')
        return None
    filtered_features = set(filter(lambda x: x is not None and
                                   _check_is_string(x, 'feature_name', 'get_treatments'), features))
    if len(filtered_features) == 0:
        _LOGGER.warning('get_treatments: features is an empty list or has None values.')
    return filtered_features
