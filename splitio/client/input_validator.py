"""Input validation module."""
from numbers import Number
import logging
import re
import math
import inspect

from splitio.client.key import Key
from splitio.engine.evaluator import CONTROL


_LOGGER = logging.getLogger(__name__)
MAX_LENGTH = 250
EVENT_TYPE_PATTERN = r'^[a-zA-Z0-9][-_.:a-zA-Z0-9]{0,79}$'
MAX_PROPERTIES_LENGTH_BYTES = 32768
_FLAG_SETS_REGEX = '^[a-z0-9][_a-z0-9]{0,49}$'


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


def _check_string_matches(value, operation, pattern, name, length):
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
    if re.search(pattern, value) is None or re.search(pattern, value).group() != value:
        _LOGGER.error(
            '%s: you passed %s, %s must ' +
            'adhere to the regular expression %s. ' +
            'This means %s must be alphanumeric, cannot be more ' +
            'than %s characters long, and can only include a dash, underscore, ' +
            'period, or colon as separators of alphanumeric characters.',
            operation, value, name, pattern, name, length
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
    if not _check_not_null(key, name, operation):
        return None

    if isinstance(key, str):
        if not _check_string_not_empty(key, name, operation):
            return None

    key_str = _check_can_convert(key, name, operation)
    if key_str is None or not _check_valid_length(key_str, name, operation):
        return None

    return key_str


def _remove_empty_spaces(value, name, operation):
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
        _LOGGER.warning("%s: %s '%s' has extra whitespace, trimming.", operation, name, value)
    return strip_value

def _convert_str_to_lower(value, name, operation):
    lower_value = value.lower()
    if value != lower_value:
        _LOGGER.warning("%s: %s '%s' should be all lowercase - converting string to lowercase", operation, name, value)
    return lower_value


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
    if not _check_not_null(key, 'key', method_name):
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


def _validate_feature_flag_name(feature_flag_name, method_name):
    if (not _check_not_null(feature_flag_name, 'feature_flag_name', method_name)) or \
       (not _check_is_string(feature_flag_name, 'feature_flag_name', method_name)) or \
       (not _check_string_not_empty(feature_flag_name, 'feature_flag_name', method_name)):
        return False

    return True


def validate_feature_flag_name(feature_flag_name, method_name):
    """
    Check if feature flag name is valid for get_treatment.

    :param feature_flag_name: feature flag name to be checked
    :type feature_flag_name: str
    :return: feature_flag_name
    :rtype: str|None
    """
    if not _validate_feature_flag_name(feature_flag_name, method_name):
        return None

    return _remove_empty_spaces(feature_flag_name, 'feature flag name', method_name)

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


def _validate_traffic_type_value(traffic_type):
    if (not _check_not_null(traffic_type, 'traffic_type', 'track')) or \
       (not _check_is_string(traffic_type, 'traffic_type', 'track')) or \
       (not _check_string_not_empty(traffic_type, 'traffic_type', 'track')):
        return False

    return True

def validate_traffic_type(traffic_type, should_validate_existance, feature_flag_storage):
    """
    Check if traffic_type is valid for track.

    :param traffic_type: traffic_type to be checked
    :type traffic_type: str
    :param should_validate_existance: Whether to check for existante in the feature flag storage.
    :type should_validate_existance: bool
    :param feature_flag_storage: Feature flag storage.
    :param feature_flag_storage: splitio.storages.SplitStorage
    :return: traffic_type
    :rtype: str|None
    """
    if not _validate_traffic_type_value(traffic_type):
        return None

    traffic_type = _convert_str_to_lower(traffic_type, 'traffic type', 'track')

    if should_validate_existance and not feature_flag_storage.is_valid_traffic_type(traffic_type):
        _LOGGER.warning(
            'track: Traffic Type %s does not have any corresponding Feature flags in this environment, '
            'make sure you\'re tracking your events to a valid traffic type defined '
            'in the Split user interface.',
            traffic_type
        )

    return traffic_type


async def validate_traffic_type_async(traffic_type, should_validate_existance, feature_flag_storage):
    """
    Check if traffic_type is valid for track.

    :param traffic_type: traffic_type to be checked
    :type traffic_type: str
    :param should_validate_existance: Whether to check for existante in the feature flag storage.
    :type should_validate_existance: bool
    :param feature_flag_storage: Feature flag storage.
    :param feature_flag_storage: splitio.storages.SplitStorage
    :return: traffic_type
    :rtype: str|None
    """
    if not _validate_traffic_type_value(traffic_type):
        return None

    traffic_type = _convert_str_to_lower(traffic_type, 'traffic type', 'track')

    if should_validate_existance and not await feature_flag_storage.is_valid_traffic_type(traffic_type):
        _LOGGER.warning(
            'track: Traffic Type %s does not have any corresponding Feature flags in this environment, '
            'make sure you\'re tracking your events to a valid traffic type defined '
            'in the Split user interface.',
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
       (not _check_string_matches(event_type, 'track', EVENT_TYPE_PATTERN, 'an event name', 80)):
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

def validate_manager_feature_flag_name(feature_flag_name, should_validate_existance, feature_flag_storage):
    """
    Check if feature flag name is valid for track.

    :param feature_flag_name: feature flag name to be checked
    :type feature_flag_name: str
    :return: feature_flag_name
    :rtype: str|None
    """
    if not _validate_feature_flag_name(feature_flag_name, 'split'):
        return None

    feature_flag = feature_flag_storage.get(feature_flag_name)
    if should_validate_existance and feature_flag is None:
        _LOGGER.warning(
            "split: you passed \"%s\" that does not exist in this environment, "
            "please double check what Feature flags exist in the Split user interface.",
            feature_flag_name
        )
        return None

    return feature_flag

async def validate_manager_feature_flag_name_async(feature_flag_name, should_validate_existance, feature_flag_storage):
    """
    Check if feature flag name is valid for track.

    :param feature_flag_name: feature flag name to be checked
    :type feature_flag_name: str
    :return: feature_flag_name
    :rtype: str|None
    """
    if not _validate_feature_flag_name(feature_flag_name, 'split'):
        return None

    feature_flag = await feature_flag_storage.get(feature_flag_name)
    if should_validate_existance and feature_flag is None:
        _LOGGER.warning(
            "split: you passed \"%s\" that does not exist in this environment, "
            "please double check what Feature flags exist in the Split user interface.",
            feature_flag_name
        )
        return None

    return feature_flag

def validate_feature_flag_names(feature_flags, method_name):
    """
    Check if feature flag name is valid for track.

    :param feature_flag_name: feature flag name to be checked
    :type feature_flag_name: str
    """
    for feature_flag in  feature_flags.keys():
        if feature_flags[feature_flag] is None:
            _LOGGER.warning(
                "%s: you passed \"%s\" that does not exist in this environment, "
                "please double check what Feature flags exist in the Split user interface.",
                method_name, feature_flag
            )

def _check_feature_flag_instance(feature_flags, method_name):
    if feature_flags is None or not isinstance(feature_flags, list):
        _LOGGER.error("%s: feature flag names must be a non-empty array.", method_name)
        return False

    if not feature_flags:
        _LOGGER.error("%s: feature flag names must be a non-empty array.", method_name)
        return False

    return True


def _get_filtered_feature_flag(feature_flags, method_name):
    return set(
        _remove_empty_spaces(feature_flag, 'feature flag name', method_name) for feature_flag in feature_flags
        if feature_flag is not None and
        _check_is_string(feature_flag, 'feature flag name', method_name) and
        _check_string_not_empty(feature_flag, 'feature flag name', method_name)
    )


def validate_feature_flags_get_treatments(  # pylint: disable=invalid-name
    method_name,
    feature_flag_names,
    ):
    """
    Check if feature flags is valid for get_treatments.

    :param feature_flags: array of feature flags
    :type feature_flags: list
    :return: filtered_feature_flags
    :rtype: tuple
    """
    if not _check_feature_flag_instance(feature_flag_names, method_name):
        return None

    filtered_feature_flags = _get_filtered_feature_flag(feature_flag_names, method_name)
    if not filtered_feature_flags:
        _LOGGER.error("%s: feature flag names must be a non-empty array.", method_name)
        return None

    valid_feature_flags = []
    for ff in filtered_feature_flags:
        ff = _remove_empty_spaces(ff, 'feature flag name', method_name)
        valid_feature_flags.append(ff)
    return valid_feature_flags

def generate_control_treatments(feature_flags):
    """
    Generate valid feature flags to control.

    :param feature_flags: array of feature flags
    :type feature_flags: list
    :return: dict
    :rtype: dict|None
    """
    if not isinstance(feature_flags, list):
        return {}

    to_return = {}
    for feature_flag in feature_flags:
        if isinstance(feature_flag, str) and len(feature_flag.strip())> 0:
            to_return[feature_flag] = (CONTROL, None)
    return to_return


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


def validate_factory_instantiation(sdk_key):
    """
    Check if the factory if being instantiated with the appropriate arguments.

    :param sdk_key: str
    :type sdk_key: str
    :return: bool
    :rtype: True|False
    """
    if sdk_key == 'localhost':
        return True

    if (not _check_not_null(sdk_key, 'sdk_key', 'factory_instantiation')) or \
       (not _check_is_string(sdk_key, 'sdk_key', 'factory_instantiation')) or \
       (not _check_string_not_empty(sdk_key, 'sdk_key', 'factory_instantiation')):
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

def validate_pluggable_adapter(config):
    """
    Check if pluggable adapter contains the expected method signature

    :param config: config parameters
    :type config: Dict

    :return: True if no issue found otherwise False
    :rtype: bool
    """
    if config.get('storageType') != 'pluggable':
        return True

    if config.get('storageWrapper') is None:
        _LOGGER.error("Expecting pluggable storage `wrapper` in options, but no valid wrapper instance was provided.")
        return False

    if config.get('storagePrefix') is not None:
        if not isinstance(config.get('storagePrefix'), str):
            _LOGGER.error("Pluggable storage prefix should be string type only")
            return False

    pluggable_adapter = config.get('storageWrapper')
    if not isinstance(pluggable_adapter, object):
        _LOGGER.error("Pluggable storage instance is not inherted from object class")
        return False

    expected_methods = {'get': 1, 'get_items': 1, 'get_many': 1, 'set': 2, 'push_items': 2,
                        'delete': 1, 'increment': 2, 'decrement': 2, 'get_keys_by_prefix': 1,
                        'get_many': 1, 'add_items' : 2, 'remove_items': 2, 'item_contains': 2,
                        'get_items_count': 1, 'expire': 2}
    methods = inspect.getmembers(pluggable_adapter, predicate=inspect.ismethod)
    for exp_method in expected_methods:
        method_found = False
        get_method_args = set()
        for method in methods:
            if exp_method == method[0]:
                method_found = True
                get_method_args = inspect.signature(method[1]).parameters
                break

        if not method_found:
            _LOGGER.error("Pluggable adapter does not have required method: %s" % exp_method)
            return False

        if len(get_method_args) < expected_methods[exp_method]:
            _LOGGER.error("Pluggable adapter method %s has less than required arguments count: %s : " % (exp_method, len(get_method_args)))
            return False

    return True

def validate_flag_sets(flag_sets, method_name):
    """
    Validate flag sets list
    :param flag_set: list of flag sets
    :type flag_set: list[str]
    :returns: Sanitized and sorted flag sets
    :rtype: list[str]
    """
    if not isinstance(flag_sets, list):
        _LOGGER.warning("%s: flag sets parameter type should be list object, parameter is discarded", method_name)
        return []

    sanitized_flag_sets = set()
    for flag_set in flag_sets:
        if not _check_not_null(flag_set, 'flag set', method_name):
            continue

        if not _check_is_string(flag_set, 'flag set', method_name):
            continue

        flag_set = _remove_empty_spaces(flag_set, 'flag set', method_name)
        flag_set = _convert_str_to_lower(flag_set, 'flag set', method_name)

        if not _check_string_matches(flag_set, method_name, _FLAG_SETS_REGEX, 'a flag set', 50):
            continue

        sanitized_flag_sets.add(flag_set)

    return list(sanitized_flag_sets)
