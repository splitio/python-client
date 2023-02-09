import hashlib
import logging

_LOGGER = logging.getLogger(__name__)

def _get_sha(fetched):
    """
    Return sha256 of given string.

    :param fetched: string variable
    :type fetched: str

    :return: hex representation of sha256
    :rtype: str
    """
    return hashlib.sha256(fetched.encode()).hexdigest()

def _sanitize_object_element(object, object_name, element_name, default_value, lower_value=None, upper_value=None, in_list=None, not_in_list=None):
    """
    Sanitize specific object element.

    :param object: split or segment dict object
    :type object: Dict
    :param element_name: element name
    :type element_name: str
    :param default_value: element default value
    :type default_value: any
    :param lower_value: Optional, element lower value limit
    :type lower_value: any
    :param upper_value: Optional, element upper value limit
    :type upper_value: any
    :param in_list: Optional, list of values expected in element
    :type in_list: [any]
    :param not_in_list: Optional, list of values not expected in element
    :type not_in_list: [any]

    :return: sanitized object
    :rtype: Dict
    """
    if element_name not in object or object[element_name] is None:
            object[element_name] = default_value
            _LOGGER.debug("Sanitized element [%s] to '%s' in %s: %s.", element_name, default_value, object_name, object['name'])
    if lower_value is not None and upper_value is not None:
        if object[element_name] < lower_value or object[element_name] > upper_value:
            object[element_name] = default_value
            _LOGGER.debug("Sanitized element [%s] to '%s' in %s: %s.", element_name, default_value, object_name, object['name'])
    elif lower_value is not None:
        if object[element_name] < lower_value:
            object[element_name] = default_value
            _LOGGER.debug("Sanitized element [%s] to '%s' in %s: %s.", element_name, default_value, object_name, object['name'])
    elif upper_value is not None:
        if object[element_name] > upper_value:
            object[element_name] = default_value
            _LOGGER.debug("Sanitized element [%s] to '%s' in %s: %s.", element_name, default_value, object_name, object['name'])
    if in_list is not None:
        if object[element_name] not in in_list:
            object[element_name] = default_value
            _LOGGER.debug("Sanitized element [%s] to '%s' in %s: %s.", element_name, default_value, object_name, object['name'])
    if not_in_list is not None:
        if object[element_name] in not_in_list:
            object[element_name] = default_value
            _LOGGER.debug("Sanitized element [%s] to '%s' in %s: %s.", element_name, default_value, object_name, object['name'])

    return object
