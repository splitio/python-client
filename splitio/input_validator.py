from __future__ import absolute_import, division, print_function, \
    unicode_literals

from numbers import Number
import logging
import six
import re
from splitio.key import Key


class InputException(Exception):
    pass


class InputValidator:
    """
    Input Validator for get_treatment, track and split
    """
    def __init__(self):
        """
        Construct InputValidator and attaching logger
        """
        self._logger = logging.getLogger(self.__class__.__name__)

    def _check_not_null(self, value, name, operation):
        """
        Checks if value is null and raises an Exception if that is the case

        :param key: value to be checked
        :type key: str
        :param name: name to inform the error
        :type feature: str
        :param operation: operation to inform the error
        :type operation: str
        """
        if value is None:
            raise InputException('{}: {} cannot be None.'.format(operation, name))
        pass

    def _check_is_string(self, value, name, operation):
        """
        Checks if value is not string and raises an Exception if that is the case

        :param key: value to be checked
        :type key: str
        :param name: name to inform the error
        :type feature: str
        :param operation: operation to inform the error
        :type operation: str
        """
        if isinstance(value, six.string_types) is False:
            raise InputException('{}: {} {} has to be of type string.'.format(
                                             operation, name, value))
        pass

    def _check_not_empty(self, value, name, operation):
        """
        Checks if value is an empty string and raises an Exception if that is the case

        :param key: value to be checked
        :type key: str
        :param name: name to inform the error
        :type feature: str
        :param operation: operation to inform the error
        :type operation: str
        """
        if not value:
            raise InputException('{}: {} must not be empty.'.format(operation, name))
        pass

    def _check_pattern_match(self, value, name, operation, pattern):
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
        """
        if not re.match(pattern, value):
            raise InputException('{}: {} must adhere to the regular expression {}.'
                                 .format(operation, name, pattern))
        pass

    def _to_string(self, value, name, operation, message):
        """
        Transforms value into string if is only a number, if is other type
        it will raises an Exception

        :param key: value to be checked
        :type key: bool|number|array|
        :param name: name to inform the error
        :type feature: str
        :param operation: operation to inform the error
        :type operation: str
        :param message: message to inform the error
        :type message: str
        """
        if not isinstance(value, bool) and isinstance(value, Number):
            self._logger.warning('{}: {} {} is not of type string, converting.'
                                 .format(operation, name, value))
            return str(value)
        raise InputException('{}: {} {} {}'.format(operation, name, value, message))

    def _validate_matching_key(self, matching_key):
        """
        Checks if matching_key is valid for get_treatment when is
        sent as Key Object

        :param matching_key: matching_key to be checked
        :type matching_key: str
        """
        try:
            if matching_key is None:
                raise InputException('get_treatment: Key should be an object with bucketingKey and '
                                     'matchingKey with valid string properties.')
            if isinstance(matching_key, six.string_types):
                self._check_not_empty(matching_key, 'matching_key', 'get_treatment')
                return matching_key
            return self._to_string(matching_key, 'matching_key', 'get_treatment',
                                   'has to be of type string.')
        except InputException as e:
            raise InputException(e.message)

    def _validate_bucketing_key(self, bucketing_key):
        """
        Checks if bucketing_key is valid for get_treatment when is
        sent as Key Object

        :param bucketing_key: bucketing_key to be checked
        :type bucketing_key: str
        """
        try:
            if bucketing_key is None:
                self._logger.warning('get_treatment: Key object should have bucketingKey set.')
                return None
            if isinstance(bucketing_key, six.string_types):
                return bucketing_key
            return self._to_string(bucketing_key, 'bucketing_key', 'get_treatment',
                                   'has to be of type string.')
        except InputException as e:
            raise InputException(e.message)

    def validate_key(self, key):
        """
        Validate Key parameter for get_treatment, if is invalid at some point
        the bucketing_key or matching_key it will return None

        :param key: user key
        :type key: mixed
        """
        try:
            self._check_not_null(key, 'key', 'get_treatment')
            if isinstance(key, Key):
                matching_key = self._validate_matching_key(key.matching_key)
                bucketing_key = self._validate_bucketing_key(key.bucketing_key)
            else:
                if isinstance(key, six.string_types):
                    matching_key = key
                else:
                    matching_key = self._to_string(key, 'key', 'get_treatment',
                                                   'has to be of type string or object Key.')
                bucketing_key = None
            return matching_key, bucketing_key
        except InputException as e:
            self._logger.error(e.message)
            return None, None

    def validate_feature_name(self, feature_name):
        """
        Checks if feature_name is valid for get_treatment

        :param feature_name: feature_name to be checked
        :type feature_name: str
        """
        try:
            self._check_not_null(feature_name, 'feature_name', 'get_treatment')
            self._check_is_string(feature_name, 'feature_name', 'get_treatment')
            return feature_name
        except InputException as e:
            self._logger.error(e.message)
            return None

    def validate_track_key(self, key):
        """
        Checks if key is valid for track

        :param key: key to be checked
        :type key: str
        """
        try:
            self._check_not_null(key, 'key', 'track')
            if isinstance(key, six.string_types):
                return key
            return self._to_string(key, 'key', 'track', 'has to be of type string.')
        except InputException as e:
            self._logger.error(e.message)
            return None

    def validate_traffic_type(self, traffic_type):
        """
        Checks if traffic_type is valid for track

        :param traffic_type: traffic_type to be checked
        :type traffic_type: str
        """
        try:
            self._check_not_null(traffic_type, 'traffic_type', 'track')
            self._check_is_string(traffic_type, 'traffic_type', 'track')
            self._check_not_empty(traffic_type, 'traffic_type', 'track')
            return traffic_type
        except InputException as e:
            self._logger.error(e.message)
            return None

    def validate_event_type(self, event_type):
        """
        Checks if event_type is valid for track

        :param event_type: event_type to be checked
        :type event_type: str
        """
        try:
            self._check_not_null(event_type, 'event_type', 'track')
            self._check_is_string(event_type, 'event_type', 'track')
            self._check_pattern_match(event_type, 'event_type', 'track',
                                      r'[a-zA-Z0-9][-_\.a-zA-Z0-9]{0,62}')
            return event_type
        except InputException as e:
            self._logger.error(e.message)
            return None

    def validate_value(self, value):
        """
        Checks if value is valid for track

        :param value: value to be checked
        :type value: number
        """
        try:
            self._check_not_null(value, 'value', 'track')
            if not isinstance(value, Number) or isinstance(value, bool):
                self._logger.error('track: value must be a number.')
                return None
            return value
        except InputException as e:
            self._logger.error(e.message)
            return None

    def validate_manager_feature_name(self, feature_name):
        """
        Checks if feature_name is valid for track

        :param feature_name: feature_name to be checked
        :type feature_name: str
        """
        try:
            self._check_not_null(feature_name, 'feature_name', 'split')
            self._check_is_string(feature_name, 'feature_name', 'split')
            return feature_name
        except InputException as e:
            self._logger.error(e.message)
            return None
