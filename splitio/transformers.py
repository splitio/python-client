from __future__ import absolute_import, division, print_function, unicode_literals

import logging

from builtins import int

from arrow import Arrow

logger = logging.getLogger(__name__)


class TransformMixin(object):
    """Base for all transform mixins"""
    def _transform_key(self, value):
        """
        Transforms a value. Subclasses need to implement this method
        :param value: The value to transform
        :type value: any
        :return: The transformed value
        :rtype: any
        """
        return value

    def transform_key(self, value):
        """
        Transforms a value
        :param value: The value to transform
        :type value: any
        :return: The transformed value
        :rtype: any
        """
        try:
            return self._transform_key(value)
        except:
            logger.exception('Exception caught transforming value. value = %s', value)

    def _transform_condition_parameter(self, source_value):
        """
        Transforms a condition parameter. Subclasses need to implement this method
        :param value: The value to transform
        :type value: any
        :return: The transformed value
        :rtype: any
        """
        return self._transform_key(source_value)

    def transform_condition_parameter(self, source_value):
        """
        Transforms a condition parameter
        :param value: The value to transform
        :type value: any
        :return: The transformed value
        :rtype: any
        """
        try:
            return self._transform_condition_parameter(source_value)
        except:
            logger.exception('Exception caught transforming source value. source_value = %s',
                             source_value)


class AsNumberTransformMixin(TransformMixin):
    """Mixin to allow transforming values to int (long)"""
    def _transform_key(self, value):
        """
        Transforms value to int (long in Python2)
        :param value: Any value suitable to be transformed
        :type value:
        :return: The value transformed to int
        :rtype: int
        """
        if value is None:
            return None

        return int(value)


class AsDateHourMinuteTimestampTransformMixin(TransformMixin):
    """Mixin to allow truncating timestamp to the minute"""
    def _transform_key(self, value):
        """
        Truncates seconds and milliseconds from a long value of seconds from epoch
        :param value: An int value representing the number of seconds from epoch (timestmap)
        :type value: int
        :return: The value truncated to minutes
        :rtype: int
        """
        if value is None:
            return None

        return Arrow.utcfromtimestamp(value).replace(second=0, microsecond=0).timestamp * 1000

    def _transform_condition_parameter(self, source_value):
        """
        Truncates seconds and milliseconds from a long value of milliseconds from epoch
        :param value: An int value representing the number of milliseconds from epoch (timestmap)
        :type value: int
        :return: The value truncated to minutes
        :rtype: int
        """
        if source_value is None:
            return None

        return Arrow.utcfromtimestamp(source_value // 1000).replace(second=0,
                                                                    microsecond=0).timestamp * 1000


class AsDateTimestampTransformMixin(TransformMixin):
    """Mixin to allow truncating timestamp to midnight"""
    def _transform_key(self, value):
        """
        Truncates seconds and milliseconds from a long value of seconds from epoch
        :param value: An int value representing the number of seconds from epoch (timestmap)
        :type value: int
        :return: The value truncated to midnight of the same day
        :rtype: int
        """
        if value is None:
            return None

        return Arrow.utcfromtimestamp(value).replace(hour=0, minute=0, second=0,
                                                     microsecond=0).timestamp * 1000

    def _transform_condition_parameter(self, source_value):
        """
        Truncates seconds and milliseconds from a long value of milliseconds from epoch
        :param value: An int value representing the number of milliseconds from epoch (timestmap)
        :type value: int
        :return: The value truncated to midnight of the same day
        :rtype: int
        """
        if source_value is None:
            return None

        return Arrow.utcfromtimestamp(source_value // 1000).replace(hour=0, minute=0, second=0,
                                                                    microsecond=0).timestamp * 1000
