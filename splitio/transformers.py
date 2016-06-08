from __future__ import absolute_import, division, print_function, unicode_literals

import logging

from builtins import int

from arrow import Arrow

logger = logging.getLogger(__name__)


class TransformMixin(object):
    """Base for all transform mixins"""
    def _transform(self, value):
        """
        Transforms a value. Subclasses need to implement this method
        :param value: The value to transform
        :type value: any
        :return: The transformed value
        :rtype: any
        """
        raise NotImplementedError()

    def transform(self, value):
        """
        Transforms a value
        :param value: The value to transform
        :type value: any
        :return: The transformed value
        :rtype: any
        """
        try:
            return self._transform(value)
        except:
            logger.exception('Exception caught transforming value. value = %s', value)


class AsNumberTransformMixin(TransformMixin):
    """Mixin to allow transforming values to int (long)"""
    def _transform(self, value):
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
    def _transform(self, value):
        """
        Truncates seconds and milliseconds from a long value of milliseconds from epoch
        :param value: An int value representing the number of milliseconds from epoch (timestmap)
        :type value: int
        :return: The value truncated to minutes
        :rtype: int
        """
        if value is None:
            return None

        return Arrow.utcfromtimestamp(value / 1000).replace(second=0,
                                                            microsecond=0).timestamp * 1000


class AsDateTimestampTransformMixin(TransformMixin):
    """Mixin to allow truncating timestamp to midnight"""
    def _transform(self, value):
        """
        Truncates seconds and milliseconds from a long value of milliseconds from epoch
        :param value: An int value representing the number of milliseconds from epoch (timestmap)
        :type value: int
        :return: The value truncated to midnight of the same day
        :rtype: int
        """
        if value is None:
            return None

        return Arrow.utcfromtimestamp(value / 1000).replace(hour=0, minute=0, second=0,
                                                            microsecond=0).timestamp * 1000
