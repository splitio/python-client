from __future__ import absolute_import, division, print_function, unicode_literals

from builtins import int

from arrow import Arrow


class TransformMixin(object):
    """Base for all transform mixins"""
    def transform(self, value):
        """
        Transforms a value
        :param value: The value to transform
        :type value: any
        :return: The transformed value
        :rtype: any
        """
        raise NotImplementedError()


class AsNumberTransformMixin(TransformMixin):
    """Mixin to allow transforming values to int (long)"""
    def transform(self, value):
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
    def transform(self, value):
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
    def transform(self, value):
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