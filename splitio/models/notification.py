"""Notification Module"""

import json

from enum import Enum


class Type(Enum):
    """Notification Type."""

    SPLIT_UPDATE = 'SPLIT_UPDATE'
    SPLIT_KILL = 'SPLIT_KILL'
    SEGMENT_UPDATE = 'SEGMENT_UPDATE'
    CONTROL = 'CONTROL'


class Control(Enum):
    """Control Type."""

    STREAMING_PAUSED = 'STREAMING_PAUSED'
    STREAMING_RESUMED = 'STREAMING_RESUMED'
    STREAMING_DISABLED = 'STREAMING_DISABLED'


class ControlNotification(object):  # pylint: disable=too-many-instance-attributes
    """ControlNotification model object."""

    def __init__(self, channel, notification_type, control_type):
        """
        Class constructor.

        :param channel: Channel of incoming notification
        :type channel: str
        :param notification_type: Type of incoming notification
        :type notification_type: str
        :param control_type: Control type of incoming CONTROL notification.
        :type control_type: str

        """
        self._channel = channel
        self._notification_type = Type(notification_type)
        self._control_type = Control(control_type)

    @property
    def channel(self):
        return self._channel

    @property
    def control_type(self):
        return self._control_type

    @property
    def notification_type(self):
        return self._notification_type


class SegmentChangeNotification(object):  # pylint: disable=too-many-instance-attributes
    """SegmentChangeNotification model object."""

    def __init__(self, channel, notification_type, change_number, segment_name):
        """
        Class constructor.

        :param channel: Channel of incoming notification
        :type channel: str
        :param notification_type: Type of incoming notification
        :type notification_type: str
        :param change_number: ChangeNumber of incoming notification.
        :type change_number: int
        :param segment_name: Segment Name of incoming notification.
        :type segment_name: str

        """
        self._channel = channel
        self._notification_type = Type(notification_type)
        self._change_number = change_number
        self._segment_name = segment_name

    @property
    def channel(self):
        return self._channel

    @property
    def change_number(self):
        return self._change_number

    @property
    def notification_type(self):
        return self._notification_type

    @property
    def segment_name(self):
        return self._segment_name


class SplitChangeNotification(object):  # pylint: disable=too-many-instance-attributes
    """SplitChangeNotification model object."""

    def __init__(self, channel, notification_type, change_number):
        """
        Class constructor.

        :param channel: Channel of incoming notification
        :type channel: str
        :param notification_type: Type of incoming notification
        :type notification_type: str
        :param change_number: ChangeNumber of incoming notification.
        :type change_number: int

        """
        self._channel = channel
        self._notification_type = Type(notification_type)
        self._change_number = change_number

    @property
    def channel(self):
        return self._channel

    @property
    def change_number(self):
        return self._change_number

    @property
    def notification_type(self):
        return self._notification_type


class SplitKillNotification(object):  # pylint: disable=too-many-instance-attributes
    """SplitKillNotification model object."""

    def __init__(self, channel, notification_type, change_number, default_treatment, split_name):
        """
        Class constructor.

        :param channel: Channel of incoming notification
        :type channel: str
        :param notification_type: Type of incoming notification
        :type notification_type: str
        :param change_number: ChangeNumber of incoming notification.
        :type change_number: int
        :param default_treatment: Default treatment of incoming SPLIT_KILL notification.
        :type default_treatment: str
        :param split_name: Split Name of incoming SPLIT or SPLIT_KILL notification.
        :type split_name: str

        """
        self._channel = channel
        self._notification_type = Type(notification_type)
        self._change_number = change_number
        self._default_treatment = default_treatment
        self._split_name = split_name

    @property
    def channel(self):
        return self._channel

    @property
    def change_number(self):
        return self._change_number

    @property
    def default_treatment(self):
        return self._default_treatment

    @property
    def notification_type(self):
        return self._notification_type

    @property
    def split_name(self):
        return self._split_name


_NOTIFICATION_MAPPERS = {
    Type.SPLIT_UPDATE: lambda c, d: SplitChangeNotification(c, Type.SPLIT_UPDATE, d['changeNumber']),
    Type.SPLIT_KILL: lambda c, d: SplitKillNotification(c, Type.SPLIT_KILL, d['changeNumber'], d['defaultTreatment'], d['splitName']),
    Type.SEGMENT_UPDATE: lambda c, d: SegmentChangeNotification(c, Type.SEGMENT_UPDATE, d['changeNumber'], d['segmentName']),
    Type.CONTROL: lambda c, d: ControlNotification(c, Type.CONTROL, d['controlType'])
}


def wrap_notification(raw_data, channel):
    """
    Parse notification from raw notification payload

    :param raw_data: data
    :type raw_data: str
    :param channel: Channel of incoming notification
    :type channel: str
    """
    try:
        if channel is None:
            raise ValueError("channel cannot be None.")
        raw_data = json.loads(raw_data)
        notification_type = Type(raw_data['type'])
        mapper = _NOTIFICATION_MAPPERS[notification_type]
        return mapper(channel, raw_data)
    except ValueError:
        raise ValueError("Wrong notification type received.")
    except KeyError:
        raise KeyError("Could not parse notification.")
    except TypeError:
        raise TypeError("Wrong JSON format.")
