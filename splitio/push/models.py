from enum import Enum
from itertools import chain

from splitio_commons.push.parser import BaseUpdate
from splitio_commons.push.event_update_type import EventUpdateType as BaseUpdateType

class EventUpdateType(Enum):
    """Message type enumeration."""

    SPLIT_UPDATE = 'SPLIT_UPDATE'
    SPLIT_KILL = 'SPLIT_KILL'

EventUpdateType = Enum('EventUpdateType', [(m.name, m.value) for m in chain(EventUpdateType, BaseUpdateType)])

class SplitChangeUpdate(BaseUpdate):
    """Feature flag Change notification."""

    def __init__(self, channel, timestamp, change_number, data, update_type_class):
        """Class constructor."""
        BaseUpdate.__init__(self, channel, timestamp, change_number)
        self._previous_change_number = data.get('pcn')
        self._object_definition = data.get('d')
        self._compression = data.get('c')
        self._update_type_class = update_type_class

    @property
    def update_type(self):  # pylint:disable=no-self-use
        """
        Return the message type.

        :returns: The type of this parsed Update.
        :rtype: UpdateType
        """
        return self._update_type_class.SPLIT_UPDATE

    @property
    def previous_change_number(self):  # pylint:disable=no-self-use
        """
        Return previous change number
        :returns: The previous change number
        :rtype: int
        """
        return self._previous_change_number

    @property
    def object_definition(self):  # pylint:disable=no-self-use
        """
        Return feature flag definition
        :returns: The new feature flag definition
        :rtype: str
        """
        return self._object_definition

    @property
    def compression(self):  # pylint:disable=no-self-use
        """
        Return previous compression type
        :returns: The compression type
        :rtype: int
        """
        return self._compression

    def __str__(self):
        """Return string representation."""
        return "SplitChange - changeNumber=%d" % (self.change_number)


class SplitKillUpdate(BaseUpdate):
    """Feature flag Kill notification."""

    def __init__(self, channel, timestamp, change_number, data, update_type_class):  # pylint:disable=too-many-arguments
        """Class constructor."""
        BaseUpdate.__init__(self, channel, timestamp, change_number)
        self._feature_flag_name = data.get('feature_flag_name')
        self._default_treatment = data.get('default_treatment')
        self._update_type_class = update_type_class

    @property
    def update_type(self):  # pylint:disable=no-self-use
        """
        Return the message type.

        :returns: The type of this parsed Update.
        :rtype: UpdateType
        """
        return self._update_type_class.SPLIT_KILL

    @property
    def feature_flag_name(self):
        """
        Return the name of the killed feature flag.

        :returns: name of the killed feature flag
        :rtype: str
        """
        return self._feature_flag_name

    @property
    def default_treatment(self):
        """
        Return the default treatment.

        :returns: default treatment
        :rtype: str
        """
        return self._default_treatment

    def __str__(self):
        """Return string representation."""
        return "SplitKill - changeNumber=%d, name=%s, defaultTreatment=%s" % \
            (self.change_number, self.feature_flag_name, self.default_treatment)
