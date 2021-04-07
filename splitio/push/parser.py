"""SSE Notification definitions."""
import abc
import json
from enum import Enum

from splitio.util.decorators import abstract_property
from splitio.util import utctime_ms
from splitio.push.sse import SSE_EVENT_ERROR, SSE_EVENT_MESSAGE


class EventType(Enum):
    """Event type enumeration."""

    MESSAGE = SSE_EVENT_MESSAGE
    ERROR = SSE_EVENT_ERROR


class MessageType(Enum):
    """Message type enumeration."""

    UPDATE = 0
    OCCUPANCY = 1
    CONTROL = 2


class UpdateType(Enum):
    """Message type enumeration."""

    SPLIT_UPDATE = 'SPLIT_UPDATE'
    SPLIT_KILL = 'SPLIT_KILL'
    SEGMENT_UPDATE = 'SEGMENT_UPDATE'


class ControlType(Enum):
    """Control type enumeration."""

    STREAMING_ENABLED = 'STREAMING_ENABLED'
    STREAMING_PAUSED = 'STREAMING_PAUSED'
    STREAMING_DISABLED = 'STREAMING_DISABLED'


TAG_OCCUPANCY = '[meta]occupancy'


class EventParsingException(Exception):
    """Exception to be raised on parser errors."""

    pass


class BaseEvent(object, metaclass=abc.ABCMeta):
    """Base event that reqiures subclasses tu have a type."""

    @abstract_property
    def event_type(self):  # pylint:disable=no-self-use
        """
        Return the event type.

        :returns: The type of this parsed event.
        :rtype: EventType
        """
        pass


class AblyError(BaseEvent):
    """Ably Error message."""

    def __init__(self, code, status_code, message, href):
        """
        Class constructor.

        :param code: error code
        :type code: int

        :param status_code: http status cude
        :type status_code: int

        :param message: error message
        :type message: str

        :param href: link to error description
        :type href: str
        """
        self._code = code
        self._status_code = status_code
        self._message = message
        self._href = href
        self._timestamp = utctime_ms()

    @property
    def event_type(self):  # pylint:disable=no-self-use
        """
        Return the event type.

        :returns: The type of this parsed event.
        :rtype: MessageType
        """
        return EventType.ERROR

    @property
    def code(self):
        """
        Return the error code.

        :returns: ably error code.
        :rtype: int
        """
        return self._code

    @property
    def status_code(self):
        """
        Return the http status code.

        :returns: http status error code.
        :rtype: int
        """
        return self._status_code

    @property
    def message(self):
        """
        Return the ably error message.

        :returns: ably error message.
        :rtype: str
        """
        return self._message

    @property
    def href(self):
        """
        Return the link of the error description.

        :returns: error description url
        :rtype: str
        """
        return self._href

    @property
    def timestamp(self):
        """
        Return a the timestamp when this error was constructed.

        :returns: approximate error timestamp
        :rtype: int
        """
        return self._timestamp

    def should_be_ignored(self):
        """
        Return whether this error should be ignored or not.

        :returns: True if this error should be ignored. False otherwise.
        :rtype: bool
        """
        return self._code < 40000 or self._code > 49999

    def is_retryable(self):
        """
        Return whether this error is retryable or not.

        :returns: True if this error is retryable. False otherwise.
        :rtype: bool
        """
        return self._code >= 40140 and self._code <= 40149

    def __str__(self):
        """Return string representation."""
        return "AblyError - code=%d, status=%d, message=%s, href=%s" % \
            (self.code, self.status_code, self.message, self.href)


class BaseMessage(BaseEvent, metaclass=abc.ABCMeta):
    """Message type event."""

    def __init__(self, channel, timestamp):
        """
        Construct a message's base structure.

        :param channel: channel where the notification was received.
        :type channel: str
        """
        self._channel = channel
        self._timestamp = timestamp

    @property
    def channel(self):
        """
        Return the channel where the message arrived.

        :returns: channel
        :rtype: str
        """
        return self._channel

    @property
    def timestamp(self):
        """
        Return the timestamp when the message was sent.

        :returns: message sending timestamp
        :rtype: int
        """
        return self._timestamp

    @property
    def event_type(self):  # pylint:disable=no-self-use
        """
        Return the event type.

        :returns: The type of this parsed event.
        :rtype: MessageType
        """
        return EventType.MESSAGE

    @abstract_property
    def message_type(self):  # pylint:disable=no-self-use
        """
        Return the message type.

        :returns: The type of this parsed Message.
        :rtype: MessageType
        """
        pass


class OccupancyMessage(BaseMessage):
    """Ably publisher occupancy notification."""

    def __init__(self, channel, timestamp, publishers):
        """
        Construct an occupancy message.

        :param channel: channel where occupancy is being announced.
        :type channel: str

        :param publishers: number of active publishers attached to this channel.
        :type data: int
        """
        BaseMessage.__init__(self, channel, timestamp)
        self._publishers = publishers

    @property
    def message_type(self):  # pylint:disable=no-self-use
        """
        Return the message type.

        :returns: The type of this parsed Message.
        :rtype: MessageType
        """
        return MessageType.OCCUPANCY

    @property
    def channel(self):
        """
        Return the channel on which this message was received.

        :returns: channel name
        :rtype: str
        """
        return self._channel.replace('[?occupancy=metrics.publishers]', '')

    @property
    def publishers(self):
        """
        Return the number of publishers of this channel.

        :returns: attahed publisher count.
        :rtype: int
        """
        return self._publishers

    def __str__(self):
        """Return string representation."""
        return "Occupancy - channel=%s, publishers=%d" % (self.channel, self.publishers)


class BaseUpdate(BaseMessage, metaclass=abc.ABCMeta):
    """Split data update notification."""

    def __init__(self, channel, timestamp, change_number):
        """
        Construct an update event.

        :param data: raw message data.
        :type data: dict

        :param channel: channel where the message came from.
        :type channel: str
        """
        BaseMessage.__init__(self, channel, timestamp)
        self._change_number = change_number

    @abstract_property
    def update_type(self):  # pylint:disable=no-self-use
        """
        Return the message type.

        :returns: The type of this parsed Update.
        :rtype: UpdateType
        """
        pass

    @property
    def message_type(self):  # pylint:disable=no-self-use
        """
        Return the message type.

        :returns: The type of this parsed event.
        :rtype: MessageType
        """
        return MessageType.UPDATE

    @property
    def change_number(self):
        """
        Return the change number associated with the data update.

        :returns: change number
        :rtype: int
        """
        return self._change_number


class SplitChangeUpdate(BaseUpdate):
    """Split Change notification."""

    def __init__(self, channel, timestamp, change_number):
        """Class constructor."""
        BaseUpdate.__init__(self, channel, timestamp, change_number)

    @property
    def update_type(self):  # pylint:disable=no-self-use
        """
        Return the message type.

        :returns: The type of this parsed Update.
        :rtype: UpdateType
        """
        return UpdateType.SPLIT_UPDATE

    def __str__(self):
        """Return string representation."""
        return "SplitChange - changeNumber=%d" % (self.change_number)


class SplitKillUpdate(BaseUpdate):
    """Split Kill notification."""

    def __init__(self, channel, timestamp, change_number, split_name, default_treatment):  # pylint:disable=too-many-arguments
        """Class constructor."""
        BaseUpdate.__init__(self, channel, timestamp, change_number)
        self._split_name = split_name
        self._default_treatment = default_treatment

    @property
    def update_type(self):  # pylint:disable=no-self-use
        """
        Return the message type.

        :returns: The type of this parsed Update.
        :rtype: UpdateType
        """
        return UpdateType.SPLIT_KILL

    @property
    def split_name(self):
        """
        Return the name of the killed split.

        :returns: name of the killed split
        :rtype: str
        """
        return self._split_name

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
            (self.change_number, self.split_name, self.default_treatment)


class SegmentChangeUpdate(BaseUpdate):
    """Segment Change notification."""

    def __init__(self, channel, timestamp, change_number, segment_name):
        """Class constructor."""
        BaseUpdate.__init__(self, channel, timestamp, change_number)
        self._segment_name = segment_name

    @property
    def update_type(self):  # pylint:disable=no-self-use
        """
        Return the message type.

        :returns: The type of this parsed Update.
        :rtype: UpdateType
        """
        return UpdateType.SEGMENT_UPDATE

    @property
    def segment_name(self):
        """
        Return the semgent name associated with the data update.

        :returns: segment name
        :rtype: str
        """
        return self._segment_name

    def __str__(self):
        """Return string representation."""
        return "SegmentChange - changeNumber=%d, name=%s" % (self.change_number, self.segment_name)


class ControlMessage(BaseMessage):
    """Control notification."""

    def __init__(self, channel, timestamp, control_type):
        """Class constructor."""
        BaseMessage.__init__(self, channel, timestamp)
        self._control_type = ControlType(control_type)

    @property
    def message_type(self):  # pylint:disable=no-self-use
        """
        Return the message type.

        :returns: The type of this parsed event.
        :rtype: MessageType
        """
        return MessageType.CONTROL

    @property
    def control_type(self):
        """
        Return the associated control type.

        :returns: control type
        :rtype: ControlType
        """
        return self._control_type

    def __str__(self):
        """Return string representation."""
        return "Control - type=%s" % (self.control_type.name)


def _parse_update(channel, timestamp, data):
    """
    Parse a message of update type.

    :param channel: channel name
    :type data: str

    :param data: raw incoming event
    :type data: dict

    :returns: Parsed ably error notification.
    :rtype: BaseUpdate
    """
    update_type = UpdateType(data['type'])
    change_number = data['changeNumber']
    if update_type == UpdateType.SPLIT_UPDATE:
        return SplitChangeUpdate(channel, timestamp, change_number)
    elif update_type == UpdateType.SPLIT_KILL:
        return SplitKillUpdate(channel, timestamp, change_number,
                               data['splitName'], data['defaultTreatment'])
    elif update_type == UpdateType.SEGMENT_UPDATE:
        return SegmentChangeUpdate(channel, timestamp, change_number, data['segmentName'])
    raise EventParsingException('unrecognized event type %s' % update_type)


def _parse_message(data):
    """
    Parse a message event into a concrete class.

    :param data: raw incoming event.
    :type data: dict:

    :returns: Parsed ably error notification.
    :rtype: BaseEvent
    """
    if not all(k in data for k in ['data', 'channel']):
        return None
    channel = data['channel']
    timestamp = data['timestamp']
    parsed_data = json.loads(data['data'])
    if data.get('name') == TAG_OCCUPANCY:
        return OccupancyMessage(channel, timestamp, parsed_data['metrics']['publishers'])
    elif parsed_data['type'] == 'CONTROL':
        return ControlMessage(channel, timestamp, parsed_data['controlType'])
    elif parsed_data['type'] in UpdateType.__members__:
        return _parse_update(channel, timestamp, parsed_data)
    raise EventParsingException('unrecognized message type %s' % parsed_data['type'])


def _parse_error(data):
    """
    Parse an error message into a concrete class.

    :param data: raw incoming event.
    :type data: dict:

    :returns: Parsed ably error notification.
    :rtype: AblyError
    """
    return AblyError(data.get('code'), data.get('statusCode'),
                     data.get('message'), data.get('href'))


def parse_incoming_event(raw_event):
    """
    Parse a raw event as received by the sse client.

    :param raw_event: raw SSE Event
    :type raw_event: splitio.push.sse.SSEEvent

    :returns: an event parsed to it's concrete type.
    :rtype: BaseEvent
    """
    if raw_event is None:
        return None

    try:
        parsed_data = json.loads(raw_event.data)
    except Exception as exc:  # pylint:disable=broad-except
        raise EventParsingException('Error parsing json') from exc

    try:
        event_type = EventType(raw_event.event)
    except ValueError as exc:
        raise Exception('unknown event type %s' % raw_event.event) from exc

    return {
        EventType.ERROR: _parse_error,
        EventType.MESSAGE: _parse_message,
    }[event_type](parsed_data)
