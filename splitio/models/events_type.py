"""Public SDK event types exposed by the Flags SDK."""
from enum import Enum
from itertools import chain

from splitio_commons.events.events_metadata import SdkEventType as EventType


class ExtendedEventType(Enum):
    """Event types specific to the Flags SDK"""

    FLAGS_UPDATE = 'FLAGS_UPDATE'


SdkEventType = Enum('Combined', [(m.name, m.value) for m in chain(EventType, ExtendedEventType)])
