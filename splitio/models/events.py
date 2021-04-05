"""
Event DTO and Storage classes.

The dto is implemented as a namedtuple for performance matters.
"""
from collections import namedtuple


Event = namedtuple('Event', [
    'key',
    'traffic_type_name',
    'event_type_id',
    'value',
    'timestamp',
    'properties',
])

EventWrapper = namedtuple('EventWrapper', [
    'event',
    'size',
])
