"""
Event DTO and Storage classes.

The dto is implemented as a namedtuple for performance matters.
"""
from collections import namedtuple
from enum import Enum

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

class SdkEvent(Enum):
    """Public SDK events"""

    SDK_READY = 'SDK_READY'
    SDK_READY_TIMED_OUT = 'SDK_READY_TIMED_OUT'
    SDK_UPDATE = 'SDK_UPDATE'

class SdkInternalEvent(Enum):
    """Internal SDK events"""

    SDK_READY = 'SDK_READY'
    SDK_TIMED_OUT = 'SDK_TIMED_OUT'
    FLAGS_UPDATED = 'FLAGS_UPDATED'
    FLAG_KILLED_NOTIFICATION = 'FLAG_KILLED_NOTIFICATION'
    SEGMENTS_UPDATED = 'SEGMENTS_UPDATED'
    RB_SEGMENTS_UPDATED = 'RB_SEGMENTS_UPDATED'
    LARGE_SEGMENTS_UPDATED = 'LARGE_SEGMENTS_UPDATED'
    

