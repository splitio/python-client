"""
Event DTO and Storage classes.

The dto is implemented as a namedtuple for performance matters.
"""

from __future__ import print_function
from collections import namedtuple


Event = namedtuple('Event', [
    'key',
    'traffic_type_name',
    'event_type_id',
    'value',
    'timestamp',
])
