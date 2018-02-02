"""
Event DTO definition
"""

from __future__ import print_function
from collections import namedtuple
from six.moves import queue


Event = namedtuple('Event', [
    'key',
    'trafficTypeName',
    'eventTypeId',
    'value',
    'timestamp',
])


def build_bulk(event_list):
    """
    Returns a list of dictionaries with all the events.
    """
    return [e._asdict() for e in event_list]


class InMemoryEventStorage(object):
    """
    TODO
    """

    def __init__(self):
        """
        TODO
        """
        self._events = queue.Queue()

    def log_event(self, event):
        """
        TODO
        """
        self._events.put(event, False)

    def pop_many(self, count):
        """
        TODO
        """
        events = []
        while not self._events.empty() and count > 0:
            events.append(self._events.get(False))
        return events
