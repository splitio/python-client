"""
Event DTO definition
"""

from __future__ import print_function
from collections import namedtuple
from six.moves import queue
from six import callable


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

    def __init__(self, eventsQueueSize):
        """
        TODO
        """
        self._events = queue.Queue(maxsize=eventsQueueSize)
        self._queue_full_hook = None

    def set_queue_full_hook(self, h):
        """
        TODO
        """
        if callable(h):
            self._queue_full_hook = h

    def log_event(self, event):
        """
        TODO
        """
        try:
            self._events.put(event, False)
            return True
        except queue.Full:
            if self._queue_full_hook is not None and callable(self._queue_full_hook):
                self._queue_full_hook()
            return False

    def pop_many(self, count):
        """
        TODO
        """
        events = []
        while not self._events.empty() and count > 0:
            events.append(self._events.get(False))
        return events
