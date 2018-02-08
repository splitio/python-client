"""
Event DTO and Storage classes.

The dto is implemented as a namedtuple for performance matters.
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
    Return a list of dictionaries with all the events.

    :param event_list: list of event tuples
    """
    return [e._asdict() for e in event_list]


class InMemoryEventStorage(object):
    """
    In memory storage for events.

    Supports adding and popping events.
    """

    def __init__(self, eventsQueueSize):
        """
        Construct an instance.

        :param eventsQueueSize: How many events to queue before forcing a submission
        """
        self._events = queue.Queue(maxsize=eventsQueueSize)
        self._queue_full_hook = None

    def set_queue_full_hook(self, hook):
        """
        Set a hook to be called when the queue is full.

        :param h: Hook to be called when the queue is full
        """
        if callable(hook):
            self._queue_full_hook = hook

    def log_event(self, event):
        """
        Add an avent to storage.

        :param event: Event to be added in the storage
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
        Pop multiple items from the storage.

        :param count: number of items to be retrieved and removed from the queue.
        """
        events = []
        while not self._events.empty() and count > 0:
            events.append(self._events.get(False))
        return events
