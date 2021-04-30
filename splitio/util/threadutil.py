"""Threading utilities."""
from threading import Event, Condition


class EventGroup(object):
    """EventGroup that can be waited with an OR condition."""

    class Event(Event):  # pylint:disable=too-few-public-methods
        """Threading event meant to be used in an group."""

        def __init__(self, shared_condition):
            """
            Construct an event.

            :param shared_condition: shared condition varaible.
            :type shared_condition: threading.Condition
            """
            Event.__init__(self)
            self._shared_cond = shared_condition

        def set(self):
            """Set the event."""
            Event.set(self)
            with self._shared_cond:
                self._shared_cond.notify()

    def __init__(self):
        """Construct an event group."""
        self._cond = Condition()

    def make_event(self):
        """
        Make a new event associated to this waitable group.

        :returns: an event that can be awaited as part of a group
        :rtype: EventGroup.Event
        """
        return EventGroup.Event(self._cond)

    def wait(self, timeout=None):
        """
        Wait until one of the events is triggered.

        :param timeout: how many seconds to wait. None means forever.
        :type timeout: int

        :returns: True if the condition was notified within the specified timeout. False otherwise.
        :rtype: bool
        """
        with self._cond:
            return self._cond.wait(timeout)
