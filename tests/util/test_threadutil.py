"""threading utilities unit tests."""

import time
import threading

from splitio.util.threadutil import EventGroup


class EventGroupTests(object):
    """EventGroup class test cases."""

    def test_basic_functionality(self):
        """Test basic functionality."""

        def fun(event):  #pylint:disable=missing-docstring
            time.sleep(1)
            event.set()

        group = EventGroup()
        event1 = group.make_event()
        event2 = group.make_event()

        task = threading.Thread(target=fun, args=(event1,))
        task.start()
        group.wait(3)
        assert event1.is_set()
        assert not event2.is_set()

        group = EventGroup()
        event1 = group.make_event()
        event2 = group.make_event()

        task = threading.Thread(target=fun, args=(event2,))
        task.start()
        group.wait(3)
        assert not event1.is_set()
        assert event2.is_set()

        group = EventGroup()
        event1 = group.make_event()
        event2 = group.make_event()
        group.wait(3)
        assert not event1.is_set()
        assert not event2.is_set()
