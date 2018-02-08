"""Unit tests for the api module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase
from splitio.events import InMemoryEventStorage


a = 1


class EventsInmemoryStorageTests(TestCase):
    def test_hook_is_called_when_queue_is_full(self):
        global a
        def ftemp():
            global a
            a += 1
        storage = InMemoryEventStorage(5)
        storage.set_queue_full_hook(ftemp)
        storage.log_event("a")
        storage.log_event("a")
        storage.log_event("a")
        storage.log_event("a")
        storage.log_event("a")
        storage.log_event("a")
        self.assertEqual(a, 2)
