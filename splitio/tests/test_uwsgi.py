"""Unit tests for the tasks module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase

from splitio.uwsgi import UWSGISegmentCache, get_uwsgi

class UWSGISegmentCacheTests(TestCase):
    def setUp(self):
        self.some_segment_name = mock.MagicMock()
        self.some_segment_name_str = 'some_segment_name'
        self.some_segment_keys = [mock.MagicMock(), mock.MagicMock()]
        self.some_key = mock.MagicMock()
        self.some_change_number = mock.MagicMock()

        self.some_uwsgi = get_uwsgi(emulator=True)
        self.segment_cache = UWSGISegmentCache(self.some_uwsgi)

    def test_register_segment(self):
        """Test that register a segment"""
        self.segment_cache.register_segment(self.some_segment_name_str)
        registered_segments = self.segment_cache.get_registered_segments()
        self.assertIn(self.some_segment_name_str, registered_segments)
