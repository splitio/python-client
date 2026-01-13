"""EventsMetadata test module."""
import pytest

from splitio.events.events_metadata import EventsMetadata
from splitio.events.events_metadata import SdkEventType

class EventsMetadataTests(object):
    """Tests for EventsMetadata."""

    def test_build_instance(self):
        metadata = EventsMetadata(SdkEventType.FLAG_UPDATE, { "feature1" })
        assert len(metadata.get_names()) == 1
        assert metadata.get_names().pop() == "feature1"
        assert len(metadata.get_names()) == 0
        assert metadata.get_type() == SdkEventType.FLAG_UPDATE

    def test_sanitize_none_input(self):
        metadata = EventsMetadata(SdkEventType.FLAG_UPDATE, { "feature1", None, 123, False })
        assert len(metadata.get_names()) == 1
        assert metadata.get_names().pop() == "feature1"
        assert len(metadata.get_names()) == 0
