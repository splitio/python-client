"""EventsMetadata test module."""
import pytest

from splitio.events.events_metadata import EventsMetadata
from splitio.models.events import SdkEvent, SdkInternalEvent

class EventsMetadataTests(object):
    """Tests for EventsMetadata."""

    def test_build_instance(self):
        data = { "updatedFlags": { "feature1" }, "sdkTimeout": 10 , "boolValue": True, "strValue": "value" }
        metadata = EventsMetadata(data)

        assert len(metadata.get_keys()) == 4
        assert metadata.get_data()["updatedFlags"].pop() == "feature1"
        assert len(metadata.get_data()["updatedFlags"]) == 0
        assert metadata.get_data()["sdkTimeout"] == 10
        assert metadata.get_data()["boolValue"] == True
        assert metadata.get_data()["strValue"] == "value"
        assert metadata.contain_key("updatedFlags")
        assert not metadata.contain_key("not_exist")
        assert len(metadata.get_values()) == 4

    def test_sanitize_none_input(self):
        data = { "updatedFlags": { "feature1" }, "sdkTimeout": None, "strValue": [1, 2, 3] }
        metadata = EventsMetadata(data)
        assert len(metadata.get_keys()) == 1
        assert metadata.get_data()["updatedFlags"].pop() == "feature1"