"""EventsManagerConfig test module."""
import pytest

from splitio.events.events_manager_config import EventsManagerConfig
from splitio.models.events import SdkEvent, SdkInternalEvent

class EventsManagerConfigTests(object):
    """Tests for EventsManagerConfig."""

    def test_build_instance(self):
        config = EventsManagerConfig()

        assert len(config.require_all[SdkEvent.SDK_READY]) == 1
        assert SdkInternalEvent.SDK_READY in config.require_all[SdkEvent.SDK_READY]

        assert SdkEvent.SDK_READY in config.prerequisites[SdkEvent.SDK_UPDATE]
                                                          
        assert config.execution_limits[SdkEvent.SDK_READY_TIMED_OUT] == -1
        assert config.execution_limits[SdkEvent.SDK_UPDATE] == -1
        assert config.execution_limits[SdkEvent.SDK_READY] == 1

        assert len(config.require_any[SdkEvent.SDK_READY_TIMED_OUT]) == 1
        assert SdkInternalEvent.SDK_TIMED_OUT in config.require_any[SdkEvent.SDK_READY_TIMED_OUT]

        assert len(config.require_any[SdkEvent.SDK_UPDATE]) == 4
        assert SdkInternalEvent.FLAG_KILLED_NOTIFICATION in config.require_any[SdkEvent.SDK_UPDATE]
        assert SdkInternalEvent.FLAGS_UPDATED in config.require_any[SdkEvent.SDK_UPDATE]
        assert SdkInternalEvent.RB_SEGMENTS_UPDATED in config.require_any[SdkEvent.SDK_UPDATE]
        assert SdkInternalEvent.SEGMENTS_UPDATED in config.require_any[SdkEvent.SDK_UPDATE]

        assert len(config.suppressed_by[SdkEvent.SDK_READY_TIMED_OUT]) == 1
        assert SdkEvent.SDK_READY in config.suppressed_by[SdkEvent.SDK_READY_TIMED_OUT]

        order = 0
        assert len(config.evaluation_order) == 3
        for sdk_event in config.evaluation_order:
            order += 1            
            if order == 1:
                assert sdk_event == SdkEvent.SDK_READY_TIMED_OUT
            if order == 2:
                assert sdk_event == SdkEvent.SDK_READY
            if order == 3:
                assert sdk_event == SdkEvent.SDK_UPDATE