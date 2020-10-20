import pytest

from splitio.models.notification import wrap_notification, SplitChangeNotification, SplitKillNotification, SegmentChangeNotification, ControlNotification

class NotificationTests(object):
    """Notification model tests."""

    def test_wrap_notification(self):
        with pytest.raises(ValueError):
            wrap_notification('{"type":"WRONG","controlType":"STREAMING_PAUSED"}', 'control_pri')

        with pytest.raises(ValueError):
            wrap_notification('sadasd', 'control_pri')

        with pytest.raises(TypeError):
            wrap_notification(None, 'control_pri')

        with pytest.raises(ValueError):
            wrap_notification('{"type":"SPLIT_UPDATE","changeNumber":1591996754396}', None)

        with pytest.raises(KeyError):
            wrap_notification('{"type":"SPLIT_UPDATE"}',  'NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits')

        with pytest.raises(ValueError):
            wrap_notification('{"type":"CONTROL","controlType":"STREAMING_PAUSEDD"}', 'control_pri')

        n0 = wrap_notification('{"type":"SPLIT_UPDATE","changeNumber":1591996754396}', 'NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits')
        assert isinstance(n0, SplitChangeNotification)
        assert n0.channel == 'NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits'
        assert n0.notification_type.name == 'SPLIT_UPDATE'

        n1 = wrap_notification('{"type":"SPLIT_KILL","changeNumber":1591996754396,"defaultTreatment":"some","splitName":"test"}', 'NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits')
        assert isinstance(n1, SplitKillNotification)
        assert n1.channel == 'NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits'
        assert n1.change_number == 1591996754396
        assert n1.default_treatment == 'some'
        assert n1.split_name == 'test'
        assert n1.notification_type.name == 'SPLIT_KILL'

        n2 = wrap_notification('{"type":"SEGMENT_UPDATE","changeNumber":1591996754396,"segmentName":"some"}', 'NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments')
        assert isinstance(n2, SegmentChangeNotification)
        assert n2.channel == 'NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments'
        assert n2.change_number == 1591996754396
        assert n2.segment_name == 'some'
        assert n2.notification_type.name == 'SEGMENT_UPDATE'

        n3 = wrap_notification('{"type":"CONTROL","controlType":"STREAMING_PAUSED"}', 'control_pri')
        assert isinstance(n3, ControlNotification)
        assert n3.channel == 'control_pri'
        assert n3.control_type.name == 'STREAMING_PAUSED'
        assert n3.notification_type.name == 'CONTROL'
