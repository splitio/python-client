"""SSE Parser unit tests."""
import json
import pytest

from splitio.push.sse import SSEEvent
from splitio.push.parser import parse_incoming_event, BaseUpdate, AblyError, OccupancyMessage, \
    SegmentChangeUpdate, SplitChangeUpdate, SplitKillUpdate, EventParsingException


def make_message(channel, data):
    return SSEEvent('123', 'message', None, json.dumps({
        'id':'ZlalwoKlXW:0:0',
        'timestamp':1591996755043,
        'encoding':'json',
        'channel': channel,
        'data': json.dumps(data)
    }))

def make_occupancy(channel, data):
    return SSEEvent('123', 'message', None, json.dumps({
        'id':'ZlalwoKlXW:0:0',
        'timestamp':1591996755043,
        'encoding':'json',
        'channel': channel,
        'name': '[meta]occupancy',
        'data': json.dumps(data)
    }))


def make_error(payload):
    return SSEEvent('123', 'error', None, json.dumps(payload))


class ParserTests(object):
    """Parser tests."""

    def test_exception(self):
        """Test exceptions."""
        assert parse_incoming_event(None) is None

        with pytest.raises(EventParsingException):
            parse_incoming_event(json.dumps({
            'data': {'a':1},
            'event': 'some'
        }))

    def test_event_parsing(self):
        """Test parse Update event."""

        e0 = make_message(
            'NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits',
            {'type':'SPLIT_KILL','changeNumber':1591996754396,'defaultTreatment':'some','splitName':'test'},
        )
        parsed0 = parse_incoming_event(e0)
        assert isinstance(parsed0, SplitKillUpdate)
        assert parsed0.default_treatment == 'some'
        assert parsed0.change_number == 1591996754396
        assert parsed0.split_name == 'test'

        e1 = make_message(
            'NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits',
            {'type':'SPLIT_UPDATE','changeNumber':1591996685190},
        )
        parsed1 = parse_incoming_event(e1)
        assert isinstance(parsed1, SplitChangeUpdate)
        assert parsed1.change_number == 1591996685190

        e2 = make_message(
            'NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments',
            {'type':'SEGMENT_UPDATE','changeNumber':1591988398533,'segmentName':'some'},
        )
        parsed2 = parse_incoming_event(e2)
        assert isinstance(parsed2, SegmentChangeUpdate)
        assert parsed2.change_number == 1591988398533
        assert parsed2.segment_name == 'some'

    def test_error_parsing(self):
        """Test parse AblyError event."""
        e0 = make_error({
            'code': 40142,
            'message': 'Token expired',
            'statusCode': 401,
            'href': 'https://help.io/error/40142',
        })
        parsed = parse_incoming_event(e0)
        assert isinstance(parsed, AblyError)
        assert parsed.code == 40142
        assert parsed.status_code == 401
        assert parsed.href == 'https://help.io/error/40142'
        assert parsed.message == 'Token expired'
        assert not parsed.should_be_ignored()
        assert parsed.is_retryable()

    def test_occupancy_parsing(self):
        """Test parse Occupancy event."""
        e0 = make_occupancy('[?occupancy=metrics.publishers]control_sec',
                            {'metrics': {'publishers': 1}})
        parsed = parse_incoming_event(e0)
        assert isinstance(parsed, OccupancyMessage)
        assert parsed.publishers == 1
        assert parsed.channel == 'control_sec'
