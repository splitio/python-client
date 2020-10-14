import json
import pytest

from splitio.push.parser import parse_incoming_event, Update, AblyError, Occupancy


def wrap_json(channel, data):
    return json.dumps({
        'data': json.dumps({
            'id':'ZlalwoKlXW:0:0',
            'timestamp':1591996755043,
            'encoding':'json',
            'channel': channel,
            'data': json.dumps(data)
        }),
        'event': 'message'
    })

class ParserTests(object):
    """Parser tests."""

    def test_exception(self):
        """Test exceptions."""
        assert parse_incoming_event(None) is None
        assert parse_incoming_event('') is None
        assert parse_incoming_event('  ') is None
        assert parse_incoming_event(json.dumps({})) is None

        with pytest.raises(ValueError):
            parse_incoming_event('asd')

    def test_event_parsing(self):
        """Test parse Update event."""
        e0 = wrap_json(
            'NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits',
            {'type':'SPLIT_KILL','changeNumber':1591996754396,'defaultTreatment':'some','splitName':'test'},
        )
        assert isinstance(parse_incoming_event(e0), Update)

        e1 = wrap_json(
            'NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits',
            {'type':'SPLIT_UPDATE','changeNumber':1591996685190},
        )
        assert isinstance(parse_incoming_event(e1), Update)
    
        e2 = wrap_json(
            'NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments',
            {'type':'SEGMENT_UPDATE','changeNumber':1591988398533,'segmentName':'some'},
        )
        assert isinstance(parse_incoming_event(e2), Update)

    def test_error_parsing(self):
        """Test parse AblyError event."""
        e0 = json.dumps({
            'data': json.dumps({
                'code': 40142,
                'message': 'Token expired',
                'statusCode': 401,
                'href': 'https://help.io/error/40142',
            }),
            'event': 'error'
        })
        assert isinstance(parse_incoming_event(e0), AblyError)
    
    def test_occupancy_parsing(self):
        """Test parse Occupancy event."""
        e0 = json.dumps({
            'data': json.dumps({
                'id':'ZlalwoKlXW:0:0',
                'timestamp':1591996755043,
                'encoding':'json',
                'channel': '[?occupancy=metrics.publishers]control_sec',
                'data': json.dumps({
                    'metrics': json.dumps({
                        'publishers': 1
                    }),
                }),
                'name': '[meta]occupancy',
            }),
            'event': 'message'
        })
        assert isinstance(parse_incoming_event(e0), Occupancy)