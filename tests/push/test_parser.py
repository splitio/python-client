import json

from splitio.push.parser import parse_incoming_event, Update, AblyError, Occupancy


def wrap_json(channel, data):
    base = '{{"channel":"{channel}","data":"{data}","id":"ZlalwoKlXW:0:0","clientId":"pri:MzIxMDYyOTg5MA==","timestamp":1591996755043,"encoding":"json"}}'
    return base.format(channel=channel, data=data)


class ParserTests(object):
    """Parser tests."""

    def test_event_parsing(self):
        """Test parse Update event."""
        e0 = wrap_json(
            'NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits',
            "{'type':'SPLIT_KILL','changeNumber':1591996754396,'defaultTreatment':'some','splitName':'test'}",
        )
        assert isinstance(parse_incoming_event(e0), Update)

        e1 = wrap_json(
            'NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_splits',
            "{'type':'SPLIT_UPDATE','changeNumber':1591996685190}",
        )
        assert isinstance(parse_incoming_event(e1), Update)
    
        e2 = wrap_json(
            'NDA5ODc2MTAyNg==_MzAyODY0NDkyOA==_segments',
            "{'type':'SEGMENT_UPDATE','changeNumber':1591988398533,'segmentName':'some'}",
        )
        assert isinstance(parse_incoming_event(e2), Update)

    def test_error_parsing(self):
        """Test parse AblyError event."""
        e0 = '{"code":40142,"message":"Token expired","statusCode":401,"href":"https://help.io/error/40142","timestamp":1591996755043,"encoding":"json"}'
        assert isinstance(parse_incoming_event(e0), AblyError)
    
    def test_occupancy_parsing(self):
        """Test parse Occupancy event."""
        e0 = '{"channel":"[?occupancy=metrics.publishers]control_sec","data":"{\\"metrics\\":{\\"publishers\\":1}}","id":"ZlalwoKlXW:0:0","clientId":"pri:MzIxMDYyOTg5MA==","timestamp":1591996755043,"encoding":"json","name":"[meta]occupancy"}'
        assert isinstance(parse_incoming_event(e0), Occupancy)
