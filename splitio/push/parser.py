import json

ERROR = 'error'
OCCUPANCY = 'occupancy'
UPDATE = 'update'
TAG_OCCUPANCY = '[meta]occupancy'

class AblyError(object):
    def __init__(self, code, status_code, message, href):
        self._code = code
        self._status_code = status_code
        self._message = message
        self._event = ERROR
        self._href = href

class Occupancy(object):
    def __init__(self, data, channel):
        self._data = data
        self._event = OCCUPANCY
        self._channel = channel

class Update(object):
    def __init__(self, data, channel):
        self._data = data
        self._event = UPDATE
        self._channel = channel


def parse_incoming_event(raw_event):
    if raw_event is None or len(raw_event.strip()) == 0:
        return None
    
    try:
        parsed_json = json.loads(raw_event)
        if parsed_json is None:
            return None

        if 'statusCode' in parsed_json and 'code' in parsed_json and 'message' in parsed_json and 'href' in parsed_json:
            return AblyError(parsed_json['code'], parsed_json['statusCode'], parsed_json['message'], parsed_json['href'])
        elif 'name' in parsed_json and parsed_json['name'] == TAG_OCCUPANCY and 'data' in parsed_json and 'channel' in parsed_json:
            return Occupancy(parsed_json['data'], parsed_json['channel'])
        elif 'data' in parsed_json and 'channel' in parsed_json:
            return Update(parsed_json['data'], parsed_json['channel'])
        return None
    except ValueError:
        raise ValueError('Cannot parse json.')
