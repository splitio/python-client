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
    
    parsed_json = json.loads(raw_event)
    if parsed_json is None:
        return None

    print(parsed_json)

    if 'statusCode' in parsed_json:
        return AblyError(parsed_json['code'], parsed_json['statusCode'], parsed_json['message'], parsed_json['href'])
    elif 'name' in parsed_json and parsed_json['name'] == TAG_OCCUPANCY:
        return Occupancy(parsed_json['data'], parsed_json['channel'])
    
    return Update(parsed_json['data'], parsed_json['channel'])
    
