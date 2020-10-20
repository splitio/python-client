import json

from future.utils import raise_from

MESSAGE = 'message'
ERROR = 'error'
OCCUPANCY = 'occupancy'
UPDATE = 'update'
TAG_OCCUPANCY = '[meta]occupancy'

class EventParserException(Exception):
    """Exception to be raised on parser errors."""

    pass


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


def parseAblyError(parsed_data):
    if 'statusCode' in parsed_data and 'code' in parsed_data and 'message' in parsed_data and 'href' in parsed_data:
        return AblyError(parsed_data['code'], parsed_data['statusCode'], parsed_data['message'], parsed_data['href'])
    return None

def parseNotification(parsed_data):
    if 'name' in parsed_data and parsed_data['name'] == TAG_OCCUPANCY and 'data' in parsed_data and 'channel' in parsed_data:
        return Occupancy(parsed_data['data'], parsed_data['channel'])
    elif 'data' in parsed_data and 'channel' in parsed_data:
        return Update(parsed_data['data'], parsed_data['channel'])
    return None

_INCOMMING_EVENT_MAPPERS = {
    ERROR: lambda d: parseAblyError(d),
    MESSAGE: lambda d: parseNotification(d),
}

def parse_incoming_event(raw_event):
    if raw_event is None or len(raw_event.strip()) == 0:
        return None
    
    try:
        parsed_raw_event = json.loads(raw_event)
        if parsed_raw_event is None:
            return None

        if not 'event' in parsed_raw_event or not 'data' in parsed_raw_event:
            return None

        parsed_data = json.loads(parsed_raw_event['data'])
        mapper = _INCOMMING_EVENT_MAPPERS[parsed_raw_event['event']]
        return mapper(parsed_data)
    except KeyError as exc:
        raise_from(EventParserException('No mapper registered for that event'), exc)
    except ValueError as exc:
        raise_from(EventParserException('Cannot parse json.'), exc)
