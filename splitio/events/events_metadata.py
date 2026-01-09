"""Events Metadata."""
from enum import Enum

class SdkEventType(Enum):
    """Public event types"""
    
    FLAG_UPDATE = 'FLAG_UPDATE'
    SEGMENT_UPDATE = 'SEGMENT_UPDATE'

class EventsMetadata(object):
    """Events Metadata class."""

    def __init__(self, type, names):
        """
        Construct Events Metadata instance.
        """
        self._type = type
        self._names = self._sanitize(names)
        
    def get_type(self):
        """Return type"""
        return self._type

    def get_names(self):
        """Return names"""
        return self._names
    
    def _sanitize(self, names):
        """Return sanitized names list with values str"""
        santized_data = set()
        for name in names:
            if isinstance(name, str): 
                santized_data.add(name)
   
        return santized_data
