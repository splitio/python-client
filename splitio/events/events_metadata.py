"""Events Metadata."""
from splitio.models.events import SdkEvent, SdkInternalEvent 

class EventsMetadata(object):
    """Events Metadata class."""

    def __init__(self, metadata):
        """
        Construct Events Metadata instance.
        """
        self._metadata = self._sanitize(metadata)
        
    def get_data(self):
        """Return metadata dict"""
        return self._metadata

    def get_keys(self):
        """Return metadata dict keys"""
        return self._metadata.keys()

    def get_values(self):
        """Return metadata dict values"""
        return self._metadata.values()
    
    def contain_key(self, key):
        """Return True if key is contained in metadata"""
        return key in self._metadata.keys()
    
    def _sanitize(self, data):
        """Return sanitized metadata dict with values either int, bool, str or list """
        santized_data = {}
        for item_name, item_value in data.items():
            if self._value_is_valid(item_value): 
                santized_data[item_name] = item_value
   
        return santized_data

    def _value_is_valid(self, value):
        """Return bool if values is int, bool, str or list[str] """
        if (value is not None) and (isinstance(value, int) or isinstance(value, bool) or isinstance(value, str)):
            return True
        
        if isinstance(value, set):
            return any([isinstance(item, str) for item in value])

        return False