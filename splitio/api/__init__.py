"""Split API module."""

class APIException(Exception):
    """Exception to raise when an API call fails."""

    def __init__(self, custom_message, status_code=None, original_exception=None):
        """Constructor."""
        Exception.__init__(self, custom_message)
        self._status_code = status_code if status_code else -1
        self._custom_message = custom_message
        self._original_exception = original_exception

    @property
    def status_code(self):
        """Return HTTP status code."""
        return self._status_code

    @property
    def custom_message(self):
        """Return custom message."""
        return self._custom_message
