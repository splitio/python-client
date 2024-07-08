class AuthException(Exception):
    """Exception to raise when an API call fails."""

    def __init__(self, custom_message, status_code=None):
        """Constructor."""
        Exception.__init__(self, custom_message)

class SplitStorageException(Exception):
    """Exception to raise when an API call fails."""

    def __init__(self, custom_message, status_code=None):
        """Constructor."""
        Exception.__init__(self, custom_message)
