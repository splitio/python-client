"""Split API module."""


class APIException(Exception):
    """Exception to raise when an API call fails."""

    def __init__(self, custom_message, status_code=None):
        """Constructor."""
        Exception.__init__(self, custom_message)
        self._status_code = status_code if status_code else -1

    @property
    def status_code(self):
        """Return HTTP status code."""
        return self._status_code

class HttpClientException(Exception):
    """HTTP Client exception."""

    def __init__(self, message):
        """
        Class constructor.

        :param message: Information on why this exception happened.
        :type message: str
        """
        Exception.__init__(self, message)

def build_basic_headers(apikey):
    """
    Build basic headers with auth.

    :param apikey: API token used to identify backend calls.
    :type apikey: str
    """
    return {
        'Content-Type': 'application/json',
        'Authorization': "Bearer %s" % apikey
    }
