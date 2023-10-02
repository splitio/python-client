class FeatureNotFoundException(Exception):
    """Exception to raise when an API call fails."""

    def __init__(self, custom_message):
        """Constructor."""
        Exception.__init__(self, custom_message)