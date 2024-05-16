class MatcherNotFoundException(Exception):
    """Exception to raise when a matcher is not found."""

    def __init__(self, custom_message):
        """Constructor."""
        Exception.__init__(self, custom_message)