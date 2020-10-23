"""Misc helpers for testing purposes."""


class Any(object):  #pylint:disable=too-few-public-methods
    """Crap that matches anything."""

    def __eq__(self, other):
        """Match anything."""
        return True
