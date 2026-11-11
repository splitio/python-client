"""Misc helpers for testing purposes."""


class Any(object):
    """Crap that matches anything."""

    def __eq__(self, other):
        """Match anything."""
        return True
