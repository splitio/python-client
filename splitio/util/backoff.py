"""Exponential Backoff duration calculator."""


class Backoff(object):
    """Backoff duration calculator."""

    MAX_ALLOWED_WAIT = 30 * 60  # half an hour

    def __init__(self, base=1):
        """
        Class constructor.

        :param base: basic unit to be multiplied on each iteration (seconds)
        :param base: float
        """
        self._base = base
        self._attempt = 0

    def get(self):
        """
        Return the current time to wait and pre-calculate the next one.

        :returns: time to wait until next retry.
        :rtype: float
        """
        to_return = min(self._base * (2 ** self._attempt), self.MAX_ALLOWED_WAIT)
        self._attempt += 1
        return to_return

    def reset(self):
        """Reset the attempt count."""
        self._attempt = 0
