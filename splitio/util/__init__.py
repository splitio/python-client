"""Utilities."""
from datetime import datetime


EPOCH_DATETIME = datetime(1970, 1, 1)

def utctime():
    """
    Return the utc time in nanoseconds.

    :returns: utc time in nanoseconds.
    :rtype: float
    """
    return (datetime.utcnow() - EPOCH_DATETIME).total_seconds()


def utctime_ms():
    """
    Return the utc time in milliseconds.

    :returns: utc time in milliseconds.
    :rtype: int
    """
    return int(utctime() * 1000)
