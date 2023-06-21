"""Utilities."""
from datetime import datetime
import time
from splitio.optional.loaders import asyncio

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

def get_current_epoch_time_ms():
    """
    Get current epoch time in milliseconds

    :return: epoch time
    :rtype: int
    """
    return int(round(time.time() * 1000))

class TimerAsync:
    """
    Timer Class that uses Asyncio lib
    """
    def __init__(self, timeout, callback):
        """
        Class init

        :param timeout: timeout in seconds
        :type timeout: int

        :param callback: callback funciton when timer is done.
        :type callback: func
        """
        self._timeout = timeout
        self._callback = callback
        self._task = asyncio.ensure_future(self._job())

    async def _job(self):
        """Run the timer and perform callback when done """

        await asyncio.sleep(self._timeout)
        await self._callback()

    def cancel(self):
        """Cancel the timer"""

        self._task.cancel()
