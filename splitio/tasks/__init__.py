"""Split synchronization tasks module."""

import abc


class BaseSynchronizationTask(object):
    """Syncrhonization task interface."""

    __metadata__ = abc.ABCMeta

    @abc.abstractmethod
    def start(self):
        """Start the task."""
        pass

    @abc.abstractmethod
    def stop(self, event=None):
        """
        Stop the task if running.

        Optionally accept an event to be set when the task finally stops.

        :param event: Event to be set as soon as the task finishes.
        :type event: Threading.Event
        """
        pass

    @abc.abstractmethod
    def is_running(self):
        """Return true if the task is running, false otherwise."""
        pass
