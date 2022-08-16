import abc

class BaseFilter(object, metaclass=abc.ABCMeta):
    """Impressions Filter interface."""

    @abc.abstractmethod
    def add(self, data):
        """
        Return a boolean flag

        """
        pass

    @abc.abstractmethod
    def contains(self, data):
        """
        Return a boolean flag

        """
        pass

    @abc.abstractmethod
    def clear(self):
        """
        No return

        """
        pass