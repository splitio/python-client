import abc

class BaseStrategy(object, metaclass=abc.ABCMeta):
    """Strategy interface."""

    @abc.abstractmethod
    def process_impressions(self):
        """
        Return a list(impressions) object

        """
        pass