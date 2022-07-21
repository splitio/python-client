import abc

class BaseStrategy(object, metaclass=abc.ABCMeta):
    """Strategy interface."""

    @abc.abstractmethod
    def process_impressions(self):
        """
        Return a list(impressions) object

        """
        pass

    @abc.abstractmethod
    def truncate_impressions_time(self):
        """
        Return list(impressions) object

        """
        pass

    def get_counts(self):
        """
        Return A list of counter objects.

        """
        pass