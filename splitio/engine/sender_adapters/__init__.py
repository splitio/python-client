import abc

class ImpressionsSenderAdapter(object, metaclass=abc.ABCMeta):
    """Impressions Sender Adapter interface."""

    @abc.abstractmethod
    def record_unique_keys(self, data):
        """
        No Return value

        """
        pass

    @abc.abstractmethod
    def record_impressions_count(self, data):
        """
        No Return value

        """
        pass
