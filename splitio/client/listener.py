"""Impression listener module."""

import abc


class ImpressionListenerException(Exception):
    """Custom Exception for Impression Listener."""

    pass


class ImpressionListenerWrapper(object):  # pylint: disable=too-few-public-methods
    """
    Impression listener safe-execution wrapper.

    Wrapper in charge of building all the data that client would require in case
    of adding some logic with the treatment and impression results.
    """

    impression_listener = None

    def __init__(self, impression_listener, sdk_metadata):
        """
        Class Constructor.

        :param impression_listener: User provided impression listener.
        :type impression_listener: ImpressionListener
        :param sdk_metadata: SDK version, instance name & IP
        :type sdk_metadata: splitio.client.util.SdkMetadata
        """
        self.impression_listener = impression_listener
        self._metadata = sdk_metadata

    def log_impression(self, impression, attributes=None):
        """
        Send an impression to the user-provided listener.

        :param impression: Imression data
        :type impression: dict
        :param attributes: User provided attributes when calling get_treatment(s)
        :type attributes: dict
        """
        data = {}
        data['impression'] = impression
        data['attributes'] = attributes
        data['sdk-language-version'] = self._metadata.sdk_version
        data['instance-id'] = self._metadata.instance_name
        try:
            self.impression_listener.log_impression(data)
        except Exception as exc:  # pylint: disable=broad-except
            raise ImpressionListenerException('Error in log_impression user\'s method is throwing exceptions') from exc


class ImpressionListener(object, metaclass=abc.ABCMeta):
    """Impression listener interface."""

    @abc.abstractmethod
    def log_impression(self, data):
        """
        Accept and impression generated after an evaluation for custom user handling.

        :param data: Impression data in a dictionary format.
        :type data: dict
        """
        pass
