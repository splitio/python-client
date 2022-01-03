import logging

from splitio.engine.evaluator import CONTROL
from splitio.client.client import input_validator


_LOGGER = logging.getLogger(__name__)


class SplitFactoryWrapped(object):
    """Class that wrapps SplitFactoryWrapped."""

    def __init__(self):
        """
        Class constructor.
        """
        self._evaluations_enabled = False
        self._ready = True
        self._destroyed = False

    def client(self):
        """
        Return a new client.

        This client is only a set of references to structures hold by the factory.
        Creating one a fast operation and safe to be used anywhere.
        """
        return SplitClientWrapped(self)

    def manager(self):
        """
        Return a new manager.

        This manager is only a set of references to structures hold by the factory.
        Creating one a fast operation and safe to be used anywhere.
        """
        return ManagerWrapped(self)

    def block_until_ready(self, timeout=None):
        """
        Blocks until the sdk is ready or the timeout specified by the user expires.

        When ready, the factory's status is updated accordingly.

        :param timeout: Number of seconds to wait (fractions allowed)
        :type timeout: int
        """
        pass

    @property
    def ready(self):
        """
        Return whether the factory is ready.

        :return: True if the factory is ready. False otherwhise.
        :rtype: bool
        """
        return self._ready

    def destroy(self, destroyed_event=None):
        """
        Destroy the factory and render clients unusable.

        Destroy frees up storage taken but split data, flushes impressions & events,
        and invalidates the clients, making them return control.

        :param destroyed_event: Event to signal when destroy process has finished.
        :type destroyed_event: threading.Event
        """
        self._destroyed = True

    @property
    def destroyed(self):
        """
        Return whether the factory has been destroyed or not.

        :return: True if the factory has been destroyed. False otherwise.
        :rtype: bool
        """
        return self._destroyed

    def resume(self):
        """
        Function in charge of starting periodic/realtime synchronization after a fork.
        """
        pass

    @property
    def evaluations_enabled(self):
        """
        Return whether the evaluations are bypassed.

        :return: True if the evaluations are bypassed. False otherwhise.
        :rtype: bool
        """
        return self._evaluations_enabled


class SplitClientWrapped(object):
    """Class that wrapps SplitClientWrapped."""

    def __init__(self, factory):
        """
        Class constructor.

        :param factory: Split factory (client & manager container)
        :type factory: SplitFactoryWrapped

        """
        self._factory = factory

    @property
    def evaluations_enabled(self):
        """
        Return whether the evaluations are bypassed.

        :return: True if the evaluations are bypassed. False otherwhise.
        :rtype: bool
        """
        return self._factory.evaluations_enabled

    def destroy(self):
        """
        Destroy the underlying factory.

        Only applicable when using in-memory operation mode.
        """
        self._factory.destroy()

    def _treatment_control(self):
        return CONTROL, None

    def get_treatment_with_config(self, key, feature, attributes=None):
        """
        Get the treatment and config for a feature and key, with optional dictionary of attributes.

        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.

        :param key: The key for which to get the treatment
        :type key: str
        :param feature: The name of the feature for which to get the treatment
        :type feature: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: The treatment for the key and feature
        :rtype: tuple(str, str)
        """
        return self._treatment_control()

    def get_treatment(self, key, feature, attributes=None):
        """
        Get the treatment for a feature and key, with an optional dictionary of attributes.

        This method never raises an exception. If there's a problem, the appropriate log message
        will be generated and the method will return the CONTROL treatment.

        :param key: The key for which to get the treatment
        :type key: str
        :param feature: The name of the feature for which to get the treatment
        :type feature: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: The treatment for the key and feature
        :rtype: str
        """
        return self._treatment_control()[0]

    def get_treatments_with_config(self, key, features, attributes=None):
        """
        Evaluate multiple features and return a dict with feature -> (treatment, config).

        Get the treatments for a list of features considering a key, with an optional dictionary of
        attributes. This method never raises an exception. If there's a problem, the appropriate
        log message will be generated and the method will return the CONTROL treatment.
        :param key: The key for which to get the treatment
        :type key: str
        :param features: Array of the names of the features for which to get the treatment
        :type feature: list
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: Dictionary with the result of all the features provided
        :rtype: dict
        """
        try:
            return input_validator.generate_control_treatments(features, 'get_treatments_with_config')
        except Exception:  # pylint: disable=broad-except
            _LOGGER.error('Error calling get_treatments_with_config')
            _LOGGER.debug('Error: ', exc_info=True)
            return {}

    def get_treatments(self, key, features, attributes=None):
        """
        Evaluate multiple features and return a dictionary with all the feature/treatments.

        Get the treatments for a list of features considering a key, with an optional dictionary of
        attributes. This method never raises an exception. If there's a problem, the appropriate
        log message will be generated and the method will return the CONTROL treatment.
        :param key: The key for which to get the treatment
        :type key: str
        :param features: Array of the names of the features for which to get the treatment
        :type feature: list
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: Dictionary with the result of all the features provided
        :rtype: dict
        """
        try:
            with_config = input_validator.generate_control_treatments(features, 'get_treatments')
            return {feature: result[0] for (feature, result) in with_config.items()}
        except Exception:  # pylint: disable=broad-except
            _LOGGER.error('Error calling get_treatments')
            _LOGGER.debug('Error: ', exc_info=True)
            return {}

    def track(self, key, traffic_type, event_type, value=None, properties=None):
        """
        Track an event.

        :param key: user key associated to the event
        :type key: str
        :param traffic_type: traffic type name
        :type traffic_type: str
        :param event_type: event type name
        :type event_type: str
        :param value: (Optional) value associated to the event
        :type value: Number
        :param properties: (Optional) properties associated to the event
        :type properties: dict

        :return: Whether the event was created or not.
        :rtype: bool
        """
        return True


class ManagerWrapped(object):
    """Class that wrapps ManagerWrapped."""

    def __init__(self, factory):
        """
        Class constructor.

        :param factory: Split factory (client & manager container)
        :type factory: SplitFactoryWrapped

        """
        self._factory = factory

    @property
    def evaluations_enabled(self):
        """
        Return whether the evaluations are bypassed.

        :return: True if the evaluations are bypassed. False otherwhise.
        :rtype: bool
        """
        return self._factory.evaluations_enabled

    def split_names(self):
        """
        Get the name of fetched splits.

        :return: A list of str
        :rtype: list
        """
        return []

    def splits(self):
        """
        Get the fetched splits. Subclasses need to override this method.

        :return: A List of SplitView.
        :rtype: list()
        """
        return []

    def split(self, feature_name):
        """
        Get the splitView of feature_name. Subclasses need to override this method.

        :param feature_name: Name of the feture to retrieve.
        :type feature_name: str

        :return: The SplitView instance.
        :rtype: splitio.models.splits.SplitView
        """
        return None
