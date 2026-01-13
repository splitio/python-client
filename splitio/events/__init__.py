"""Base storage interfaces."""
import abc

class EventsManagerInterface(object, metaclass=abc.ABCMeta):
    """Events manager interface implemented as an abstract class."""

    @abc.abstractmethod
    def register(self, sdk_event, event_handler):
        pass
    
    @abc.abstractmethod
    def unregister(self, sdk_event):
        pass
    
    @abc.abstractmethod
    def notify_internal_event(self, sdk_internal_event, event_metadata):
        pass
    

class EventsDeliveryInterface(object, metaclass=abc.ABCMeta):
    """Events Delivery interface."""

    @abc.abstractmethod
    def deliver(self, sdk_event, event_metadata, event_handler):
        pass