"""Events Manager."""
import threading
import logging
from collections import namedtuple
from splitio.optional.loaders import asyncio

from splitio.events import EventsManagerInterface
from splitio.models.events import SdkEvent

_LOGGER = logging.getLogger(__name__)

ValidSdkEvent = namedtuple('ValidSdkEvent', ['sdk_event', 'valid'])
ActiveSubscriptions = namedtuple('ActiveSubscriptions', ['triggered', 'handler'])

class EventsManagerBase(EventsManagerInterface):
    """Events Manager class."""

    def __init__(self, events_configurations, events_delivery):
        """
        Construct Events Manager instance.
        """
        self._active_subscriptions = {}
        self._internal_events_status = {}
        self._events_delivery = events_delivery
        self._manager_config = events_configurations
        
    def register(self, sdk_event, event_handler):
        # Implement in child class
        pass
    
    def unregister(self, sdk_event):
        # Implement in child class
        pass
    
    def notify_internal_event(self, sdk_internal_event, event_metadata):
        # Implement in child class
        pass
    
    def destroy(self):
        # Implement in child class
        pass

    def _event_already_triggered(self, sdk_event):
        if self._active_subscriptions.get(sdk_event) != None:
            return self._active_subscriptions.get(sdk_event).triggered
        
        return False
    
    def _get_internal_event_status(self, sdk_internal_event):
        if self._internal_events_status.get(sdk_internal_event) != None:
            return self._internal_events_status[sdk_internal_event]
        
        return False
                    
    def _update_internal_event_status(self, sdk_internal_event, status):
        self._internal_events_status[sdk_internal_event] = status

    def _set_sdk_event_triggered(self, sdk_event):
        if self._active_subscriptions.get(sdk_event) == None:
            return
        
        if self._active_subscriptions.get(sdk_event).triggered == True:
            return
        
        self._active_subscriptions[sdk_event] = self._active_subscriptions[sdk_event]._replace(triggered = True)

    def _get_event_handler(self, sdk_event):
        if self._active_subscriptions.get(sdk_event) == None:
            return None
        
        return self._active_subscriptions.get(sdk_event).handler

    def _get_sdk_event_if_applicable(self, sdk_internal_event):
        final_sdk_event = ValidSdkEvent(None, False)
         
        events_to_fire = []
        require_any_sdk_event = self._check_require_any(sdk_internal_event)
        if require_any_sdk_event.valid:
            if (not self._event_already_triggered(require_any_sdk_event.sdk_event) and 
                    self._execution_limit(require_any_sdk_event.sdk_event) == 1)  or \
                    self._execution_limit(require_any_sdk_event.sdk_event) == -1:
                final_sdk_event = final_sdk_event._replace(sdk_event = require_any_sdk_event.sdk_event, 
                                                           valid = self._check_prerequisites(require_any_sdk_event.sdk_event) and \
                                                            self._check_suppressed_by(require_any_sdk_event.sdk_event))
                                
                if final_sdk_event.valid:
                    events_to_fire.append(final_sdk_event.sdk_event)
                    
        [events_to_fire.append(sdk_event) for sdk_event in self._check_require_all()]   
        
        return events_to_fire
    
    def _check_require_all(self):
        events = []
        for require_name, require_value in self._manager_config.require_all.items():
            final_status = True
            for val in require_value:
                final_status &= self._get_internal_event_status(val)
            
            if final_status and  \
                self._check_prerequisites(require_name) and \
                ((not self._event_already_triggered(require_name) and 
                self._execution_limit(require_name) == 1)  or \
                self._execution_limit(require_name) == -1) and \
                len(require_value) > 0:
                    
                events.append(require_name)
        
        return events
    
    def _check_prerequisites(self, sdk_event):
        for name, value in self._manager_config.prerequisites.items():
            for val in value:
                if name == sdk_event and not self._event_already_triggered(val):
                    return False
        
        return True
    
    def _check_suppressed_by(self, sdk_event):
        for name, value in self._manager_config.suppressed_by.items():
            for val in value:
                if name == sdk_event and self._event_already_triggered(val):
                    return False
        
        return True

    def _execution_limit(self, sdk_event):
        limit = self._manager_config.execution_limits.get(sdk_event)
        if limit == None:
            return -1
        
        return limit
    
    def _check_require_any(self, sdk_internal_event):
        valid_sdk_event = ValidSdkEvent(None, False)
        for name, val in self._manager_config.require_any.items():
            if sdk_internal_event in val:
                valid_sdk_event = valid_sdk_event._replace(valid = True, sdk_event = name)
                return valid_sdk_event
        
        return valid_sdk_event
    
class EventsManager(EventsManagerBase):
    """Events Manager class."""

    def __init__(self, events_configurations, events_delivery):
        """
        Construct Events Manager instance.
        """
        EventsManagerBase.__init__(self, events_configurations, events_delivery)
        self._lock = threading.RLock()
        
    def register(self, sdk_event, event_handler):
        if self._active_subscriptions.get(sdk_event) != None and self._get_event_handler(sdk_event) != None:
            return

        with self._lock:
            # SDK ready already fired
            if sdk_event == SdkEvent.SDK_READY and self._event_already_triggered(sdk_event):
                self._active_subscriptions[sdk_event] = ActiveSubscriptions(True, event_handler)
                _LOGGER.debug("EventsManager: Firing SDK_READY event for new subscription")                
                self._fire_sdk_event(sdk_event, None)
                return

            self._active_subscriptions[sdk_event] = ActiveSubscriptions(False, event_handler)

    def unregister(self, sdk_event):
        if self._active_subscriptions.get(sdk_event) == None:
            return
        
        with self._lock:
            del self._active_subscriptions[sdk_event]

    def notify_internal_event(self, sdk_internal_event, event_metadata):
        with self._lock:
            self._update_internal_event_status(sdk_internal_event, True)
            for sorted_event in self._manager_config.evaluation_order:
                if sorted_event in self._get_sdk_event_if_applicable(sdk_internal_event):
                    if self._get_event_handler(sorted_event) != None:
                        self._fire_sdk_event(sorted_event, event_metadata)
                        
                    # if client is not subscribed to SDK_READY    
                    if sorted_event == SdkEvent.SDK_READY and self._get_event_handler(sorted_event) == None:
                        _LOGGER.debug("EventsManager: Registering SDK_READY event as fired")
                        self._active_subscriptions[SdkEvent.SDK_READY] = ActiveSubscriptions(True, None)
    
    def destroy(self):
        with self._lock:
            self._active_subscriptions = {}
            self._internal_events_status = {}
                        
    def _fire_sdk_event(self, sdk_event, event_metadata):
        _LOGGER.debug("EventsManager: Firing Sdk event %s", sdk_event)
        notify_event = threading.Thread(target=self._events_delivery.deliver, args=[sdk_event, event_metadata, self._get_event_handler(sdk_event)],
                                name='SplitSDKEventNotify', daemon=True)
        notify_event.start()
        self._set_sdk_event_triggered(sdk_event)

class EventsManagerAsync(EventsManagerBase):
    """Events Manager Async class."""

    def __init__(self, events_configurations, events_delivery):
        """
        Construct Events Manager instance.
        """
        EventsManagerBase.__init__(self, events_configurations, events_delivery)
        self._lock = asyncio.Lock()
        
    async def register(self, sdk_event, event_handler):
        if self._active_subscriptions.get(sdk_event) != None and self._get_event_handler(sdk_event) != None:
            return

        async with self._lock:
            # SDK ready already fired
            if sdk_event == SdkEvent.SDK_READY and self._event_already_triggered(sdk_event):
                self._active_subscriptions[sdk_event] = ActiveSubscriptions(True, event_handler)
                _LOGGER.debug("EventsManager: Firing SDK_READY event for new subscription")                
                await self._fire_sdk_event(sdk_event, None)
                return

            self._active_subscriptions[sdk_event] = ActiveSubscriptions(False, event_handler)

    async def unregister(self, sdk_event):
        if self._active_subscriptions.get(sdk_event) == None:
            return
        
        async with self._lock:
            del self._active_subscriptions[sdk_event]

    async def notify_internal_event(self, sdk_internal_event, event_metadata):
        async with self._lock:
            self._update_internal_event_status(sdk_internal_event, True)
            for sorted_event in self._manager_config.evaluation_order:
                if sorted_event in self._get_sdk_event_if_applicable(sdk_internal_event):
                    if self._get_event_handler(sorted_event) != None:
                        await self._fire_sdk_event(sorted_event, event_metadata)
                        
                    # if client is not subscribed to SDK_READY    
                    if sorted_event == SdkEvent.SDK_READY and self._get_event_handler(sorted_event) == None:
                        _LOGGER.debug("EventsManager: Registering SDK_READY event as fired")
                        self._active_subscriptions[SdkEvent.SDK_READY] = ActiveSubscriptions(True, None)

    async def destroy(self):
        async with self._lock:
            self._active_subscriptions = {}
            self._internal_events_status = {}

    def _fire_sdk_event(self, sdk_event, event_metadata):
        _LOGGER.debug("EventsManager: Firing Sdk event %s", sdk_event)
        asyncio.get_running_loop().create_task(self._events_delivery.deliver_async(sdk_event, event_metadata, self._get_event_handler(sdk_event)))
        self._set_sdk_event_triggered(sdk_event)