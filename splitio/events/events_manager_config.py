"""Events Manager Configuration."""
from splitio.models.events import SdkEvent, SdkInternalEvent

class EventsManagerConfig(object):
    """Events Manager Configurations class."""

    def __init__(self):
        """
        Construct Events Manager Configuration instance.
        """
        self._require_all = self._get_require_all()
        self._prerequisites = self._get_prerequisites()
        self._require_any = self._get_require_any()
        self._suppressed_by = self._get_suppressed_by()
        self._execution_limits = self._get_execution_limits()
        self._evaluation_order = self._get_sorted_events()
        
    @property
    def require_all(self):
        """Return require all dict"""
        return self._require_all    

    @property
    def prerequisites(self):
        """Return prerequisites dict"""
        return self._prerequisites

    @property
    def require_any(self):
        """Return require_any dict"""
        return self._require_any
    
    @property
    def suppressed_by(self):
        """Return suppressed_by dict"""
        return self._suppressed_by
    
    @property
    def execution_limits(self):
        """Return execution_limits dict"""
        return self._execution_limits
        
    @property
    def evaluation_order(self):
        """Return evaluation_order dict"""
        return self._evaluation_order
            
    def _get_require_all(self):
        """Return require all dict"""
        return  {
                    SdkEvent.SDK_READY: {SdkInternalEvent.SDK_READY}
                }

    def _get_prerequisites(self):
        """Return prerequisites dict"""
        return  {
                    SdkEvent.SDK_UPDATE: {SdkEvent.SDK_READY}
                }

    def _get_require_any(self):
        """Return require_any dict"""
        return  {
                    SdkEvent.SDK_UPDATE: {SdkInternalEvent.FLAG_KILLED_NOTIFICATION, SdkInternalEvent.FLAGS_UPDATED, 
                                             SdkInternalEvent.RB_SEGMENTS_UPDATED, SdkInternalEvent.SEGMENTS_UPDATED}
                }

    def _get_suppressed_by(self):
        """Return suppressed_by dict"""
        return  {
                }

    def _get_execution_limits(self):
        """Return execution_limits dict"""
        return  {
                    SdkEvent.SDK_READY: 1,
                    SdkEvent.SDK_UPDATE: -1
                }

    def _get_sorted_events(self):
        """Return dorted events set"""
        sorted_events = []
        for sdk_event in [SdkEvent.SDK_READY, SdkEvent.SDK_UPDATE]:
            sorted_events = self._dfs_recursive(sdk_event, sorted_events)

        return sorted_events


    def _dfs_recursive(self, sdk_event, added):
        """Return sorted events set based on the dependency rules"""
        if sdk_event in added:
            return added

        for dependent_event in self._get_dependencies(sdk_event):
            added = self._dfs_recursive(dependent_event, added)

        added.append(sdk_event)
        return added

    def _get_dependencies(self, sdk_event):
        """Return dependencies set from prerequisites and suppressed events for a given event"""
        dependencies = set()
        for prerequisites_event_name, prerequisites_event_value in self.prerequisites.items(): 
            if prerequisites_event_name == sdk_event:
                for prereq_event in prerequisites_event_value:
                    dependencies.add(prereq_event)

        for suppressed_event_name, suppressed_event_value in self.suppressed_by.items():
            if sdk_event in suppressed_event_value:
                dependencies.add(suppressed_event_name)

        return dependencies
