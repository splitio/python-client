"""Synchronizer module."""

import logging
import threading

# Tasks
from splitio.tasks.split_sync import SplitSynchronizationTask
from splitio.tasks.segment_sync import SegmentSynchronizationTask
from splitio.tasks.impressions_sync import ImpressionsSyncTask
from splitio.tasks.events_sync import EventsSyncTask
from splitio.tasks.telemetry_sync import TelemetrySynchronizationTask

_LOGGER = logging.getLogger(__name__)

class SplitTasks(object):
    """SplitTasks."""

    def __init__(self, split_task, segment_task, impressions_task, events_task, telemetry_task):
        if not isinstance(split_task, SplitSynchronizationTask):
            return None
        self._split_task = split_task
        if not isinstance(segment_task, SegmentSynchronizationTask):
            return None
        self._segment_task = segment_task
        if not isinstance(impressions_task, ImpressionsSyncTask):
            return None
        self._impressions_task = impressions_task
        if not isinstance(events_task, EventsSyncTask):
            return None
        self._events_task = events_task
        if not isinstance(telemetry_task, TelemetrySynchronizationTask):
            return None
        self._telemetry_task = telemetry_task
    
    @property
    def split_task(self):
        return self._split_task
    
    @property
    def segment_task(self):
        return self._segment_task
        
    @property
    def impressions_task(self):
        return self._impressions_task

    @property
    def events_task(self):
        return self._events_task
    
    @property
    def telemetry_task(self):
        return self._telemetry_task


class Synchronizer(object):
    """Synchronizer."""

    def __init__(self, split_tasks):
        if not isinstance(split_tasks, SplitTasks):
            _LOGGER.error('Unexpected type of split_tasks')
            return None
        self._split_tasks = split_tasks
    
    def _synchronize_segments(self):
        _LOGGER.debug('Starting segments synchronization')
        return self._split_tasks.segment_task.update_segments()
    
    def synchronize_segment(self, segment_name, till):
        _LOGGER.debug('Synchronizing segment %s', segment_name)
        return self._split_tasks.segment_task.update_segment(segment_name, till)

    def synchronize_splits(self, till):
        _LOGGER.debug('Starting splits synchronization')
        return self._split_tasks.split_task.update_splits(till)
    
    def sync_all(self):
        if self.synchronize_splits(None) is False:
            _LOGGER.error('Failed fetching splits')
        return self._synchronize_segments()
    
    def start_periodic_fetching(self):
        _LOGGER.debug('Starting periodic data fetching')
        self._split_tasks.split_task.start()
        self._split_tasks.segment_task.start()

    def stop_periodic_fetching(self):
        _LOGGER.debug('Stopping periodic fetching')
        self._split_tasks.split_task.stop()
        self._split_tasks.segment_task.stop()

    def start_periodic_data_recording(self):
        _LOGGER.debug('Starting periodic data recording')
        self._split_tasks.impressions_task.start()
        self._split_tasks.events_task.start()
        self._split_tasks.telemetry_task.start()

    def stop_periodic_data_recording(self):
        _LOGGER.debug('Stopping periodic data recording')
        stop_event = threading.Event()
        self._split_tasks.impressions_task.stop(stop_event)
        self._split_tasks.events_task.stop(stop_event)
        stop_event.wait()
        self._split_tasks.telemetry_task.stop()
