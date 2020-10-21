"""Synchronizer module."""

import logging
import threading

from splitio.api import APIException

# Synchronizers
from splitio.synchronizers.split import SplitSynchronizer
from splitio.synchronizers.segment import SegmentSynchronizer
from splitio.synchronizers.impression import ImpressionSynchronizer, ImpressionsCountSynchronizer
from splitio.synchronizers.event import EventSynchronizer
from splitio.synchronizers.telemetry import TelemetrySynchronizer

# Tasks
from splitio.tasks.split_sync import SplitSynchronizationTask
from splitio.tasks.segment_sync import SegmentSynchronizationTask
from splitio.tasks.impressions_sync import ImpressionsSyncTask, ImpressionsCountSyncTask
from splitio.tasks.events_sync import EventsSyncTask
from splitio.tasks.telemetry_sync import TelemetrySynchronizationTask


_LOGGER = logging.getLogger(__name__)


class SplitSynchronizers(object):
    """SplitSynchronizers."""
    def __init__(self, split_sync, segment_sync, impressions_sync, events_sync, telemetry_sync,
                 impressions_count_sync):
        if not isinstance(split_sync, SplitSynchronizer):
            return None
        self._split_sync = split_sync
        if not isinstance(segment_sync, SegmentSynchronizer):
            return None
        self._segment_sync = segment_sync
        if not isinstance(impressions_sync, ImpressionSynchronizer):
            return None
        self._impressions_sync = impressions_sync
        if not isinstance(events_sync, EventSynchronizer):
            return None
        self._events_sync = events_sync
        if not isinstance(telemetry_sync, TelemetrySynchronizer):
            return None
        self._telemetry_sync = telemetry_sync
        if not isinstance(impressions_count_sync, ImpressionsCountSynchronizer):
            return None
        self._impressions_count_sync = impressions_count_sync

    @property
    def split_sync(self):
        return self._split_sync

    @property
    def segment_sync(self):
        return self._segment_sync

    @property
    def impressions_sync(self):
        return self._impressions_sync

    @property
    def events_sync(self):
        return self._events_sync

    @property
    def telemetry_sync(self):
        return self._telemetry_sync

    @property
    def impressions_count_sync(self):
        return self._impressions_count_sync


class SplitTasks(object):
    """SplitTasks."""

    def __init__(self, split_task, segment_task, impressions_task, events_task, telemetry_task,
                 impressions_count_task):
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
        if not isinstance(impressions_count_task, ImpressionsCountSyncTask):
            return None
        self._impressions_count_task = impressions_count_task

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

    @property
    def impressions_count_task(self):
        return self._impressions_count_task


class Synchronizer(object):
    """Synchronizer."""

    def __init__(self, split_synchronizers, split_tasks):
        if not isinstance(split_synchronizers, SplitSynchronizers):
            _LOGGER.error('Unexpected type of split_synchronizers')
            return None
        self._split_synchronizers = split_synchronizers
        if not isinstance(split_tasks, SplitTasks):
            _LOGGER.error('Unexpected type of split_tasks')
            return None
        self._split_tasks = split_tasks

    def _synchronize_segments(self):
        _LOGGER.debug('Starting segments synchronization')
        return self._split_synchronizers.segment_sync.synchronize_segments()

    def synchronize_segment(self, segment_name, till):
        _LOGGER.debug('Synchronizing segment %s', segment_name)
        return self._split_synchronizers.segment_sync.synchronize_segment(segment_name, till)

    def synchronize_splits(self, till):
        _LOGGER.debug('Starting splits synchronization')
        return self._split_synchronizers.split_sync.synchronize_splits(till)

    def sync_all(self):
        try:
            self.synchronize_splits(None)
            if self._synchronize_segments() is True:
                _LOGGER.error('Failed syncing segments')
        except APIException as exc:
            _LOGGER.error('Failed syncing splits')
            raise(exc)

    def start_periodic_fetching(self):
        _LOGGER.debug('Starting periodic data fetching')
        self._split_tasks.split_task.start()
        self._split_tasks.segment_task.start()

    def stop_periodic_fetching(self, shutdown):
        _LOGGER.debug('Stopping periodic fetching')
        self._split_tasks.split_task.stop()
        if shutdown:  # stops task and worker pool
            self._split_tasks.segment_task.stop()
        else:  # pauses task not worker pool
            self._split_tasks.segment_task.pause()

    def start_periodic_data_recording(self):
        _LOGGER.debug('Starting periodic data recording')
        self._split_tasks.impressions_task.start()
        self._split_tasks.events_task.start()
        self._split_tasks.telemetry_task.start()
        self._split_tasks.impressions_count_task.start()

    def stop_periodic_data_recording(self):
        _LOGGER.debug('Stopping periodic data recording')
        stop_event = threading.Event()
        self._split_tasks.impressions_task.stop(stop_event)
        self._split_tasks.events_task.stop(stop_event)
        self._split_tasks.impressions_count_task.stop(stop_event)
        stop_event.wait()
        self._split_tasks.telemetry_task.stop()
