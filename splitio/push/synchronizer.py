"""Synchronizer module."""

import abc
import logging
import threading

from six import add_metaclass
from future.utils import raise_from
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
        """
        SplitSynchronizer constructor.

        :param split_sync: sync for splits
        :type split_sync: splitio.synchronizers.split.SplitSynchronizer
        :param segment_sync: sync for segments
        :type segment_sync: splitio.synchronizers.segment.SegmentSynchronizer
        :param impressions_sync: sync for impressions
        :type impressions_sync: splitio.synchronizers.impression.ImpressionSynchronizer
        :param events_sync: sync for events
        :type events_sync: splitio.synchronizers.event.EventSynchronizer
        :param telemetry_sync: sync for telemetry
        :type telemetry_sync: splitio.synchronizers.telemetry.TelemetrySynchronizer
        :param impressions_count_sync: sync for impression_counts
        :type impressions_count_sync: splitio.synchronizers.impression.ImpressionsCountSynchronizer
        """
        self._split_sync = split_sync
        self._segment_sync = segment_sync
        self._impressions_sync = impressions_sync
        self._events_sync = events_sync
        self._telemetry_sync = telemetry_sync
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
        """
        SplitTasks constructor.

        :param split_task: sync for splits
        :type split_task: splitio.tasks.split_sync.SplitSynchronizationTask
        :param segment_task: sync for segments
        :type segment_task: splitio.tasks.segment_sync.SegmentSynchronizationTask
        :param impressions_task: sync for impressions
        :type impressions_task: splitio.tasks.impressions_sync.ImpressionsSyncTask
        :param events_task: sync for events
        :type events_task: splitio.tasks.events_sync.EventsSyncTask
        :param telemetry_task: sync for telemetry
        :type telemetry_task: splitio.tasks.telemetry_sync.TelemetrySynchronizationTask
        :param impressions_count_task: sync for impression_counts
        :type impressions_count_task: splitio.tasks.impressions_sync.ImpressionsCountSyncTask
        """
        self._split_task = split_task
        self._segment_task = segment_task
        self._impressions_task = impressions_task
        self._events_task = events_task
        self._telemetry_task = telemetry_task
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


class BaseSynchronizer(object):
    """Synchronizer interface."""

    __metadata__ = abc.ABCMeta

    @abc.abstractmethod
    def synchronize_segment(self, segment_name, till):
        """
        Synchronize particular segment.

        :param segment_name: segment associated
        :type segment_name: str
        :param till: to fetch
        :type till: int
        """
        pass

    @abc.abstractmethod
    def synchronize_splits(self, till):
        """
        Synchronize all splits.

        :param till: to fetch
        :type till: int
        """
        pass

    @abc.abstractmethod
    def sync_all(self):
        """Synchronize all split data."""
        pass

    @abc.abstractmethod
    def start_periodic_fetching(self):
        """Start fetchers for splits and segments."""
        pass

    @abc.abstractmethod
    def stop_periodic_fetching(self, shutdown=False):
        """
        Stop fetchers for splits and segments.

        :param shutdown: flag to indicates if should pause or stop tasks
        :type shutdown: bool
        """
        pass

    @abc.abstractmethod
    def start_periodic_data_recording(self):
        """Start recorders."""
        pass

    @abc.abstractmethod
    def stop_periodic_data_recording(self):
        """Stop recorders."""
        pass

    @abc.abstractmethod
    def kill_split(self, split_name, default_treatment, change_number):
        """
        Local kill for split

        :param split_name: name of the split to perform kill
        :type split_name: str
        :param default_treatment: name of the default treatment to return
        :type default_treatment: str
        :param change_number: change_number
        :type change_number: int
        """
        pass


class Synchronizer(BaseSynchronizer):
    """Synchronizer."""

    def __init__(self, split_synchronizers, split_tasks):
        """
        Synchronizer constructor.

        :param split_synchronizers: syncs for performing synchronization of segments and splits
        :type split_synchronizers: splitio.push.synchronizer.SplitSynchronizers
        :param split_tasks: tasks for starting/stopping tasks
        :type split_tasks: splitio.push.synchronizer.SplitTasks
        """
        self._split_synchronizers = split_synchronizers
        self._split_tasks = split_tasks

    def _synchronize_segments(self):
        _LOGGER.debug('Starting segments synchronization')
        return self._split_synchronizers.segment_sync.synchronize_segments()

    def synchronize_segment(self, segment_name, till):
        """
        Synchronize particular segment.

        :param segment_name: segment associated
        :type segment_name: str
        :param till: to fetch
        :type till: int
        """
        _LOGGER.debug('Synchronizing segment %s', segment_name)
        return self._split_synchronizers.segment_sync.synchronize_segment(segment_name, till)

    def synchronize_splits(self, till):
        """
        Synchronize all splits.

        :param till: to fetch
        :type till: int
        """
        _LOGGER.debug('Starting splits synchronization')
        return self._split_synchronizers.split_sync.synchronize_splits(till)

    def sync_all(self):
        """Synchronize all split data."""
        try:
            self.synchronize_splits(None)
            if not self._synchronize_segments():
                _LOGGER.error('Failed syncing segments')
                raise RuntimeError('Failed syncing segments')
        except APIException as exc:
            _LOGGER.error('Failed syncing splits')
            raise_from(APIException('Failed to sync splits'), exc)

    def start_periodic_fetching(self):
        """Start fetchers for splits and segments."""
        _LOGGER.debug('Starting periodic data fetching')
        self._split_tasks.split_task.start()
        self._split_tasks.segment_task.start()

    def stop_periodic_fetching(self, shutdown=False):
        """
        Stop fetchers for splits and segments.

        :param shutdown: flag to indicates if should pause or stop tasks
        :type shutdown: bool
        """
        _LOGGER.debug('Stopping periodic fetching')
        self._split_tasks.split_task.stop()
        if shutdown:  # stops task and worker pool
            self._split_tasks.segment_task.stop()
        else:  # pauses task not worker pool
            self._split_tasks.segment_task.pause()

    def start_periodic_data_recording(self):
        """Start recorders."""
        _LOGGER.debug('Starting periodic data recording')
        self._split_tasks.impressions_task.start()
        self._split_tasks.events_task.start()
        self._split_tasks.telemetry_task.start()
        self._split_tasks.impressions_count_task.start()

    def stop_periodic_data_recording(self):
        """Stop recorders."""
        _LOGGER.debug('Stopping periodic data recording')
        events = []
        for task in [
            self._split_tasks.impressions_task,
            self._split_tasks.events_task,
            self._split_tasks.impressions_count_task
        ]:
            stop_event = threading.Event()
            task.stop(stop_event)
            events.append(stop_event)
        if all(event.wait() for event in events):
            _LOGGER.debug('all tasks finished successfully.')
        self._split_tasks.telemetry_task.stop()

    def kill_split(self, split_name, default_treatment, change_number):
        """
        Local kill for split

        :param split_name: name of the split to perform kill
        :type split_name: str
        :param default_treatment: name of the default treatment to return
        :type default_treatment: str
        :param change_number: change_number
        :type change_number: int
        """
        self._split_synchronizers.split_sync.kill_split(split_name, default_treatment,
                                                        change_number)


class LocalhostSynchronizer(BaseSynchronizer):
    """LocalhostSynchronizer."""

    def __init__(self, split_synchronizers, split_tasks):
        """
        LocalhostSynchronizer constructor.

        :param split_synchronizers: syncs for performing synchronization of segments and splits
        :type split_synchronizers: splitio.push.synchronizer.SplitSynchronizers
        :param split_tasks: tasks for starting/stopping tasks
        :type split_tasks: splitio.push.synchronizer.SplitTasks
        """
        self._split_synchronizers = split_synchronizers
        self._split_tasks = split_tasks

    def sync_all(self):
        """Synchronize all split data."""
        try:
            self._split_synchronizers.split_sync.synchronize_splits(None)
        except APIException as exc:
            _LOGGER.error('Failed syncing splits')
            raise_from(APIException('Failed to sync splits'), exc)

    def start_periodic_fetching(self):
        """Start fetchers for splits and segments."""
        _LOGGER.debug('Starting periodic data fetching')
        self._split_tasks.split_task.start()

    def stop_periodic_fetching(self, shutdown=False):
        """Stop fetchers for splits and segments."""
        _LOGGER.debug('Stopping periodic fetching')
        self._split_tasks.split_task.stop()
