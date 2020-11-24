"""Synchronizer module."""

import abc
import logging
import threading

from six import add_metaclass
from future.utils import raise_from
from splitio.api import APIException


_LOGGER = logging.getLogger(__name__)


class SplitSynchronizers(object):
    """SplitSynchronizers."""

    def __init__(self, split_sync, segment_sync, impressions_sync, events_sync, telemetry_sync,  # pylint:disable=too-many-arguments
                 impressions_count_sync):
        """
        Class constructor.

        :param split_sync: sync for splits
        :type split_sync: splitio.sync.split.SplitSynchronizer
        :param segment_sync: sync for segments
        :type segment_sync: splitio.sync.segment.SegmentSynchronizer
        :param impressions_sync: sync for impressions
        :type impressions_sync: splitio.sync.impression.ImpressionSynchronizer
        :param events_sync: sync for events
        :type events_sync: splitio.sync.event.EventSynchronizer
        :param telemetry_sync: sync for telemetry
        :type telemetry_sync: splitio.sync.telemetry.TelemetrySynchronizer
        :param impressions_count_sync: sync for impression_counts
        :type impressions_count_sync: splitio.sync.impression.ImpressionsCountSynchronizer
        """
        self._split_sync = split_sync
        self._segment_sync = segment_sync
        self._impressions_sync = impressions_sync
        self._events_sync = events_sync
        self._telemetry_sync = telemetry_sync
        self._impressions_count_sync = impressions_count_sync

    @property
    def split_sync(self):
        """Return split synchonizer."""
        return self._split_sync

    @property
    def segment_sync(self):
        """Return segment synchonizer."""
        return self._segment_sync

    @property
    def impressions_sync(self):
        """Return impressions synchonizer."""
        return self._impressions_sync

    @property
    def events_sync(self):
        """Return events synchonizer."""
        return self._events_sync

    @property
    def telemetry_sync(self):
        """Return telemetry synchonizer."""
        return self._telemetry_sync

    @property
    def impressions_count_sync(self):
        """Return impressions count synchonizer."""
        return self._impressions_count_sync


class SplitTasks(object):
    """SplitTasks."""

    def __init__(self, split_task, segment_task, impressions_task, events_task, telemetry_task,  # pylint:disable=too-many-arguments
                 impressions_count_task):
        """
        Class constructor.

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
        """Return split sync task."""
        return self._split_task

    @property
    def segment_task(self):
        """Return segment sync task."""
        return self._segment_task

    @property
    def impressions_task(self):
        """Return impressions sync task."""
        return self._impressions_task

    @property
    def events_task(self):
        """Return events sync task."""
        return self._events_task

    @property
    def telemetry_task(self):
        """Return telemetry sync task."""
        return self._telemetry_task

    @property
    def impressions_count_task(self):
        """Return impressions count sync task."""
        return self._impressions_count_task


@add_metaclass(abc.ABCMeta)
class BaseSynchronizer(object):
    """Synchronizer interface."""

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
    def stop_periodic_fetching(self):
        """Stop fetchers for splits and segments."""
        pass

    @abc.abstractmethod
    def start_periodic_data_recording(self):
        """Start recorders."""
        pass

    @abc.abstractmethod
    def stop_periodic_data_recording(self, blocking):
        """Stop recorders."""
        pass

    @abc.abstractmethod
    def kill_split(self, split_name, default_treatment, change_number):
        """
        Kill a split locally.

        :param split_name: name of the split to perform kill
        :type split_name: str
        :param default_treatment: name of the default treatment to return
        :type default_treatment: str
        :param change_number: change_number
        :type change_number: int
        """
        pass

    @abc.abstractmethod
    def shutdown(self, blocking):
        """
        Stop tasks.

        :param blocking:flag to wait until tasks are stopped
        :type blocking: bool
        """
        pass


class Synchronizer(BaseSynchronizer):
    """Synchronizer."""

    def __init__(self, split_synchronizers, split_tasks):
        """
        Class constructor.

        :param split_synchronizers: syncs for performing synchronization of segments and splits
        :type split_synchronizers: splitio.sync.synchronizer.SplitSynchronizers
        :param split_tasks: tasks for starting/stopping tasks
        :type split_tasks: splitio.sync.synchronizer.SplitTasks
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
        success = self._split_synchronizers.segment_sync.synchronize_segment(segment_name, till)
        if not success:
            _LOGGER.error('Failed to sync some segments.')
        return success

    def synchronize_splits(self, till):
        """
        Synchronize all splits.

        :param till: to fetch
        :type till: int

        :returns: whether the synchronization was successful or not.
        :rtype: bool
        """
        _LOGGER.debug('Starting splits synchronization')
        try:
            self._split_synchronizers.split_sync.synchronize_splits(till)
            return True
        except APIException:
            _LOGGER.error('Failed syncing splits')
            _LOGGER.debug('Error: ', exc_info=True)
            return False

    def sync_all(self):
        """Synchronize all split data."""
        attempts = 3
        while attempts > 0:
            try:
                if not self.synchronize_splits(None):
                    attempts -= 1
                    continue

                # Only retrying splits, since segments may trigger too many calls.
                if not self._synchronize_segments():
                    _LOGGER.warn('Segments failed to synchronize.')

                # All is good
                return
            except Exception as exc:  # pylint:disable=broad-except
                attempts -= 1
                _LOGGER.error("Exception caught when trying to sync all data: %s", str(exc))
                _LOGGER.debug('Error: ', exc_info=True)

        _LOGGER.error("Could not correctly synchronize splits and segments after 3 attempts.")

    def shutdown(self, blocking):
        """
        Stop tasks.

        :param blocking:flag to wait until tasks are stopped
        :type blocking: bool
        """
        _LOGGER.debug('Shutting down tasks.')
        self._split_synchronizers.segment_sync.shutdown()
        self.stop_periodic_fetching()
        self.stop_periodic_data_recording(blocking)

    def start_periodic_fetching(self):
        """Start fetchers for splits and segments."""
        _LOGGER.debug('Starting periodic data fetching')
        self._split_tasks.split_task.start()
        self._split_tasks.segment_task.start()

    def stop_periodic_fetching(self):
        """Stop fetchers for splits and segments."""
        _LOGGER.debug('Stopping periodic fetching')
        self._split_tasks.split_task.stop()
        self._split_tasks.segment_task.stop()

    def start_periodic_data_recording(self):
        """Start recorders."""
        _LOGGER.debug('Starting periodic data recording')
        self._split_tasks.impressions_task.start()
        self._split_tasks.events_task.start()
        self._split_tasks.telemetry_task.start()
        self._split_tasks.impressions_count_task.start()

    def stop_periodic_data_recording(self, blocking):
        """
        Stop recorders.

        :param blocking: flag to wait until tasks are stopped
        :type blocking: bool
        """
        _LOGGER.debug('Stopping periodic data recording')
        if blocking:
            events = []
            for task in [self._split_tasks.impressions_task,
                         self._split_tasks.events_task,
                         self._split_tasks.impressions_count_task]:
                stop_event = threading.Event()
                task.stop(stop_event)
                events.append(stop_event)
            if all(event.wait() for event in events):
                _LOGGER.debug('all tasks finished successfully.')
        else:
            self._split_tasks.impressions_task.stop()
            self._split_tasks.events_task.stop()
            self._split_tasks.impressions_count_task.stop()
        self._split_tasks.telemetry_task.stop()

    def kill_split(self, split_name, default_treatment, change_number):
        """
        Kill a split locally.

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
        Class constructor.

        :param split_synchronizers: syncs for performing synchronization of segments and splits
        :type split_synchronizers: splitio.sync.synchronizer.SplitSynchronizers
        :param split_tasks: tasks for starting/stopping tasks
        :type split_tasks: splitio.sync.synchronizer.SplitTasks
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

    def stop_periodic_fetching(self):
        """Stop fetchers for splits and segments."""
        _LOGGER.debug('Stopping periodic fetching')
        self._split_tasks.split_task.stop()

    def kill_split(self, split_name, default_treatment, change_number):
        """Kill a split locally."""
        raise NotImplementedError()

    def synchronize_splits(self, till):
        """Synchronize all splits."""
        raise NotImplementedError()

    def synchronize_segment(self, segment_name, till):
        """Synchronize particular segment."""
        raise NotImplementedError()

    def start_periodic_data_recording(self):
        """Start recorders."""
        pass

    def stop_periodic_data_recording(self, blocking):
        """Stop recorders."""
        pass

    def shutdown(self, blocking):
        """
        Stop tasks.

        :param blocking:flag to wait until tasks are stopped
        :type blocking: bool
        """
        self.stop_periodic_fetching()
