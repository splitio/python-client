"""Synchronizer module."""

import abc
import logging
import threading
import time

from splitio.api import APIException
from splitio.util.backoff import Backoff


_LOGGER = logging.getLogger(__name__)
_SYNC_ALL_NO_RETRIES = -1

class SplitSynchronizers(object):
    """SplitSynchronizers."""

    def __init__(self, split_sync, segment_sync, impressions_sync, events_sync,  # pylint:disable=too-many-arguments
                 impressions_count_sync, unique_keys_sync = None, clear_filter_sync = None):
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
        :param impressions_count_sync: sync for impression_counts
        :type impressions_count_sync: splitio.sync.impression.ImpressionsCountSynchronizer
        """
        self._split_sync = split_sync
        self._segment_sync = segment_sync
        self._impressions_sync = impressions_sync
        self._events_sync = events_sync
        self._impressions_count_sync = impressions_count_sync
        self._unique_keys_sync = unique_keys_sync
        self._clear_filter_sync = clear_filter_sync

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
    def impressions_count_sync(self):
        """Return impressions count synchonizer."""
        return self._impressions_count_sync

    @property
    def unique_keys_sync(self):
        """Return unique keys synchonizer."""
        return self._unique_keys_sync

    @property
    def clear_filter_sync(self):
        """Return clear filter synchonizer."""
        return self._clear_filter_sync

class SplitTasks(object):
    """SplitTasks."""

    def __init__(self, split_task, segment_task, impressions_task, events_task,  # pylint:disable=too-many-arguments
                 impressions_count_task, unique_keys_task = None, clear_filter_task = None):
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
        :param impressions_count_task: sync for impression_counts
        :type impressions_count_task: splitio.tasks.impressions_sync.ImpressionsCountSyncTask
        """
        self._split_task = split_task
        self._segment_task = segment_task
        self._impressions_task = impressions_task
        self._events_task = events_task
        self._impressions_count_task = impressions_count_task
        self._unique_keys_task = unique_keys_task
        self._clear_filter_task = clear_filter_task

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
    def impressions_count_task(self):
        """Return impressions count sync task."""
        return self._impressions_count_task

    @property
    def unique_keys_task(self):
        """Return unique keys sync task."""
        return self._unique_keys_task

    @property
    def clear_filter_task(self):
        """Return clear filter sync task."""
        return self._clear_filter_task

class BaseSynchronizer(object, metaclass=abc.ABCMeta):
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

    _ON_DEMAND_FETCH_BACKOFF_BASE = 10  # backoff base starting at 10 seconds
    _ON_DEMAND_FETCH_BACKOFF_MAX_WAIT = 30  # don't sleep for more than 1 minute

    def __init__(self, split_synchronizers, split_tasks):
        """
        Class constructor.

        :param split_synchronizers: syncs for performing synchronization of segments and splits
        :type split_synchronizers: splitio.sync.synchronizer.SplitSynchronizers
        :param split_tasks: tasks for starting/stopping tasks
        :type split_tasks: splitio.sync.synchronizer.SplitTasks
        """
        self._backoff = Backoff(
                                self._ON_DEMAND_FETCH_BACKOFF_BASE,
                                self._ON_DEMAND_FETCH_BACKOFF_MAX_WAIT)
        self._split_synchronizers = split_synchronizers
        self._split_tasks = split_tasks
        self._periodic_data_recording_tasks = [
            self._split_tasks.impressions_task,
            self._split_tasks.events_task
        ]
        if self._split_tasks.impressions_count_task:
            self._periodic_data_recording_tasks.append(self._split_tasks.impressions_count_task)
        if self._split_tasks.unique_keys_task:
            self._periodic_data_recording_tasks.append(self._split_tasks.unique_keys_task)
        if self._split_tasks.clear_filter_task:
            self._periodic_data_recording_tasks.append(self._split_tasks.clear_filter_task)


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

    def synchronize_splits(self, till, sync_segments=True):
        """
        Synchronize all splits.

        :param till: to fetch
        :type till: int

        :returns: whether the synchronization was successful or not.
        :rtype: bool
        """
        _LOGGER.debug('Starting splits synchronization')
        try:
            new_segments = []
            for segment in self._split_synchronizers.split_sync.synchronize_splits(till):
                    if not self._split_synchronizers.segment_sync.segment_exist_in_storage(segment):
                        new_segments.append(segment)
            if sync_segments and len(new_segments) != 0:
                _LOGGER.debug('Synching Segments: %s', ','.join(new_segments))
                success = self._split_synchronizers.segment_sync.synchronize_segments(new_segments, True)
                if not success:
                    _LOGGER.error('Failed to schedule sync one or all segment(s) below.')
                    _LOGGER.error(','.join(new_segments))
                else:
                    _LOGGER.debug('Segment sync scheduled.')
            return True
        except APIException:
            _LOGGER.error('Failed syncing splits')
            _LOGGER.debug('Error: ', exc_info=True)
            return False

    def sync_all(self, max_retry_attempts=_SYNC_ALL_NO_RETRIES):
        """
        Synchronize all splits.

        :param max_retry_attempts: apply max attempts if it set to absilute integer.
        :type max_retry_attempts: int
        """
        retry_attempts = 0
        while True:
            try:
                if not self.synchronize_splits(None, False):
                    raise Exception("split sync failed")

                # Only retrying splits, since segments may trigger too many calls.
                if not self._synchronize_segments():
                    _LOGGER.warning('Segments failed to synchronize.')

                # All is good
                return
            except Exception as exc:  # pylint:disable=broad-except
                _LOGGER.error("Exception caught when trying to sync all data: %s", str(exc))
                _LOGGER.debug('Error: ', exc_info=True)
                if max_retry_attempts != _SYNC_ALL_NO_RETRIES:
                    retry_attempts += 1
                    if retry_attempts > max_retry_attempts:
                        break
                how_long = self._backoff.get()
                time.sleep(how_long)

        _LOGGER.error("Could not correctly synchronize splits and segments after %d attempts.", retry_attempts)

    def _retry_block(self, max_retry_attempts, retry_attempts):
        return retry_attempts

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
        for task in self._periodic_data_recording_tasks:
            task.start()

    def stop_periodic_data_recording(self, blocking):
        """
        Stop recorders.

        :param blocking: flag to wait until tasks are stopped
        :type blocking: bool
        """
        _LOGGER.debug('Stopping periodic data recording')
        if blocking:
            events = []
            for task in self._periodic_data_recording_tasks:
                stop_event = threading.Event()
                task.stop(stop_event)
                events.append(stop_event)
            if all(event.wait() for event in events):
                _LOGGER.debug('all tasks finished successfully.')
        else:
            for task in self._periodic_data_recording_tasks:
                task.stop()

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

class RedisSynchronizer(BaseSynchronizer):
    """Redis Synchronizer."""

    def __init__(self, split_synchronizers, split_tasks):
        """
        Class constructor.

        :param split_synchronizers: syncs for performing synchronization of segments and splits
        :type split_synchronizers: splitio.sync.synchronizer.SplitSynchronizers
        :param split_tasks: tasks for starting/stopping tasks
        :type split_tasks: splitio.sync.synchronizer.SplitTasks
        """
        self._split_synchronizers = split_synchronizers
        self._tasks = []
        if split_tasks.impressions_count_task is not None:
            self._tasks.append(split_tasks.impressions_count_task)
        if split_tasks.unique_keys_task is not None:
            self._tasks.append(split_tasks.unique_keys_task)
        if split_tasks.clear_filter_task is not None:
            self._tasks.append(split_tasks.clear_filter_task)

    def sync_all(self):
        """
        Not implemented
        """
        pass

    def shutdown(self, blocking):
        """
        Stop tasks.

        :param blocking:flag to wait until tasks are stopped
        :type blocking: bool
        """
        _LOGGER.debug('Shutting down tasks.')
        self.stop_periodic_data_recording(blocking)

    def start_periodic_data_recording(self):
        """Start recorders."""
        _LOGGER.debug('Starting periodic data recording')
        for task in self._tasks:
            task.start()

    def stop_periodic_data_recording(self, blocking):
        """
        Stop recorders.

        :param blocking: flag to wait until tasks are stopped
        :type blocking: bool
        """
        _LOGGER.debug('Stopping periodic data recording')
        if blocking:
            events = []
            for task in self._tasks:
                stop_event = threading.Event()
                task.stop(stop_event)
                events.append(stop_event)
            if all(event.wait() for event in events):
                _LOGGER.debug('all tasks finished successfully.')
        else:
            for task in self._tasks:
                task.stop()

    def kill_split(self, split_name, default_treatment, change_number):
        """Kill a split locally."""
        raise NotImplementedError()

    def synchronize_splits(self, till):
        """Synchronize all splits."""
        raise NotImplementedError()

    def synchronize_segment(self, segment_name, till):
        """Synchronize particular segment."""
        raise NotImplementedError()

    def start_periodic_fetching(self):
        """Start fetchers for splits and segments."""
        raise NotImplementedError()

    def stop_periodic_fetching(self):
        """Stop fetchers for splits and segments."""
        raise NotImplementedError()

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

    def sync_all(self, max_retry_attempts=-1):
        """
        Synchronize all splits.

        :param max_retry_attempts: Not used, added for compatibility
        """
        try:
            self._split_synchronizers.split_sync.synchronize_splits(None)
        except APIException as exc:
            _LOGGER.error('Failed syncing splits')
            raise APIException('Failed to sync splits') from exc

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
