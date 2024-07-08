"""Synchronizer module."""

import abc
import logging
import threading
import time
from collections import namedtuple

from splitio.optional.loaders import asyncio
from splitio.api import APIException, APIUriException
from splitio.util.backoff import Backoff
from splitio.sync.split import _ON_DEMAND_FETCH_BACKOFF_BASE, _ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES, _ON_DEMAND_FETCH_BACKOFF_MAX_WAIT, LocalhostMode

SplitSyncResult = namedtuple('SplitSyncResult', ['success', 'error_code'])

_LOGGER = logging.getLogger(__name__)


_SYNC_ALL_NO_RETRIES = -1

class SplitSynchronizers(object):
    """SplitSynchronizers."""

    def __init__(self, feature_flag_sync, segment_sync, impressions_sync, events_sync,  # pylint:disable=too-many-arguments
                 impressions_count_sync, telemetry_sync=None, unique_keys_sync = None, clear_filter_sync = None):
        """
        Class constructor.

        :param feature_flag_sync: sync for feature flags
        :type feature_flag_sync: splitio.sync.split.SplitSynchronizer
        :param segment_sync: sync for segments
        :type segment_sync: splitio.sync.segment.SegmentSynchronizer
        :param impressions_sync: sync for impressions
        :type impressions_sync: splitio.sync.impression.ImpressionSynchronizer
        :param events_sync: sync for events
        :type events_sync: splitio.sync.event.EventSynchronizer
        :param impressions_count_sync: sync for impression_counts
        :type impressions_count_sync: splitio.sync.impression.ImpressionsCountSynchronizer
        """
        self._feature_flag_sync = feature_flag_sync
        self._segment_sync = segment_sync
        self._impressions_sync = impressions_sync
        self._events_sync = events_sync
        self._impressions_count_sync = impressions_count_sync
        self._unique_keys_sync = unique_keys_sync
        self._clear_filter_sync = clear_filter_sync
        self._telemetry_sync = telemetry_sync

    @property
    def split_sync(self):
        """Return split synchonizer."""
        return self._feature_flag_sync

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

    @property
    def telemetry_sync(self):
        """Return clear filter synchonizer."""
        return self._telemetry_sync

class SplitTasks(object):
    """SplitTasks."""

    def __init__(self, feature_flag_task, segment_task, impressions_task, events_task,  # pylint:disable=too-many-arguments
                 impressions_count_task, telemetry_task=None, unique_keys_task = None, clear_filter_task = None):
        """
        Class constructor.

        :param feature_flag_task: sync for feature_flags
        :type feature_flag_task: splitio.tasks.split_sync.SplitSynchronizationTask
        :param segment_task: sync for segments
        :type segment_task: splitio.tasks.segment_sync.SegmentSynchronizationTask
        :param impressions_task: sync for impressions
        :type impressions_task: splitio.tasks.impressions_sync.ImpressionsSyncTask
        :param events_task: sync for events
        :type events_task: splitio.tasks.events_sync.EventsSyncTask
        :param impressions_count_task: sync for impression_counts
        :type impressions_count_task: splitio.tasks.impressions_sync.ImpressionsCountSyncTask
        """
        self._feature_flag_task = feature_flag_task
        self._segment_task = segment_task
        self._impressions_task = impressions_task
        self._events_task = events_task
        self._impressions_count_task = impressions_count_task
        self._unique_keys_task = unique_keys_task
        self._clear_filter_task = clear_filter_task
        self._telemetry_task = telemetry_task

    @property
    def split_task(self):
        """Return feature_flag sync task."""
        return self._feature_flag_task

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

    @property
    def telemetry_task(self):
        """Return clear filter sync task."""
        return self._telemetry_task

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
        Synchronize all feature flags.

        :param till: to fetch
        :type till: int
        """
        pass

    @abc.abstractmethod
    def sync_all(self):
        """Synchronize all feature flag data."""
        pass

    @abc.abstractmethod
    def start_periodic_fetching(self):
        """Start fetchers for feature flags and segments."""
        pass

    @abc.abstractmethod
    def stop_periodic_fetching(self):
        """Stop fetchers for feature flags and segments."""
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
    def kill_split(self, feature_flag_name, default_treatment, change_number):
        """
        Kill a feature flag locally.

        :param feature_flag_name: name of the feature flag to perform kill
        :type feature_flag_name: str
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


class SynchronizerInMemoryBase(BaseSynchronizer):
    """Synchronizer."""

    def __init__(self, split_synchronizers, split_tasks):
        """
        Class constructor.

        :param split_synchronizers: syncs for performing synchronization of segments and feature flags
        :type split_synchronizers: splitio.sync.synchronizer.SplitSynchronizers
        :param split_tasks: tasks for starting/stopping tasks
        :type split_tasks: splitio.sync.synchronizer.SplitTasks
        """
        self._backoff = Backoff(
                                _ON_DEMAND_FETCH_BACKOFF_BASE,
                                _ON_DEMAND_FETCH_BACKOFF_MAX_WAIT)
        self._split_synchronizers = split_synchronizers
        self._split_tasks = split_tasks
        self._periodic_data_recording_tasks = [
            self._split_tasks.impressions_task,
            self._split_tasks.events_task,
            self._split_tasks.telemetry_task
        ]
        if self._split_tasks.impressions_count_task:
            self._periodic_data_recording_tasks.append(self._split_tasks.impressions_count_task)
        if self._split_tasks.unique_keys_task:
            self._periodic_data_recording_tasks.append(self._split_tasks.unique_keys_task)
        if self._split_tasks.clear_filter_task:
            self._periodic_data_recording_tasks.append(self._split_tasks.clear_filter_task)

    @property
    def split_sync(self):
        return self._split_synchronizers.split_sync

    @property
    def segment_storage(self):
        return self._split_synchronizers.segment_sync._segment_storage

    def synchronize_segment(self, segment_name, till):
        """
        Synchronize particular segment.

        :param segment_name: segment associated
        :type segment_name: str
        :param till: to fetch
        :type till: int
        """
        pass

    def synchronize_splits(self, till, sync_segments=True):
        """
        Synchronize all feature flags.

        :param till: to fetch
        :type till: int

        :returns: whether the synchronization was successful or not.
        :rtype: bool
        """
        pass

    def sync_all(self, max_retry_attempts=_SYNC_ALL_NO_RETRIES):
        """
        Synchronize all feature flags.

        :param max_retry_attempts: apply max attempts if it set to absilute integer.
        :type max_retry_attempts: int
        """
        pass

    def shutdown(self, blocking):
        """
        Stop tasks.

        :param blocking:flag to wait until tasks are stopped
        :type blocking: bool
        """
        pass

    def start_periodic_fetching(self):
        """Start fetchers for feature flags and segments."""
        _LOGGER.debug('Starting periodic data fetching')
        self._split_tasks.split_task.start()
        self._split_tasks.segment_task.start()

    def stop_periodic_fetching(self):
        """Stop fetchers for feature flags and segments."""
        pass

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
        pass

    def kill_split(self, feature_flag_name, default_treatment, change_number):
        """
        Kill a feature flag locally.

        :param feature_flag_name: name of the feature flag to perform kill
        :type feature_flag_name: str
        :param default_treatment: name of the default treatment to return
        :type default_treatment: str
        :param change_number: change_number
        :type change_number: int
        """
        pass


class Synchronizer(SynchronizerInMemoryBase):
    """Synchronizer."""

    def __init__(self, split_synchronizers, split_tasks):
        """
        Class constructor.

        :param split_synchronizers: syncs for performing synchronization of segments and feature flags
        :type split_synchronizers: splitio.sync.synchronizer.SplitSynchronizers
        :param split_tasks: tasks for starting/stopping tasks
        :type split_tasks: splitio.sync.synchronizer.SplitTasks
        """
        SynchronizerInMemoryBase.__init__(self, split_synchronizers, split_tasks)

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
        Synchronize all feature flags.

        :param till: to fetch
        :type till: int

        :returns: whether the synchronization was successful or not.
        :rtype: bool
        """
        _LOGGER.debug('Starting feature flags synchronization')
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
            return SplitSyncResult(True, 0)
        except APIUriException as exc:
            _LOGGER.error('Failed syncing feature flags due to long URI')
            _LOGGER.debug('Error: ', exc_info=True)
            return SplitSyncResult(False, exc._status_code)

        except APIException as exc:
            _LOGGER.error('Failed syncing feature flags')
            _LOGGER.debug('Error: ', exc_info=True)
            return SplitSyncResult(False, exc._status_code)

    def sync_all(self, max_retry_attempts=_SYNC_ALL_NO_RETRIES):
        """
        Synchronize all feature flags.

        :param max_retry_attempts: apply max attempts if it set to absilute integer.
        :type max_retry_attempts: int
        """
        retry_attempts = 0
        while True:
            try:
                sync_result = self.synchronize_splits(None, False)
                if not sync_result.success and sync_result.error_code is not None and sync_result.error_code == 414:
                    _LOGGER.error("URI too long exception caught, aborting retries")
                    break

                if not sync_result.success:
                    raise Exception("feature flags sync failed")

                # Only retrying feature flags, since segments may trigger too many calls.

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

        _LOGGER.error("Could not correctly synchronize feature flags and segments after %d attempts.", retry_attempts)

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

    def stop_periodic_fetching(self):
        """Stop fetchers for feature flags and segments."""
        _LOGGER.debug('Stopping periodic fetching')
        self._split_tasks.split_task.stop()
        self._split_tasks.segment_task.stop()

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
                if task != self._split_tasks.telemetry_task:
                    stop_event = threading.Event()
                    task.stop(stop_event)
                    events.append(stop_event)
            all(event.wait() for event in events)
            telemetry_event = threading.Event()
            self._split_tasks.telemetry_task.stop(telemetry_event)
            if telemetry_event.wait():
                _LOGGER.debug('all tasks finished successfully.')
        else:
            for task in self._periodic_data_recording_tasks:
                task.stop()

    def kill_split(self, feature_flag_name, default_treatment, change_number):
        """
        Kill a feature flag locally.

        :param feature_flag_name: name of the feature flag to perform kill
        :type feature_flag_name: str
        :param default_treatment: name of the default treatment to return
        :type default_treatment: str
        :param change_number: change_number
        :type change_number: int
        """
        self._split_synchronizers.split_sync.kill_split(feature_flag_name, default_treatment,
                                                        change_number)

class SynchronizerAsync(SynchronizerInMemoryBase):
    """Synchronizer async."""

    def __init__(self, split_synchronizers, split_tasks):
        """
        Class constructor.

        :param split_synchronizers: syncs for performing synchronization of segments and feature flags
        :type split_synchronizers: splitio.sync.synchronizer.SplitSynchronizers
        :param split_tasks: tasks for starting/stopping tasks
        :type split_tasks: splitio.sync.synchronizer.SplitTasks
        """
        SynchronizerInMemoryBase.__init__(self, split_synchronizers, split_tasks)
        self._shutdown = False

    async def _synchronize_segments(self):
        _LOGGER.debug('Starting segments synchronization')
        return await self._split_synchronizers.segment_sync.synchronize_segments()

    async def synchronize_segment(self, segment_name, till):
        """
        Synchronize particular segment.

        :param segment_name: segment associated
        :type segment_name: str
        :param till: to fetch
        :type till: int
        """
        _LOGGER.debug('Synchronizing segment %s', segment_name)
        success = await self._split_synchronizers.segment_sync.synchronize_segment(segment_name, till)
        if not success:
            _LOGGER.error('Failed to sync some segments.')
        return success

    async def synchronize_splits(self, till, sync_segments=True):
        """
        Synchronize all feature flags.

        :param till: to fetch
        :type till: int

        :returns: whether the synchronization was successful or not.
        :rtype: bool
        """
        if self._shutdown:
            return

        _LOGGER.debug('Starting feature flags synchronization')
        try:
            new_segments = []
            for segment in await self._split_synchronizers.split_sync.synchronize_splits(till):
                if not await self._split_synchronizers.segment_sync.segment_exist_in_storage(segment):
                    new_segments.append(segment)
            if sync_segments and len(new_segments) != 0:
                _LOGGER.debug('Synching Segments: %s', ','.join(new_segments))
                success = await self._split_synchronizers.segment_sync.synchronize_segments(new_segments, True)
                if not success:
                    _LOGGER.error('Failed to schedule sync one or all segment(s) below.')
                    _LOGGER.error(','.join(new_segments))
                else:
                    _LOGGER.debug('Segment sync scheduled.')
            return SplitSyncResult(True, 0)
        except APIUriException as exc:
            _LOGGER.error('Failed syncing feature flags due to long URI')
            _LOGGER.debug('Error: ', exc_info=True)
            return SplitSyncResult(False, exc._status_code)

        except APIException as exc:
            _LOGGER.error('Failed syncing feature flags')
            _LOGGER.debug('Error: ', exc_info=True)
            return SplitSyncResult(False, exc._status_code)

    async def sync_all(self, max_retry_attempts=_SYNC_ALL_NO_RETRIES):
        """
        Synchronize all feature flags.

        :param max_retry_attempts: apply max attempts if it set to absilute integer.
        :type max_retry_attempts: int
        """
        self._shutdown = False
        retry_attempts = 0
        while not self._shutdown:
            try:
                sync_result = await self.synchronize_splits(None, False)
                if not sync_result.success and sync_result.error_code is not None and sync_result.error_code == 414:
                    _LOGGER.error("URI too long exception caught, aborting retries")
                    break

                if not sync_result.success:
                    raise Exception("feature flags sync failed")

                # Only retrying feature flags, since segments may trigger too many calls.

                if not await self._synchronize_segments():
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
                if not self._shutdown:
                    await asyncio.sleep(how_long)

        _LOGGER.error("Could not correctly synchronize feature flags and segments after %d attempts.", retry_attempts)

    async def shutdown(self, blocking):
        """
        Stop tasks.

        :param blocking:flag to wait until tasks are stopped
        :type blocking: bool
        """
        _LOGGER.debug('Shutting down tasks.')
        self._shutdown = True
        await self._split_synchronizers.segment_sync.shutdown()
        await self.stop_periodic_fetching()
        await self.stop_periodic_data_recording(blocking)

    async def stop_periodic_fetching(self):
        """Stop fetchers for feature flags and segments."""
        _LOGGER.debug('Stopping periodic fetching')
        await self._split_tasks.split_task.stop()
        await self._split_tasks.segment_task.stop()

    async def stop_periodic_data_recording(self, blocking):
        """
        Stop recorders.

        :param blocking: flag to wait until tasks are stopped
        :type blocking: bool
        """
        _LOGGER.debug('Stopping periodic data recording')
        if blocking:
            await self._stop_periodic_data_recording()
            _LOGGER.debug('all tasks finished successfully.')
        else:
            asyncio.get_running_loop().create_task(self._stop_periodic_data_recording())

    async def _stop_periodic_data_recording(self):
        """
        Stop recorders.

        :param blocking: flag to wait until tasks are stopped
        :type blocking: bool
        """
        for task in self._periodic_data_recording_tasks:
            await task.stop()

    async def kill_split(self, feature_flag_name, default_treatment, change_number):
        """
        Kill a feature flag locally.

        :param feature_flag_name: name of the feature flag to perform kill
        :type feature_flag_name: str
        :param default_treatment: name of the default treatment to return
        :type default_treatment: str
        :param change_number: change_number
        :type change_number: int
        """
        await self._split_synchronizers.split_sync.kill_split(feature_flag_name, default_treatment,
                                                        change_number)

class RedisSynchronizerBase(BaseSynchronizer):
    """Redis Synchronizer."""

    def __init__(self, split_synchronizers, split_tasks):
        """
        Class constructor.

        :param split_synchronizers: syncs for performing synchronization of segments and feature flags
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
        pass

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
        pass

    def kill_split(self, feature_flag_name, default_treatment, change_number):
        """Kill a feature flag locally."""
        raise NotImplementedError()

    def synchronize_splits(self, till):
        """Synchronize all feature flags."""
        raise NotImplementedError()

    def synchronize_segment(self, segment_name, till):
        """Synchronize particular segment."""
        raise NotImplementedError()

    def start_periodic_fetching(self):
        """Start fetchers for feature flags and segments."""
        raise NotImplementedError()

    def stop_periodic_fetching(self):
        """Stop fetchers for feature flags and segments."""
        raise NotImplementedError()


class RedisSynchronizer(RedisSynchronizerBase):
    """Redis Synchronizer."""

    def __init__(self, split_synchronizers, split_tasks):
        """
        Class constructor.

        :param split_synchronizers: syncs for performing synchronization of segments and feature flags
        :type split_synchronizers: splitio.sync.synchronizer.SplitSynchronizers
        :param split_tasks: tasks for starting/stopping tasks
        :type split_tasks: splitio.sync.synchronizer.SplitTasks
        """
        RedisSynchronizerBase.__init__(self, split_synchronizers, split_tasks)

    def shutdown(self, blocking):
        """
        Stop tasks.

        :param blocking:flag to wait until tasks are stopped
        :type blocking: bool
        """
        _LOGGER.debug('Shutting down tasks.')
        self.stop_periodic_data_recording(blocking)

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


class RedisSynchronizerAsync(RedisSynchronizerBase):
    """Redis Synchronizer."""

    def __init__(self, split_synchronizers, split_tasks):
        """
        Class constructor.

        :param split_synchronizers: syncs for performing synchronization of segments and feature flags
        :type split_synchronizers: splitio.sync.synchronizer.SplitSynchronizers
        :param split_tasks: tasks for starting/stopping tasks
        :type split_tasks: splitio.sync.synchronizer.SplitTasks
        """
        RedisSynchronizerBase.__init__(self, split_synchronizers, split_tasks)

    async def shutdown(self, blocking):
        """
        Stop tasks.

        :param blocking:flag to wait until tasks are stopped
        :type blocking: bool
        """
        _LOGGER.debug('Shutting down tasks.')
        await self.stop_periodic_data_recording(blocking)

    async def _stop_periodic_data_recording(self):
        """
        Stop recorders.
        """
        for task in self._tasks:
            await task.stop()

    async def stop_periodic_data_recording(self, blocking):
        """
        Stop recorders.

        :param blocking: flag to wait until tasks are stopped
        :type blocking: bool
        """
        _LOGGER.debug('Stopping periodic data recording')
        if blocking:
            await self._stop_periodic_data_recording()
            _LOGGER.debug('all tasks finished successfully.')
        else:
            asyncio.get_running_loop().create_task(self._stop_periodic_data_recording)


class LocalhostSynchronizerBase(BaseSynchronizer):
    """LocalhostSynchronizer base."""

    def __init__(self, split_synchronizers, split_tasks, localhost_mode):
        """
        Class constructor.

        :param split_synchronizers: syncs for performing synchronization of segments and feature flags
        :type split_synchronizers: splitio.sync.synchronizer.SplitSynchronizers
        :param split_tasks: tasks for starting/stopping tasks
        :type split_tasks: splitio.sync.synchronizer.SplitTasks
        """
        self._split_synchronizers = split_synchronizers
        self._split_tasks = split_tasks
        self._localhost_mode = localhost_mode
        self._backoff = Backoff(
                                _ON_DEMAND_FETCH_BACKOFF_BASE,
                                _ON_DEMAND_FETCH_BACKOFF_MAX_WAIT)

    def sync_all(self, till=None):
        """
        Synchronize all feature flags.
        """
        # TODO: to be removed when legacy and yaml use BUR
        pass

    def start_periodic_fetching(self):
        """Start fetchers for feature flags and segments."""
        if self._split_tasks.split_task is not None:
            _LOGGER.debug('Starting periodic data fetching')
            self._split_tasks.split_task.start()
        if self._split_tasks.segment_task is not None:
            self._split_tasks.segment_task.start()

    def stop_periodic_fetching(self):
        """Stop fetchers for feature flags and segments."""
        pass

    def kill_split(self, split_name, default_treatment, change_number):
        """Kill a feature flag locally."""
        raise NotImplementedError()

    def synchronize_splits(self):
        """Synchronize all feature flags."""
        pass

    def synchronize_segment(self, segment_name, till):
        """Synchronize particular segment."""
        pass

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
        pass


class LocalhostSynchronizer(LocalhostSynchronizerBase):
    """LocalhostSynchronizer."""

    def __init__(self, split_synchronizers, split_tasks, localhost_mode):
        """
        Class constructor.

        :param split_synchronizers: syncs for performing synchronization of segments and feature flags
        :type split_synchronizers: splitio.sync.synchronizer.SplitSynchronizers
        :param split_tasks: tasks for starting/stopping tasks
        :type split_tasks: splitio.sync.synchronizer.SplitTasks
        """
        LocalhostSynchronizerBase.__init__(self, split_synchronizers, split_tasks, localhost_mode)

    def sync_all(self, till=None):
        """
        Synchronize all feature flags.
        """
        # TODO: to be removed when legacy and yaml use BUR
        if self._localhost_mode != LocalhostMode.JSON:
            return self.synchronize_splits()

        self._backoff.reset()
        remaining_attempts = _ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES
        while remaining_attempts > 0:
            remaining_attempts -= 1
            try:
                return self.synchronize_splits()
            except APIException as exc:
                _LOGGER.error('Failed syncing all')
                _LOGGER.error(str(exc))

            how_long = self._backoff.get()
            time.sleep(how_long)

    def stop_periodic_fetching(self):
        """Stop fetchers for feature flags and segments."""
        if self._split_tasks.split_task is not None:
            _LOGGER.debug('Stopping periodic fetching')
            self._split_tasks.split_task.stop()
        if self._split_tasks.segment_task is not None:
            self._split_tasks.segment_task.stop()

    def synchronize_splits(self):
        """Synchronize all feature flags."""
        try:
            new_segments = []
            for segment in self._split_synchronizers.split_sync.synchronize_splits():
                    if not self._split_synchronizers.segment_sync.segment_exist_in_storage(segment):
                        new_segments.append(segment)
            if len(new_segments) > 0:
                _LOGGER.debug('Synching Segments: %s', ','.join(new_segments))
                success = self._split_synchronizers.segment_sync.synchronize_segments(new_segments)
                if not success:
                    _LOGGER.error('Failed to schedule sync one or all segment(s) below.')
                    _LOGGER.error(','.join(new_segments))
                else:
                    _LOGGER.debug('Segment sync scheduled.')
            return True

        except APIException as exc:
            _LOGGER.error('Failed syncing feature flags')
            raise APIException('Failed to sync feature flags') from exc

    def shutdown(self, blocking):
        """
        Stop tasks.

        :param blocking:flag to wait until tasks are stopped
        :type blocking: bool
        """
        self.stop_periodic_fetching()


class LocalhostSynchronizerAsync(LocalhostSynchronizerBase):
    """LocalhostSynchronizer Async."""

    def __init__(self, split_synchronizers, split_tasks, localhost_mode):
        """
        Class constructor.

        :param split_synchronizers: syncs for performing synchronization of segments and feature flags
        :type split_synchronizers: splitio.sync.synchronizer.SplitSynchronizers
        :param split_tasks: tasks for starting/stopping tasks
        :type split_tasks: splitio.sync.synchronizer.SplitTasks
        """
        LocalhostSynchronizerBase.__init__(self, split_synchronizers, split_tasks, localhost_mode)

    async def sync_all(self, till=None):
        """
        Synchronize all feature flags.
        """
        # TODO: to be removed when legacy and yaml use BUR
        if self._localhost_mode != LocalhostMode.JSON:
            return await self.synchronize_splits()

        self._backoff.reset()
        remaining_attempts = _ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES
        while remaining_attempts > 0:
            remaining_attempts -= 1
            try:
                return await self.synchronize_splits()
            except APIException as exc:
                _LOGGER.error('Failed syncing all')
                _LOGGER.error(str(exc))

            how_long = self._backoff.get()
            await asyncio.sleep(how_long)

    async def stop_periodic_fetching(self):
        """Stop fetchers for feature flags and segments."""
        if self._split_tasks.split_task is not None:
            _LOGGER.debug('Stopping periodic fetching')
            await self._split_tasks.split_task.stop()
        if self._split_tasks.segment_task is not None:
            await self._split_tasks.segment_task.stop()

    async def synchronize_splits(self):
        """Synchronize all feature flags."""
        try:
            new_segments = []
            for segment in await self._split_synchronizers.split_sync.synchronize_splits():
                    if not await self._split_synchronizers.segment_sync.segment_exist_in_storage(segment):
                        new_segments.append(segment)
            if len(new_segments) > 0:
                _LOGGER.debug('Synching Segments: %s', ','.join(new_segments))
                success = await self._split_synchronizers.segment_sync.synchronize_segments(new_segments)
                if not success:
                    _LOGGER.error('Failed to schedule sync one or all segment(s) below.')
                    _LOGGER.error(','.join(new_segments))
                else:
                    _LOGGER.debug('Segment sync scheduled.')
            return True

        except APIException as exc:
            _LOGGER.error('Failed syncing feature flags')
            raise APIException('Failed to sync feature flags') from exc

    async def shutdown(self, blocking):
        """
        Stop tasks.

        :param blocking:flag to wait until tasks are stopped
        :type blocking: bool
        """
        await self.stop_periodic_fetching()


class PluggableSynchronizer(BaseSynchronizer):
    """Plugable Synchronizer."""

    def synchronize_segment(self, segment_name, till):
        """
        Synchronize particular segment.

        :param segment_name: segment associated
        :type segment_name: str
        :param till: to fetch
        :type till: int
        """
        pass

    def synchronize_splits(self, till):
        """
        Synchronize all feature flags.

        :param till: to fetch
        :type till: int
        """
        pass

    def sync_all(self):
        """Synchronize all feature flag data."""
        pass

    def start_periodic_fetching(self):
        """Start fetchers for feature flags and segments."""
        pass

    def stop_periodic_fetching(self):
        """Stop fetchers for feature flags and segments."""
        pass

    def start_periodic_data_recording(self):
        """Start recorders."""
        pass

    def stop_periodic_data_recording(self, blocking):
        """Stop recorders."""
        pass

    def kill_split(self, feature_flag_name, default_treatment, change_number):
        """
        Kill a feature_flag locally.

        :param feature_flag_name: name of the feature_flag to perform kill
        :type feature_flag_name: str
        :param default_treatment: name of the default treatment to return
        :type default_treatment: str
        :param change_number: change_number
        :type change_number: int
        """
        pass

    def shutdown(self, blocking):
        """
        Stop tasks.

        :param blocking:flag to wait until tasks are stopped
        :type blocking: bool
        """
        pass

class PluggableSynchronizerAsync(BaseSynchronizer):
    """Plugable Synchronizer."""

    async def synchronize_segment(self, segment_name, till):
        """
        Synchronize particular segment.

        :param segment_name: segment associated
        :type segment_name: str
        :param till: to fetch
        :type till: int
        """
        pass

    async def synchronize_splits(self, till):
        """
        Synchronize all feature flags.

        :param till: to fetch
        :type till: int
        """
        pass

    async def sync_all(self):
        """Synchronize all split data."""
        pass

    async def start_periodic_fetching(self):
        """Start fetchers for feature flags and segments."""
        pass

    async def stop_periodic_fetching(self):
        """Stop fetchers for feature flags and segments."""
        pass

    async def start_periodic_data_recording(self):
        """Start recorders."""
        pass

    async def stop_periodic_data_recording(self, blocking):
        """Stop recorders."""
        pass

    async def kill_split(self, feature_flag_name, default_treatment, change_number):
        """
        Kill a feature_flag locally.

        :param feature_flag_name: name of the feature flag to perform kill
        :type feature_flag_name: str
        :param default_treatment: name of the default treatment to return
        :type default_treatment: str
        :param change_number: change_number
        :type change_number: int
        """
        pass

    async def shutdown(self, blocking):
        """
        Stop tasks.

        :param blocking:flag to wait until tasks are stopped
        :type blocking: bool
        """
        pass
