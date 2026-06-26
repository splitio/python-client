"""Segment changes processing worker."""
import logging
import threading
import abc
import gzip
import zlib
import base64
import json
from enum import Enum
from queue import Queue

from splitio.optional.loaders import asyncio
from splitio.models.splits import from_raw
from splitio_commons.models.rule_based_segments import from_raw as rbs_from_raw
from splitio_commons.models.telemetry import UpdateFromSSE
from splitio_commons.push import SplitStorageException
from splitio_commons.push.parser import UpdateType
from splitio_commons.push.workers import WorkerBase
from splitio_commons.optional.loaders import asyncio
from splitio_commons.util.storage_helper import update_definition_storage, update_definition_storage_async, \
    update_rule_based_segment_storage, update_rule_based_segment_storage_async

_LOGGER = logging.getLogger(__name__)

class SplitWorker(WorkerBase):
    """Feature Flag Worker for processing updates."""

    _centinel = object()

    def __init__(self, synchronize_feature_flag, synchronize_segment, feature_flag_queue, split_synchronizer, feature_flag_storage, segment_storage, telemetry_runtime_producer, rule_based_segment_storage):
        """
        Class constructor.

        :param synchronize_feature_flag: handler to perform feature flag synchronization on incoming event
        :type synchronize_feature_flag: callable
        :param synchronize_segment: handler to perform segment synchronization on incoming event
        :type synchronize_segment: function
        :param feature_flag_queue: queue with feature flag updates notifications
        :type feature_flag_queue: queue
        :param feature_flag_storage: feature flag storage instance
        :type feature_flag_storage: splitio.storage.inmemory.InMemorySplitStorage
        :param segment_storage: segment storage instance
        :type segment_storage: splitio.storage.inmemory.InMemorySegmentStorage
        :param telemetry_runtime_producer: Telemetry runtime producer instance
        :type telemetry_runtime_producer: splitio.engine.telemetry.TelemetryRuntimeProducer
        :param rule_based_segment_storage: Rule based segment Storage.
        :type rule_based_segment_storage: splitio.storage.InMemoryRuleBasedStorage
        """
        self._feature_flag_queue = feature_flag_queue
        self._handler = synchronize_feature_flag
        self._segment_handler = synchronize_segment
        self._running = False
        self._worker = None
        self._feature_flag_storage = feature_flag_storage
        self._segment_storage = segment_storage
        self._telemetry_runtime_producer = telemetry_runtime_producer
        self._rule_based_segment_storage = rule_based_segment_storage
        self._synchronizer = split_synchronizer

    def is_running(self):
        """Return whether the working is running."""
        return self._running

    def _apply_iff_if_needed(self, event):
        if not self._check_instant_ff_update(event):
            return False
        try:
            if event.update_type == UpdateType.SPLIT_UPDATE:                
                new_feature_flag = from_raw(json.loads(self._get_object_definition(event)))                
                segment_list = update_definition_storage(self._feature_flag_storage, [new_feature_flag], event.change_number)
                for segment_name in segment_list:
                    if self._segment_storage.get(segment_name) is None:
                        _LOGGER.debug(self._fetching_segment.format(segment_name=segment_name))
                        self._segment_handler(segment_name, event.change_number)

                referenced_rbs = self._get_referenced_rbs(new_feature_flag)
                self._fetch_rbs_segment_if_needed(referenced_rbs, event)
                self._telemetry_runtime_producer.record_update_from_sse(UpdateFromSSE.SPLIT_UPDATE)
            else:
                new_rbs = rbs_from_raw(json.loads(self._get_object_definition(event)))
                segment_list = update_rule_based_segment_storage(self._rule_based_segment_storage, [new_rbs], event.change_number)
                for segment_name in segment_list:
                    if self._segment_storage.get(segment_name) is None:
                        _LOGGER.debug(self._fetching_segment.format(segment_name=segment_name))
                        self._segment_handler(segment_name, event.change_number)
                self._telemetry_runtime_producer.record_update_from_sse(UpdateFromSSE.RBS_UPDATE)
            return True
                
        except Exception as e:
            raise SplitStorageException(e)

    def _fetch_rbs_segment_if_needed(self, referenced_rbs, event):
        if len(referenced_rbs) > 0 and not self._rule_based_segment_storage.contains(referenced_rbs):
            _LOGGER.debug('Fetching new rule based segment(s) %s', referenced_rbs)
            self._handler(None, event.change_number)
        
    def _check_instant_ff_update(self, event):
        if event.update_type == UpdateType.SPLIT_UPDATE and event.compression is not None and event.previous_change_number == self._feature_flag_storage.get_change_number():
            return True

        if event.update_type == UpdateType.RB_SEGMENT_UPDATE and event.compression is not None and event.previous_change_number == self._rule_based_segment_storage.get_change_number():
            return True

        return False

    def _run(self):
        """Run worker handler."""
        _LOGGER.error("Run worker handler")
        while self.is_running():
            event = self._feature_flag_queue.get()
            _LOGGER.error(event)
            if not self.is_running():
                break
            if event == self._centinel:
                continue
            
            _LOGGER.debug('Processing feature flag update %d', event.change_number)
            try:
                if self._apply_iff_if_needed(event):
                    continue
                
                till = None
                rbs_till = None
                till, rbs_till = self._check_update_type(till, rbs_till, event)
                _LOGGER.error("synching")
                sync_result = self._handler(till, rbs_till)
                if not sync_result.success and sync_result.error_code is not None and sync_result.error_code == 414:
                    _LOGGER.error("URI too long exception caught, sync failed")

                if not sync_result.success:
                    _LOGGER.error("feature flags sync failed")

            except SplitStorageException as e:  # pylint: disable=broad-except
                _LOGGER.error('Exception Updating Feature Flag')
                _LOGGER.debug('Exception information: ', exc_info=True)
            except Exception as e:  # pylint: disable=broad-except
                _LOGGER.error('Exception raised in feature flag synchronization')
                _LOGGER.debug('Exception information: ', exc_info=True)

    def _check_update_type(self, till, rbs_till, event):
        if event.update_type == UpdateType.SPLIT_UPDATE:
            till = event.change_number
        else:
            rbs_till = event.change_number

        return till, rbs_till

    def start(self):
        """Start worker."""
        if self.is_running():
            _LOGGER.debug('Worker is already running')
            return
        self._running = True

        _LOGGER.debug('Starting Feature Flag Worker')
        self._worker = threading.Thread(target=self._run, name='PushFeatureFlagWorker', daemon=True)
        self._worker.start()

    def stop(self):
        """Stop worker."""
        _LOGGER.debug('Stopping Feature Flag Worker')
        if not self.is_running():
            _LOGGER.debug('Worker is not running')
            return
        self._running = False
        self._feature_flag_queue.put(self._centinel)

    def handle_feature_flag_update(self, event):
        """
        Handle incoming feature_flag update notification.

        :param event: Incoming feature_flag change event
        :type event: splitio.push.parser.SplitChangeUpdate
        """
        self._feature_flag_queue.put(event)

    def handle_feature_flag_kill(self, event):
        """
        Handle incoming feature flag kill notification.

        :param event: Incoming feature flag kill event
        :type event: splitio.push.parser.SplitKillUpdate
        """
        self._synchronizer.kill_definition(event.definition_name, event.default_treatment,
                                      event.change_number)
        self._feature_flag_queue.put(event)

class SplitWorkerAsync(WorkerBase):
    """Split Worker for processing updates."""

    _centinel = object()

    def __init__(self, synchronize_feature_flag, synchronize_segment, feature_flag_queue, split_synchronizer, feature_flag_storage, segment_storage, telemetry_runtime_producer, rule_based_segment_storage):
        """
        Class constructor.

        :param synchronize_feature_flag: handler to perform feature_flag synchronization on incoming event
        :type synchronize_feature_flag: callable
        :param synchronize_segment: handler to perform segment synchronization on incoming event
        :type synchronize_segment: function
        :param feature_flag_queue: queue with feature_flag updates notifications
        :type feature_flag_queue: queue
        :param feature_flag_storage: feature flag storage instance
        :type feature_flag_storage: splitio.storage.inmemory.InMemorySplitStorage
        :param segment_storage: segment storage instance
        :type segment_storage: splitio.storage.inmemory.InMemorySegmentStorage
        :param telemetry_runtime_producer: Telemetry runtime producer instance
        :type telemetry_runtime_producer: splitio.engine.telemetry.TelemetryRuntimeProducer
        :param rule_based_segment_storage: Rule based segment Storage.
        :type rule_based_segment_storage: splitio.storage.InMemoryRuleBasedStorage
        """
        self._feature_flag_queue = feature_flag_queue
        self._handler = synchronize_feature_flag
        self._segment_handler = synchronize_segment
        self._running = False
        self._feature_flag_storage = feature_flag_storage
        self._segment_storage = segment_storage
        self._telemetry_runtime_producer = telemetry_runtime_producer
        self._rule_based_segment_storage = rule_based_segment_storage
        self._synchronizer = split_synchronizer
        
    def is_running(self):
        """Return whether the working is running."""
        return self._running

    async def _apply_iff_if_needed(self, event):
        if not await self._check_instant_ff_update(event):
            return False
        try:
            if event.update_type == UpdateType.SPLIT_UPDATE:                
                new_feature_flag = from_raw(json.loads(self._get_object_definition(event)))
                segment_list = await update_definition_storage_async(self._feature_flag_storage, [new_feature_flag], event.change_number)
                for segment_name in segment_list:
                    if await self._segment_storage.get(segment_name) is None:
                        _LOGGER.debug(self._fetching_segment.format(segment_name=segment_name))
                        await self._segment_handler(segment_name, event.change_number)

                referenced_rbs = self._get_referenced_rbs(new_feature_flag)
                await self._fetch_rbs_segment_if_needed(referenced_rbs, event)
                await self._telemetry_runtime_producer.record_update_from_sse(UpdateFromSSE.SPLIT_UPDATE)
            else:
                new_rbs = rbs_from_raw(json.loads(self._get_object_definition(event)))                
                segment_list = await update_rule_based_segment_storage_async(self._rule_based_segment_storage, [new_rbs], event.change_number)
                for segment_name in segment_list:
                    if await self._segment_storage.get(segment_name) is None:
                        _LOGGER.debug(self._fetching_segment.format(segment_name=segment_name))
                        await self._segment_handler(segment_name, event.change_number)
                await self._telemetry_runtime_producer.record_update_from_sse(UpdateFromSSE.RBS_UPDATE)                        
            return True

        except Exception as e:
            raise SplitStorageException(e)

    async def _fetch_rbs_segment_if_needed(self, referenced_rbs, event):
        if len(referenced_rbs) > 0 and not await self._rule_based_segment_storage.contains(referenced_rbs):
            _LOGGER.debug('Fetching new rule based segment(s) %s', referenced_rbs)
            await self._handler(None, event.change_number)

    async def _check_instant_ff_update(self, event):
        if event.update_type == UpdateType.SPLIT_UPDATE and event.compression is not None and event.previous_change_number == await self._feature_flag_storage.get_change_number():
            return True

        if event.update_type == UpdateType.RB_SEGMENT_UPDATE and event.compression is not None and event.previous_change_number == await self._rule_based_segment_storage.get_change_number():
            return True
        
        return False

    async def _run(self):
        """Run worker handler."""
        while self.is_running():
            event = await self._feature_flag_queue.get()
            if not self.is_running():
                break
            if event == self._centinel:
                continue
            _LOGGER.debug('Processing split_update %d', event.change_number)
            try:
                if await self._apply_iff_if_needed(event):
                    continue
                till = None
                rbs_till = None
                if event.update_type == UpdateType.SPLIT_UPDATE:
                    till = event.change_number
                else:
                    rbs_till = event.change_number                
                await self._handler(till, rbs_till)
            except SplitStorageException as e:  # pylint: disable=broad-except
                _LOGGER.error('Exception Updating Feature Flag')
                _LOGGER.debug('Exception information: ', exc_info=True)
            except Exception as e:  # pylint: disable=broad-except
                _LOGGER.error('Exception raised in split synchronization')
                _LOGGER.debug('Exception information: ', exc_info=True)

    def start(self):
        """Start worker."""
        if self.is_running():
            _LOGGER.debug('Worker is already running')
            return
        self._running = True

        _LOGGER.debug('Starting Split Worker')
        asyncio.get_running_loop().create_task(self._run())

    async def stop(self):
        """Stop worker."""
        _LOGGER.debug('Stopping Split Worker')
        if not self.is_running():
            _LOGGER.debug('Worker is not running')
            return
        self._running = False
        await self._feature_flag_queue.put(self._centinel)

    async def handle_feature_flag_update(self, event):
        """
        Handle incoming feature_flag update notification.

        :param event: Incoming feature_flag change event
        :type event: splitio.push.parser.SplitChangeUpdate
        """
        await self._feature_flag_queue.put(event)

    async def handle_feature_flag_kill(self, event):
        """
        Handle incoming feature_flag kill notification.

        :param event: Incoming feature_flag kill event
        :type event: splitio.push.parser.SplitKillUpdate
        """
        await self._synchronizer.kill_definition(event.definition_name, event.default_treatment,
                                      event.change_number)
        await self._feature_flag_queue.put(event)
