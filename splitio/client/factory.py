"""A module for Split.io Factories."""
import logging
import threading
from collections import Counter
from enum import Enum
import queue 

from splitio.optional.loaders import asyncio
from splitio.client.client import Client, ClientAsync
from splitio.client import input_validator
from splitio.client.config import sanitize as sanitize_config, DEFAULT_DATA_SAMPLING, AuthenticateScheme
from splitio.client.manager import SplitManager, SplitManagerAsync
from splitio.client import util
from splitio.client.listener import ImpressionListenerWrapper, ImpressionListenerWrapperAsync

# events
from harness_commons.events.events_manager import EventsManager, EventsManagerAsync
from harness_commons.events.events_manager_config import EventsManagerConfig
from harness_commons.events.events_task import EventsTask, EventsTaskAsync
from harness_commons.events.events_delivery import EventsDelivery
from harness_commons.models.fallback_config import FallbackTreatmentCalculator
from harness_commons.models.notification import SdkInternalEventNotification
from harness_commons.models.events import SdkInternalEvent

# push
from harness_commons.push.manager import PushManager, PushManagerAsync
from harness_commons.push.processor import MessageProcessor, MessageProcessorAsync
from harness_commons.push.parser import UpdateType
from splitio.push.workers import SplitWorker, SplitWorkerAsync

# Storage
from splitio.storage.inmemory import InMemorySplitStorage, InMemorySplitStorageAsync
from harness_commons.storage.inmemmory import InMemorySegmentStorage, \
    InMemoryImpressionStorage, InMemoryEventStorage, InMemoryTelemetryStorage, LocalhostTelemetryStorage, \
    InMemorySegmentStorageAsync, InMemoryImpressionStorageAsync, \
    InMemoryEventStorageAsync, InMemoryTelemetryStorageAsync, LocalhostTelemetryStorageAsync, \
    InMemoryRuleBasedSegmentStorage, InMemoryRuleBasedSegmentStorageAsync
from splitio.storage.adapters import redis
from splitio.storage.redis import RedisSplitStorage, RedisSegmentStorage, RedisImpressionsStorage, \
    RedisEventsStorage, RedisTelemetryStorage, RedisSplitStorageAsync, RedisEventsStorageAsync,\
    RedisSegmentStorageAsync, RedisImpressionsStorageAsync, RedisTelemetryStorageAsync, \
    RedisRuleBasedSegmentsStorage, RedisRuleBasedSegmentsStorageAsync
from splitio.storage.pluggable import PluggableEventsStorage, PluggableImpressionsStorage, PluggableSegmentStorage, \
    PluggableSplitStorage, PluggableTelemetryStorage, PluggableTelemetryStorageAsync, PluggableEventsStorageAsync, \
    PluggableImpressionsStorageAsync, PluggableSegmentStorageAsync, PluggableSplitStorageAsync, \
    PluggableRuleBasedSegmentsStorage, PluggableRuleBasedSegmentsStorageAsync

# APIs
from harness_commons.api.client import HttpClient, HttpClientAsync
from splitio.api.kerberos_client import HttpClientKerberos
from splitio.api.splits import SplitsAPI, SplitsAPIAsync
from harness_commons.api.segments import SegmentsAPI, SegmentsAPIAsync
from harness_commons.api.impressions import ImpressionsAPI, ImpressionsAPIAsync
from harness_commons.api.events import EventsAPI, EventsAPIAsync
from harness_commons.api.auth import AuthAPI, AuthAPIAsync
from harness_commons.api.telemetry import TelemetryAPI, TelemetryAPIAsync
from harness_commons.util.time import get_current_epoch_time_ms
from splitio.spec import SPEC_VERSION

# engine
from harness_commons.engine.impressions.impressions import ImpressionsMode
from harness_commons.engine.impressions.strategies import StrategyNoneMode, StrategyDebugMode, StrategyOptimizedMode
from harness_commons.engine.impressions.adapters import InMemorySenderAdapter, RedisSenderAdapter, PluggableSenderAdapter, RedisSenderAdapterAsync, \
    InMemorySenderAdapterAsync, PluggableSenderAdapterAsync
from harness_commons.engine.impressions.strategies import StrategyDebugMode, StrategyNoneMode
from harness_commons.engine.telemetry import TelemetryStorageProducer, TelemetryStorageConsumer, \
    TelemetryStorageProducerAsync, TelemetryStorageConsumerAsync
from harness_commons.engine.impressions.manager import Counter as ImpressionsCounter
from harness_commons.engine.impressions.unique_keys_tracker import UniqueKeysTracker, UniqueKeysTrackerAsync
from harness_commons.engine.impressions.impressions import Manager as ImpressionsManager

# Tasks
from splitio.tasks.split_sync import SplitSynchronizationTask, SplitSynchronizationTaskAsync
from harness_commons.tasks.segment_sync import SegmentSynchronizationTask, SegmentSynchronizationTaskAsync
from harness_commons.tasks.impressions_sync import ImpressionsSyncTask, ImpressionsCountSyncTask,\
    ImpressionsCountSyncTaskAsync, ImpressionsSyncTaskAsync
from harness_commons.tasks.events_sync import EventsSyncTask, EventsSyncTaskAsync
from harness_commons.tasks.telemetry_sync import TelemetrySyncTask, TelemetrySyncTaskAsync
from harness_commons.tasks.unique_keys_sync import UniqueKeysSyncTask, ClearFilterSyncTask, UniqueKeysSyncTaskAsync, ClearFilterSyncTaskAsync
from harness_commons.sync.unique_keys import UniqueKeysSynchronizer, ClearFilterSynchronizer, UniqueKeysSynchronizerAsync, ClearFilterSynchronizerAsync
from harness_commons.sync.impression import ImpressionsCountSynchronizer, ImpressionsCountSynchronizerAsync
from harness_commons.tasks.impressions_sync import ImpressionsCountSyncTask, ImpressionsCountSyncTaskAsync

# Synchronizer
from harness_commons.sync.synchronizer import HarnessTasks, HarnessSynchronizers, Synchronizer, \
    LocalhostSynchronizer, RedisSynchronizer, PluggableSynchronizer,\
    SynchronizerAsync, RedisSynchronizerAsync, LocalhostSynchronizerAsync
from harness_commons.sync.manager import Manager, RedisManager, ManagerAsync, RedisManagerAsync
from splitio.sync.split import SplitSynchronizer, LocalSplitSynchronizer, LocalhostMode,\
    SplitSynchronizerAsync, LocalSplitSynchronizerAsync
from harness_commons.sync.segment import SegmentSynchronizer, LocalSegmentSynchronizer, SegmentSynchronizerAsync,\
    LocalSegmentSynchronizerAsync
from harness_commons.sync.impression import ImpressionSynchronizer, ImpressionsCountSynchronizer, \
    ImpressionsCountSynchronizerAsync, ImpressionSynchronizerAsync
from harness_commons.sync.event import EventSynchronizer, EventSynchronizerAsync
from harness_commons.sync.telemetry import TelemetrySynchronizer, InMemoryTelemetrySubmitter, \
    LocalhostTelemetrySubmitter, RedisTelemetrySubmitter, LocalhostTelemetrySubmitterAsync, \
    InMemoryTelemetrySubmitterAsync, TelemetrySynchronizerAsync, RedisTelemetrySubmitterAsync
from harness_commons.sync.auth import AuthSynchronizer, AuthSynchronizerAsync
from harness_commons.sync.unique_keys import UniqueKeysSynchronizer, ClearFilterSynchronizer, UniqueKeysSynchronizerAsync, ClearFilterSynchronizerAsync
from harness_commons.sync.impression import ImpressionsCountSynchronizer, ImpressionsCountSynchronizerAsync

# Recorder
from harness_commons.recorder.recorder import StandardRecorder, PipelinedRecorder, StandardRecorderAsync, PipelinedRecorderAsync

# Localhost stuff
from splitio.client.localhost import LocalhostEventsStorage, LocalhostImpressionsStorage, \
    LocalhostImpressionsStorageAsync, LocalhostEventsStorageAsync


_LOGGER = logging.getLogger(__name__)
_INSTANTIATED_FACTORIES = Counter()
_INSTANTIATED_FACTORIES_LOCK = threading.RLock()
_MIN_DEFAULT_DATA_SAMPLING_ALLOWED = 0.1  # 10%
_MAX_RETRY_SYNC_ALL = 3
_UNIQUE_KEYS_CACHE_SIZE = 30000

class ThreadingMode(Enum):
    """Threading mode."""

    THREADED = 'THREADED'
    ASYNC = 'ASYNC'

class Status(Enum):
    """Factory Status."""

    NOT_INITIALIZED = 'NOT_INITIALIZED'
    READY = 'READY'
    DESTROYED = 'DESTROYED'
    WAITING_FORK = 'WAITING_FORK'


class TimeoutException(Exception):
    """Exception to be raised upon a block_until_ready call when a timeout expires."""

    pass


class SplitFactoryBase(object):  # pylint: disable=too-many-instance-attributes
    """Split Factory/Container class."""

    def __init__(self, sdk_key, storages):
        self._sdk_key = sdk_key
        self._storages = storages
        self._status = None

    def _get_storage(self, name):
        """
        Return a reference to the specified storage.

        :param name: Name of the requested storage.
        :type name: str

        :return: requested factory.
        :rtype: object
        """
        return self._storages[name]

    @property
    def ready(self):
        """
        Return whether the factory is ready.

        :return: True if the factory is ready. False otherwhise.
        :rtype: bool
        """
        return self._status == Status.READY

    def _update_instantiated_factories(self):
        self._status = Status.DESTROYED
        with _INSTANTIATED_FACTORIES_LOCK:
            _INSTANTIATED_FACTORIES.subtract([self._sdk_key])

    @property
    def destroyed(self):
        """
        Return whether the factory has been destroyed or not.

        :return: True if the factory has been destroyed. False otherwise.
        :rtype: bool
        """
        return self._status == Status.DESTROYED

    def _waiting_fork(self):
        """
        Return whether the factory is waiting to be recreated by forking or not.

        :return: True if the factory is waiting to be recreated by forking. False otherwise.
        :rtype: bool
        """
        return self._status == Status.WAITING_FORK


class SplitFactory(SplitFactoryBase):  # pylint: disable=too-many-instance-attributes
    """Split Factory/Container class."""

    def __init__(  # pylint: disable=too-many-arguments
            self,
            sdk_key,
            storages,
            labels_enabled,
            recorder,
            internal_events_queue,
            events_manager,
            sync_manager=None,
            sdk_ready_flag=None,
            telemetry_producer=None,
            telemetry_init_producer=None,
            telemetry_submitter=None,
            preforked_initialization=False,
            fallback_treatment_calculator=None
    ):
        """
        Class constructor.

        :param storages: Dictionary of storages for all split models.
        :type storages: dict
        :param labels_enabled: Whether the impressions should store labels or not.
        :type labels_enabled: bool
        :param apis: Dictionary of apis client wrappers
        :type apis: dict
        :param sync_manager: Manager synchronization
        :type sync_manager: harness_commons.sync.manager.Manager
        :param sdk_ready_flag: Event to set when the sdk is ready.
        :type sdk_ready_flag: threading.Event
        :param recorder: StatsRecorder instance
        :type recorder: StatsRecorder
        :param preforked_initialization: Whether should be instantiated as preforked or not.
        :type preforked_initialization: bool
        """
        SplitFactoryBase.__init__(self, sdk_key, storages)
        self._labels_enabled = labels_enabled
        self._sync_manager = sync_manager
        self._recorder = recorder
        self._preforked_initialization = preforked_initialization
        self._telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()
        self._telemetry_init_producer = telemetry_init_producer
        self._telemetry_submitter = telemetry_submitter
        self._ready_time = get_current_epoch_time_ms()
        _LOGGER.debug("Running in threading mode")
        self._sdk_internal_ready_flag = sdk_ready_flag
        self._fallback_treatment_calculator = fallback_treatment_calculator
        self._internal_events_queue = internal_events_queue
        self._events_manager = events_manager
        self._start_status_updater()

    def _start_status_updater(self):
        """
        Perform status updater
        """
        if self._preforked_initialization:
            self._status = Status.WAITING_FORK
            return
        # If we have a ready flag, it means we have sync tasks that need to finish
        # before the SDK client becomes ready.
        if self._sdk_internal_ready_flag is not None:
            self._sdk_ready_flag = threading.Event()
            self._status = Status.NOT_INITIALIZED
            # add a listener that updates the status to READY once the flag is set.
            ready_updater = threading.Thread(target=self._update_status_when_ready,
                                             name='SDKReadyFlagUpdater', daemon=True)
            ready_updater.start()
        else:
            self._status = Status.READY
            self._internal_events_queue.put(SdkInternalEventNotification(SdkInternalEvent.SDK_READY, None))
            
    def _update_status_when_ready(self):
        """Wait until the sdk is ready and update the status."""
        self._sdk_internal_ready_flag.wait()
        self._status = Status.READY
        self._sdk_ready_flag.set()
        self._internal_events_queue.put(SdkInternalEventNotification(SdkInternalEvent.SDK_READY, None))

        self._telemetry_init_producer.record_ready_time(get_current_epoch_time_ms() - self._ready_time)
        redundant_factory_count, active_factory_count = _get_active_and_redundant_count()
        self._telemetry_init_producer.record_active_and_redundant_factories(active_factory_count, redundant_factory_count)

        config_post_thread = threading.Thread(target=self._telemetry_submitter.synchronize_config(), name="PostConfigData")
        config_post_thread.setDaemon(True)
        config_post_thread.start()

    def client(self):
        """
        Return a new client.

        This client is only a set of references to structures hold by the factory.
        Creating one a fast operation and safe to be used anywhere.
        """
        return Client(self, self._recorder, self._events_manager, self._labels_enabled, self._fallback_treatment_calculator)

    def manager(self):
        """
        Return a new manager.

        This manager is only a set of references to structures hold by the factory.
        Creating one a fast operation and safe to be used anywhere.
        """
        return SplitManager(self)

    def block_until_ready(self, timeout=None):
        """
        Blocks until the sdk is ready or the timeout specified by the user expires.

        When ready, the factory's status is updated accordingly.

        :param timeout: Number of seconds to wait (fractions allowed)
        :type timeout: int
        """
        if self._sdk_internal_ready_flag is not None:
            ready = self._sdk_ready_flag.wait(timeout)

            if not ready:
                self._telemetry_init_producer.record_bur_time_out()
                raise TimeoutException('SDK Initialization: time of %d exceeded' % timeout)

    def destroy(self, destroyed_event=None):
        """
        Destroy the factory and render clients unusable.

        Destroy frees up storage taken but split data, flushes impressions & events,
        and invalidates the clients, making them return control.

        :param destroyed_event: Event to signal when destroy process has finished.
        :type destroyed_event: threading.Event
        """
        if self.destroyed:
            _LOGGER.info('Factory already destroyed.')
            return

        try:
            _LOGGER.info('Factory destroy called, stopping tasks.')
            self._events_manager.destroy()
            if self._sync_manager is not None:
                if destroyed_event is not None:

                    def _wait_for_tasks_to_stop():
                        self._sync_manager.stop(True)
                        destroyed_event.set()

                    wait_thread = threading.Thread(target=_wait_for_tasks_to_stop, daemon=True)
                    wait_thread.start()
                else:
                    self._sync_manager.stop(False)
            elif destroyed_event is not None:
                destroyed_event.set()
        finally:
            self._update_instantiated_factories()

    def resume(self):
        """
        Function in charge of starting periodic/realtime synchronization after a fork.
        """
        if not self._waiting_fork():
            _LOGGER.warning('Cannot call resume')
            return
        self._sync_manager.recreate()
        sdk_ready_flag = threading.Event()
        self._sdk_internal_ready_flag = sdk_ready_flag
        self._sync_manager._ready_flag = sdk_ready_flag
        self._get_storage('impressions').clear()
        self._get_storage('events').clear()
        initialization_thread = threading.Thread(
            target=self._sync_manager.start,
            name="SDKInitializer",
            daemon=True
        )
        initialization_thread.start()
        self._preforked_initialization = False  # reset for status updater
        self._start_status_updater()


class SplitFactoryAsync(SplitFactoryBase):  # pylint: disable=too-many-instance-attributes
    """Split Factory/Container async class."""

    def __init__(  # pylint: disable=too-many-arguments
            self,
            sdk_key,
            storages,
            labels_enabled,
            recorder,
            internal_events_queue,
            events_manager,
            sync_manager=None,
            telemetry_producer=None,
            telemetry_init_producer=None,
            telemetry_submitter=None,
            manager_start_task=None,
            api_client=None,
            fallback_treatment_calculator=None
    ):
        """
        Class constructor.

        :param storages: Dictionary of storages for all split models.
        :type storages: dict
        :param labels_enabled: Whether the impressions should store labels or not.
        :type labels_enabled: bool
        :param apis: Dictionary of apis client wrappers
        :type apis: dict
        :param sync_manager: Manager synchronization
        :type sync_manager: harness_commons.sync.manager.Manager
        :param sdk_ready_flag: Event to set when the sdk is ready.
        :type sdk_ready_flag: threading.Event
        :param recorder: StatsRecorder instance
        :type recorder: StatsRecorder
        :param preforked_initialization: Whether should be instantiated as preforked or not.
        :type preforked_initialization: bool
        """
        SplitFactoryBase.__init__(self, sdk_key, storages)
        self._labels_enabled = labels_enabled
        self._sync_manager = sync_manager
        self._recorder = recorder
        self._telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()
        self._telemetry_init_producer = telemetry_init_producer
        self._telemetry_submitter = telemetry_submitter
        self._ready_time = get_current_epoch_time_ms()
        _LOGGER.debug("Running in asyncio mode")
        self._internal_events_queue = internal_events_queue
        self._events_manager = events_manager
        self._manager_start_task = manager_start_task
        self._status = Status.NOT_INITIALIZED
        self._sdk_ready_flag = asyncio.Event()
        self._ready_task = asyncio.get_running_loop().create_task(self._update_status_when_ready_async())
        self._api_client = api_client
        self._fallback_treatment_calculator = fallback_treatment_calculator

    async def _update_status_when_ready_async(self):
        """Wait until the sdk is ready and update the status for async mode."""
        if self._manager_start_task is not None:
            await self._manager_start_task
            self._manager_start_task = None
        await self._telemetry_init_producer.record_ready_time(get_current_epoch_time_ms() - self._ready_time)
        redundant_factory_count, active_factory_count = _get_active_and_redundant_count()
        await self._telemetry_init_producer.record_active_and_redundant_factories(active_factory_count, redundant_factory_count)
        try:
            await self._telemetry_submitter.synchronize_config()
        except Exception as e:
            _LOGGER.error("Failed to post Telemetry config")
            _LOGGER.debug(str(e))
        self._status = Status.READY
        self._sdk_ready_flag.set()
        await self._internal_events_queue.put(SdkInternalEventNotification(SdkInternalEvent.SDK_READY, None))

    def manager(self):
        """
        Return a new manager.

        This manager is only a set of references to structures hold by the factory.
        Creating one a fast operation and safe to be used anywhere.
        """
        return SplitManagerAsync(self)

    async def block_until_ready(self, timeout=None):
        """
        Blocks until the sdk is ready or the timeout specified by the user expires.

        When ready, the factory's status is updated accordingly.

        :param timeout: Number of seconds to wait (fractions allowed)
        :type timeout: int
        """
        try:
            await asyncio.wait_for(asyncio.shield(self._sdk_ready_flag.wait()), timeout)
        except asyncio.TimeoutError as e:
            _LOGGER.error("Exception initializing SDK")
            _LOGGER.debug(str(e))
            await self._telemetry_init_producer.record_bur_time_out()
            raise TimeoutException('SDK Initialization: time of %d exceeded' % timeout)

    async def destroy(self, destroyed_event=None):
        """
        Destroy the factory and render clients unusable.

        Destroy frees up storage taken but split data, flushes impressions & events,
        and invalidates the clients, making them return control.

        :param destroyed_event: Event to signal when destroy process has finished.
        :type destroyed_event: threading.Event
        """
        if self.destroyed:
            _LOGGER.info('Factory already destroyed.')
            return

        try:
            _LOGGER.info('Factory destroy called, stopping tasks.')
            if self._manager_start_task is not None and not self._manager_start_task.done():
                self._manager_start_task.cancel()

            if self._sync_manager is not None:
                await self._sync_manager.stop(True)

                if not self._ready_task.done():
                    self._ready_task.cancel()
                    self._ready_task = None

                if isinstance(self._storages['splits'], RedisSplitStorageAsync):
                    await self._get_storage('splits').redis.close()

                if isinstance(self._sync_manager, ManagerAsync) and isinstance(self._telemetry_submitter, InMemoryTelemetrySubmitterAsync):
                    await self._api_client.close_session()

        except Exception as e:
            _LOGGER.error('Exception destroying factory.')
            _LOGGER.debug(str(e))
        finally:
            self._update_instantiated_factories()

    def client(self):
        """
        Return a new client.

        This client is only a set of references to structures hold by the factory.
        Creating one a fast operation and safe to be used anywhere.
        """
        return ClientAsync(self, self._recorder, self._events_manager, self._labels_enabled, self._fallback_treatment_calculator)

def _wrap_impression_listener(listener, metadata):
    """
    Wrap the impression listener if any.

    :param listener: User supplied impression listener or None
    :type listener: splitio.client.listener.ImpressionListener | None
    :param metadata: SDK Metadata
    :type metadata: splitio.client.util.SdkMetadata
    """
    if listener is not None:
        return ImpressionListenerWrapper(listener, metadata)

    return None

def _wrap_impression_listener_async(listener, metadata):
    """
    Wrap the impression listener if any.

    :param listener: User supplied impression listener or None
    :type listener: splitio.client.listener.ImpressionListener | None
    :param metadata: SDK Metadata
    :type metadata: splitio.client.util.SdkMetadata
    """
    if listener is not None:
        return ImpressionListenerWrapperAsync(listener, metadata)

    return None

def _build_telemetry_classes(storage_mode, threading_mode, telemetry_storage):
    if storage_mode in ['redis', 'pluggable']:
        if threading_mode == ThreadingMode.ASYNC:
            telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
            telemetry_submitter = RedisTelemetrySubmitterAsync(telemetry_storage)
        else:
            telemetry_producer = TelemetryStorageProducer(telemetry_storage)
            telemetry_submitter = RedisTelemetrySubmitter(telemetry_storage)
            
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_init_producer = telemetry_producer.get_telemetry_init_producer()
        return telemetry_producer, telemetry_runtime_producer, telemetry_init_producer, telemetry_submitter
        
    if threading_mode == ThreadingMode.ASYNC:
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_consumer = TelemetryStorageConsumerAsync(telemetry_storage)
    else:    
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)

    telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
    telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()
    telemetry_init_producer = telemetry_producer.get_telemetry_init_producer()
    return telemetry_producer, telemetry_consumer, telemetry_runtime_producer, telemetry_evaluation_producer, telemetry_init_producer

def _build_sdk_metadata(cfg):
    return util.get_metadata(cfg)

def _build_api_classes(threading_mode, cfg, sdk_url, events_url, auth_api_base_url, telemetry_api_base_url, api_key,
                       telemetry_runtime_producer, sdk_metadata):
    authentication_params = None
    if cfg.get("httpAuthenticateScheme") in [AuthenticateScheme.KERBEROS_SPNEGO, AuthenticateScheme.KERBEROS_PROXY]:
        authentication_params = [cfg.get("kerberosPrincipalUser"),
                                cfg.get("kerberosPrincipalPassword")]
        http_client = HttpClientKerberos(
            sdk_url=sdk_url,
            events_url=events_url,
            auth_url=auth_api_base_url,
            telemetry_url=telemetry_api_base_url,
            timeout=cfg.get('connectionTimeout'),
            authentication_scheme = cfg.get("httpAuthenticateScheme"),
            authentication_params = authentication_params
        )
    else:
        if threading_mode == ThreadingMode.THREADED:
            http_client = HttpClient(
                sdk_url=sdk_url,
                events_url=events_url,
                auth_url=auth_api_base_url,
                telemetry_url=telemetry_api_base_url,
                timeout=cfg.get('connectionTimeout'),
            )
            return http_client, {
                'auth': AuthAPI(http_client, api_key, sdk_metadata, telemetry_runtime_producer, SPEC_VERSION),
                'splits': SplitsAPI(http_client, api_key, sdk_metadata, telemetry_runtime_producer),
                'segments': SegmentsAPI(http_client, api_key, sdk_metadata, telemetry_runtime_producer),
                'impressions': ImpressionsAPI(http_client, api_key, sdk_metadata, telemetry_runtime_producer, cfg['impressionsMode']),
                'events': EventsAPI(http_client, api_key, sdk_metadata, telemetry_runtime_producer),
                'telemetry': TelemetryAPI(http_client, api_key, sdk_metadata, telemetry_runtime_producer),
            }
            
        http_client = HttpClientAsync(
            sdk_url=sdk_url,
            events_url=events_url,
            auth_url=auth_api_base_url,
            telemetry_url=telemetry_api_base_url,
            timeout=cfg.get('connectionTimeout')
        )
        apis = {
            'auth': AuthAPIAsync(http_client, api_key, sdk_metadata, telemetry_runtime_producer, SPEC_VERSION),
            'splits': SplitsAPIAsync(http_client, api_key, sdk_metadata, telemetry_runtime_producer),
            'segments': SegmentsAPIAsync(http_client, api_key, sdk_metadata, telemetry_runtime_producer),
            'impressions': ImpressionsAPIAsync(http_client, api_key, sdk_metadata, telemetry_runtime_producer, cfg['impressionsMode']),
            'events': EventsAPIAsync(http_client, api_key, sdk_metadata, telemetry_runtime_producer),
            'telemetry': TelemetryAPIAsync(http_client, api_key, sdk_metadata, telemetry_runtime_producer),
        }
        return http_client, apis

def _build_events_manager_classes(threading_mode):
    if threading_mode == ThreadingMode.ASYNC:    
        internal_events_queue = asyncio.Queue()
        events_manager = EventsManagerAsync(EventsManagerConfig(), EventsDelivery())
        internal_events_task = EventsTaskAsync(events_manager.notify_internal_event, internal_events_queue)
        return internal_events_queue, events_manager, internal_events_task
    
    internal_events_queue = queue.Queue()
    events_manager = EventsManager(EventsManagerConfig(), EventsDelivery())
    internal_events_task = EventsTask(events_manager.notify_internal_event, internal_events_queue)
    return internal_events_queue, events_manager, internal_events_task

def _build_storage_classes(threading_mode, internal_events_queue, telemetry_runtime_producer, cfg, sdk_metadata=None, db_adapter=None, storage_prefix=None):
    if cfg['storageType'] == 'redis':
        cache_enabled = cfg.get('redisLocalCacheEnabled', False)
        cache_ttl = cfg.get('redisLocalCacheTTL', 5)        
        if threading_mode == ThreadingMode.ASYNC:
            return {
                'splits': RedisSplitStorageAsync(db_adapter, cache_enabled, cache_ttl),
                'segments': RedisSegmentStorageAsync(db_adapter),
                'rule_based_segments': RedisRuleBasedSegmentsStorageAsync(db_adapter),        
                'impressions': RedisImpressionsStorageAsync(db_adapter, sdk_metadata),
                'events': RedisEventsStorageAsync(db_adapter, sdk_metadata),
            }
        
        return {
            'splits': RedisSplitStorage(db_adapter, cache_enabled, cache_ttl, []),
            'segments': RedisSegmentStorage(db_adapter),
            'rule_based_segments': RedisRuleBasedSegmentsStorage(db_adapter),        
            'impressions': RedisImpressionsStorage(db_adapter, sdk_metadata),
            'events': RedisEventsStorage(db_adapter, sdk_metadata),
            'telemetry': RedisTelemetryStorage(db_adapter, sdk_metadata)
        }

    if cfg['storageType'] == 'pluggable':
        if threading_mode == ThreadingMode.ASYNC:
            return {
                'splits': PluggableSplitStorageAsync(db_adapter, storage_prefix),
                'segments': PluggableSegmentStorageAsync(db_adapter, storage_prefix),
                'rule_based_segments': PluggableRuleBasedSegmentsStorageAsync(db_adapter, storage_prefix),                
                'impressions': PluggableImpressionsStorageAsync(db_adapter, sdk_metadata, storage_prefix),
                'events': PluggableEventsStorageAsync(db_adapter, sdk_metadata, storage_prefix),
            }

        return {
            'splits': PluggableSplitStorage(db_adapter, storage_prefix, []),
            'segments': PluggableSegmentStorage(db_adapter, storage_prefix),
            'rule_based_segments': PluggableRuleBasedSegmentsStorage(db_adapter, storage_prefix),                
            'impressions': PluggableImpressionsStorage(db_adapter, sdk_metadata, storage_prefix),
            'events': PluggableEventsStorage(db_adapter, sdk_metadata, storage_prefix),
            'telemetry': PluggableTelemetryStorage(db_adapter, sdk_metadata, storage_prefix)
        }
        
        
    if threading_mode == ThreadingMode.ASYNC:
        return {
            'splits': InMemorySplitStorageAsync(internal_events_queue, cfg['flagSetsFilter'] if cfg['flagSetsFilter'] is not None else []),
            'segments': InMemorySegmentStorageAsync(internal_events_queue),
            'rule_based_segments': InMemoryRuleBasedSegmentStorageAsync(internal_events_queue),
            'impressions': InMemoryImpressionStorageAsync(cfg['impressionsQueueSize'], telemetry_runtime_producer),
            'events': InMemoryEventStorageAsync(cfg['eventsQueueSize'], telemetry_runtime_producer),
        }
        
    return {
        'splits': InMemorySplitStorage(internal_events_queue, cfg['flagSetsFilter'] if cfg['flagSetsFilter'] is not None else []),
        'segments': InMemorySegmentStorage(internal_events_queue),
        'rule_based_segments': InMemoryRuleBasedSegmentStorage(internal_events_queue),
        'impressions': InMemoryImpressionStorage(cfg['impressionsQueueSize'], telemetry_runtime_producer),
        'events': InMemoryEventStorage(cfg['eventsQueueSize'], telemetry_runtime_producer),
    }
    
def _build_engine_classes(impressions_mode, telemetry_runtime_producer):
    imp_counter = ImpressionsCounter()
    none_strategy = StrategyNoneMode()
    if impressions_mode == ImpressionsMode.NONE:
        imp_strategy = StrategyNoneMode()
    elif impressions_mode == ImpressionsMode.DEBUG:
        imp_strategy = StrategyDebugMode()
    else:
        imp_strategy = StrategyOptimizedMode()
    imp_manager = ImpressionsManager(imp_strategy, none_strategy, telemetry_runtime_producer)
    return imp_counter, imp_manager

def _build_synchronizer_classes(threading_mode, storages, apis, cfg, telemetry_submitter, unique_keys_tracker, imp_counter, sender_adapter):
    if cfg['storageType'] in ['redis', 'pluggable']:
        if threading_mode == ThreadingMode.ASYNC:
            return HarnessSynchronizers(None, None, None, None,
                ImpressionsCountSynchronizer(sender_adapter, imp_counter),
                None,
                UniqueKeysSynchronizerAsync(sender_adapter, unique_keys_tracker),
                ClearFilterSynchronizerAsync(unique_keys_tracker)
            )
        
        return HarnessSynchronizers(None, None, None, None,
            ImpressionsCountSynchronizer(sender_adapter, imp_counter),
            None,
            UniqueKeysSynchronizer(sender_adapter, unique_keys_tracker),
            ClearFilterSynchronizer(unique_keys_tracker)
        )

    if threading_mode == ThreadingMode.ASYNC:
        return HarnessSynchronizers(
            SplitSynchronizerAsync(apis['splits'], storages['splits'], storages['rule_based_segments']),
            SegmentSynchronizerAsync(apis['segments'], storages['splits'], storages['segments'], storages['rule_based_segments']),
            ImpressionSynchronizerAsync(apis['impressions'], storages['impressions'],
                                cfg['impressionsBulkSize']),
            EventSynchronizerAsync(apis['events'], storages['events'], cfg['eventsBulkSize']),
            ImpressionsCountSynchronizerAsync(apis['impressions'], imp_counter),
            TelemetrySynchronizerAsync(telemetry_submitter),
            UniqueKeysSynchronizerAsync(sender_adapter, unique_keys_tracker),
            ClearFilterSynchronizerAsync(unique_keys_tracker),
        )

    return HarnessSynchronizers(
        SplitSynchronizer(apis['splits'], storages['splits'], storages['rule_based_segments']),
        SegmentSynchronizer(apis['segments'], storages['splits'], storages['segments'], storages['rule_based_segments']),
        ImpressionSynchronizer(apis['impressions'], storages['impressions'],
                               cfg['impressionsBulkSize']),
        EventSynchronizer(apis['events'], storages['events'], cfg['eventsBulkSize']),
        ImpressionsCountSynchronizer(apis['impressions'], imp_counter),
        TelemetrySynchronizer(telemetry_submitter),
        UniqueKeysSynchronizer(sender_adapter, unique_keys_tracker),
        ClearFilterSynchronizer(unique_keys_tracker),
    )

def _build_sync_tasks(threading_mode, synchronizers, cfg, internal_events_task):
    if cfg['storageType'] in ['redis', 'pluggable']:
        if threading_mode == ThreadingMode.ASYNC:
            return HarnessTasks(None, None, None, None,
                ImpressionsCountSyncTaskAsync(synchronizers.impressions_count_sync.synchronize_counters),
                None,
                UniqueKeysSyncTaskAsync(synchronizers.unique_keys_sync.send_all),
                ClearFilterSyncTaskAsync(synchronizers.clear_filter_sync.clear_all)
            )
        
        return HarnessTasks(None, None, None, None,
            ImpressionsCountSyncTask(synchronizers.impressions_count_sync.synchronize_counters),
            None,
            UniqueKeysSyncTask(synchronizers.unique_keys_sync.send_all),
            ClearFilterSyncTask(synchronizers.clear_filter_sync.clear_all)
        )
    
    if threading_mode == ThreadingMode.ASYNC:
        return HarnessTasks(
            SplitSynchronizationTaskAsync(
                synchronizers.definition_sync.synchronize_definitions,
                cfg['featuresRefreshRate'],
            ),
            SegmentSynchronizationTaskAsync(
                synchronizers.segment_sync.synchronize_segments,
                cfg['segmentsRefreshRate'],
            ),
            ImpressionsSyncTaskAsync(
                synchronizers.impressions_sync.synchronize_impressions,
                cfg['impressionsRefreshRate'],
            ),
            EventsSyncTaskAsync(synchronizers.events_sync.synchronize_events, cfg['eventsPushRate']),
            ImpressionsCountSyncTaskAsync(synchronizers.impressions_count_sync.synchronize_counters),
            TelemetrySyncTaskAsync(synchronizers.telemetry_sync.synchronize_stats, cfg['metricsRefreshRate']),
            UniqueKeysSyncTaskAsync(synchronizers.unique_keys_sync.send_all),
            ClearFilterSyncTaskAsync(synchronizers.clear_filter_sync.clear_all),
            internal_events_task
        )

    return HarnessTasks(
        SplitSynchronizationTask(
            synchronizers.definition_sync.synchronize_definitions,
            cfg['featuresRefreshRate'],
        ),
        SegmentSynchronizationTask(
            synchronizers.segment_sync.synchronize_segments,
            cfg['segmentsRefreshRate'],
        ),
        ImpressionsSyncTask(
            synchronizers.impressions_sync.synchronize_impressions,
            cfg['impressionsRefreshRate'],
        ),
        EventsSyncTask(synchronizers.events_sync.synchronize_events, cfg['eventsPushRate']),
        ImpressionsCountSyncTask(synchronizers.impressions_count_sync.synchronize_counters),
        TelemetrySyncTask(synchronizers.telemetry_sync.synchronize_stats, cfg['metricsRefreshRate']),
        UniqueKeysSyncTask(synchronizers.unique_keys_sync.send_all),
        ClearFilterSyncTask(synchronizers.clear_filter_sync.clear_all),
        internal_events_task
    )
    
def _build_push_classes(threading_mode, synchronizer, cfg, telemetry_runtime_producer, apis, sdk_metadata, streaming_api_base_url, api_key):
    push_queue = None
    push_manager = None
    if cfg['streamingEnabled']:
        if threading_mode == ThreadingMode.ASYNC:
            push_queue = asyncio.Queue()
            split_worker = SplitWorkerAsync(synchronizer.synchronize_definitions, synchronizer.synchronize_segment, asyncio.Queue(), synchronizer.definition_sync, synchronizer.definition_sync.definition_storage, synchronizer.segment_storage, telemetry_runtime_producer, synchronizer.definition_sync.rule_based_segment_storage)
            split_handlers = {
                UpdateType.SPLIT_UPDATE: split_worker.handle_feature_flag_update,
                UpdateType.SPLIT_KILL: split_worker.handle_feature_flag_kill,
                UpdateType.RB_SEGMENT_UPDATE: split_worker.handle_feature_flag_update
            }
            auth_synchronizer = AuthSynchronizerAsync(apis['auth'], telemetry_runtime_producer, push_queue)
            processor = MessageProcessorAsync(synchronizer, split_worker, split_handlers)
            push_manager = PushManagerAsync(apis['auth'], push_queue, sdk_metadata, telemetry_runtime_producer, processor, auth_synchronizer, streaming_api_base_url, api_key[-4:])
            return push_manager, push_queue

        push_queue = queue.Queue()
        split_worker = SplitWorker(synchronizer.synchronize_definitions, synchronizer.synchronize_segment, queue.Queue(), synchronizer.definition_sync, synchronizer.definition_sync.definition_storage, synchronizer.segment_storage, telemetry_runtime_producer, synchronizer.definition_sync.rule_based_segment_storage)
        split_handlers = {
            UpdateType.SPLIT_UPDATE: split_worker.handle_feature_flag_update,
            UpdateType.SPLIT_KILL: split_worker.handle_feature_flag_kill,
            UpdateType.RB_SEGMENT_UPDATE: split_worker.handle_feature_flag_update
        }
        auth_synchronizer = AuthSynchronizer(apis['auth'], telemetry_runtime_producer, push_queue)
        processor = MessageProcessor(synchronizer, split_worker, split_handlers)
        push_manager = PushManager(apis['auth'], push_queue, sdk_metadata, telemetry_runtime_producer, processor, auth_synchronizer, streaming_api_base_url, api_key[-4:])
    
    return push_manager, push_queue

def _build_recorder(threading_mode, cfg, imp_manager, storages, telemetry_evaluation_producer, telemetry_runtime_producer, sdk_metadata, imp_counter, unique_keys_tracker, redis_adapter=None):
    if cfg['storageType'] == 'redis':
        data_sampling = cfg.get('dataSampling', DEFAULT_DATA_SAMPLING)
        if data_sampling < _MIN_DEFAULT_DATA_SAMPLING_ALLOWED:
            _LOGGER.warning("dataSampling cannot be less than %.2f, defaulting to minimum",
                            _MIN_DEFAULT_DATA_SAMPLING_ALLOWED)
            data_sampling = _MIN_DEFAULT_DATA_SAMPLING_ALLOWED
    
        if threading_mode == ThreadingMode.ASYNC:
            return PipelinedRecorderAsync(
                redis_adapter.pipeline,
                imp_manager,
                storages['events'],
                storages['impressions'],
                storages['telemetry'],
                data_sampling,
                _wrap_impression_listener(cfg['impressionListener'], sdk_metadata),
                imp_counter=imp_counter,
                unique_keys_tracker=unique_keys_tracker
            )

        return PipelinedRecorder(
            redis_adapter.pipeline,
            imp_manager,
            storages['events'],
            storages['impressions'],
            storages['telemetry'],
            data_sampling,
            _wrap_impression_listener(cfg['impressionListener'], sdk_metadata),
            imp_counter=imp_counter,
            unique_keys_tracker=unique_keys_tracker
        )

    if threading_mode == ThreadingMode.ASYNC:
        return StandardRecorderAsync(
            imp_manager,
            storages['events'],
            storages['impressions'],
            telemetry_evaluation_producer,
            telemetry_runtime_producer,
            _wrap_impression_listener_async(cfg['impressionListener'], sdk_metadata),
            imp_counter=imp_counter,
            unique_keys_tracker=unique_keys_tracker
        )

    return StandardRecorder(
        imp_manager,
        storages['events'],
        storages['impressions'],
        telemetry_evaluation_producer,
        telemetry_runtime_producer,
        _wrap_impression_listener(cfg['impressionListener'], sdk_metadata),
        imp_counter=imp_counter,
        unique_keys_tracker=unique_keys_tracker
    )
    
def _build_extra_cfg(sdk_url, events_url, auth_api_base_url, streaming_api_base_url, telemetry_api_base_url):
    extra_cfg = {}
    extra_cfg['sdk_url'] = sdk_url
    extra_cfg['events_url'] = events_url
    extra_cfg['auth_url'] = auth_api_base_url
    extra_cfg['streaming_url'] = streaming_api_base_url
    extra_cfg['telemetry_url'] = telemetry_api_base_url
    return extra_cfg
    
def _build_in_memory_factory(api_key, cfg, sdk_url=None, events_url=None,  # pylint:disable=too-many-arguments,too-many-locals
                             auth_api_base_url=None, streaming_api_base_url=None, telemetry_api_base_url=None,
                             total_flag_sets=0, invalid_flag_sets=0):
    """Build and return a split factory tailored to the supplied config."""
    if not input_validator.validate_factory_instantiation(api_key):
        return None

    extra_cfg = _build_extra_cfg(sdk_url, events_url, auth_api_base_url, streaming_api_base_url, telemetry_api_base_url)

    telemetry_storage = InMemoryTelemetryStorage()
    telemetry_producer, telemetry_consumer, telemetry_runtime_producer, \
    telemetry_evaluation_producer, telemetry_init_producer = _build_telemetry_classes(cfg['storageType'], ThreadingMode.THREADED, telemetry_storage)
    
    sdk_metadata = _build_sdk_metadata(cfg)
    _, apis = _build_api_classes(ThreadingMode.THREADED, cfg, sdk_url, events_url, auth_api_base_url, telemetry_api_base_url, api_key,
                       telemetry_runtime_producer, sdk_metadata)
  
    internal_events_queue, events_manager, internal_events_task = _build_events_manager_classes(ThreadingMode.THREADED)
    storages = _build_storage_classes(ThreadingMode.THREADED, internal_events_queue, telemetry_runtime_producer, cfg)

    telemetry_submitter = InMemoryTelemetrySubmitter(telemetry_consumer, storages['splits'], storages['segments'], apis['telemetry'])
    unique_keys_tracker = UniqueKeysTracker(_UNIQUE_KEYS_CACHE_SIZE)
    imp_counter, imp_manager = _build_engine_classes(cfg['impressionsMode'], telemetry_runtime_producer)

    sender_adapter = InMemorySenderAdapter(apis['telemetry'])
    synchronizers = _build_synchronizer_classes(ThreadingMode.THREADED, storages, apis, cfg, telemetry_submitter, unique_keys_tracker, imp_counter, sender_adapter)
    tasks = _build_sync_tasks(ThreadingMode.THREADED, synchronizers, cfg, internal_events_task)
    synchronizer = Synchronizer(synchronizers, tasks)

    preforked_initialization = cfg.get('preforkedInitialization', False)

    sdk_ready_flag = threading.Event() if not preforked_initialization else None
    
    push_manager, push_queue = _build_push_classes(ThreadingMode.THREADED, synchronizer, cfg, telemetry_runtime_producer, apis, sdk_metadata, streaming_api_base_url, api_key)    
    manager = Manager(sdk_ready_flag, synchronizer, apis['auth'], cfg['streamingEnabled'],
                      sdk_metadata, telemetry_runtime_producer, streaming_api_base_url, api_key[-4:], push_manager, push_queue)
    recorder = _build_recorder(ThreadingMode.THREADED, cfg, imp_manager, storages, telemetry_evaluation_producer, telemetry_runtime_producer, sdk_metadata, imp_counter, unique_keys_tracker)

    storages['events'].set_queue_full_hook(tasks.events_task.flush)
    storages['impressions'].set_queue_full_hook(tasks.impressions_task.flush)
    telemetry_init_producer.record_config(cfg, extra_cfg, total_flag_sets, invalid_flag_sets)
    internal_events_task.start()
    
    if preforked_initialization:
        synchronizer.sync_all(max_retry_attempts=_MAX_RETRY_SYNC_ALL)
        synchronizer._harness_synchronizers._segment_sync.shutdown()

        return SplitFactory(api_key, storages, cfg['labelsEnabled'],
                            recorder, internal_events_queue, events_manager, manager, None, telemetry_producer, telemetry_init_producer, telemetry_submitter, preforked_initialization=preforked_initialization,
                            fallback_treatment_calculator=FallbackTreatmentCalculator(cfg['fallbackTreatments']))

    initialization_thread = threading.Thread(target=manager.start, name="SDKInitializer", daemon=True)
    initialization_thread.start()

    return SplitFactory(api_key, storages, cfg['labelsEnabled'],
                        recorder, internal_events_queue, events_manager, manager, sdk_ready_flag,
                        telemetry_producer, telemetry_init_producer,
                        telemetry_submitter, fallback_treatment_calculator = FallbackTreatmentCalculator(cfg['fallbackTreatments']))
            
async def _build_in_memory_factory_async(api_key, cfg, sdk_url=None, events_url=None,  # pylint:disable=too-many-arguments,too-many-localsa
                             auth_api_base_url=None, streaming_api_base_url=None, telemetry_api_base_url=None,
                             total_flag_sets=0, invalid_flag_sets=0):
    """Build and return a split factory tailored to the supplied config in async mode."""
    if not input_validator.validate_factory_instantiation(api_key):
        return None

    extra_cfg = _build_extra_cfg(sdk_url, events_url, auth_api_base_url, streaming_api_base_url, telemetry_api_base_url)

    telemetry_storage = await InMemoryTelemetryStorageAsync.create()
    telemetry_producer, telemetry_consumer, telemetry_runtime_producer, \
    telemetry_evaluation_producer, telemetry_init_producer = _build_telemetry_classes(cfg['storageType'], ThreadingMode.ASYNC, telemetry_storage)

    sdk_metadata = _build_sdk_metadata(cfg)
    http_client, apis = _build_api_classes(ThreadingMode.ASYNC, cfg, sdk_url, events_url, auth_api_base_url, telemetry_api_base_url, api_key,
                       telemetry_runtime_producer, sdk_metadata)

    internal_events_queue, events_manager, internal_events_task = _build_events_manager_classes(ThreadingMode.ASYNC)
    storages = _build_storage_classes(ThreadingMode.ASYNC, internal_events_queue, telemetry_runtime_producer, cfg)

    telemetry_submitter = InMemoryTelemetrySubmitterAsync(telemetry_consumer, storages['splits'], storages['segments'], apis['telemetry'])
    unique_keys_tracker = UniqueKeysTrackerAsync(_UNIQUE_KEYS_CACHE_SIZE)
    imp_counter, imp_manager = _build_engine_classes(cfg['impressionsMode'], telemetry_runtime_producer)
    
    sender_adapter = InMemorySenderAdapterAsync(apis['telemetry'])
    synchronizers = _build_synchronizer_classes(ThreadingMode.ASYNC, storages, apis, cfg, telemetry_submitter, unique_keys_tracker, imp_counter, sender_adapter)
    tasks = _build_sync_tasks(ThreadingMode.ASYNC, synchronizers, cfg, internal_events_task)
    synchronizer = SynchronizerAsync(synchronizers, tasks)

    push_manager, push_queue = _build_push_classes(ThreadingMode.ASYNC, synchronizer, cfg, telemetry_runtime_producer, apis, sdk_metadata, streaming_api_base_url, api_key)    
    manager = ManagerAsync(synchronizer, apis['auth'], cfg['streamingEnabled'],
                      sdk_metadata, telemetry_runtime_producer, streaming_api_base_url, api_key[-4:], push_manager, push_queue)

    storages['events'].set_queue_full_hook(tasks.events_task.flush)
    storages['impressions'].set_queue_full_hook(tasks.impressions_task.flush)
    recorder = _build_recorder(ThreadingMode.ASYNC, cfg, imp_manager, storages, telemetry_evaluation_producer, telemetry_runtime_producer, sdk_metadata, imp_counter, unique_keys_tracker)

    await telemetry_init_producer.record_config(cfg, extra_cfg, total_flag_sets, invalid_flag_sets)
    internal_events_task.start()

    manager_start_task = asyncio.get_running_loop().create_task(manager.start())

    return SplitFactoryAsync(api_key, storages, cfg['labelsEnabled'],
                        recorder, internal_events_queue, events_manager, manager,
                        telemetry_producer, telemetry_init_producer,
                        telemetry_submitter, manager_start_task=manager_start_task,
                        api_client=http_client, fallback_treatment_calculator=FallbackTreatmentCalculator(cfg['fallbackTreatments']))
            
def _build_redis_factory(api_key, cfg):
    """Build and return a split factory with redis-based storage."""
    sdk_metadata = util.get_metadata(cfg)
    redis_adapter = redis.build(cfg)
    storages = _build_storage_classes(ThreadingMode.THREADED, None, None, cfg, sdk_metadata, redis_adapter)

    telemetry_producer, telemetry_runtime_producer, telemetry_init_producer, telemetry_submitter = _build_telemetry_classes(cfg['storageType'], ThreadingMode.THREADED, storages['telemetry'])
    unique_keys_tracker = UniqueKeysTracker(_UNIQUE_KEYS_CACHE_SIZE)
    imp_counter, imp_manager = _build_engine_classes(cfg['impressionsMode'], telemetry_runtime_producer)

    sender_adapter = RedisSenderAdapter(redis_adapter)
    synchronizers = _build_synchronizer_classes(ThreadingMode.THREADED, storages, None, cfg, None, unique_keys_tracker, imp_counter, sender_adapter)
    tasks = _build_sync_tasks(ThreadingMode.THREADED, synchronizers, cfg, None)
    synchronizer = RedisSynchronizer(synchronizers, tasks)

    recorder = _build_recorder(ThreadingMode.THREADED, cfg, imp_manager, storages, None, None, sdk_metadata, imp_counter, unique_keys_tracker, redis_adapter)
    manager = RedisManager(synchronizer)
    initialization_thread = threading.Thread(target=manager.start, name="SDKInitializer", daemon=True)
    initialization_thread.start()

    telemetry_init_producer.record_config(cfg, {}, 0, 0)
    internal_events_queue, events_manager, _ = _build_events_manager_classes(ThreadingMode.THREADED)

    split_factory = SplitFactory(
        api_key,
        storages,
        cfg['labelsEnabled'],
        recorder,
        internal_events_queue,
        events_manager,
        manager,
        sdk_ready_flag=None,
        telemetry_producer=telemetry_producer,
        telemetry_init_producer=telemetry_init_producer,
        fallback_treatment_calculator=FallbackTreatmentCalculator(cfg['fallbackTreatments'])
    )
    redundant_factory_count, active_factory_count = _get_active_and_redundant_count()
    storages['telemetry'].record_active_and_redundant_factories(active_factory_count, redundant_factory_count)
    telemetry_submitter.synchronize_config()

    return split_factory

async def _build_redis_factory_async(api_key, cfg):
    """Build and return a split factory with redis-based storage."""
    sdk_metadata = util.get_metadata(cfg)
    redis_adapter = await redis.build_async(cfg)
    storages = _build_storage_classes(ThreadingMode.ASYNC, None, None, cfg, sdk_metadata, redis_adapter)
    storages['telemetry'] = await RedisTelemetryStorageAsync.create(redis_adapter, sdk_metadata)

    telemetry_producer, telemetry_runtime_producer, telemetry_init_producer, telemetry_submitter = _build_telemetry_classes(cfg['storageType'], ThreadingMode.ASYNC, storages['telemetry'])
    unique_keys_tracker = UniqueKeysTrackerAsync(_UNIQUE_KEYS_CACHE_SIZE)
    imp_counter, imp_manager = _build_engine_classes(cfg['impressionsMode'], telemetry_runtime_producer)

    sender_adapter = RedisSenderAdapterAsync(redis_adapter)
    synchronizers = _build_synchronizer_classes(ThreadingMode.ASYNC, storages, None, cfg, None, unique_keys_tracker, imp_counter, sender_adapter)
    tasks = _build_sync_tasks(ThreadingMode.ASYNC, synchronizers, cfg, None)
    synchronizer = RedisSynchronizerAsync(synchronizers, tasks)
    
    recorder = _build_recorder(ThreadingMode.ASYNC, cfg, imp_manager, storages, None, None, sdk_metadata, imp_counter, unique_keys_tracker, redis_adapter)
    manager = RedisManagerAsync(synchronizer)
    await telemetry_init_producer.record_config(cfg, {}, 0, 0)
    manager.start()
    internal_events_queue, events_manager, _ = _build_events_manager_classes(ThreadingMode.ASYNC)

    split_factory = SplitFactoryAsync(
        api_key,
        storages,
        cfg['labelsEnabled'],
        recorder,
        internal_events_queue,
        events_manager,
        manager,
        telemetry_producer=telemetry_producer,
        telemetry_init_producer=telemetry_init_producer,
        telemetry_submitter=telemetry_submitter,
        fallback_treatment_calculator=FallbackTreatmentCalculator(cfg['fallbackTreatments'])
    )
    redundant_factory_count, active_factory_count = _get_active_and_redundant_count()
    await storages['telemetry'].record_active_and_redundant_factories(active_factory_count, redundant_factory_count)
    await telemetry_submitter.synchronize_config()

    return split_factory

def _build_pluggable_factory(api_key, cfg):
    """Build and return a split factory with pluggable storage."""
    sdk_metadata = util.get_metadata(cfg)
    if not input_validator.validate_pluggable_adapter(cfg):
        raise Exception("Pluggable Adapter validation failed, exiting")

    pluggable_adapter = cfg.get('storageWrapper')
    storage_prefix = cfg.get('storagePrefix')
    storages = _build_storage_classes(ThreadingMode.THREADED, None, None, cfg, sdk_metadata, pluggable_adapter, storage_prefix)

    telemetry_producer, telemetry_runtime_producer, telemetry_init_producer, telemetry_submitter = _build_telemetry_classes(cfg['storageType'], ThreadingMode.THREADED, storages['telemetry'])
    unique_keys_tracker = UniqueKeysTracker(_UNIQUE_KEYS_CACHE_SIZE)
    imp_counter, imp_manager = _build_engine_classes(cfg['impressionsMode'], telemetry_runtime_producer)

    sender_adapter = PluggableSenderAdapter(pluggable_adapter, storage_prefix)
    synchronizers = _build_synchronizer_classes(ThreadingMode.THREADED, storages, None, cfg, None, unique_keys_tracker, imp_counter, sender_adapter)
    tasks = _build_sync_tasks(ThreadingMode.THREADED, synchronizers, cfg, None)
    synchronizer = RedisSynchronizer(synchronizers, tasks)

    recorder = _build_recorder(ThreadingMode.THREADED, cfg, imp_manager, storages, None, None, sdk_metadata, imp_counter, unique_keys_tracker, pluggable_adapter)
    # Using same class as redis for consumer mode only
    manager = RedisManager(synchronizer)
    initialization_thread = threading.Thread(target=manager.start, name="SDKInitializer", daemon=True)
    initialization_thread.start()

    telemetry_init_producer.record_config(cfg, {}, 0, 0)
    internal_events_queue, events_manager, _ = _build_events_manager_classes(ThreadingMode.THREADED)

    split_factory = SplitFactory(
        api_key,
        storages,
        cfg['labelsEnabled'],
        recorder,
        internal_events_queue,
        events_manager,
        manager,
        sdk_ready_flag=None,
        telemetry_producer=telemetry_producer,
        telemetry_init_producer=telemetry_init_producer,
        fallback_treatment_calculator=FallbackTreatmentCalculator(cfg['fallbackTreatments'])
    )
    redundant_factory_count, active_factory_count = _get_active_and_redundant_count()
    storages['telemetry'].record_active_and_redundant_factories(active_factory_count, redundant_factory_count)
    telemetry_submitter.synchronize_config()

    return split_factory

async def _build_pluggable_factory_async(api_key, cfg):
    """Build and return a split factory with pluggable storage."""
    sdk_metadata = util.get_metadata(cfg)
    if not input_validator.validate_pluggable_adapter(cfg):
        raise Exception("Pluggable Adapter validation failed, exiting")

    pluggable_adapter = cfg.get('storageWrapper')
    storage_prefix = cfg.get('storagePrefix')
    storages = _build_storage_classes(ThreadingMode.ASYNC, None, None, cfg, sdk_metadata, pluggable_adapter, storage_prefix)
    storages['telemetry'] = await PluggableTelemetryStorageAsync.create(pluggable_adapter, sdk_metadata, storage_prefix)

    telemetry_producer, telemetry_runtime_producer, telemetry_init_producer, telemetry_submitter = _build_telemetry_classes(cfg['storageType'], ThreadingMode.ASYNC, storages['telemetry'])
    unique_keys_tracker = UniqueKeysTracker(_UNIQUE_KEYS_CACHE_SIZE)
    imp_counter, imp_manager = _build_engine_classes(cfg['impressionsMode'], telemetry_runtime_producer)

    sender_adapter = PluggableSenderAdapter(pluggable_adapter, storage_prefix)
    synchronizers = _build_synchronizer_classes(ThreadingMode.ASYNC, storages, None, cfg, None, unique_keys_tracker, imp_counter, sender_adapter)
    tasks = _build_sync_tasks(ThreadingMode.ASYNC, synchronizers, cfg, None)
    synchronizer = RedisSynchronizer(synchronizers, tasks)

    recorder = _build_recorder(ThreadingMode.ASYNC, cfg, imp_manager, storages, None, None, sdk_metadata, imp_counter, unique_keys_tracker, pluggable_adapter)
    # Using same class as redis for consumer mode only
    manager = RedisManagerAsync(synchronizer)
    manager.start()
    await telemetry_init_producer.record_config(cfg, {}, 0, 0)
    internal_events_queue, events_manager, _ = _build_events_manager_classes(ThreadingMode.ASYNC)

    split_factory = SplitFactoryAsync(
        api_key,
        storages,
        cfg['labelsEnabled'],
        recorder,
        internal_events_queue,
        events_manager,
        manager,
        telemetry_producer=telemetry_producer,
        telemetry_init_producer=telemetry_init_producer,
        telemetry_submitter=telemetry_submitter,
        fallback_treatment_calculator=FallbackTreatmentCalculator(cfg['fallbackTreatments'])
    )
    redundant_factory_count, active_factory_count = _get_active_and_redundant_count()
    await storages['telemetry'].record_active_and_redundant_factories(active_factory_count, redundant_factory_count)
    await telemetry_submitter.synchronize_config()

    return split_factory

def _build_localhost_factory(cfg):
    """Build and return a localhost factory for testing/development purposes."""
    telemetry_storage = LocalhostTelemetryStorage()
    telemetry_producer = TelemetryStorageProducer(telemetry_storage)
    telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
    telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()

    internal_events_queue = queue.Queue()
    storages = {
        'splits': InMemorySplitStorage(internal_events_queue, cfg['flagSetsFilter'] if cfg['flagSetsFilter'] is not None else []),
        'segments': InMemorySegmentStorage(internal_events_queue),  # not used, just to avoid possible future errors.
        'rule_based_segments': InMemoryRuleBasedSegmentStorage(internal_events_queue),   
        'impressions': LocalhostImpressionsStorage(),
        'events': LocalhostEventsStorage(),
    }
    localhost_mode = LocalhostMode.JSON if cfg['splitFile'][-5:].lower() == '.json' else LocalhostMode.LEGACY
    synchronizers = HarnessSynchronizers(
        LocalSplitSynchronizer(cfg['splitFile'],
                               storages['splits'],
                               storages['rule_based_segments'],
                               localhost_mode),
        LocalSegmentSynchronizer(cfg['segmentDirectory'], storages['splits'], storages['segments']),
        None, None, None,
    )
    events_manager = EventsManager(EventsManagerConfig(), EventsDelivery())
    internal_events_task = EventsTask(events_manager.notify_internal_event, internal_events_queue)

    feature_flag_sync_task = None
    segment_sync_task = None
    if cfg['localhostRefreshEnabled'] and localhost_mode == LocalhostMode.JSON:
        feature_flag_sync_task = SplitSynchronizationTask(
            synchronizers.definition_sync.synchronize_definitions,
            cfg['featuresRefreshRate'],
        )
        segment_sync_task = SegmentSynchronizationTask(
            synchronizers.segment_sync.synchronize_segments,
            cfg['segmentsRefreshRate'],
        )
    tasks = HarnessTasks(
        feature_flag_sync_task,
        segment_sync_task,
        None, None, None,
        internal_events_task=internal_events_task
    )

    sdk_metadata = util.get_metadata(cfg)
    ready_event = threading.Event()
    synchronizer = LocalhostSynchronizer(synchronizers, tasks, localhost_mode)
    manager = Manager(ready_event, synchronizer, None, False, sdk_metadata, telemetry_runtime_producer)

# TODO: BUR is only applied for Localhost JSON mode, in future legacy and yaml will also use BUR
    if localhost_mode == LocalhostMode.JSON:
        initialization_thread = threading.Thread(target=manager.start, name="SDKInitializer", daemon=True)
        initialization_thread.start()
    else:
        manager.start()

    recorder = StandardRecorder(
        ImpressionsManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer),
        storages['events'],
        storages['impressions'],
        telemetry_evaluation_producer,
        telemetry_runtime_producer
    )
    internal_events_task.start()

    return SplitFactory(
        'localhost',
        storages,
        False,
        recorder,
        internal_events_queue,
        events_manager,
        manager,
        ready_event,
        telemetry_producer=telemetry_producer,
        telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
        telemetry_submitter=LocalhostTelemetrySubmitter(),
        fallback_treatment_calculator=FallbackTreatmentCalculator(cfg['fallbackTreatments'])
    )

async def _build_localhost_factory_async(cfg):
    """Build and return a localhost async factory for testing/development purposes."""
    telemetry_storage = LocalhostTelemetryStorageAsync()
    telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
    telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
    telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()

    internal_events_queue = asyncio.Queue()
    events_manager = EventsManagerAsync(EventsManagerConfig(), EventsDelivery())
    internal_events_task = EventsTaskAsync(events_manager.notify_internal_event, internal_events_queue)

    storages = {
        'splits': InMemorySplitStorageAsync(internal_events_queue),
        'segments': InMemorySegmentStorageAsync(internal_events_queue),  # not used, just to avoid possible future errors.
        'rule_based_segments': InMemoryRuleBasedSegmentStorageAsync(internal_events_queue),
        'impressions': LocalhostImpressionsStorageAsync(),
        'events': LocalhostEventsStorageAsync(),
    }
    localhost_mode = LocalhostMode.JSON if cfg['splitFile'][-5:].lower() == '.json' else LocalhostMode.LEGACY
    synchronizers = HarnessSynchronizers(
        LocalSplitSynchronizerAsync(cfg['splitFile'],
                               storages['splits'],
                               storages['rule_based_segments'],
                               localhost_mode),
        LocalSegmentSynchronizerAsync(cfg['segmentDirectory'], storages['splits'], storages['segments']),
        None, None, None,
    )

    feature_flag_sync_task = None
    segment_sync_task = None
    if cfg['localhostRefreshEnabled'] and localhost_mode == LocalhostMode.JSON:
        feature_flag_sync_task = SplitSynchronizationTaskAsync(
            synchronizers.definition_sync.synchronize_definitions,
            cfg['featuresRefreshRate'],
        )
        segment_sync_task = SegmentSynchronizationTaskAsync(
            synchronizers.segment_sync.synchronize_segments,
            cfg['segmentsRefreshRate'],
        )
    tasks = HarnessTasks(
        feature_flag_sync_task,
        segment_sync_task,
        None, None, None,
        internal_events_task=internal_events_task
    )

    sdk_metadata = util.get_metadata(cfg)
    synchronizer = LocalhostSynchronizerAsync(synchronizers, tasks, localhost_mode)
    manager = ManagerAsync(synchronizer, None, False, sdk_metadata, telemetry_runtime_producer)

# TODO: BUR is only applied for Localhost JSON mode, in future legacy and yaml will also use BUR
    manager_start_task = None
    if localhost_mode == LocalhostMode.JSON:
        manager_start_task = asyncio.get_running_loop().create_task(manager.start())
    else:
        await manager.start()

    recorder = StandardRecorderAsync(
        ImpressionsManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer),
        storages['events'],
        storages['impressions'],
        telemetry_evaluation_producer,
        telemetry_runtime_producer
    )        
    internal_events_task.start()

    return SplitFactoryAsync(
        'localhost',
        storages,
        False,
        recorder,
        internal_events_queue,
        events_manager,
        manager,
        telemetry_producer=telemetry_producer,
        telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
        telemetry_submitter=LocalhostTelemetrySubmitterAsync(),
        manager_start_task=manager_start_task,
        fallback_treatment_calculator=FallbackTreatmentCalculator(cfg['fallbackTreatments'])
    )

def get_factory(api_key, **kwargs):
    """Build and return the appropriate factory."""
    _INSTANTIATED_FACTORIES_LOCK.acquire()
    if _INSTANTIATED_FACTORIES:
        if api_key in _INSTANTIATED_FACTORIES:
            if _INSTANTIATED_FACTORIES[api_key] > 0:
                _LOGGER.warning(
                    "factory instantiation: You already have %d %s with this SDK Key. "
                    "We recommend keeping only one instance of the factory at all times "
                    "(Singleton pattern) and reusing it throughout your application.",
                    _INSTANTIATED_FACTORIES[api_key],
                    'factory' if _INSTANTIATED_FACTORIES[api_key] == 1 else 'factories'
                )
        else:
            _LOGGER.warning(
                "factory instantiation: You already have an instance of the Split factory. "
                "Make sure you definitely want this additional instance. "
                "We recommend keeping only one instance of the factory at all times "
                "(Singleton pattern) and reusing it throughout your application."
            )

    _INSTANTIATED_FACTORIES.update([api_key])
    _INSTANTIATED_FACTORIES_LOCK.release()

    config_raw = kwargs.get('config', {})
    total_flag_sets, invalid_flag_sets = _get_total_and_invalid_flag_sets(config_raw)

    config = sanitize_config(api_key, config_raw)

    if config['operationMode'] == 'localhost':
        split_factory =  _build_localhost_factory(config)
    elif config['storageType'] == 'redis':
        split_factory = _build_redis_factory(api_key, config)
    elif config['storageType'] == 'pluggable':
        split_factory = _build_pluggable_factory(api_key, config)
    else:
        split_factory = _build_in_memory_factory(
        api_key,
        config,
        kwargs.get('sdk_api_base_url'),
        kwargs.get('events_api_base_url'),
        kwargs.get('auth_api_base_url'),
        kwargs.get('streaming_api_base_url'),
        kwargs.get('telemetry_api_base_url'),
        total_flag_sets,
        invalid_flag_sets)

    return split_factory

async def get_factory_async(api_key, **kwargs):
    """Build and return the appropriate factory."""
    _INSTANTIATED_FACTORIES_LOCK.acquire()
    if _INSTANTIATED_FACTORIES:
        if api_key in _INSTANTIATED_FACTORIES:
            if _INSTANTIATED_FACTORIES[api_key] > 0:
                _LOGGER.warning(
                    "factory instantiation: You already have %d %s with this SDK Key. "
                    "We recommend keeping only one instance of the factory at all times "
                    "(Singleton pattern) and reusing it throughout your application.",
                    _INSTANTIATED_FACTORIES[api_key],
                    'factory' if _INSTANTIATED_FACTORIES[api_key] == 1 else 'factories'
                )
        else:
            _LOGGER.warning(
                "factory instantiation: You already have an instance of the Split factory. "
                "Make sure you definitely want this additional instance. "
                "We recommend keeping only one instance of the factory at all times "
                "(Singleton pattern) and reusing it throughout your application."
            )

    _INSTANTIATED_FACTORIES.update([api_key])
    _INSTANTIATED_FACTORIES_LOCK.release()

    config_raw = kwargs.get('config', {})
    total_flag_sets, invalid_flag_sets = _get_total_and_invalid_flag_sets(config_raw)

    config = sanitize_config(api_key, config_raw)
    if config['operationMode'] == 'localhost':
        split_factory =  await _build_localhost_factory_async(config)
    elif config['storageType'] == 'redis':
        split_factory = await _build_redis_factory_async(api_key, config)
    elif config['storageType'] == 'pluggable':
        split_factory = await _build_pluggable_factory_async(api_key, config)
    else:
        split_factory = await _build_in_memory_factory_async(
        api_key,
        config,
        kwargs.get('sdk_api_base_url'),
        kwargs.get('events_api_base_url'),
        kwargs.get('auth_api_base_url'),
        kwargs.get('streaming_api_base_url'),
        kwargs.get('telemetry_api_base_url'),
        total_flag_sets,
        invalid_flag_sets)
    return split_factory

def _get_active_and_redundant_count():
    redundant_factory_count = 0
    active_factory_count = 0
    _INSTANTIATED_FACTORIES_LOCK.acquire()
    for item in _INSTANTIATED_FACTORIES:
        redundant_factory_count += _INSTANTIATED_FACTORIES[item] - 1
        active_factory_count += _INSTANTIATED_FACTORIES[item]
    _INSTANTIATED_FACTORIES_LOCK.release()
    return redundant_factory_count, active_factory_count

def _get_total_and_invalid_flag_sets(config_raw):
    total_flag_sets = 0
    invalid_flag_sets = 0
    if config_raw.get('flagSetsFilter') is not None and isinstance(config_raw.get('flagSetsFilter'), list):
        total_flag_sets = len(config_raw.get('flagSetsFilter'))
        invalid_flag_sets = total_flag_sets - len(input_validator.validate_flag_sets(config_raw.get('flagSetsFilter'), 'Telemetry Init'))

    return total_flag_sets, invalid_flag_sets