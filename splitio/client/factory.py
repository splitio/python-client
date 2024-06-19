"""A module for Split.io Factories."""
import logging
import threading
from collections import Counter
from enum import Enum

from splitio.optional.loaders import asyncio
from splitio.client.client import Client, ClientAsync
from splitio.client import input_validator
from splitio.client.manager import SplitManager, SplitManagerAsync
from splitio.client.config import sanitize as sanitize_config, DEFAULT_DATA_SAMPLING
from splitio.client import util
from splitio.client.listener import ImpressionListenerWrapper, ImpressionListenerWrapperAsync
from splitio.engine.impressions.impressions import Manager as ImpressionsManager
from splitio.engine.impressions import set_classes, set_classes_async
from splitio.engine.impressions.strategies import StrategyDebugMode
from splitio.engine.telemetry import TelemetryStorageProducer, TelemetryStorageConsumer, \
    TelemetryStorageProducerAsync, TelemetryStorageConsumerAsync
from splitio.engine.impressions.manager import Counter as ImpressionsCounter
from splitio.engine.impressions.unique_keys_tracker import UniqueKeysTracker, UniqueKeysTrackerAsync

# Storage
from splitio.storage.inmemmory import InMemorySplitStorage, InMemorySegmentStorage, \
    InMemoryImpressionStorage, InMemoryEventStorage, InMemoryTelemetryStorage, LocalhostTelemetryStorage, \
    InMemorySplitStorageAsync, InMemorySegmentStorageAsync, InMemoryImpressionStorageAsync, \
    InMemoryEventStorageAsync, InMemoryTelemetryStorageAsync, LocalhostTelemetryStorageAsync
from splitio.storage.adapters import redis
from splitio.storage.redis import RedisSplitStorage, RedisSegmentStorage, RedisImpressionsStorage, \
    RedisEventsStorage, RedisTelemetryStorage, RedisSplitStorageAsync, RedisEventsStorageAsync,\
    RedisSegmentStorageAsync, RedisImpressionsStorageAsync, RedisTelemetryStorageAsync
from splitio.storage.pluggable import PluggableEventsStorage, PluggableImpressionsStorage, PluggableSegmentStorage, \
    PluggableSplitStorage, PluggableTelemetryStorage, PluggableTelemetryStorageAsync, PluggableEventsStorageAsync, \
    PluggableImpressionsStorageAsync, PluggableSegmentStorageAsync, PluggableSplitStorageAsync

# APIs
from splitio.api.client import HttpClient, HttpClientAsync
from splitio.api.splits import SplitsAPI, SplitsAPIAsync
from splitio.api.segments import SegmentsAPI, SegmentsAPIAsync
from splitio.api.impressions import ImpressionsAPI, ImpressionsAPIAsync
from splitio.api.events import EventsAPI, EventsAPIAsync
from splitio.api.auth import AuthAPI, AuthAPIAsync
from splitio.api.telemetry import TelemetryAPI, TelemetryAPIAsync
from splitio.util.time import get_current_epoch_time_ms

# Tasks
from splitio.tasks.split_sync import SplitSynchronizationTask, SplitSynchronizationTaskAsync
from splitio.tasks.segment_sync import SegmentSynchronizationTask, SegmentSynchronizationTaskAsync
from splitio.tasks.impressions_sync import ImpressionsSyncTask, ImpressionsCountSyncTask,\
    ImpressionsCountSyncTaskAsync, ImpressionsSyncTaskAsync
from splitio.tasks.events_sync import EventsSyncTask, EventsSyncTaskAsync
from splitio.tasks.telemetry_sync import TelemetrySyncTask, TelemetrySyncTaskAsync

# Synchronizer
from splitio.sync.synchronizer import SplitTasks, SplitSynchronizers, Synchronizer, \
    LocalhostSynchronizer, RedisSynchronizer, PluggableSynchronizer,\
    SynchronizerAsync, RedisSynchronizerAsync, LocalhostSynchronizerAsync
from splitio.sync.manager import Manager, RedisManager, ManagerAsync, RedisManagerAsync
from splitio.sync.split import SplitSynchronizer, LocalSplitSynchronizer, LocalhostMode,\
    SplitSynchronizerAsync, LocalSplitSynchronizerAsync
from splitio.sync.segment import SegmentSynchronizer, LocalSegmentSynchronizer, SegmentSynchronizerAsync,\
    LocalSegmentSynchronizerAsync
from splitio.sync.impression import ImpressionSynchronizer, ImpressionsCountSynchronizer, \
    ImpressionsCountSynchronizerAsync, ImpressionSynchronizerAsync
from splitio.sync.event import EventSynchronizer, EventSynchronizerAsync
from splitio.sync.telemetry import TelemetrySynchronizer, InMemoryTelemetrySubmitter, \
    LocalhostTelemetrySubmitter, RedisTelemetrySubmitter, LocalhostTelemetrySubmitterAsync, \
    InMemoryTelemetrySubmitterAsync, TelemetrySynchronizerAsync, RedisTelemetrySubmitterAsync


# Recorder
from splitio.recorder.recorder import StandardRecorder, PipelinedRecorder, StandardRecorderAsync, PipelinedRecorderAsync

# Localhost stuff
from splitio.client.localhost import LocalhostEventsStorage, LocalhostImpressionsStorage, \
    LocalhostImpressionsStorageAsync, LocalhostEventsStorageAsync


_LOGGER = logging.getLogger(__name__)
_INSTANTIATED_FACTORIES = Counter()
_INSTANTIATED_FACTORIES_LOCK = threading.RLock()
_MIN_DEFAULT_DATA_SAMPLING_ALLOWED = 0.1  # 10%
_MAX_RETRY_SYNC_ALL = 3
_UNIQUE_KEYS_CACHE_SIZE = 30000


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
            sync_manager=None,
            sdk_ready_flag=None,
            telemetry_producer=None,
            telemetry_init_producer=None,
            telemetry_submitter=None,
            preforked_initialization=False
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
        :type sync_manager: splitio.sync.manager.Manager
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

    def _update_status_when_ready(self):
        """Wait until the sdk is ready and update the status."""
        self._sdk_internal_ready_flag.wait()
        self._status = Status.READY
        self._sdk_ready_flag.set()
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
        return Client(self, self._recorder, self._labels_enabled)

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
            sync_manager=None,
            telemetry_producer=None,
            telemetry_init_producer=None,
            telemetry_submitter=None,
            manager_start_task=None,
            api_client=None
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
        :type sync_manager: splitio.sync.manager.Manager
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
        self._manager_start_task = manager_start_task
        self._status = Status.NOT_INITIALIZED
        self._sdk_ready_flag = asyncio.Event()
        self._ready_task = asyncio.get_running_loop().create_task(self._update_status_when_ready_async())
        self._api_client = api_client

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
        return ClientAsync(self, self._recorder, self._labels_enabled)

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

def _build_in_memory_factory(api_key, cfg, sdk_url=None, events_url=None,  # pylint:disable=too-many-arguments,too-many-locals
                             auth_api_base_url=None, streaming_api_base_url=None, telemetry_api_base_url=None,
                             total_flag_sets=0, invalid_flag_sets=0):
    """Build and return a split factory tailored to the supplied config."""
    if not input_validator.validate_factory_instantiation(api_key):
        return None

    extra_cfg = {}
    extra_cfg['sdk_url'] = sdk_url
    extra_cfg['events_url'] = events_url
    extra_cfg['auth_url'] = auth_api_base_url
    extra_cfg['streaming_url'] = streaming_api_base_url
    extra_cfg['telemetry_url'] = telemetry_api_base_url

    telemetry_storage = InMemoryTelemetryStorage()
    telemetry_producer = TelemetryStorageProducer(telemetry_storage)
    telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
    telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
    telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()
    telemetry_init_producer = telemetry_producer.get_telemetry_init_producer()

    http_client = HttpClient(
        sdk_url=sdk_url,
        events_url=events_url,
        auth_url=auth_api_base_url,
        telemetry_url=telemetry_api_base_url,
        timeout=cfg.get('connectionTimeout')
    )

    sdk_metadata = util.get_metadata(cfg)
    apis = {
        'auth': AuthAPI(http_client, api_key, sdk_metadata, telemetry_runtime_producer),
        'splits': SplitsAPI(http_client, api_key, sdk_metadata, telemetry_runtime_producer),
        'segments': SegmentsAPI(http_client, api_key, sdk_metadata, telemetry_runtime_producer),
        'impressions': ImpressionsAPI(http_client, api_key, sdk_metadata, telemetry_runtime_producer, cfg['impressionsMode']),
        'events': EventsAPI(http_client, api_key, sdk_metadata, telemetry_runtime_producer),
        'telemetry': TelemetryAPI(http_client, api_key, sdk_metadata, telemetry_runtime_producer),
    }

    storages = {
        'splits': InMemorySplitStorage(cfg['flagSetsFilter'] if cfg['flagSetsFilter'] is not None else []),
        'segments': InMemorySegmentStorage(),
        'impressions': InMemoryImpressionStorage(cfg['impressionsQueueSize'], telemetry_runtime_producer),
        'events': InMemoryEventStorage(cfg['eventsQueueSize'], telemetry_runtime_producer),
    }

    telemetry_submitter = InMemoryTelemetrySubmitter(telemetry_consumer, storages['splits'], storages['segments'], apis['telemetry'])

    imp_counter = ImpressionsCounter()
    unique_keys_tracker = UniqueKeysTracker(_UNIQUE_KEYS_CACHE_SIZE)
    unique_keys_synchronizer, clear_filter_sync, unique_keys_task, \
    clear_filter_task, impressions_count_sync, impressions_count_task, \
    imp_strategy = set_classes('MEMORY', cfg['impressionsMode'], apis, imp_counter, unique_keys_tracker)

    imp_manager = ImpressionsManager(
        imp_strategy, telemetry_runtime_producer)

    synchronizers = SplitSynchronizers(
        SplitSynchronizer(apis['splits'], storages['splits']),
        SegmentSynchronizer(apis['segments'], storages['splits'], storages['segments']),
        ImpressionSynchronizer(apis['impressions'], storages['impressions'],
                               cfg['impressionsBulkSize']),
        EventSynchronizer(apis['events'], storages['events'], cfg['eventsBulkSize']),
        impressions_count_sync,
        TelemetrySynchronizer(telemetry_submitter),
        unique_keys_synchronizer,
        clear_filter_sync,
    )

    tasks = SplitTasks(
        SplitSynchronizationTask(
            synchronizers.split_sync.synchronize_splits,
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
        impressions_count_task,
        TelemetrySyncTask(synchronizers.telemetry_sync.synchronize_stats, cfg['metricsRefreshRate']),
        unique_keys_task,
        clear_filter_task,
    )

    synchronizer = Synchronizer(synchronizers, tasks)

    preforked_initialization = cfg.get('preforkedInitialization', False)

    sdk_ready_flag = threading.Event() if not preforked_initialization else None
    manager = Manager(sdk_ready_flag, synchronizer, apis['auth'], cfg['streamingEnabled'],
                      sdk_metadata, telemetry_runtime_producer, streaming_api_base_url, api_key[-4:])

    storages['events'].set_queue_full_hook(tasks.events_task.flush)
    storages['impressions'].set_queue_full_hook(tasks.impressions_task.flush)

    recorder = StandardRecorder(
        imp_manager,
        storages['events'],
        storages['impressions'],
        telemetry_evaluation_producer,
        telemetry_runtime_producer,
        _wrap_impression_listener(cfg['impressionListener'], sdk_metadata),
        imp_counter=imp_counter,
        unique_keys_tracker=unique_keys_tracker
    )

    telemetry_init_producer.record_config(cfg, extra_cfg, total_flag_sets, invalid_flag_sets)

    if preforked_initialization:
        synchronizer.sync_all(max_retry_attempts=_MAX_RETRY_SYNC_ALL)
        synchronizer._split_synchronizers._segment_sync.shutdown()

        return SplitFactory(api_key, storages, cfg['labelsEnabled'],
                            recorder, manager, None, telemetry_producer, telemetry_init_producer, telemetry_submitter, preforked_initialization=preforked_initialization)

    initialization_thread = threading.Thread(target=manager.start, name="SDKInitializer", daemon=True)
    initialization_thread.start()

    return SplitFactory(api_key, storages, cfg['labelsEnabled'],
                        recorder, manager, sdk_ready_flag,
                        telemetry_producer, telemetry_init_producer,
                        telemetry_submitter)

async def _build_in_memory_factory_async(api_key, cfg, sdk_url=None, events_url=None,  # pylint:disable=too-many-arguments,too-many-localsa
                             auth_api_base_url=None, streaming_api_base_url=None, telemetry_api_base_url=None,
                             total_flag_sets=0, invalid_flag_sets=0):
    """Build and return a split factory tailored to the supplied config in async mode."""
    if not input_validator.validate_factory_instantiation(api_key):
        return None

    extra_cfg = {}
    extra_cfg['sdk_url'] = sdk_url
    extra_cfg['events_url'] = events_url
    extra_cfg['auth_url'] = auth_api_base_url
    extra_cfg['streaming_url'] = streaming_api_base_url
    extra_cfg['telemetry_url'] = telemetry_api_base_url

    telemetry_storage = await InMemoryTelemetryStorageAsync.create()
    telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
    telemetry_consumer = TelemetryStorageConsumerAsync(telemetry_storage)
    telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
    telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()
    telemetry_init_producer = telemetry_producer.get_telemetry_init_producer()

    http_client = HttpClientAsync(
        sdk_url=sdk_url,
        events_url=events_url,
        auth_url=auth_api_base_url,
        telemetry_url=telemetry_api_base_url,
        timeout=cfg.get('connectionTimeout')
    )

    sdk_metadata = util.get_metadata(cfg)
    apis = {
        'auth': AuthAPIAsync(http_client, api_key, sdk_metadata, telemetry_runtime_producer),
        'splits': SplitsAPIAsync(http_client, api_key, sdk_metadata, telemetry_runtime_producer),
        'segments': SegmentsAPIAsync(http_client, api_key, sdk_metadata, telemetry_runtime_producer),
        'impressions': ImpressionsAPIAsync(http_client, api_key, sdk_metadata, telemetry_runtime_producer, cfg['impressionsMode']),
        'events': EventsAPIAsync(http_client, api_key, sdk_metadata, telemetry_runtime_producer),
        'telemetry': TelemetryAPIAsync(http_client, api_key, sdk_metadata, telemetry_runtime_producer),
    }

    storages = {
        'splits': InMemorySplitStorageAsync(cfg['flagSetsFilter'] if cfg['flagSetsFilter'] is not None else []),
        'segments': InMemorySegmentStorageAsync(),
        'impressions': InMemoryImpressionStorageAsync(cfg['impressionsQueueSize'], telemetry_runtime_producer),
        'events': InMemoryEventStorageAsync(cfg['eventsQueueSize'], telemetry_runtime_producer),
    }

    telemetry_submitter = InMemoryTelemetrySubmitterAsync(telemetry_consumer, storages['splits'], storages['segments'], apis['telemetry'])

    imp_counter = ImpressionsCounter()
    unique_keys_tracker = UniqueKeysTrackerAsync(_UNIQUE_KEYS_CACHE_SIZE)
    unique_keys_synchronizer, clear_filter_sync, unique_keys_task, \
    clear_filter_task, impressions_count_sync, impressions_count_task, \
    imp_strategy = set_classes_async('MEMORY', cfg['impressionsMode'], apis, imp_counter, unique_keys_tracker)

    imp_manager = ImpressionsManager(
        imp_strategy, telemetry_runtime_producer)

    synchronizers = SplitSynchronizers(
        SplitSynchronizerAsync(apis['splits'], storages['splits']),
        SegmentSynchronizerAsync(apis['segments'], storages['splits'], storages['segments']),
        ImpressionSynchronizerAsync(apis['impressions'], storages['impressions'],
                               cfg['impressionsBulkSize']),
        EventSynchronizerAsync(apis['events'], storages['events'], cfg['eventsBulkSize']),
        impressions_count_sync,
        TelemetrySynchronizerAsync(telemetry_submitter),
        unique_keys_synchronizer,
        clear_filter_sync,
    )

    tasks = SplitTasks(
        SplitSynchronizationTaskAsync(
            synchronizers.split_sync.synchronize_splits,
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
        impressions_count_task,
        TelemetrySyncTaskAsync(synchronizers.telemetry_sync.synchronize_stats, cfg['metricsRefreshRate']),
        unique_keys_task,
        clear_filter_task,
    )

    synchronizer = SynchronizerAsync(synchronizers, tasks)

    manager = ManagerAsync(synchronizer, apis['auth'], cfg['streamingEnabled'],
                      sdk_metadata, telemetry_runtime_producer, streaming_api_base_url, api_key[-4:])

    storages['events'].set_queue_full_hook(tasks.events_task.flush)
    storages['impressions'].set_queue_full_hook(tasks.impressions_task.flush)

    recorder = StandardRecorderAsync(
        imp_manager,
        storages['events'],
        storages['impressions'],
        telemetry_evaluation_producer,
        telemetry_runtime_producer,
        _wrap_impression_listener_async(cfg['impressionListener'], sdk_metadata),
        imp_counter=imp_counter,
        unique_keys_tracker=unique_keys_tracker
    )

    await telemetry_init_producer.record_config(cfg, extra_cfg, total_flag_sets, invalid_flag_sets)

    manager_start_task = asyncio.get_running_loop().create_task(manager.start())

    return SplitFactoryAsync(api_key, storages, cfg['labelsEnabled'],
                        recorder, manager,
                        telemetry_producer, telemetry_init_producer,
                        telemetry_submitter, manager_start_task=manager_start_task,
                        api_client=http_client)

def _build_redis_factory(api_key, cfg):
    """Build and return a split factory with redis-based storage."""
    sdk_metadata = util.get_metadata(cfg)
    redis_adapter = redis.build(cfg)
    cache_enabled = cfg.get('redisLocalCacheEnabled', False)
    cache_ttl = cfg.get('redisLocalCacheTTL', 5)
    storages = {
        'splits': RedisSplitStorage(redis_adapter, cache_enabled, cache_ttl, []),
        'segments': RedisSegmentStorage(redis_adapter),
        'impressions': RedisImpressionsStorage(redis_adapter, sdk_metadata),
        'events': RedisEventsStorage(redis_adapter, sdk_metadata),
        'telemetry': RedisTelemetryStorage(redis_adapter, sdk_metadata)
    }
    telemetry_producer = TelemetryStorageProducer(storages['telemetry'])
    telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
    telemetry_init_producer = telemetry_producer.get_telemetry_init_producer()
    telemetry_submitter = RedisTelemetrySubmitter(storages['telemetry'])

    data_sampling = cfg.get('dataSampling', DEFAULT_DATA_SAMPLING)
    if data_sampling < _MIN_DEFAULT_DATA_SAMPLING_ALLOWED:
        _LOGGER.warning("dataSampling cannot be less than %.2f, defaulting to minimum",
                        _MIN_DEFAULT_DATA_SAMPLING_ALLOWED)
        data_sampling = _MIN_DEFAULT_DATA_SAMPLING_ALLOWED

    imp_counter = ImpressionsCounter()
    unique_keys_tracker = UniqueKeysTracker(_UNIQUE_KEYS_CACHE_SIZE)
    unique_keys_synchronizer, clear_filter_sync, unique_keys_task, \
    clear_filter_task, impressions_count_sync, impressions_count_task, \
    imp_strategy = set_classes('REDIS', cfg['impressionsMode'], redis_adapter, imp_counter, unique_keys_tracker)

    imp_manager = ImpressionsManager(
        imp_strategy,
        telemetry_runtime_producer)

    synchronizers = SplitSynchronizers(None, None, None, None,
        impressions_count_sync,
        None,
        unique_keys_synchronizer,
        clear_filter_sync
    )

    tasks = SplitTasks(None, None, None, None,
        impressions_count_task,
        None,
        unique_keys_task,
        clear_filter_task
    )

    synchronizer = RedisSynchronizer(synchronizers, tasks)
    recorder = PipelinedRecorder(
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

    manager = RedisManager(synchronizer)
    initialization_thread = threading.Thread(target=manager.start, name="SDKInitializer", daemon=True)
    initialization_thread.start()

    telemetry_init_producer.record_config(cfg, {}, 0, 0)

    split_factory = SplitFactory(
        api_key,
        storages,
        cfg['labelsEnabled'],
        recorder,
        manager,
        sdk_ready_flag=None,
        telemetry_producer=telemetry_producer,
        telemetry_init_producer=telemetry_init_producer
    )
    redundant_factory_count, active_factory_count = _get_active_and_redundant_count()
    storages['telemetry'].record_active_and_redundant_factories(active_factory_count, redundant_factory_count)
    telemetry_submitter.synchronize_config()

    return split_factory

async def _build_redis_factory_async(api_key, cfg):
    """Build and return a split factory with redis-based storage."""
    sdk_metadata = util.get_metadata(cfg)
    redis_adapter = await redis.build_async(cfg)
    cache_enabled = cfg.get('redisLocalCacheEnabled', False)
    cache_ttl = cfg.get('redisLocalCacheTTL', 5)
    storages = {
        'splits': RedisSplitStorageAsync(redis_adapter, cache_enabled, cache_ttl),
        'segments': RedisSegmentStorageAsync(redis_adapter),
        'impressions': RedisImpressionsStorageAsync(redis_adapter, sdk_metadata),
        'events': RedisEventsStorageAsync(redis_adapter, sdk_metadata),
        'telemetry': await RedisTelemetryStorageAsync.create(redis_adapter, sdk_metadata)
    }
    telemetry_producer = TelemetryStorageProducerAsync(storages['telemetry'])
    telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
    telemetry_init_producer = telemetry_producer.get_telemetry_init_producer()
    telemetry_submitter = RedisTelemetrySubmitterAsync(storages['telemetry'])

    data_sampling = cfg.get('dataSampling', DEFAULT_DATA_SAMPLING)
    if data_sampling < _MIN_DEFAULT_DATA_SAMPLING_ALLOWED:
        _LOGGER.warning("dataSampling cannot be less than %.2f, defaulting to minimum",
                        _MIN_DEFAULT_DATA_SAMPLING_ALLOWED)
        data_sampling = _MIN_DEFAULT_DATA_SAMPLING_ALLOWED

    imp_counter = ImpressionsCounter()
    unique_keys_tracker = UniqueKeysTrackerAsync(_UNIQUE_KEYS_CACHE_SIZE)
    unique_keys_synchronizer, clear_filter_sync, unique_keys_task, \
    clear_filter_task, impressions_count_sync, impressions_count_task, \
    imp_strategy = set_classes_async('REDIS', cfg['impressionsMode'], redis_adapter, imp_counter, unique_keys_tracker)

    imp_manager = ImpressionsManager(
        imp_strategy,
        telemetry_runtime_producer)

    synchronizers = SplitSynchronizers(None, None, None, None,
        impressions_count_sync,
        None,
        unique_keys_synchronizer,
        clear_filter_sync
    )

    tasks = SplitTasks(None, None, None, None,
        impressions_count_task,
        None,
        unique_keys_task,
        clear_filter_task
    )

    synchronizer = RedisSynchronizerAsync(synchronizers, tasks)
    recorder = PipelinedRecorderAsync(
        redis_adapter.pipeline,
        imp_manager,
        storages['events'],
        storages['impressions'],
        storages['telemetry'],
        data_sampling,
        _wrap_impression_listener_async(cfg['impressionListener'], sdk_metadata),
        imp_counter=imp_counter,
        unique_keys_tracker=unique_keys_tracker
    )

    manager = RedisManagerAsync(synchronizer)
    await telemetry_init_producer.record_config(cfg, {}, 0, 0)
    manager.start()

    split_factory = SplitFactoryAsync(
        api_key,
        storages,
        cfg['labelsEnabled'],
        recorder,
        manager,
        telemetry_producer=telemetry_producer,
        telemetry_init_producer=telemetry_init_producer,
        telemetry_submitter=telemetry_submitter
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
    storages = {
        'splits': PluggableSplitStorage(pluggable_adapter, storage_prefix, []),
        'segments': PluggableSegmentStorage(pluggable_adapter, storage_prefix),
        'impressions': PluggableImpressionsStorage(pluggable_adapter, sdk_metadata, storage_prefix),
        'events': PluggableEventsStorage(pluggable_adapter, sdk_metadata, storage_prefix),
        'telemetry': PluggableTelemetryStorage(pluggable_adapter, sdk_metadata, storage_prefix)
    }
    telemetry_producer = TelemetryStorageProducer(storages['telemetry'])
    telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
    telemetry_init_producer = telemetry_producer.get_telemetry_init_producer()
    # Using same class as redis
    telemetry_submitter = RedisTelemetrySubmitter(storages['telemetry'])

    imp_counter = ImpressionsCounter()
    unique_keys_tracker = UniqueKeysTracker(_UNIQUE_KEYS_CACHE_SIZE)
    unique_keys_synchronizer, clear_filter_sync, unique_keys_task, \
    clear_filter_task, impressions_count_sync, impressions_count_task, \
    imp_strategy = set_classes('PLUGGABLE', cfg['impressionsMode'], pluggable_adapter, imp_counter, unique_keys_tracker, storage_prefix)

    imp_manager = ImpressionsManager(
        imp_strategy,
        telemetry_runtime_producer)

    synchronizers = SplitSynchronizers(None, None, None, None,
        impressions_count_sync,
        None,
        unique_keys_synchronizer,
        clear_filter_sync
    )

    tasks = SplitTasks(None, None, None, None,
        impressions_count_task,
        None,
        unique_keys_task,
        clear_filter_task
    )

    # Using same class as redis for consumer mode only
    synchronizer = RedisSynchronizer(synchronizers, tasks)
    recorder = StandardRecorder(
        imp_manager,
        storages['events'],
        storages['impressions'],
        telemetry_producer.get_telemetry_evaluation_producer(),
        telemetry_runtime_producer,
        _wrap_impression_listener(cfg['impressionListener'], sdk_metadata),
        imp_counter=imp_counter,
        unique_keys_tracker=unique_keys_tracker
    )

    # Using same class as redis for consumer mode only
    manager = RedisManager(synchronizer)
    initialization_thread = threading.Thread(target=manager.start, name="SDKInitializer", daemon=True)
    initialization_thread.start()

    telemetry_init_producer.record_config(cfg, {}, 0, 0)

    split_factory = SplitFactory(
        api_key,
        storages,
        cfg['labelsEnabled'],
        recorder,
        manager,
        sdk_ready_flag=None,
        telemetry_producer=telemetry_producer,
        telemetry_init_producer=telemetry_init_producer
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
    storages = {
        'splits': PluggableSplitStorageAsync(pluggable_adapter, storage_prefix),
        'segments': PluggableSegmentStorageAsync(pluggable_adapter, storage_prefix),
        'impressions': PluggableImpressionsStorageAsync(pluggable_adapter, sdk_metadata, storage_prefix),
        'events': PluggableEventsStorageAsync(pluggable_adapter, sdk_metadata, storage_prefix),
        'telemetry': await PluggableTelemetryStorageAsync.create(pluggable_adapter, sdk_metadata, storage_prefix)
    }
    telemetry_producer = TelemetryStorageProducerAsync(storages['telemetry'])
    telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
    telemetry_init_producer = telemetry_producer.get_telemetry_init_producer()
    # Using same class as redis
    telemetry_submitter = RedisTelemetrySubmitterAsync(storages['telemetry'])

    imp_counter = ImpressionsCounter()
    unique_keys_tracker = UniqueKeysTrackerAsync(_UNIQUE_KEYS_CACHE_SIZE)
    unique_keys_synchronizer, clear_filter_sync, unique_keys_task, \
    clear_filter_task, impressions_count_sync, impressions_count_task, \
    imp_strategy = set_classes_async('PLUGGABLE', cfg['impressionsMode'], pluggable_adapter, imp_counter, unique_keys_tracker, storage_prefix)

    imp_manager = ImpressionsManager(
        imp_strategy,
        telemetry_runtime_producer)

    synchronizers = SplitSynchronizers(None, None, None, None,
        impressions_count_sync,
        None,
        unique_keys_synchronizer,
        clear_filter_sync
    )

    tasks = SplitTasks(None, None, None, None,
        impressions_count_task,
        None,
        unique_keys_task,
        clear_filter_task
    )

    # Using same class as redis for consumer mode only
    synchronizer = RedisSynchronizerAsync(synchronizers, tasks)
    recorder = StandardRecorderAsync(
        imp_manager,
        storages['events'],
        storages['impressions'],
        telemetry_producer.get_telemetry_evaluation_producer(),
        telemetry_runtime_producer,
        _wrap_impression_listener_async(cfg['impressionListener'], sdk_metadata),
        imp_counter=imp_counter,
        unique_keys_tracker=unique_keys_tracker
    )

    # Using same class as redis for consumer mode only
    manager = RedisManagerAsync(synchronizer)
    manager.start()
    await telemetry_init_producer.record_config(cfg, {}, 0, 0)

    split_factory = SplitFactoryAsync(
        api_key,
        storages,
        cfg['labelsEnabled'],
        recorder,
        manager,
        telemetry_producer=telemetry_producer,
        telemetry_init_producer=telemetry_init_producer,
        telemetry_submitter=telemetry_submitter
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

    storages = {
        'splits': InMemorySplitStorage(cfg['flagSetsFilter'] if cfg['flagSetsFilter'] is not None else []),
        'segments': InMemorySegmentStorage(),  # not used, just to avoid possible future errors.
        'impressions': LocalhostImpressionsStorage(),
        'events': LocalhostEventsStorage(),
    }
    localhost_mode = LocalhostMode.JSON if cfg['splitFile'][-5:].lower() == '.json' else LocalhostMode.LEGACY
    synchronizers = SplitSynchronizers(
        LocalSplitSynchronizer(cfg['splitFile'],
                               storages['splits'],
                               localhost_mode),
        LocalSegmentSynchronizer(cfg['segmentDirectory'], storages['splits'], storages['segments']),
        None, None, None,
    )

    feature_flag_sync_task = None
    segment_sync_task = None
    if cfg['localhostRefreshEnabled'] and localhost_mode == LocalhostMode.JSON:
        feature_flag_sync_task = SplitSynchronizationTask(
            synchronizers.split_sync.synchronize_splits,
            cfg['featuresRefreshRate'],
        )
        segment_sync_task = SegmentSynchronizationTask(
            synchronizers.segment_sync.synchronize_segments,
            cfg['segmentsRefreshRate'],
        )
    tasks = SplitTasks(
        feature_flag_sync_task,
        segment_sync_task,
        None, None, None,
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
        ImpressionsManager(StrategyDebugMode(), telemetry_runtime_producer),
        storages['events'],
        storages['impressions'],
        telemetry_evaluation_producer,
        telemetry_runtime_producer
    )
    return SplitFactory(
        'localhost',
        storages,
        False,
        recorder,
        manager,
        ready_event,
        telemetry_producer=telemetry_producer,
        telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
        telemetry_submitter=LocalhostTelemetrySubmitter(),
    )

async def _build_localhost_factory_async(cfg):
    """Build and return a localhost async factory for testing/development purposes."""
    telemetry_storage = LocalhostTelemetryStorageAsync()
    telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
    telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
    telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()

    storages = {
        'splits': InMemorySplitStorageAsync(),
        'segments': InMemorySegmentStorageAsync(),  # not used, just to avoid possible future errors.
        'impressions': LocalhostImpressionsStorageAsync(),
        'events': LocalhostEventsStorageAsync(),
    }
    localhost_mode = LocalhostMode.JSON if cfg['splitFile'][-5:].lower() == '.json' else LocalhostMode.LEGACY
    synchronizers = SplitSynchronizers(
        LocalSplitSynchronizerAsync(cfg['splitFile'],
                               storages['splits'],
                               localhost_mode),
        LocalSegmentSynchronizerAsync(cfg['segmentDirectory'], storages['splits'], storages['segments']),
        None, None, None,
    )

    feature_flag_sync_task = None
    segment_sync_task = None
    if cfg['localhostRefreshEnabled'] and localhost_mode == LocalhostMode.JSON:
        feature_flag_sync_task = SplitSynchronizationTaskAsync(
            synchronizers.split_sync.synchronize_splits,
            cfg['featuresRefreshRate'],
        )
        segment_sync_task = SegmentSynchronizationTaskAsync(
            synchronizers.segment_sync.synchronize_segments,
            cfg['segmentsRefreshRate'],
        )
    tasks = SplitTasks(
        feature_flag_sync_task,
        segment_sync_task,
        None, None, None,
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
        ImpressionsManager(StrategyDebugMode(), telemetry_runtime_producer),
        storages['events'],
        storages['impressions'],
        telemetry_evaluation_producer,
        telemetry_runtime_producer
    )
    return SplitFactoryAsync(
        'localhost',
        storages,
        False,
        recorder,
        manager,
        telemetry_producer=telemetry_producer,
        telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
        telemetry_submitter=LocalhostTelemetrySubmitterAsync(),
        manager_start_task=manager_start_task
    )

def get_factory(api_key, **kwargs):
    """Build and return the appropriate factory."""
    _INSTANTIATED_FACTORIES_LOCK.acquire()
    if _INSTANTIATED_FACTORIES:
        if api_key in _INSTANTIATED_FACTORIES:
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