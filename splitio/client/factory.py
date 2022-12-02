"""A module for Split.io Factories."""
import logging
import threading
from collections import Counter

from enum import Enum

from splitio.client.client import Client
from splitio.client import input_validator
from splitio.client.manager import SplitManager
from splitio.client.config import sanitize as sanitize_config, DEFAULT_DATA_SAMPLING
from splitio.client import util
from splitio.client.listener import ImpressionListenerWrapper
from splitio.engine.impressions.impressions import Manager as ImpressionsManager
from splitio.engine.impressions.impressions import ImpressionsMode
from splitio.engine.impressions.manager import Counter as ImpressionsCounter
from splitio.engine.impressions.strategies import StrategyNoneMode, StrategyDebugMode, StrategyOptimizedMode
from splitio.engine.impressions.adapters import InMemorySenderAdapter, RedisSenderAdapter
from splitio.engine.impressions import set_classes

# Storage
from splitio.storage.inmemmory import InMemorySplitStorage, InMemorySegmentStorage, \
    InMemoryImpressionStorage, InMemoryEventStorage
from splitio.storage.adapters import redis
from splitio.storage.redis import RedisSplitStorage, RedisSegmentStorage, RedisImpressionsStorage, \
    RedisEventsStorage

# APIs
from splitio.api.client import HttpClient
from splitio.api.splits import SplitsAPI
from splitio.api.segments import SegmentsAPI
from splitio.api.impressions import ImpressionsAPI
from splitio.api.events import EventsAPI
from splitio.api.auth import AuthAPI
from splitio.api.telemetry import TelemetryAPI

# Tasks
from splitio.tasks.split_sync import SplitSynchronizationTask
from splitio.tasks.segment_sync import SegmentSynchronizationTask
from splitio.tasks.impressions_sync import ImpressionsSyncTask, ImpressionsCountSyncTask
from splitio.tasks.events_sync import EventsSyncTask
from splitio.tasks.unique_keys_sync import UniqueKeysSyncTask, ClearFilterSyncTask

# Synchronizer
from splitio.sync.synchronizer import SplitTasks, SplitSynchronizers, Synchronizer, \
    LocalhostSynchronizer, RedisSynchronizer
from splitio.sync.manager import Manager, RedisManager
from splitio.sync.split import SplitSynchronizer, LocalSplitSynchronizer
from splitio.sync.segment import SegmentSynchronizer
from splitio.sync.impression import ImpressionSynchronizer, ImpressionsCountSynchronizer
from splitio.sync.event import EventSynchronizer
from splitio.sync.unique_keys import UniqueKeysSynchronizer, ClearFilterSynchronizer


# Recorder
from splitio.recorder.recorder import StandardRecorder, PipelinedRecorder

# Localhost stuff
from splitio.client.localhost import LocalhostEventsStorage, LocalhostImpressionsStorage


_LOGGER = logging.getLogger(__name__)
_INSTANTIATED_FACTORIES = Counter()
_INSTANTIATED_FACTORIES_LOCK = threading.RLock()
_MIN_DEFAULT_DATA_SAMPLING_ALLOWED = 0.1  # 10%
_MAX_RETRY_SYNC_ALL = 3


class Status(Enum):
    """Factory Status."""

    NOT_INITIALIZED = 'NOT_INITIALIZED'
    READY = 'READY'
    DESTROYED = 'DESTROYED'
    WAITING_FORK = 'WAITING_FORK'


class TimeoutException(Exception):
    """Exception to be raised upon a block_until_ready call when a timeout expires."""

    pass


class SplitFactory(object):  # pylint: disable=too-many-instance-attributes
    """Split Factory/Container class."""

    def __init__(  # pylint: disable=too-many-arguments
            self,
            apikey,
            storages,
            labels_enabled,
            recorder,
            sync_manager=None,
            sdk_ready_flag=None,
            preforked_initialization=False,
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
        self._apikey = apikey
        self._storages = storages
        self._labels_enabled = labels_enabled
        self._sync_manager = sync_manager
        self._sdk_internal_ready_flag = sdk_ready_flag
        self._recorder = recorder
        self._preforked_initialization = preforked_initialization
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
                                             name='SDKReadyFlagUpdater')
            ready_updater.setDaemon(True)
            ready_updater.start()
        else:
            self._status = Status.READY

    def _update_status_when_ready(self):
        """Wait until the sdk is ready and update the status."""
        self._sdk_internal_ready_flag.wait()
        self._status = Status.READY
        self._sdk_ready_flag.set()

    def _get_storage(self, name):
        """
        Return a reference to the specified storage.

        :param name: Name of the requested storage.
        :type name: str

        :return: requested factory.
        :rtype: object
        """
        return self._storages[name]

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
                raise TimeoutException('SDK Initialization: time of %d exceeded' % timeout)

    @property
    def ready(self):
        """
        Return whether the factory is ready.

        :return: True if the factory is ready. False otherwhise.
        :rtype: bool
        """
        return self._status == Status.READY

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

                    wait_thread = threading.Thread(target=_wait_for_tasks_to_stop)
                    wait_thread.setDaemon(True)
                    wait_thread.start()
                else:
                    self._sync_manager.stop(False)
            elif destroyed_event is not None:
                destroyed_event.set()
        finally:
            self._status = Status.DESTROYED
            with _INSTANTIATED_FACTORIES_LOCK:
                _INSTANTIATED_FACTORIES.subtract([self._apikey])

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
        )
        initialization_thread.setDaemon(True)
        initialization_thread.start()
        self._preforked_initialization = False  # reset for status updater
        self._start_status_updater()


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


def _build_in_memory_factory(api_key, cfg, sdk_url=None, events_url=None,  # pylint:disable=too-many-arguments,too-many-locals
                             auth_api_base_url=None, streaming_api_base_url=None, telemetry_api_base_url=None):
    """Build and return a split factory tailored to the supplied config."""
    if not input_validator.validate_factory_instantiation(api_key):
        return None

    http_client = HttpClient(
        sdk_url=sdk_url,
        events_url=events_url,
        auth_url=auth_api_base_url,
        telemetry_url=telemetry_api_base_url,
        timeout=cfg.get('connectionTimeout')
    )

    sdk_metadata = util.get_metadata(cfg)
    apis = {
        'auth': AuthAPI(http_client, api_key, sdk_metadata),
        'splits': SplitsAPI(http_client, api_key, sdk_metadata),
        'segments': SegmentsAPI(http_client, api_key, sdk_metadata),
        'impressions': ImpressionsAPI(http_client, api_key, sdk_metadata, cfg['impressionsMode']),
        'events': EventsAPI(http_client, api_key, sdk_metadata),
        'telemetry': TelemetryAPI(http_client, api_key, sdk_metadata),
    }

    storages = {
        'splits': InMemorySplitStorage(),
        'segments': InMemorySegmentStorage(),
        'impressions': InMemoryImpressionStorage(cfg['impressionsQueueSize']),
        'events': InMemoryEventStorage(cfg['eventsQueueSize']),
    }

    unique_keys_synchronizer, clear_filter_sync, unique_keys_task, \
    clear_filter_task, impressions_count_sync, impressions_count_task, \
    imp_strategy = set_classes('MEMORY', cfg['impressionsMode'], apis)

    imp_manager = ImpressionsManager(
        _wrap_impression_listener(cfg['impressionListener'], sdk_metadata),
        imp_strategy)

    synchronizers = SplitSynchronizers(
        SplitSynchronizer(apis['splits'], storages['splits']),
        SegmentSynchronizer(apis['segments'], storages['splits'], storages['segments']),
        ImpressionSynchronizer(apis['impressions'], storages['impressions'],
                               cfg['impressionsBulkSize']),
        EventSynchronizer(apis['events'], storages['events'], cfg['eventsBulkSize']),
        impressions_count_sync,
        unique_keys_synchronizer,
        clear_filter_sync
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
        unique_keys_task,
        clear_filter_task
    )

    synchronizer = Synchronizer(synchronizers, tasks)

    preforked_initialization = cfg.get('preforkedInitialization', False)

    sdk_ready_flag = threading.Event() if not preforked_initialization else None
    manager = Manager(sdk_ready_flag, synchronizer, apis['auth'], cfg['streamingEnabled'],
                      sdk_metadata, streaming_api_base_url, api_key[-4:])

    storages['events'].set_queue_full_hook(tasks.events_task.flush)
    storages['impressions'].set_queue_full_hook(tasks.impressions_task.flush)

    recorder = StandardRecorder(
        imp_manager,
        storages['events'],
        storages['impressions'],
    )

    if preforked_initialization:
        synchronizer.sync_all(max_retry_attempts=_MAX_RETRY_SYNC_ALL)
        synchronizer._split_synchronizers._segment_sync.shutdown()
        return SplitFactory(api_key, storages, cfg['labelsEnabled'],
                            recorder, manager, preforked_initialization=preforked_initialization)

    initialization_thread = threading.Thread(target=manager.start, name="SDKInitializer")
    initialization_thread.setDaemon(True)
    initialization_thread.start()

    return SplitFactory(api_key, storages, cfg['labelsEnabled'],
                        recorder, manager, sdk_ready_flag)

def _build_redis_factory(api_key, cfg):
    """Build and return a split factory with redis-based storage."""
    sdk_metadata = util.get_metadata(cfg)
    redis_adapter = redis.build(cfg)
    cache_enabled = cfg.get('redisLocalCacheEnabled', False)
    cache_ttl = cfg.get('redisLocalCacheTTL', 5)
    storages = {
        'splits': RedisSplitStorage(redis_adapter, cache_enabled, cache_ttl),
        'segments': RedisSegmentStorage(redis_adapter),
        'impressions': RedisImpressionsStorage(redis_adapter, sdk_metadata),
        'events': RedisEventsStorage(redis_adapter, sdk_metadata),
    }
    data_sampling = cfg.get('dataSampling', DEFAULT_DATA_SAMPLING)
    if data_sampling < _MIN_DEFAULT_DATA_SAMPLING_ALLOWED:
        _LOGGER.warning("dataSampling cannot be less than %.2f, defaulting to minimum",
                        _MIN_DEFAULT_DATA_SAMPLING_ALLOWED)
        data_sampling = _MIN_DEFAULT_DATA_SAMPLING_ALLOWED

    unique_keys_synchronizer, clear_filter_sync, unique_keys_task, \
    clear_filter_task, impressions_count_sync, impressions_count_task, \
    imp_strategy = set_classes('REDIS', cfg['impressionsMode'], redis_adapter)

    imp_manager = ImpressionsManager(
        _wrap_impression_listener(cfg['impressionListener'], sdk_metadata),
        imp_strategy)

    synchronizers = SplitSynchronizers(None, None, None, None,
        impressions_count_sync,
        unique_keys_synchronizer,
        clear_filter_sync
    )

    tasks = SplitTasks(None, None, None, None,
        impressions_count_task,
        unique_keys_task,
        clear_filter_task
    )

    synchronizer = RedisSynchronizer(synchronizers, tasks)
    recorder = PipelinedRecorder(
        redis_adapter.pipeline,
        imp_manager,
        storages['events'],
        storages['impressions'],
        data_sampling,
    )

    manager = RedisManager(synchronizer)
    initialization_thread = threading.Thread(target=manager.start, name="SDKInitializer")
    initialization_thread.setDaemon(True)
    initialization_thread.start()

    return SplitFactory(
        api_key,
        storages,
        cfg['labelsEnabled'],
        recorder,
        manager,
    )


def _build_localhost_factory(cfg):
    """Build and return a localhost factory for testing/development purposes."""
    storages = {
        'splits': InMemorySplitStorage(),
        'segments': InMemorySegmentStorage(),  # not used, just to avoid possible future errors.
        'impressions': LocalhostImpressionsStorage(),
        'events': LocalhostEventsStorage(),
    }

    synchronizers = SplitSynchronizers(
        LocalSplitSynchronizer(cfg['splitFile'], storages['splits']),
        None, None, None, None,
    )

    tasks = SplitTasks(
        SplitSynchronizationTask(
            synchronizers.split_sync.synchronize_splits,
            cfg['featuresRefreshRate'],
        ), None, None, None, None,
    )

    sdk_metadata = util.get_metadata(cfg)
    ready_event = threading.Event()
    synchronizer = LocalhostSynchronizer(synchronizers, tasks)
    manager = Manager(ready_event, synchronizer, None, False, sdk_metadata)
    manager.start()
    recorder = StandardRecorder(
        ImpressionsManager(None, StrategyDebugMode()),
        storages['events'],
        storages['impressions'],
    )
    return SplitFactory(
        'localhost',
        storages,
        False,
        recorder,
        manager,
        ready_event
    )


def get_factory(api_key, **kwargs):
    """Build and return the appropriate factory."""
    try:
        _INSTANTIATED_FACTORIES_LOCK.acquire()
        if _INSTANTIATED_FACTORIES:
            if api_key in _INSTANTIATED_FACTORIES:
                _LOGGER.warning(
                    "factory instantiation: You already have %d %s with this API Key. "
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

        config = sanitize_config(api_key, kwargs.get('config', {}))

        if config['operationMode'] == 'localhost-standalone':
            return _build_localhost_factory(config)

        if config['operationMode'] == 'redis-consumer':
            return _build_redis_factory(api_key, config)

        return _build_in_memory_factory(
            api_key,
            config,
            kwargs.get('sdk_api_base_url'),
            kwargs.get('events_api_base_url'),
            kwargs.get('auth_api_base_url'),
            kwargs.get('streaming_api_base_url'),
            kwargs.get('telemetry_api_base_url')
        )
    finally:
        _INSTANTIATED_FACTORIES.update([api_key])
        _INSTANTIATED_FACTORIES_LOCK.release()
