"""A module for Split.io Factories."""
from __future__ import absolute_import, division, print_function, unicode_literals


import logging
import threading
from enum import Enum

import six

from splitio.client.client import Client
from splitio.client import input_validator
from splitio.client.manager import SplitManager
from splitio.client.config import DEFAULT_CONFIG
from splitio.client import util
from splitio.client.listener import ImpressionListenerWrapper

#Storage
from splitio.storage.inmemmory import InMemorySplitStorage, InMemorySegmentStorage, \
    InMemoryImpressionStorage, InMemoryEventStorage, InMemoryTelemetryStorage
from splitio.storage.adapters import redis
from splitio.storage.redis import RedisSplitStorage, RedisSegmentStorage, RedisImpressionsStorage, \
    RedisEventsStorage, RedisTelemetryStorage
from splitio.storage.adapters.uwsgi_cache import get_uwsgi
from splitio.storage.uwsgi import UWSGIEventStorage, UWSGIImpressionStorage, UWSGISegmentStorage, \
    UWSGISplitStorage, UWSGITelemetryStorage

# APIs
from splitio.api.client import HttpClient
from splitio.api.splits import SplitsAPI
from splitio.api.segments import SegmentsAPI
from splitio.api.impressions import ImpressionsAPI
from splitio.api.events import EventsAPI
from splitio.api.telemetry import TelemetryAPI

# Tasks
from splitio.tasks.split_sync import SplitSynchronizationTask
from splitio.tasks.segment_sync import SegmentSynchronizationTask
from splitio.tasks.impressions_sync import ImpressionsSyncTask
from splitio.tasks.events_sync import EventsSyncTask
from splitio.tasks.telemetry_sync import TelemetrySynchronizationTask

# Localhost stuff
from splitio.client.localhost import LocalhostEventsStorage, LocalhostImpressionsStorage, \
    LocalhostSplitSynchronizationTask, LocalhostTelemetryStorage


class Status(Enum):
    """Factory Status."""

    NOT_INITIALIZED = 'NOT_INITIALIZED'
    READY = 'READY'
    DESTROYED = 'DESTROYED'


class TimeoutException(Exception):
    """Exception to be raised upon a block_until_ready call when a timeout expires."""

    pass


class SplitFactory(object):  #pylint: disable=too-many-instance-attributes
    """Split Factory/Container class."""

    def __init__(  #pylint: disable=too-many-arguments
            self,
            storages,
            labels_enabled,
            apis=None,
            tasks=None,
            sdk_ready_flag=None,
            impression_listener=None
    ):
        """
        Class constructor.

        :param storages: Dictionary of storages for all split models.
        :type storages: dict
        :param labels_enabled: Whether the impressions should store labels or not.
        :type labels_enabled: bool
        :param apis: Dictionary of apis client wrappers
        :type apis: dict
        :param tasks: Dictionary of sychronization tasks
        :type tasks: dict
        :param sdk_ready_flag: Event to set when the sdk is ready.
        :type sdk_ready_flag: threading.Event
        :param impression_listener: User custom listener to handle impressions locally.
        :type impression_listener: splitio.client.listener.ImpressionListener
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._storages = storages
        self._labels_enabled = labels_enabled
        self._apis = apis if apis else {}
        self._tasks = tasks if tasks else {}
        self._sdk_ready_flag = sdk_ready_flag
        self._impression_listener = impression_listener

        # If we have a ready flag, it means we have sync tasks that need to finish
        # before the SDK client becomes ready.
        if self._sdk_ready_flag is not None:
            self._status = Status.NOT_INITIALIZED
            # add a listener that updates the status to READY once the flag is set.
            ready_updater = threading.Thread(target=self._update_status_when_ready)
            ready_updater.setDaemon(True)
            ready_updater.start()
        else:
            self._status = Status.READY

    def _update_status_when_ready(self):
        """Wait until the sdk is ready and update the status."""
        self._sdk_ready_flag.wait()
        self._status = Status.READY

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
        return Client(self, self._labels_enabled, self._impression_listener)

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

        :param timeout: Number of seconds to wait (fractions allowed)
        :type timeout: int
        """
        if self._sdk_ready_flag is not None:
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
            self._logger.info('Factory already destroyed.')
            return

        try:
            if destroyed_event is not None:
                stop_events = {name: threading.Event() for name in self._tasks.keys()}
                for name, task in six.iteritems(self._tasks):
                    task.stop(stop_events[name])

                def _wait_for_tasks_to_stop():
                    for event in stop_events.values():
                        event.wait()
                    destroyed_event.set()

                wait_thread = threading.Thread(target=_wait_for_tasks_to_stop)
                wait_thread.setDaemon(True)
                wait_thread.start()
            else:
                for task in self._tasks.values():
                    task.stop()
        finally:
            self._status = Status.DESTROYED

    @property
    def destroyed(self):
        """
        Return whether the factory has been destroyed or not.

        :return: True if the factory has been destroyed. False otherwise.
        :rtype: bool
        """
        return self._status == Status.DESTROYED


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


def _build_in_memory_factory(api_key, config, sdk_url=None, events_url=None):  #pylint: disable=too-many-locals
    """Build and return a split factory tailored to the supplied config."""
    if not input_validator.validate_factory_instantiation(api_key):
        return None

    cfg = DEFAULT_CONFIG.copy()
    cfg.update(config)
    http_client = HttpClient(
        sdk_url=sdk_url,
        events_url=events_url,
        timeout=cfg.get('connectionTimeout')
    )

    sdk_metadata = util.get_metadata(config)
    apis = {
        'splits': SplitsAPI(http_client, api_key),
        'segments': SegmentsAPI(http_client, api_key),
        'impressions': ImpressionsAPI(http_client, api_key, sdk_metadata),
        'events': EventsAPI(http_client, api_key, sdk_metadata),
        'telemetry': TelemetryAPI(http_client, api_key, sdk_metadata)
    }

    if not input_validator.validate_apikey_type(apis['segments']):
        return None

    storages = {
        'splits': InMemorySplitStorage(),
        'segments': InMemorySegmentStorage(),
        'impressions': InMemoryImpressionStorage(cfg['impressionsQueueSize']),
        'events': InMemoryEventStorage(cfg['eventsQueueSize']),
        'telemetry': InMemoryTelemetryStorage()
    }

    # Synchronization flags
    splits_ready_flag = threading.Event()
    segments_ready_flag = threading.Event()
    sdk_ready_flag = threading.Event()

    tasks = {
        'splits': SplitSynchronizationTask(
            apis['splits'],
            storages['splits'],
            cfg['featuresRefreshRate'],
            splits_ready_flag
        ),

        'segments': SegmentSynchronizationTask(
            apis['segments'],
            storages['segments'],
            storages['splits'],
            cfg['segmentsRefreshRate'],
            segments_ready_flag
        ),

        'impressions': ImpressionsSyncTask(
            apis['impressions'],
            storages['impressions'],
            cfg['impressionsRefreshRate'],
            cfg['impressionsBulkSize']
        ),

        'events': EventsSyncTask(
            apis['events'],
            storages['events'],
            cfg['eventsPushRate'],
            cfg['eventsBulkSize'],
        ),

        'telemetry': TelemetrySynchronizationTask(
            apis['telemetry'],
            storages['telemetry'],
            cfg['metricsRefreshRate']
        )
    }

    # Start tasks that have no dependencies
    tasks['splits'].start()
    tasks['impressions'].start()
    tasks['events'].start()
    tasks['telemetry'].start()

    def split_ready_task():
        """Wait for splits to be ready and start fetching segments."""
        splits_ready_flag.wait()
        tasks['segments'].start()

    def segment_ready_task():
        """Wait for segments to be ready and set the main ready flag."""
        segments_ready_flag.wait()
        sdk_ready_flag.set()

    split_completion_thread = threading.Thread(target=split_ready_task)
    split_completion_thread.setDaemon(True)
    split_completion_thread.start()
    segment_completion_thread = threading.Thread(target=segment_ready_task)
    segment_completion_thread.setDaemon(True)
    segment_completion_thread.start()
    return SplitFactory(
        storages,
        cfg['labelsEnabled'],
        apis,
        tasks,
        sdk_ready_flag,
        impression_listener=_wrap_impression_listener(cfg['impressionListener'], sdk_metadata)
    )


def _build_redis_factory(config):
    """Build and return a split factory with redis-based storage."""
    cfg = DEFAULT_CONFIG.copy()
    cfg.update(config)
    sdk_metadata = util.get_metadata(config)
    redis_adapter = redis.build(config)
    storages = {
        'splits': RedisSplitStorage(redis_adapter),
        'segments': RedisSegmentStorage(redis_adapter),
        'impressions': RedisImpressionsStorage(redis_adapter, sdk_metadata),
        'events': RedisEventsStorage(redis_adapter, sdk_metadata),
        'telemetry': RedisTelemetryStorage(redis_adapter, sdk_metadata)
    }
    return SplitFactory(
        storages,
        cfg['labelsEnabled'],
        impression_listener=_wrap_impression_listener(cfg['impressionListener'], sdk_metadata)
    )


def _build_uwsgi_factory(config):
    """Build and return a split factory with redis-based storage."""
    cfg = DEFAULT_CONFIG.copy()
    cfg.update(config)
    sdk_metadata = util.get_metadata(cfg)
    uwsgi_adapter = get_uwsgi()
    storages = {
        'splits': UWSGISplitStorage(uwsgi_adapter),
        'segments': UWSGISegmentStorage(uwsgi_adapter),
        'impressions': UWSGIImpressionStorage(uwsgi_adapter),
        'events': UWSGIEventStorage(uwsgi_adapter),
        'telemetry': UWSGITelemetryStorage(uwsgi_adapter)
    }
    return SplitFactory(
        storages,
        cfg['labelsEnabled'],
        impression_listener=_wrap_impression_listener(cfg['impressionListener'], sdk_metadata)
    )


def _build_localhost_factory(config):
    """Build and return a localhost factory for testing/development purposes."""
    cfg = DEFAULT_CONFIG.copy()
    cfg.update(config)
    storages = {
        'splits': InMemorySplitStorage(),
        'segments': InMemorySegmentStorage(),  # not used, just to avoid possible future errors.
        'impressions': LocalhostImpressionsStorage(),
        'events': LocalhostEventsStorage(),
        'telemetry': LocalhostTelemetryStorage()
    }

    ready_event = threading.Event()
    tasks = {'splits': LocalhostSplitSynchronizationTask(
        cfg['splitFile'],
        storages['splits'],
        cfg['featuresRefreshRate'],
        ready_event
    )}
    tasks['splits'].start()
    return SplitFactory(storages, False, None, tasks, ready_event)


def get_factory(api_key, **kwargs):
    """Build and return the appropriate factory."""
    config = kwargs.get('config', {})

    if api_key == 'localhost':
        return _build_localhost_factory(config)

    if 'redisHost' in config or 'redisSentinels' in config:
        return _build_redis_factory(config)

    if 'uwsgiClient' in config:
        return _build_uwsgi_factory(config)

    return _build_in_memory_factory(
        api_key,
        config,
        kwargs.get('sdk_api_base_url'),
        kwargs.get('events_api_base_url')
    )
