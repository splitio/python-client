"""Split factory test module."""
# pylint: disable=no-self-use,protected-access,line-too-long,too-many-statements
# pylint: disable=too-many-locals, too-many-arguments

import os
import time
import threading
import pytest
from splitio.optional.loaders import asyncio
from splitio.client.factory import get_factory, get_factory_async, SplitFactory, _INSTANTIATED_FACTORIES, Status,\
    _LOGGER as _logger, SplitFactoryAsync
from splitio.client.config import DEFAULT_CONFIG
from splitio.storage import redis, inmemmory, pluggable
from splitio.tasks.util import asynctask
from splitio.engine.impressions.impressions import Manager as ImpressionsManager
from splitio.sync.manager import Manager, ManagerAsync
from splitio.sync.synchronizer import Synchronizer, SynchronizerAsync, SplitSynchronizers, SplitTasks
from splitio.sync.split import SplitSynchronizer, SplitSynchronizerAsync
from splitio.sync.segment import SegmentSynchronizer, SegmentSynchronizerAsync
from splitio.recorder.recorder import PipelinedRecorder, StandardRecorder, StandardRecorderAsync
from splitio.storage.adapters.redis import RedisAdapter, RedisPipelineAdapter
from tests.storage.test_pluggable import StorageMockAdapter, StorageMockAdapterAsync


class SplitFactoryTests(object):
    """Split factory test cases."""

    def test_inmemory_client_creation_streaming_false(self, mocker):
        """Test that a client with in-memory storage is created correctly."""

        # Setup synchronizer
        def _split_synchronizer(self, ready_flag, some, auth_api, streaming_enabled, sdk_matadata, telemetry_runtime_producer, sse_url=None, client_key=None):
            synchronizer = mocker.Mock(spec=Synchronizer)
            synchronizer.sync_all.return_values = None
            self._ready_flag = ready_flag
            self._synchronizer = synchronizer
            self._streaming_enabled = False
            self._telemetry_runtime_producer = telemetry_runtime_producer

        mocker.patch('splitio.sync.manager.Manager.__init__', new=_split_synchronizer)

        # Start factory and make assertions
        factory = get_factory('some_api_key')
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        assert isinstance(factory._storages['splits'], inmemmory.InMemorySplitStorage)
        assert isinstance(factory._storages['segments'], inmemmory.InMemorySegmentStorage)
        assert isinstance(factory._storages['impressions'], inmemmory.InMemoryImpressionStorage)
        assert factory._storages['impressions']._impressions.maxsize == 10000
        assert isinstance(factory._storages['events'], inmemmory.InMemoryEventStorage)
        assert factory._storages['events']._events.maxsize == 10000

        assert isinstance(factory._sync_manager, Manager)
        assert isinstance(factory._recorder, StandardRecorder)
        assert isinstance(factory._recorder._impressions_manager, ImpressionsManager)
        assert isinstance(factory._recorder._event_sotrage, inmemmory.EventStorage)
        assert isinstance(factory._recorder._impression_storage, inmemmory.ImpressionStorage)

        assert factory._labels_enabled is True
        try:
            factory.block_until_ready(1)
        except:
            pass
        assert factory.ready
        factory.destroy()

    def test_redis_client_creation(self, mocker):
        """Test that a client with redis storage is created correctly."""
        strict_redis_mock = mocker.Mock()
        mocker.patch('splitio.storage.adapters.redis.StrictRedis', new=strict_redis_mock)

        config = {
            'labelsEnabled': False,
            'impressionListener': 123,
            'redisHost': 'some_host',
            'redisPort': 1234,
            'redisDb': 1,
            'redisPassword': 'some_password',
            'redisSocketTimeout': 123,
            'redisSocketConnectTimeout': 123,
            'redisSocketKeepalive': 123,
            'redisSocketKeepaliveOptions': False,
            'redisConnectionPool': False,
            'redisUnixSocketPath': '/some_path',
            'redisEncodingErrors': 'non-strict',
            'redisErrors': True,
            'redisDecodeResponses': True,
            'redisRetryOnTimeout': True,
            'redisSsl': True,
            'redisSslKeyfile': 'some_file',
            'redisSslCertfile': 'some_cert_file',
            'redisSslCertReqs': 'some_cert_req',
            'redisSslCaCerts': 'some_ca_cert',
            'redisMaxConnections': 999,
        }
        factory = get_factory('some_api_key', config=config)
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        assert isinstance(factory._get_storage('splits'), redis.RedisSplitStorage)
        assert isinstance(factory._get_storage('segments'), redis.RedisSegmentStorage)
        assert isinstance(factory._get_storage('impressions'), redis.RedisImpressionsStorage)
        assert isinstance(factory._get_storage('events'), redis.RedisEventsStorage)

        adapter = factory._get_storage('splits')._redis
        assert adapter == factory._get_storage('segments')._redis
        assert adapter == factory._get_storage('impressions')._redis
        assert adapter == factory._get_storage('events')._redis

        assert strict_redis_mock.mock_calls[0] == mocker.call(
            host='some_host',
            port=1234,
            db=1,
            password='some_password',
            socket_timeout=123,
            socket_connect_timeout=123,
            socket_keepalive=123,
            socket_keepalive_options=False,
            connection_pool=False,
            unix_socket_path='/some_path',
            encoding='utf-8',
            encoding_errors='non-strict',
            errors=True,
            decode_responses=True,
            retry_on_timeout=True,
            ssl=True,
            ssl_keyfile='some_file',
            ssl_certfile='some_cert_file',
            ssl_cert_reqs='some_cert_req',
            ssl_ca_certs='some_ca_cert',
            max_connections=999,
        )
        assert factory._labels_enabled is False
        assert isinstance(factory._recorder, PipelinedRecorder)
        assert isinstance(factory._recorder._impressions_manager, ImpressionsManager)
        assert isinstance(factory._recorder._make_pipe(), RedisPipelineAdapter)
        assert isinstance(factory._recorder._event_sotrage, redis.RedisEventsStorage)
        assert isinstance(factory._recorder._impression_storage, redis.RedisImpressionsStorage)

        try:
            factory.block_until_ready(1)
        except:
            pass
        assert factory.ready
        factory.destroy()

    def test_destroy(self, mocker):
        """Test that tasks are shutdown and data is flushed when destroy is called."""

        def stop_mock():
            return

        split_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        split_async_task_mock.stop.side_effect = stop_mock

        def _split_task_init_mock(self, synchronize_splits, period):
            self._task = split_async_task_mock
            self._period = period
        mocker.patch('splitio.client.factory.SplitSynchronizationTask.__init__',
                     new=_split_task_init_mock)

        segment_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        segment_async_task_mock.stop.side_effect = stop_mock

        def _segment_task_init_mock(self, synchronize_segments, period):
            self._task = segment_async_task_mock
            self._period = period
        mocker.patch('splitio.client.factory.SegmentSynchronizationTask.__init__',
                     new=_segment_task_init_mock)

        imp_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        imp_async_task_mock.stop.side_effect = stop_mock

        def _imppression_task_init_mock(self, synchronize_impressions, period):
            self._period = period
            self._task = imp_async_task_mock
        mocker.patch('splitio.client.factory.ImpressionsSyncTask.__init__',
                     new=_imppression_task_init_mock)

        evt_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        evt_async_task_mock.stop.side_effect = stop_mock

        def _event_task_init_mock(self, synchronize_events, period):
            self._period = period
            self._task = evt_async_task_mock
        mocker.patch('splitio.client.factory.EventsSyncTask.__init__', new=_event_task_init_mock)

        imp_count_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        imp_count_async_task_mock.stop.side_effect = stop_mock

        def _imppression_count_task_init_mock(self, synchronize_counters):
            self._task = imp_count_async_task_mock
        mocker.patch('splitio.client.factory.ImpressionsCountSyncTask.__init__',
                     new=_imppression_count_task_init_mock)

        telemetry_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        telemetry_async_task_mock.stop.side_effect = stop_mock

        def _telemetry_task_init_mock(self, synchronize_telemetry, synchronize_telemetry2):
            self._task = telemetry_async_task_mock
        mocker.patch('splitio.client.factory.TelemetrySyncTask.__init__',
                     new=_telemetry_task_init_mock)

        split_sync = mocker.Mock(spec=SplitSynchronizer)
        split_sync.synchronize_splits.return_value = []
        segment_sync = mocker.Mock(spec=SegmentSynchronizer)
        segment_sync.synchronize_segments.return_values = None
        syncs = SplitSynchronizers(split_sync, segment_sync, mocker.Mock(),
                                   mocker.Mock(), mocker.Mock(), mocker.Mock())
        tasks = SplitTasks(split_async_task_mock, segment_async_task_mock, imp_async_task_mock,
                           evt_async_task_mock, imp_count_async_task_mock, telemetry_async_task_mock)

        # Setup synchronizer
        def _split_synchronizer(self, ready_flag, some, auth_api, streaming_enabled, sdk_matadata, telemetry_runtime_producer, sse_url=None, client_key=None):
            synchronizer = Synchronizer(syncs, tasks)
            self._ready_flag = ready_flag
            self._synchronizer = synchronizer
            self._streaming_enabled = False
            self._telemetry_runtime_producer = telemetry_runtime_producer
        mocker.patch('splitio.sync.manager.Manager.__init__', new=_split_synchronizer)

        # Start factory and make assertions
        # Using invalid key should result in a timeout exception
        factory = get_factory('some_api_key')
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        try:
            factory.block_until_ready(1)
        except:
            pass
        assert factory.ready
        assert factory.destroyed is False

        factory.destroy()
        assert len(imp_async_task_mock.stop.mock_calls) == 1
        assert len(evt_async_task_mock.stop.mock_calls) == 1
        assert len(imp_count_async_task_mock.stop.mock_calls) == 1
        assert factory.destroyed is True

    def test_destroy_with_event(self, mocker):
        """Test that tasks are shutdown and data is flushed when destroy is called."""

        def stop_mock(event):
            time.sleep(0.1)
            event.set()
            return

        def stop_mock_2():
            return

        split_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        split_async_task_mock.stop.side_effect = stop_mock_2

        def _split_task_init_mock(self, synchronize_splits, period):
            self._task = split_async_task_mock
            self._period = period
        mocker.patch('splitio.client.factory.SplitSynchronizationTask.__init__',
                     new=_split_task_init_mock)

        segment_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        segment_async_task_mock.stop.side_effect = stop_mock_2

        def _segment_task_init_mock(self, synchronize_segments, period):
            self._task = segment_async_task_mock
            self._period = period
        mocker.patch('splitio.client.factory.SegmentSynchronizationTask.__init__',
                     new=_segment_task_init_mock)

        imp_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        imp_async_task_mock.stop.side_effect = stop_mock

        def _imppression_task_init_mock(self, synchronize_impressions, period):
            self._period = period
            self._task = imp_async_task_mock
        mocker.patch('splitio.client.factory.ImpressionsSyncTask.__init__',
                     new=_imppression_task_init_mock)

        evt_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        evt_async_task_mock.stop.side_effect = stop_mock

        def _event_task_init_mock(self, synchronize_events, period):
            self._period = period
            self._task = evt_async_task_mock
        mocker.patch('splitio.client.factory.EventsSyncTask.__init__', new=_event_task_init_mock)

        imp_count_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        imp_count_async_task_mock.stop.side_effect = stop_mock

        def _imppression_count_task_init_mock(self, synchronize_counters):
            self._task = imp_count_async_task_mock
        mocker.patch('splitio.client.factory.ImpressionsCountSyncTask.__init__',
                     new=_imppression_count_task_init_mock)

        telemetry_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        telemetry_async_task_mock.stop.side_effect = stop_mock

        def _telemetry_task_init_mock(self, synchronize_telemetry, synchronize_telemetry2):
            self._task = telemetry_async_task_mock
        mocker.patch('splitio.client.factory.TelemetrySyncTask.__init__',
                     new=_telemetry_task_init_mock)

        split_sync = mocker.Mock(spec=SplitSynchronizer)
        split_sync.synchronize_splits.return_value = []
        segment_sync = mocker.Mock(spec=SegmentSynchronizer)
        segment_sync.synchronize_segments.return_values = None
        syncs = SplitSynchronizers(split_sync, segment_sync, mocker.Mock(),
                                   mocker.Mock(), mocker.Mock(), mocker.Mock())
        tasks = SplitTasks(split_async_task_mock, segment_async_task_mock, imp_async_task_mock,
                           evt_async_task_mock, imp_count_async_task_mock, telemetry_async_task_mock)

        # Setup synchronizer
        def _split_synchronizer(self, ready_flag, some, auth_api, streaming_enabled, sdk_matadata, telemetry_runtime_producer, sse_url=None, client_key=None):
            synchronizer = Synchronizer(syncs, tasks)
            self._ready_flag = ready_flag
            self._synchronizer = synchronizer
            self._streaming_enabled = False
            self._telemetry_runtime_producer = telemetry_runtime_producer
        mocker.patch('splitio.sync.manager.Manager.__init__', new=_split_synchronizer)

        # Start factory and make assertions
        factory = get_factory('some_api_key')
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        try:
            factory.block_until_ready(1)
        except:
            pass
#        pytest.set_trace()
        assert factory._status == Status.READY
        assert factory.destroyed is False

        event = threading.Event()
        factory.destroy(event)
        assert not event.is_set()
        time.sleep(1)
        assert event.is_set()
        assert len(imp_async_task_mock.stop.mock_calls) == 1
        assert len(evt_async_task_mock.stop.mock_calls) == 1
        assert len(imp_count_async_task_mock.stop.mock_calls) == 1
        assert factory.destroyed is True

    def test_destroy_with_event_redis(self, mocker):
        def _make_factory_with_apikey(apikey, *_, **__):
            return SplitFactory(apikey, {}, True, mocker.Mock(spec=ImpressionsManager), None, mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())

        factory_module_logger = mocker.Mock()
        build_redis = mocker.Mock()
        build_redis.side_effect = _make_factory_with_apikey
        mocker.patch('splitio.client.factory._LOGGER', new=factory_module_logger)
        mocker.patch('splitio.client.factory._build_redis_factory', new=build_redis)

        config = {
            'redisDb': 0,
            'redisHost': 'localhost',
            'redisPosrt': 6379,
        }

        factory = get_factory("none", config=config)
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        event = threading.Event()
        factory.destroy(event)
        event.wait()
        assert factory.destroyed
        assert len(build_redis.mock_calls) == 1

        factory = get_factory("none", config=config)
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        factory.destroy(None)
        time.sleep(0.1)
        assert factory.destroyed
        assert len(build_redis.mock_calls) == 2

    def test_multiple_factories(self, mocker):
        """Test multiple factories instantiation and tracking."""
        sdk_ready_flag = threading.Event()

        def _init(self, ready_flag, some, auth_api, streaming_enabled, telemetry_runtime_producer, telemetry_init_consumer, sse_url=None):
            self._ready_flag = ready_flag
            self._synchronizer = mocker.Mock(spec=Synchronizer)
            self._streaming_enabled = False
            self._telemetry_runtime_producer = telemetry_runtime_producer
            self._telemetry_init_consumer = telemetry_init_consumer
        mocker.patch('splitio.sync.manager.Manager.__init__', new=_init)

        def _start(self, *args, **kwargs):
            sdk_ready_flag.set()
        mocker.patch('splitio.sync.manager.Manager.start', new=_start)

        def _stop(self, *args, **kwargs):
            pass
        mocker.patch('splitio.sync.manager.Manager.stop', new=_stop)

        mockManager = Manager(sdk_ready_flag, mocker.Mock(), mocker.Mock(), False, mocker.Mock(), mocker.Mock())

        def _make_factory_with_apikey(apikey, *_, **__):
            return SplitFactory(apikey, {}, True, mocker.Mock(spec=ImpressionsManager), mockManager, mocker.Mock(), mocker.Mock(), mocker.Mock())

        factory_module_logger = mocker.Mock()
        build_in_memory = mocker.Mock()
        build_in_memory.side_effect = _make_factory_with_apikey
        build_redis = mocker.Mock()
        build_redis.side_effect = _make_factory_with_apikey
        build_localhost = mocker.Mock()
        build_localhost.side_effect = _make_factory_with_apikey
        mocker.patch('splitio.client.factory._LOGGER', new=factory_module_logger)
        mocker.patch('splitio.client.factory._build_in_memory_factory', new=build_in_memory)
        mocker.patch('splitio.client.factory._build_redis_factory', new=build_redis)
        mocker.patch('splitio.client.factory._build_localhost_factory', new=build_localhost)

        _INSTANTIATED_FACTORIES.clear()  # Clear all factory counters for testing purposes

        factory1 = get_factory('some_api_key')
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory1._telemetry_submitter = TelemetrySubmitterMock()

        assert _INSTANTIATED_FACTORIES['some_api_key'] == 1
        assert factory_module_logger.warning.mock_calls == []

        factory2 = get_factory('some_api_key')
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory2._telemetry_submitter = TelemetrySubmitterMock()

        assert _INSTANTIATED_FACTORIES['some_api_key'] == 2
        assert factory_module_logger.warning.mock_calls == [mocker.call(
            "factory instantiation: You already have %d %s with this SDK Key. "
            "We recommend keeping only one instance of the factory at all times "
            "(Singleton pattern) and reusing it throughout your application.",
            1,
            'factory'
        )]

        factory_module_logger.reset_mock()
        factory3 = get_factory('some_api_key')
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory3._telemetry_submitter = TelemetrySubmitterMock()

        assert _INSTANTIATED_FACTORIES['some_api_key'] == 3
        assert factory_module_logger.warning.mock_calls == [mocker.call(
            "factory instantiation: You already have %d %s with this SDK Key. "
            "We recommend keeping only one instance of the factory at all times "
            "(Singleton pattern) and reusing it throughout your application.",
            2,
            'factories'
        )]

        factory_module_logger.reset_mock()
        factory4 = get_factory('some_other_api_key')
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory4._telemetry_submitter = TelemetrySubmitterMock()

        assert _INSTANTIATED_FACTORIES['some_api_key'] == 3
        assert _INSTANTIATED_FACTORIES['some_other_api_key'] == 1
        assert factory_module_logger.warning.mock_calls == [mocker.call(
            "factory instantiation: You already have an instance of the Split factory. "
            "Make sure you definitely want this additional instance. "
            "We recommend keeping only one instance of the factory at all times "
            "(Singleton pattern) and reusing it throughout your application."
        )]

        event = threading.Event()
        factory1.destroy(event)
        event.wait()
        assert _INSTANTIATED_FACTORIES['some_other_api_key'] == 1
        assert _INSTANTIATED_FACTORIES['some_api_key'] == 2
        factory2.destroy()
        factory3.destroy()
        factory4.destroy()

    def test_uwsgi_preforked(self, mocker):
        """Test preforked initializations."""

        def clear_impressions():
            clear_impressions._called += 1

        def clear_events():
            clear_events._called += 1

        clear_impressions._called = 0
        clear_events._called = 0
        split_storage = mocker.Mock(spec=inmemmory.SplitStorage)
        segment_storage = mocker.Mock(spec=inmemmory.SegmentStorage)
        impression_storage = mocker.Mock(spec=inmemmory.ImpressionStorage)
        impression_storage.clear.side_effect = clear_impressions
        event_storage = mocker.Mock(spec=inmemmory.EventStorage)
        event_storage.clear.side_effect = clear_events

        def _get_storage_mock(self, name):
            return {
                'splits': split_storage,
                'segments': segment_storage,
                'impressions': impression_storage,
                'events': event_storage,
            }[name]

        mocker.patch('splitio.client.factory.SplitFactory._get_storage', new=_get_storage_mock)

        sync_all_mock = mocker.Mock()
        mocker.patch('splitio.sync.synchronizer.Synchronizer.sync_all', new=sync_all_mock)

        start_mock = mocker.Mock()
        mocker.patch('splitio.sync.manager.Manager.start', new=start_mock)

        recreate_mock = mocker.Mock()
        mocker.patch('splitio.sync.manager.Manager.recreate', new=recreate_mock)

        config = {
            'preforkedInitialization': True,
        }
        factory = get_factory("none", config=config)
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        try:
            factory.block_until_ready(10)
        except:
            pass
        assert factory._status == Status.WAITING_FORK
        assert len(sync_all_mock.mock_calls) == 1
        assert len(start_mock.mock_calls) == 0

        factory.resume()
        assert len(recreate_mock.mock_calls) == 1
        assert len(start_mock.mock_calls) == 1

        assert clear_impressions._called == 1
        assert clear_events._called == 1
        factory.destroy()

    def test_error_prefork(self, mocker):
        """Test not handling fork."""
        expected_msg = [
            mocker.call('Cannot call resume')
        ]

        filename = os.path.join(os.path.dirname(__file__), '../integration/files', 'file2.yaml')
        factory = get_factory('localhost', config={'splitFile': filename})
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        try:
            factory.block_until_ready(1)
        except:
            pass
        _logger = mocker.Mock()
        mocker.patch('splitio.client.factory._LOGGER', new=_logger)
        factory.resume()
        assert _logger.warning.mock_calls == expected_msg
        factory.destroy()

    def test_pluggable_client_creation(self, mocker):
        """Test that a client with pluggable storage is created correctly."""
        config = {
            'labelsEnabled': False,
            'impressionListener': 123,
            'storageType': 'pluggable',
            'storageWrapper': StorageMockAdapter()
        }
        factory = get_factory('some_api_key', config=config)
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        assert isinstance(factory._get_storage('splits'), pluggable.PluggableSplitStorage)
        assert isinstance(factory._get_storage('segments'), pluggable.PluggableSegmentStorage)
        assert isinstance(factory._get_storage('impressions'), pluggable.PluggableImpressionsStorage)
        assert isinstance(factory._get_storage('events'), pluggable.PluggableEventsStorage)

        adapter = factory._get_storage('splits')._pluggable_adapter
        assert adapter == factory._get_storage('segments')._pluggable_adapter
        assert adapter == factory._get_storage('impressions')._pluggable_adapter
        assert adapter == factory._get_storage('events')._pluggable_adapter

        assert factory._labels_enabled is False
        assert isinstance(factory._recorder, StandardRecorder)
        assert isinstance(factory._recorder._impressions_manager, ImpressionsManager)
        assert isinstance(factory._recorder._event_sotrage, pluggable.PluggableEventsStorage)
        assert isinstance(factory._recorder._impression_storage, pluggable.PluggableImpressionsStorage)

        try:
            factory.block_until_ready(1)
        except:
            pass
        assert factory.ready
        factory.destroy()

    def test_destroy_with_event_pluggable(self, mocker):
        config = {
            'labelsEnabled': False,
            'impressionListener': 123,
            'storageType': 'pluggable',
            'storageWrapper': StorageMockAdapter()
        }

        factory = get_factory("none", config=config)
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        event = threading.Event()
        factory.destroy(event)
        event.wait()
        assert factory.destroyed

        factory = get_factory("none", config=config)
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        factory.destroy(None)
        time.sleep(0.1)
        assert factory.destroyed

    def test_uwsgi_forked_client_creation(self):
        """Test client with preforked initialization."""
        # Invalid API Key with preforked should exit after 3 attempts.
        factory = get_factory('some_api_key', config={'preforkedInitialization': True})
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        assert isinstance(factory._storages['splits'], inmemmory.InMemorySplitStorage)
        assert isinstance(factory._storages['segments'], inmemmory.InMemorySegmentStorage)
        assert isinstance(factory._storages['impressions'], inmemmory.InMemoryImpressionStorage)
        assert factory._storages['impressions']._impressions.maxsize == 10000
        assert isinstance(factory._storages['events'], inmemmory.InMemoryEventStorage)
        assert factory._storages['events']._events.maxsize == 10000

        assert isinstance(factory._sync_manager, Manager)

        assert isinstance(factory._recorder, StandardRecorder)
        assert isinstance(factory._recorder._impressions_manager, ImpressionsManager)
        assert isinstance(factory._recorder._event_sotrage, inmemmory.EventStorage)
        assert isinstance(factory._recorder._impression_storage, inmemmory.ImpressionStorage)

        assert factory._status == Status.WAITING_FORK
        factory.destroy()


class SplitFactoryAsyncTests(object):
    """Split factory async test cases."""

    @pytest.mark.asyncio
    async def test_inmemory_client_creation_streaming_false_async(self, mocker):
        """Test that a client with in-memory storage is created correctly for async."""

        # Setup synchronizer
        def _split_synchronizer(self, ready_flag, some, auth_api, streaming_enabled, sdk_matadata, telemetry_runtime_producer, sse_url=None, client_key=None):
            synchronizer = mocker.Mock(spec=SynchronizerAsync)
            async def sync_all(*_):
                return None
            synchronizer.sync_all = sync_all
            self._ready_flag = ready_flag
            self._synchronizer = synchronizer
            self._streaming_enabled = False
            self._telemetry_runtime_producer = telemetry_runtime_producer
        mocker.patch('splitio.sync.manager.ManagerAsync.__init__', new=_split_synchronizer)

        async def synchronize_config(*_):
            pass
        mocker.patch('splitio.sync.telemetry.InMemoryTelemetrySubmitterAsync.synchronize_config', new=synchronize_config)

        # Start factory and make assertions
        factory = await get_factory_async('some_api_key')
        assert isinstance(factory, SplitFactoryAsync)
        assert isinstance(factory._storages['splits'], inmemmory.InMemorySplitStorageAsync)
        assert isinstance(factory._storages['segments'], inmemmory.InMemorySegmentStorageAsync)
        assert isinstance(factory._storages['impressions'], inmemmory.InMemoryImpressionStorageAsync)
        assert factory._storages['impressions']._impressions.maxsize == 10000
        assert isinstance(factory._storages['events'], inmemmory.InMemoryEventStorageAsync)
        assert factory._storages['events']._events.maxsize == 10000

        assert isinstance(factory._sync_manager, ManagerAsync)

        assert isinstance(factory._recorder, StandardRecorderAsync)
        assert isinstance(factory._recorder._impressions_manager, ImpressionsManager)
        assert isinstance(factory._recorder._event_sotrage, inmemmory.EventStorage)
        assert isinstance(factory._recorder._impression_storage, inmemmory.ImpressionStorage)

        assert factory._labels_enabled is True
        try:
            await factory.block_until_ready(1)
        except:
            pass
        assert factory.ready
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_destroy_async(self, mocker):
        """Test that tasks are shutdown and data is flushed when destroy is called."""

        async def stop_mock():
            return

        split_async_task_mock = mocker.Mock(spec=asynctask.AsyncTaskAsync)
        split_async_task_mock.stop.side_effect = stop_mock

        def _split_task_init_mock(self, synchronize_splits, period):
            self._task = split_async_task_mock
            self._period = period
        mocker.patch('splitio.client.factory.SplitSynchronizationTaskAsync.__init__',
                     new=_split_task_init_mock)

        segment_async_task_mock = mocker.Mock(spec=asynctask.AsyncTaskAsync)
        segment_async_task_mock.stop.side_effect = stop_mock

        def _segment_task_init_mock(self, synchronize_segments, period):
            self._task = segment_async_task_mock
            self._period = period
        mocker.patch('splitio.client.factory.SegmentSynchronizationTaskAsync.__init__',
                     new=_segment_task_init_mock)

        imp_async_task_mock = mocker.Mock(spec=asynctask.AsyncTaskAsync)
        imp_async_task_mock.stop.side_effect = stop_mock

        def _imppression_task_init_mock(self, synchronize_impressions, period):
            self._period = period
            self._task = imp_async_task_mock
        mocker.patch('splitio.client.factory.ImpressionsSyncTaskAsync.__init__',
                     new=_imppression_task_init_mock)

        evt_async_task_mock = mocker.Mock(spec=asynctask.AsyncTaskAsync)
        evt_async_task_mock.stop.side_effect = stop_mock

        def _event_task_init_mock(self, synchronize_events, period):
            self._period = period
            self._task = evt_async_task_mock
        mocker.patch('splitio.client.factory.EventsSyncTaskAsync.__init__', new=_event_task_init_mock)

        imp_count_async_task_mock = mocker.Mock(spec=asynctask.AsyncTaskAsync)
        imp_count_async_task_mock.stop.side_effect = stop_mock

        def _imppression_count_task_init_mock(self, synchronize_counters):
            self._task = imp_count_async_task_mock
        mocker.patch('splitio.client.factory.ImpressionsCountSyncTaskAsync.__init__',
                     new=_imppression_count_task_init_mock)

        telemetry_async_task_mock = mocker.Mock(spec=asynctask.AsyncTaskAsync)
        telemetry_async_task_mock.stop.side_effect = stop_mock

        def _telemetry_task_init_mock(self, synchronize_telemetry, synchronize_telemetry2):
            self._task = telemetry_async_task_mock
        mocker.patch('splitio.client.factory.TelemetrySyncTaskAsync.__init__',
                     new=_telemetry_task_init_mock)

        split_sync = mocker.Mock(spec=SplitSynchronizerAsync)
        async def synchronize_splits(*_):
            return []
        split_sync.synchronize_splits = synchronize_splits

        segment_sync = mocker.Mock(spec=SegmentSynchronizerAsync)
        async def synchronize_segments(*_):
            return True
        segment_sync.synchronize_segments = synchronize_segments

        syncs = SplitSynchronizers(split_sync, segment_sync, mocker.Mock(),
                                   mocker.Mock(), mocker.Mock(), mocker.Mock())
        tasks = SplitTasks(split_async_task_mock, segment_async_task_mock, imp_async_task_mock,
                           evt_async_task_mock, imp_count_async_task_mock, telemetry_async_task_mock)

        # Setup synchronizer
        def _split_synchronizer(self, ready_flag, some, auth_api, streaming_enabled, sdk_matadata, telemetry_runtime_producer, sse_url=None, client_key=None):
            synchronizer = SynchronizerAsync(syncs, tasks)
            self._ready_flag = ready_flag
            self._synchronizer = synchronizer
            self._streaming_enabled = False
            self._telemetry_runtime_producer = telemetry_runtime_producer
        mocker.patch('splitio.sync.manager.ManagerAsync.__init__', new=_split_synchronizer)

        async def synchronize_config(*_):
            pass
        mocker.patch('splitio.sync.telemetry.InMemoryTelemetrySubmitterAsync.synchronize_config', new=synchronize_config)
        # Start factory and make assertions
        # Using invalid key should result in a timeout exception
        factory = await get_factory_async('some_api_key')
        self.manager_called = False
        async def stop(*_):
            self.manager_called = True
            pass
        factory._sync_manager.stop = stop

        try:
            await factory.block_until_ready(1)
        except:
            pass
        assert factory.ready
        assert factory.destroyed is False

        await factory.destroy()
        assert self.manager_called
        assert factory.destroyed is True

    @pytest.mark.asyncio
    async def test_pluggable_client_creation_async(self, mocker):
        """Test that a client with pluggable storage is created correctly."""
        config = {
            'labelsEnabled': False,
            'impressionListener': 123,
            'featuresRefreshRate': 1,
            'segmentsRefreshRate': 1,
            'metricsRefreshRate': 1,
            'impressionsRefreshRate': 1,
            'eventsPushRate': 1,
            'storageType': 'pluggable',
            'storageWrapper': StorageMockAdapterAsync()
        }
        factory = await get_factory_async('some_api_key', config=config)
        assert isinstance(factory._get_storage('splits'), pluggable.PluggableSplitStorageAsync)
        assert isinstance(factory._get_storage('segments'), pluggable.PluggableSegmentStorageAsync)
        assert isinstance(factory._get_storage('impressions'), pluggable.PluggableImpressionsStorageAsync)
        assert isinstance(factory._get_storage('events'), pluggable.PluggableEventsStorageAsync)

        adapter = factory._get_storage('splits')._pluggable_adapter
        assert adapter == factory._get_storage('segments')._pluggable_adapter
        assert adapter == factory._get_storage('impressions')._pluggable_adapter
        assert adapter == factory._get_storage('events')._pluggable_adapter

        assert factory._labels_enabled is False
        assert isinstance(factory._recorder, StandardRecorderAsync)
        assert isinstance(factory._recorder._impressions_manager, ImpressionsManager)
        assert isinstance(factory._recorder._event_sotrage, pluggable.PluggableEventsStorageAsync)
        assert isinstance(factory._recorder._impression_storage, pluggable.PluggableImpressionsStorageAsync)
        try:
            await factory.block_until_ready(1)
        except:
            pass
        assert factory.ready
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_destroy_redis_async(self, mocker):
        async def _make_factory_with_apikey(apikey, *_, **__):
            return SplitFactoryAsync(apikey, {}, True, mocker.Mock(spec=ImpressionsManager), None, mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())

        factory_module_logger = mocker.Mock()
        build_redis = mocker.Mock()
        build_redis.side_effect = _make_factory_with_apikey
        mocker.patch('splitio.client.factory._LOGGER', new=factory_module_logger)
        mocker.patch('splitio.client.factory._build_redis_factory_async', new=build_redis)

        config = {
            'redisDb': 0,
            'redisHost': 'localhost',
            'redisPosrt': 6379,
        }
        factory = await get_factory_async("none", config=config)
        await factory.destroy()
        assert factory.destroyed
        assert len(build_redis.mock_calls) == 1

        factory = await get_factory_async("none", config=config)
        await factory.destroy()
        await asyncio.sleep(0.1)
        assert factory.destroyed
        assert len(build_redis.mock_calls) == 2
