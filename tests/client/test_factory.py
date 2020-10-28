"""Split factory test module."""
# pylint: disable=no-self-use,protected-access,line-too-long,too-many-statements
# pylint: disable=too-many-locals, too-many-arguments

import time
import threading
from splitio.client.factory import get_factory, SplitFactory, _INSTANTIATED_FACTORIES
from splitio.client.config import DEFAULT_CONFIG
from splitio.storage import redis, inmemmory, uwsgi
from splitio.tasks import events_sync, impressions_sync, split_sync, segment_sync, telemetry_sync
from splitio.tasks.util import asynctask, workerpool
from splitio.api.splits import SplitsAPI
from splitio.api.segments import SegmentsAPI
from splitio.api.impressions import ImpressionsAPI
from splitio.api.events import EventsAPI
from splitio.api.telemetry import TelemetryAPI
from splitio.engine.impressions import Manager as ImpressionsManager
from splitio.sync.manager import Manager
from splitio.push.synchronizer import Synchronizer, SplitSynchronizers, SplitTasks
from splitio.synchronizers.split import SplitSynchronizer
from splitio.synchronizers.segment import SegmentSynchronizer


class SplitFactoryTests(object):
    """Split factory test cases."""

    def test_inmemory_client_creation_streaming_false(self, mocker):
        """Test that a client with in-memory storage is created correctly."""

        # Setup synchronizer
        def _split_synchronizer(self, ready_flag, synchronizer, auth_api, streaming_enabled, sse_url=None):
            synchronizer = mocker.Mock(spec=Synchronizer)
            synchronizer.sync_all.return_values = None
            self._ready_flag = ready_flag
            self._synchronizer = synchronizer
            self._streaming_enabled = False
        mocker.patch('splitio.sync.manager.Manager.__init__', new=_split_synchronizer)

        # Start factory and make assertions
        factory = get_factory('some_api_key')
        assert isinstance(factory._storages['splits'], inmemmory.InMemorySplitStorage)
        assert isinstance(factory._storages['segments'], inmemmory.InMemorySegmentStorage)
        assert isinstance(factory._storages['impressions'], inmemmory.InMemoryImpressionStorage)
        assert factory._storages['impressions']._impressions.maxsize == 10000
        assert isinstance(factory._storages['events'], inmemmory.InMemoryEventStorage)
        assert factory._storages['events']._events.maxsize == 10000
        assert isinstance(factory._storages['telemetry'], inmemmory.InMemoryTelemetryStorage)

        assert isinstance(factory._sync_manager, Manager)

        assert factory._labels_enabled is True
        factory.block_until_ready()
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
            'redisEncoding': 'ascii',
            'redisEncodingErrors': 'non-strict',
            'redisCharset': 'ascii',
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
        assert isinstance(factory._get_storage('splits'), redis.RedisSplitStorage)
        assert isinstance(factory._get_storage('segments'), redis.RedisSegmentStorage)
        assert isinstance(factory._get_storage('impressions'), redis.RedisImpressionsStorage)
        assert isinstance(factory._get_storage('events'), redis.RedisEventsStorage)
        assert isinstance(factory._get_storage('telemetry'), redis.RedisTelemetryStorage)

        assert factory._sync_manager is None

        adapter = factory._get_storage('splits')._redis
        assert adapter == factory._get_storage('segments')._redis
        assert adapter == factory._get_storage('impressions')._redis
        assert adapter == factory._get_storage('events')._redis
        assert adapter == factory._get_storage('telemetry')._redis

        assert strict_redis_mock.mock_calls == [mocker.call(
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
            encoding='ascii',
            encoding_errors='non-strict',
            charset='ascii',
            errors=True,
            decode_responses=True,
            retry_on_timeout=True,
            ssl=True,
            ssl_keyfile='some_file',
            ssl_certfile='some_cert_file',
            ssl_cert_reqs='some_cert_req',
            ssl_ca_certs='some_ca_cert',
            max_connections=999
        )]
        assert factory._labels_enabled is False
        assert isinstance(factory._impressions_manager, ImpressionsManager)
        factory.block_until_ready()
        assert factory.ready
        factory.destroy()

    def test_uwsgi_client_creation(self):
        """Test that a client with redis storage is created correctly."""
        factory = get_factory('some_api_key', config={'uwsgiClient': True})
        assert isinstance(factory._get_storage('splits'), uwsgi.UWSGISplitStorage)
        assert isinstance(factory._get_storage('segments'), uwsgi.UWSGISegmentStorage)
        assert isinstance(factory._get_storage('impressions'), uwsgi.UWSGIImpressionStorage)
        assert isinstance(factory._get_storage('events'), uwsgi.UWSGIEventStorage)
        assert isinstance(factory._get_storage('telemetry'), uwsgi.UWSGITelemetryStorage)
        assert factory._sync_manager is None
        assert factory._labels_enabled is True
        factory.block_until_ready()
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

        def _segment_task_init_mock(self, synchronize_segments, worker_pool, period):
            self._task = segment_async_task_mock
            self._worker_pool = mocker.Mock()
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

        telemetry_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        telemetry_async_task_mock.stop.side_effect = stop_mock

        def _telemetry_task_init_mock(self, synchronize_counters, period):
            self._period = period
            self._task = telemetry_async_task_mock
        mocker.patch('splitio.client.factory.ImpressionsCountSyncTask.__init__',
                     new=_telemetry_task_init_mock)

        imp_count_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        imp_count_async_task_mock.stop.side_effect = stop_mock

        def _imppression_count_task_init_mock(self, synchronize_counters):
            self._task = imp_count_async_task_mock
        mocker.patch('splitio.client.factory.ImpressionsCountSyncTask.__init__',
                     new=_imppression_count_task_init_mock)

        split_sync = mocker.Mock(spec=SplitSynchronizer)
        split_sync.synchronize_splits.return_values = None
        segment_sync = mocker.Mock(spec=SegmentSynchronizer)
        segment_sync.synchronize_segments.return_values = None
        syncs = SplitSynchronizers(split_sync, segment_sync, mocker.Mock(),
                                   mocker.Mock(), mocker.Mock(), mocker.Mock())
        tasks = SplitTasks(split_async_task_mock, segment_async_task_mock, imp_async_task_mock,
                           evt_async_task_mock, telemetry_async_task_mock,
                           imp_count_async_task_mock)

        # Setup synchronizer
        def _split_synchronizer(self, ready_flag, some, auth_api, streaming_enabled, sse_url=None):
            synchronizer = Synchronizer(syncs, tasks)
            self._ready_flag = ready_flag
            self._synchronizer = synchronizer
            self._streaming_enabled = False
        mocker.patch('splitio.sync.manager.Manager.__init__', new=_split_synchronizer)

        # Start factory and make assertions
        factory = get_factory('some_api_key')
        factory.block_until_ready()
        assert factory.ready
        assert factory.destroyed is False

        factory.destroy()
        assert len(imp_async_task_mock.stop.mock_calls) == 1
        assert len(evt_async_task_mock.stop.mock_calls) == 1
        assert len(telemetry_async_task_mock.stop.mock_calls) == 1
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

        def _segment_task_init_mock(self, synchronize_segments, worker_pool, period):
            self._task = segment_async_task_mock
            self._worker_pool = mocker.Mock()
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

        telemetry_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        telemetry_async_task_mock.stop.side_effect = stop_mock_2

        def _telemetry_task_init_mock(self, synchronize_counters, period):
            self._period = period
            self._task = telemetry_async_task_mock
        mocker.patch('splitio.client.factory.ImpressionsCountSyncTask.__init__',
                     new=_telemetry_task_init_mock)

        imp_count_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        imp_count_async_task_mock.stop.side_effect = stop_mock

        def _imppression_count_task_init_mock(self, synchronize_counters):
            self._task = imp_count_async_task_mock
        mocker.patch('splitio.client.factory.ImpressionsCountSyncTask.__init__',
                     new=_imppression_count_task_init_mock)

        split_sync = mocker.Mock(spec=SplitSynchronizer)
        split_sync.synchronize_splits.return_values = None
        segment_sync = mocker.Mock(spec=SegmentSynchronizer)
        segment_sync.synchronize_segments.return_values = None
        syncs = SplitSynchronizers(split_sync, segment_sync, mocker.Mock(),
                                   mocker.Mock(), mocker.Mock(), mocker.Mock())
        tasks = SplitTasks(split_async_task_mock, segment_async_task_mock, imp_async_task_mock,
                           evt_async_task_mock, telemetry_async_task_mock,
                           imp_count_async_task_mock)

        # Setup synchronizer
        def _split_synchronizer(self, ready_flag, some, auth_api, streaming_enabled, sse_url=None):
            synchronizer = Synchronizer(syncs, tasks)
            self._ready_flag = ready_flag
            self._synchronizer = synchronizer
            self._streaming_enabled = False
        mocker.patch('splitio.sync.manager.Manager.__init__', new=_split_synchronizer)

        # Start factory and make assertions
        factory = get_factory('some_api_key')
        factory.block_until_ready()
        assert factory.ready
        assert factory.destroyed is False

        event = threading.Event()
        factory.destroy(event)
        assert not event.is_set()
        time.sleep(1)
        assert event.is_set()
        assert len(imp_async_task_mock.stop.mock_calls) == 1
        assert len(evt_async_task_mock.stop.mock_calls) == 1
        assert len(telemetry_async_task_mock.stop.mock_calls) == 1
        assert len(imp_count_async_task_mock.stop.mock_calls) == 1
        assert factory.destroyed is True

    def test_multiple_factories(self, mocker):
        """Test multiple factories instantiation and tracking."""
        sdk_ready_flag = threading.Event()

        mockManager = Manager

        def manager(self, ready_flag, some, auth_api, streaming_enabled, sse_url=None):
            self._ready_flag = ready_flag
            self._synchronizer = mocker.Mock(spec=Synchronizer)
            self._streaming_enabled = False
        mocker.patch.object(Manager, '__init__', new=manager)

        def start(self, *args, **kwargs):
            sdk_ready_flag.set()
        mocker.patch.object(Manager, 'start', new=start)

        def stop(self, *args, **kwargs):
            pass
        mocker.patch.object(Manager, 'stop', new=stop)

        def _make_factory_with_apikey(apikey, *_, **__):
            return SplitFactory(apikey, {}, True, mocker.Mock(spec=ImpressionsManager), mockManager)

        factory_module_logger = mocker.Mock()
        build_in_memory = mocker.Mock()
        build_in_memory.side_effect = _make_factory_with_apikey
        build_redis = mocker.Mock()
        build_redis.side_effect = _make_factory_with_apikey
        build_uwsgi = mocker.Mock()
        build_uwsgi.side_effect = _make_factory_with_apikey
        build_localhost = mocker.Mock()
        build_localhost.side_effect = _make_factory_with_apikey
        mocker.patch('splitio.client.factory._LOGGER', new=factory_module_logger)
        mocker.patch('splitio.client.factory._build_in_memory_factory', new=build_in_memory)
        mocker.patch('splitio.client.factory._build_redis_factory', new=build_redis)
        mocker.patch('splitio.client.factory._build_uwsgi_factory', new=build_uwsgi)
        mocker.patch('splitio.client.factory._build_localhost_factory', new=build_localhost)

        _INSTANTIATED_FACTORIES.clear()  # Clear all factory counters for testing purposes

        factory1 = get_factory('some_api_key')
        assert _INSTANTIATED_FACTORIES['some_api_key'] == 1
        assert factory_module_logger.warning.mock_calls == []

        factory2 = get_factory('some_api_key')
        assert _INSTANTIATED_FACTORIES['some_api_key'] == 2
        assert factory_module_logger.warning.mock_calls == [mocker.call(
            "factory instantiation: You already have %d %s with this API Key. "
            "We recommend keeping only one instance of the factory at all times "
            "(Singleton pattern) and reusing it throughout your application.",
            1,
            'factory'
        )]

        factory_module_logger.reset_mock()
        factory3 = get_factory('some_api_key')
        assert _INSTANTIATED_FACTORIES['some_api_key'] == 3
        assert factory_module_logger.warning.mock_calls == [mocker.call(
            "factory instantiation: You already have %d %s with this API Key. "
            "We recommend keeping only one instance of the factory at all times "
            "(Singleton pattern) and reusing it throughout your application.",
            2,
            'factories'
        )]

        factory_module_logger.reset_mock()
        factory4 = get_factory('some_other_api_key')
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
