"""Split factory test module."""
#pylint: disable=no-self-use,protected-access,line-too-long,too-many-statements
#pylint: disable=too-many-locals, too-many-arguments

import time
import threading
from splitio.client.listener import ImpressionListenerWrapper
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


class SplitFactoryTests(object):
    """Split factory test cases."""

    def test_inmemory_client_creation(self, mocker):
        """Test that a client with in-memory storage is created correctly."""
        # Setup task mocks
        def _split_task_init_mock(self, api, storage, period, event):
            self._task = mocker.Mock()
            self._api = api
            self._storage = storage
            self._period = period
            self._event = event
            event.set()
        mocker.patch('splitio.client.factory.SplitSynchronizationTask.__init__', new=_split_task_init_mock)
        def _segment_task_init_mock(self, api, storage, split_storage, period, event):
            self._task = mocker.Mock()
            self._worker_pool = mocker.Mock()
            self._api = api
            self._segment_storage = storage
            self._split_storage = split_storage
            self._period = period
            self._event = event
            event.set()
        mocker.patch('splitio.client.factory.SegmentSynchronizationTask.__init__', new=_segment_task_init_mock)

        # Start factory and make assertions
        factory = get_factory('some_api_key')
        assert isinstance(factory._storages['splits'], inmemmory.InMemorySplitStorage)
        assert isinstance(factory._storages['segments'], inmemmory.InMemorySegmentStorage)
        assert isinstance(factory._storages['impressions'], inmemmory.InMemoryImpressionStorage)
        assert factory._storages['impressions']._impressions.maxsize == 10000
        assert isinstance(factory._storages['events'], inmemmory.InMemoryEventStorage)
        assert factory._storages['events']._events.maxsize == 10000
        assert isinstance(factory._storages['telemetry'], inmemmory.InMemoryTelemetryStorage)

        assert isinstance(factory._apis['splits'], SplitsAPI)
        assert factory._apis['splits']._client._timeout == 1.5
        assert isinstance(factory._apis['segments'], SegmentsAPI)
        assert factory._apis['segments']._client._timeout == 1.5
        assert isinstance(factory._apis['impressions'], ImpressionsAPI)
        assert factory._apis['impressions']._client._timeout == 1.5
        assert isinstance(factory._apis['events'], EventsAPI)
        assert factory._apis['events']._client._timeout == 1.5
        assert isinstance(factory._apis['telemetry'], TelemetryAPI)
        assert factory._apis['telemetry']._client._timeout == 1.5

        assert isinstance(factory._tasks['splits'], split_sync.SplitSynchronizationTask)
        assert factory._tasks['splits']._period == DEFAULT_CONFIG['featuresRefreshRate']
        assert factory._tasks['splits']._storage == factory._storages['splits']
        assert factory._tasks['splits']._api == factory._apis['splits']
        assert isinstance(factory._tasks['segments'], segment_sync.SegmentSynchronizationTask)
        assert factory._tasks['segments']._period == DEFAULT_CONFIG['segmentsRefreshRate']
        assert factory._tasks['segments']._segment_storage == factory._storages['segments']
        assert factory._tasks['segments']._split_storage == factory._storages['splits']
        assert factory._tasks['segments']._api == factory._apis['segments']
        assert isinstance(factory._tasks['impressions'], impressions_sync.ImpressionsSyncTask)
        assert factory._tasks['impressions']._period == DEFAULT_CONFIG['impressionsRefreshRate']
        assert factory._tasks['impressions']._storage == factory._storages['impressions']
        assert factory._tasks['impressions']._impressions_api == factory._apis['impressions']
        assert isinstance(factory._tasks['events'], events_sync.EventsSyncTask)
        assert factory._tasks['events']._period == DEFAULT_CONFIG['eventsPushRate']
        assert factory._tasks['events']._storage == factory._storages['events']
        assert factory._tasks['events']._events_api == factory._apis['events']
        assert isinstance(factory._tasks['telemetry'], telemetry_sync.TelemetrySynchronizationTask)
        assert factory._tasks['telemetry']._period == DEFAULT_CONFIG['metricsRefreshRate']
        assert factory._tasks['telemetry']._storage == factory._storages['telemetry']
        assert factory._tasks['telemetry']._api == factory._apis['telemetry']
        assert factory._labels_enabled is True
        factory.block_until_ready()
        time.sleep(1) # give a chance for the bg thread to set the ready status
        assert factory.ready

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
            'redisErrors':True,
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

        assert factory._apis == {}
        assert factory._tasks == {}

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
        assert isinstance(factory._impression_listener, ImpressionListenerWrapper)
        factory.block_until_ready()
        time.sleep(1) # give a chance for the bg thread to set the ready status
        assert factory.ready


    def test_uwsgi_client_creation(self):
        """Test that a client with redis storage is created correctly."""
        factory = get_factory('some_api_key', config={'uwsgiClient': True})
        assert isinstance(factory._get_storage('splits'), uwsgi.UWSGISplitStorage)
        assert isinstance(factory._get_storage('segments'), uwsgi.UWSGISegmentStorage)
        assert isinstance(factory._get_storage('impressions'), uwsgi.UWSGIImpressionStorage)
        assert isinstance(factory._get_storage('events'), uwsgi.UWSGIEventStorage)
        assert isinstance(factory._get_storage('telemetry'), uwsgi.UWSGITelemetryStorage)
        assert factory._apis == {}
        assert factory._tasks == {}
        assert factory._labels_enabled is True
        assert factory._impression_listener is None
        factory.block_until_ready()
        time.sleep(1) # give a chance for the bg thread to set the ready status
        assert factory.ready

    def test_destroy(self, mocker):
        """Test that tasks are shutdown and data is flushed when destroy is called."""
        def _split_task_init_mock(self, api, storage, period, event):
            self._task = mocker.Mock()
            self._api = api
            self._storage = storage
            self._period = period
            self._event = event
            event.set()
        mocker.patch('splitio.client.factory.SplitSynchronizationTask.__init__', new=_split_task_init_mock)

        def _segment_task_init_mock(self, api, storage, split_storage, period, event):
            self._task = mocker.Mock()
            self._worker_pool = mocker.Mock()
            self._api = api
            self._segment_storage = storage
            self._split_storage = split_storage
            self._period = period
            self._event = event
            event.set()
        mocker.patch('splitio.client.factory.SegmentSynchronizationTask.__init__', new=_segment_task_init_mock)

        imp_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        def _imppression_task_init_mock(self, api, storage, refresh_rate, bulk_size):
            self._logger = mocker.Mock()
            self._impressions_api = api
            self._storage = storage
            self._period = refresh_rate
            self._task = imp_async_task_mock
            self._failed = mocker.Mock()
            self._bulk_size = bulk_size
        mocker.patch('splitio.client.factory.ImpressionsSyncTask.__init__', new=_imppression_task_init_mock)

        evt_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        def _event_task_init_mock(self, api, storage, refresh_rate, bulk_size):
            self._logger = mocker.Mock()
            self._impressions_api = api
            self._storage = storage
            self._period = refresh_rate
            self._task = evt_async_task_mock
            self._failed = mocker.Mock()
            self._bulk_size = bulk_size
        mocker.patch('splitio.client.factory.EventsSyncTask.__init__', new=_event_task_init_mock)

        # Start factory and make assertions
        factory = get_factory('some_api_key')
        factory.block_until_ready()
        time.sleep(1) # give a chance for the bg thread to set the ready status
        assert factory.ready
        assert factory.destroyed is False

        factory.destroy()
        assert imp_async_task_mock.stop.mock_calls == [mocker.call(None)]
        assert evt_async_task_mock.stop.mock_calls == [mocker.call(None)]
        assert factory.destroyed is True

    def test_destroy_with_event(self, mocker):
        """Test that tasks are shutdown and data is flushed when destroy is called."""
        spl_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        def _split_task_init_mock(self, api, storage, period, event):
            self._task = spl_async_task_mock
            self._api = api
            self._storage = storage
            self._period = period
            self._event = event
            event.set()
        mocker.patch('splitio.client.factory.SplitSynchronizationTask.__init__', new=_split_task_init_mock)

        sgm_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        worker_pool_mock = mocker.Mock(spec=workerpool.WorkerPool)
        def _segment_task_init_mock(self, api, storage, split_storage, period, event):
            self._task = sgm_async_task_mock
            self._worker_pool = worker_pool_mock
            self._api = api
            self._segment_storage = storage
            self._split_storage = split_storage
            self._period = period
            self._event = event
            event.set()
        mocker.patch('splitio.client.factory.SegmentSynchronizationTask.__init__', new=_segment_task_init_mock)

        imp_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        def _imppression_task_init_mock(self, api, storage, refresh_rate, bulk_size):
            self._logger = mocker.Mock()
            self._impressions_api = api
            self._storage = storage
            self._period = refresh_rate
            self._task = imp_async_task_mock
            self._failed = mocker.Mock()
            self._bulk_size = bulk_size
        mocker.patch('splitio.client.factory.ImpressionsSyncTask.__init__', new=_imppression_task_init_mock)

        evt_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        def _event_task_init_mock(self, api, storage, refresh_rate, bulk_size):
            self._logger = mocker.Mock()
            self._impressions_api = api
            self._storage = storage
            self._period = refresh_rate
            self._task = evt_async_task_mock
            self._failed = mocker.Mock()
            self._bulk_size = bulk_size
        mocker.patch('splitio.client.factory.EventsSyncTask.__init__', new=_event_task_init_mock)

        tmt_async_task_mock = mocker.Mock(spec=asynctask.AsyncTask)
        def _telemetry_task_init_mock(self, api, storage, refresh_rate):
            self._task = tmt_async_task_mock
            self._logger = mocker.Mock()
            self._api = api
            self._storage = storage
            self._period = refresh_rate
        mocker.patch('splitio.client.factory.TelemetrySynchronizationTask.__init__', new=_telemetry_task_init_mock)

        # Start factory and make assertions
        factory = get_factory('some_api_key')
        assert factory.destroyed is False

        factory.block_until_ready()
        time.sleep(1) # give a chance for the bg thread to set the ready status
        assert factory.ready

        event = threading.Event()
        factory.destroy(event)

        # When destroy is called an event is created and passed to each task when
        # stop() is called. We will extract those events assert their type, and assert that
        # by setting them, the main event gets set.
        splits_event = spl_async_task_mock.stop.mock_calls[0][1][0]
        segments_event = worker_pool_mock.stop.mock_calls[0][1][0]  # Segment task stops when wp finishes.
        impressions_event = imp_async_task_mock.stop.mock_calls[0][1][0]
        events_event = evt_async_task_mock.stop.mock_calls[0][1][0]
        telemetry_event = tmt_async_task_mock.stop.mock_calls[0][1][0]

        # python2 & 3 compatibility
        try:
            from threading import _Event as __EVENT_CLASS
        except ImportError:
            from threading import Event as __EVENT_CLASS

        assert isinstance(splits_event, __EVENT_CLASS)
        assert isinstance(segments_event, __EVENT_CLASS)
        assert isinstance(impressions_event, __EVENT_CLASS)
        assert isinstance(events_event, __EVENT_CLASS)
        assert isinstance(telemetry_event, __EVENT_CLASS)
        assert not event.is_set()

        splits_event.set()
        segments_event.set()
        impressions_event.set()
        events_event.set()
        telemetry_event.set()

        time.sleep(1)   # I/O wait to trigger context switch, to give the waiting thread
                        # a chance to run and set the main event.

        assert event.is_set()
        assert factory.destroyed

    def test_multiple_factories(self, mocker):
        """Test multiple factories instantiation and tracking."""
        def _make_factory_with_apikey(apikey, *_, **__):
            return SplitFactory(apikey, {}, True)

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

        get_factory('some_api_key')
        assert _INSTANTIATED_FACTORIES['some_api_key'] == 2
        assert factory_module_logger.warning.mock_calls == [mocker.call(
            "factory instantiation: You already have %d factories with this API Key. "
            "We recommend keeping only one instance of the factory at all times "
            "(Singleton pattern) and reusing it throughout your application.",
            1
        )]

        factory_module_logger.reset_mock()
        get_factory('some_other_api_key')
        assert _INSTANTIATED_FACTORIES['some_api_key'] == 2
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
        assert _INSTANTIATED_FACTORIES['some_api_key'] == 1
