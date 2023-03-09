import unittest.mock as mock

from splitio.engine.telemetry import TelemetryEvaluationConsumer, TelemetryEvaluationProducer, TelemetryInitConsumer, \
    TelemetryInitProducer, TelemetryRuntimeConsumer, TelemetryRuntimeProducer, TelemetryStorageConsumer, TelemetryStorageProducer
from splitio.storage.inmemmory import InMemoryTelemetryStorage

class TelemetryStorageProducerTests(object):
    """TelemetryStorageProducer test."""

    def test_instances(self):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)

        assert(isinstance(telemetry_producer._telemetry_evaluation_producer, TelemetryEvaluationProducer))
        assert(isinstance(telemetry_producer._telemetry_init_producer, TelemetryInitProducer))
        assert(isinstance(telemetry_producer._telemetry_runtime_producer, TelemetryRuntimeProducer))

        assert(telemetry_producer._telemetry_evaluation_producer == telemetry_producer.get_telemetry_evaluation_producer())
        assert(telemetry_producer._telemetry_init_producer == telemetry_producer.get_telemetry_init_producer())
        assert(telemetry_producer._telemetry_runtime_producer == telemetry_producer.get_telemetry_runtime_producer())

    def test_record_config(self, mocker):
        telemetry_storage = mocker.Mock()
        telemetry_init_producer = TelemetryInitProducer(telemetry_storage)

        def record_config(*args, **kwargs):
            self.passed_config = args[0]

        telemetry_storage.record_config.side_effect = record_config
        telemetry_init_producer.record_config({'bT':0, 'nR':0, 'uC': 0}, {})
        assert(self.passed_config == {'bT':0, 'nR':0, 'uC': 0})

    def test_record_ready_time(self, mocker):
        telemetry_storage = mocker.Mock()
        telemetry_init_producer = TelemetryInitProducer(telemetry_storage)

        def record_ready_time(*args, **kwargs):
            self.passed_arg = args[0]

        telemetry_storage.record_ready_time.side_effect = record_ready_time
        telemetry_init_producer.record_ready_time(10)
        assert(self.passed_arg == 10)

    @mock.patch('splitio.storage.inmemmory.InMemoryTelemetryStorage.record_bur_time_out')
    def test_record_bur_timeout(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_init_producer = TelemetryInitProducer(telemetry_storage)
        telemetry_init_producer.record_bur_time_out()
        assert(mocker.called)

    @mock.patch('splitio.storage.inmemmory.InMemoryTelemetryStorage.record_not_ready_usage')
    def test_record_not_ready_usage(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_init_producer = TelemetryInitProducer(telemetry_storage)
        telemetry_init_producer.record_not_ready_usage()
        assert(mocker.called)

    def test_record_latency(self, mocker):
        telemetry_storage = mocker.Mock()
        telemetry_evaluation_producer = TelemetryEvaluationProducer(telemetry_storage)

        def record_latency(*args, **kwargs):
            self.passed_args = args

        telemetry_storage.record_latency.side_effect = record_latency
        telemetry_evaluation_producer.record_latency('method', 10)
        assert(self.passed_args[0] == 'method')
        assert(self.passed_args[1] == 10)

    def test_record_exception(self, mocker):
        telemetry_storage = mocker.Mock()
        telemetry_evaluation_producer = TelemetryEvaluationProducer(telemetry_storage)

        def record_exception(*args, **kwargs):
            self.passed_method = args[0]

        telemetry_storage.record_exception.side_effect = record_exception
        telemetry_evaluation_producer.record_exception('method')
        assert(self.passed_method == 'method')

    def test_add_tag(self, mocker):
        telemetry_storage = mocker.Mock()
        telemetry_runtime_producer = TelemetryRuntimeProducer(telemetry_storage)

        def add_tag(*args, **kwargs):
            self.passed_tag = args[0]

        telemetry_storage.add_tag.side_effect = add_tag
        telemetry_runtime_producer.add_tag('tag')
        assert(self.passed_tag == 'tag')

    def test_record_impression_stats(self, mocker):
        telemetry_storage = mocker.Mock()
        telemetry_runtime_producer = TelemetryRuntimeProducer(telemetry_storage)

        def record_impression_stats(*args, **kwargs):
            self.passed_args = args

        telemetry_storage.record_impression_stats.side_effect = record_impression_stats
        telemetry_runtime_producer.record_impression_stats('imp', 10)
        assert(self.passed_args[0] == 'imp')
        assert(self.passed_args[1] == 10)

    def test_record_event_stats(self, mocker):
        telemetry_storage = mocker.Mock()
        telemetry_runtime_producer = TelemetryRuntimeProducer(telemetry_storage)

        def record_event_stats(*args, **kwargs):
            self.passed_args = args

        telemetry_storage.record_event_stats.side_effect = record_event_stats
        telemetry_runtime_producer.record_event_stats('ev', 20)
        assert(self.passed_args[0] == 'ev')
        assert(self.passed_args[1] == 20)

    def test_record_successful_sync(self, mocker):
        telemetry_storage = mocker.Mock()
        telemetry_runtime_producer = TelemetryRuntimeProducer(telemetry_storage)

        def record_successful_sync(*args, **kwargs):
            self.passed_args = args

        telemetry_storage.record_successful_sync.side_effect = record_successful_sync
        telemetry_runtime_producer.record_successful_sync('split', 50)
        assert(self.passed_args[0] == 'split')
        assert(self.passed_args[1] == 50)

    def test_record_sync_error(self, mocker):
        telemetry_storage = mocker.Mock()
        telemetry_runtime_producer = TelemetryRuntimeProducer(telemetry_storage)

        def record_sync_error(*args, **kwargs):
            self.passed_args = args

        telemetry_storage.record_sync_error.side_effect = record_sync_error
        telemetry_runtime_producer.record_sync_error('segment', {'500': 1})
        assert(self.passed_args[0] == 'segment')
        assert(self.passed_args[1] == {'500': 1})

    def test_record_sync_latency(self, mocker):
        telemetry_storage = mocker.Mock()
        telemetry_runtime_producer = TelemetryRuntimeProducer(telemetry_storage)

        def record_sync_latency(*args, **kwargs):
            self.passed_args = args

        telemetry_storage.record_sync_latency.side_effect = record_sync_latency
        telemetry_runtime_producer.record_sync_latency('t', 40)
        assert(self.passed_args[0] == 't')
        assert(self.passed_args[1] == 40)

    @mock.patch('splitio.storage.inmemmory.InMemoryTelemetryStorage.record_auth_rejections')
    def test_record_auth_rejections(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_runtime_producer = TelemetryRuntimeProducer(telemetry_storage)
        telemetry_runtime_producer.record_auth_rejections()
        assert(mocker.called)

    @mock.patch('splitio.storage.inmemmory.InMemoryTelemetryStorage.record_token_refreshes')
    def test_record_token_refreshes(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_runtime_producer = TelemetryRuntimeProducer(telemetry_storage)
        telemetry_runtime_producer.record_token_refreshes()
        assert(mocker.called)

    def test_record_streaming_event(self, mocker):
        telemetry_storage = mocker.Mock()
        telemetry_runtime_producer = TelemetryRuntimeProducer(telemetry_storage)

        def record_streaming_event(*args, **kwargs):
            self.passed_event = args[0]

        telemetry_storage.record_streaming_event.side_effect = record_streaming_event
        telemetry_runtime_producer.record_streaming_event({'t', 40})
        assert(self.passed_event == {'t', 40})

    def test_record_session_length(self, mocker):
        telemetry_storage = mocker.Mock()
        telemetry_runtime_producer = TelemetryRuntimeProducer(telemetry_storage)

        def record_session_length(*args, **kwargs):
            self.passed_session = args[0]

        telemetry_storage.record_session_length.side_effect = record_session_length
        telemetry_runtime_producer.record_session_length(30)
        assert(self.passed_session == 30)

class TelemetryStorageConsumerTests(object):
    """TelemetryStorageConsumer test."""

    def test_instances(self):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)

        assert(isinstance(telemetry_consumer._telemetry_evaluation_consumer, TelemetryEvaluationConsumer))
        assert(isinstance(telemetry_consumer._telemetry_init_consumer, TelemetryInitConsumer))
        assert(isinstance(telemetry_consumer._telemetry_runtime_consumer, TelemetryRuntimeConsumer))

        assert(telemetry_consumer._telemetry_evaluation_consumer == telemetry_consumer.get_telemetry_evaluation_consumer())
        assert(telemetry_consumer._telemetry_init_consumer == telemetry_consumer.get_telemetry_init_consumer())
        assert(telemetry_consumer._telemetry_runtime_consumer == telemetry_consumer.get_telemetry_runtime_consumer())

    @mock.patch('splitio.storage.inmemmory.InMemoryTelemetryStorage.get_bur_time_outs')
    def test_get_bur_time_outs(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_init_consumer = TelemetryInitConsumer(telemetry_storage)
        telemetry_init_consumer.get_bur_time_outs()
        assert(mocker.called)

    @mock.patch('splitio.storage.inmemmory.InMemoryTelemetryStorage.get_not_ready_usage')
    def get_not_ready_usage(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_init_consumer = TelemetryInitConsumer(telemetry_storage)
        telemetry_init_consumer.get_not_ready_usage()
        assert(mocker.called)

    @mock.patch('splitio.storage.inmemmory.InMemoryTelemetryStorage.get_config_stats')
    def get_not_ready_usage(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_init_consumer = TelemetryInitConsumer(telemetry_storage)
        telemetry_init_consumer.get_config_stats()
        assert(mocker.called)

    @mock.patch('splitio.storage.inmemmory.InMemoryTelemetryStorage.pop_exceptions')
    def pop_exceptions(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_evaluation_consumer = TelemetryEvaluationConsumer(telemetry_storage)
        telemetry_evaluation_consumer.pop_exceptions()
        assert(mocker.called)

    @mock.patch('splitio.storage.inmemmory.InMemoryTelemetryStorage.pop_latencies')
    def pop_latencies(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_evaluation_consumer = TelemetryEvaluationConsumer(telemetry_storage)
        telemetry_evaluation_consumer.pop_latencies()
        assert(mocker.called)

    def test_get_impressions_stats(self, mocker):
        telemetry_storage = mocker.Mock()
        telemetry_runtime_consumer = TelemetryRuntimeConsumer(telemetry_storage)

        def get_impressions_stats(*args, **kwargs):
            self.passed_type = args[0]

        telemetry_storage.get_impressions_stats.side_effect = get_impressions_stats
        telemetry_runtime_consumer.get_impressions_stats('iQ')
        assert(self.passed_type == 'iQ')

    def test_get_events_stats(self, mocker):
        telemetry_storage = mocker.Mock()
        telemetry_runtime_consumer = TelemetryRuntimeConsumer(telemetry_storage)

        def get_events_stats(*args, **kwargs):
            self.event_type = args[0]

        telemetry_storage.get_events_stats.side_effect = get_events_stats
        telemetry_runtime_consumer.get_events_stats('eQ')
        assert(self.event_type == 'eQ')

    @mock.patch('splitio.storage.inmemmory.InMemoryTelemetryStorage.get_last_synchronization')
    def test_get_last_synchronization(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_runtime_consumer = TelemetryRuntimeConsumer(telemetry_storage)
        telemetry_runtime_consumer.get_last_synchronization()
        assert(mocker.called)

    @mock.patch('splitio.storage.inmemmory.InMemoryTelemetryStorage.pop_tags')
    def test_pop_tags(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_runtime_consumer = TelemetryRuntimeConsumer(telemetry_storage)
        telemetry_runtime_consumer.pop_tags()
        assert(mocker.called)

    @mock.patch('splitio.storage.inmemmory.InMemoryTelemetryStorage.pop_http_errors')
    def test_pop_http_errors(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_runtime_consumer = TelemetryRuntimeConsumer(telemetry_storage)
        telemetry_runtime_consumer.pop_http_errors()
        assert(mocker.called)

    @mock.patch('splitio.storage.inmemmory.InMemoryTelemetryStorage.pop_http_latencies')
    def test_pop_http_latencies(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_runtime_consumer = TelemetryRuntimeConsumer(telemetry_storage)
        telemetry_runtime_consumer.pop_http_latencies()

    @mock.patch('splitio.storage.inmemmory.InMemoryTelemetryStorage.pop_auth_rejections')
    def test_pop_auth_rejections(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_runtime_consumer = TelemetryRuntimeConsumer(telemetry_storage)
        telemetry_runtime_consumer.pop_auth_rejections()

    @mock.patch('splitio.storage.inmemmory.InMemoryTelemetryStorage.pop_token_refreshes')
    def test_pop_token_refreshes(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_runtime_consumer = TelemetryRuntimeConsumer(telemetry_storage)
        telemetry_runtime_consumer.pop_token_refreshes()

    @mock.patch('splitio.storage.inmemmory.InMemoryTelemetryStorage.pop_streaming_events')
    def test_pop_streaming_events(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_runtime_consumer = TelemetryRuntimeConsumer(telemetry_storage)
        telemetry_runtime_consumer.pop_streaming_events()

    @mock.patch('splitio.storage.inmemmory.InMemoryTelemetryStorage.get_session_length')
    def test_get_session_length(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_runtime_consumer = TelemetryRuntimeConsumer(telemetry_storage)
        telemetry_runtime_consumer.get_session_length()
