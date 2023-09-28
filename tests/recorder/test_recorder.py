"""Recorder unit tests."""

import pytest

from splitio.recorder.recorder import StandardRecorder, PipelinedRecorder, StandardRecorderAsync, PipelinedRecorderAsync
from splitio.engine.impressions.impressions import Manager as ImpressionsManager
from splitio.engine.telemetry import TelemetryStorageProducer, TelemetryStorageProducerAsync
from splitio.storage.inmemmory import EventStorage, ImpressionStorage, InMemoryTelemetryStorage, InMemoryEventStorageAsync, InMemoryImpressionStorageAsync
from splitio.storage.redis import ImpressionPipelinedStorage, EventStorage, RedisEventsStorage, RedisImpressionsStorage, RedisImpressionsStorageAsync, RedisEventsStorageAsync
from splitio.storage.adapters.redis import RedisAdapter, RedisAdapterAsync
from splitio.models.impressions import Impression
from splitio.models.telemetry import MethodExceptionsAndLatencies


class StandardRecorderTests(object):
    """StandardRecorderTests test cases."""

    def test_standard_recorder(self, mocker):
        impressions = [
            Impression('k1', 'f1', 'on', 'l1', 123, None, None),
            Impression('k1', 'f2', 'on', 'l1', 123, None, None)
        ]
        impmanager = mocker.Mock(spec=ImpressionsManager)
        impmanager.process_impressions.return_value = impressions, 0
        event = mocker.Mock(spec=EventStorage)
        impression = mocker.Mock(spec=ImpressionStorage)
        telemetry_storage = mocker.Mock(spec=InMemoryTelemetryStorage)
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)

        def record_latency(*args, **kwargs):
            self.passed_args = args

        telemetry_storage.record_latency.side_effect = record_latency

        recorder = StandardRecorder(impmanager, event, impression, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        recorder.record_treatment_stats(impressions, 1, MethodExceptionsAndLatencies.TREATMENT, 'get_treatment')

        assert recorder._impression_storage.put.mock_calls[0][1][0] == impressions
        assert(self.passed_args[0] == MethodExceptionsAndLatencies.TREATMENT)
        assert(self.passed_args[1] == 1)

    def test_pipelined_recorder(self, mocker):
        impressions = [
            Impression('k1', 'f1', 'on', 'l1', 123, None, None),
            Impression('k1', 'f2', 'on', 'l1', 123, None, None)
        ]
        redis = mocker.Mock(spec=RedisAdapter)
        impmanager = mocker.Mock(spec=ImpressionsManager)
        impmanager.process_impressions.return_value = impressions, 0
        event = mocker.Mock(spec=RedisEventsStorage)
        impression = mocker.Mock(spec=RedisImpressionsStorage)
        recorder = PipelinedRecorder(redis, impmanager, event, impression, mocker.Mock())
        recorder.record_treatment_stats(impressions, 1, MethodExceptionsAndLatencies.TREATMENT, 'get_treatment')
#        pytest.set_trace()
        assert recorder._impression_storage.add_impressions_to_pipe.mock_calls[0][1][0] == impressions
        assert recorder._telemetry_redis_storage.add_latency_to_pipe.mock_calls[0][1][0] == MethodExceptionsAndLatencies.TREATMENT
        assert recorder._telemetry_redis_storage.add_latency_to_pipe.mock_calls[0][1][1] == 1

    def test_sampled_recorder(self, mocker):
        impressions = [
            Impression('k1', 'f1', 'on', 'l1', 123, None, None),
            Impression('k1', 'f2', 'on', 'l1', 123, None, None)
        ]
        redis = mocker.Mock(spec=RedisAdapter)
        impmanager = mocker.Mock(spec=ImpressionsManager)
        impmanager.process_impressions.return_value = impressions, 0
        event = mocker.Mock(spec=EventStorage)
        impression = mocker.Mock(spec=ImpressionStorage)
        recorder = PipelinedRecorder(redis, impmanager, event, impression, 0.5, mocker.Mock())

        def put(x):
            return

        recorder._impression_storage.put.side_effect = put

        for _ in range(100):
            recorder.record_treatment_stats(impressions, 1, 'some', 'get_treatment')
        print(recorder._impression_storage.put.call_count)
        assert recorder._impression_storage.put.call_count < 80


class StandardRecorderAsyncTests(object):
    """StandardRecorder async test cases."""

    @pytest.mark.asyncio
    async def test_standard_recorder(self, mocker):
        impressions = [
            Impression('k1', 'f1', 'on', 'l1', 123, None, None),
            Impression('k1', 'f2', 'on', 'l1', 123, None, None)
        ]
        impmanager = mocker.Mock(spec=ImpressionsManager)
        impmanager.process_impressions.return_value = impressions, 0
        event = mocker.Mock(spec=InMemoryEventStorageAsync)
        impression = mocker.Mock(spec=InMemoryImpressionStorageAsync)
        telemetry_storage = mocker.Mock(spec=InMemoryTelemetryStorage)
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)

        async def record_latency(*args, **kwargs):
            self.passed_args = args

        telemetry_storage.record_latency.side_effect = record_latency

        recorder = StandardRecorderAsync(impmanager, event, impression, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await recorder.record_treatment_stats(impressions, 1, MethodExceptionsAndLatencies.TREATMENT, 'get_treatment')

        assert recorder._impression_storage.put.mock_calls[0][1][0] == impressions
        assert(self.passed_args[0] == MethodExceptionsAndLatencies.TREATMENT)
        assert(self.passed_args[1] == 1)

    @pytest.mark.asyncio
    async def test_pipelined_recorder(self, mocker):
        impressions = [
            Impression('k1', 'f1', 'on', 'l1', 123, None, None),
            Impression('k1', 'f2', 'on', 'l1', 123, None, None)
        ]
        redis = mocker.Mock(spec=RedisAdapterAsync)
        impmanager = mocker.Mock(spec=ImpressionsManager)
        impmanager.process_impressions.return_value = impressions, 0
        event = mocker.Mock(spec=RedisEventsStorageAsync)
        impression = mocker.Mock(spec=RedisImpressionsStorageAsync)
        recorder = PipelinedRecorderAsync(redis, impmanager, event, impression, mocker.Mock())
        await recorder.record_treatment_stats(impressions, 1, MethodExceptionsAndLatencies.TREATMENT, 'get_treatment')
        assert recorder._impression_storage.add_impressions_to_pipe.mock_calls[0][1][0] == impressions
        assert recorder._telemetry_redis_storage.add_latency_to_pipe.mock_calls[0][1][0] == MethodExceptionsAndLatencies.TREATMENT
        assert recorder._telemetry_redis_storage.add_latency_to_pipe.mock_calls[0][1][1] == 1

    @pytest.mark.asyncio
    async def test_sampled_recorder(self, mocker):
        impressions = [
            Impression('k1', 'f1', 'on', 'l1', 123, None, None),
            Impression('k1', 'f2', 'on', 'l1', 123, None, None)
        ]
        redis = mocker.Mock(spec=RedisAdapterAsync)
        impmanager = mocker.Mock(spec=ImpressionsManager)
        impmanager.process_impressions.return_value = impressions, 0
        event = mocker.Mock(spec=RedisEventsStorageAsync)
        impression = mocker.Mock(spec=RedisImpressionsStorageAsync)
        recorder = PipelinedRecorderAsync(redis, impmanager, event, impression, 0.5, mocker.Mock())

        async def put(x):
            return

        recorder._impression_storage.put.side_effect = put

        for _ in range(100):
            await recorder.record_treatment_stats(impressions, 1, 'some', 'get_treatment')
        print(recorder._impression_storage.put.call_count)
        assert recorder._impression_storage.put.call_count < 80
