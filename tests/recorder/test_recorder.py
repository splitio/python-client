"""Recorder unit tests."""

import pytest

from splitio.client.listener import ImpressionListenerWrapper, ImpressionListenerWrapperAsync
from splitio.recorder.recorder import StandardRecorder, PipelinedRecorder, StandardRecorderAsync, PipelinedRecorderAsync
from splitio.engine.impressions.impressions import Manager as ImpressionsManager
from splitio.engine.telemetry import TelemetryStorageProducer, TelemetryStorageProducerAsync
from splitio.engine.impressions.manager import Counter as ImpressionsCounter
from splitio.engine.impressions.unique_keys_tracker import UniqueKeysTracker, UniqueKeysTrackerAsync
from splitio.storage.inmemmory import EventStorage, ImpressionStorage, InMemoryTelemetryStorage, InMemoryEventStorageAsync, InMemoryImpressionStorageAsync
from splitio.storage.redis import ImpressionPipelinedStorage, EventStorage, RedisEventsStorage, RedisImpressionsStorage, RedisImpressionsStorageAsync, RedisEventsStorageAsync
from splitio.storage.adapters.redis import RedisAdapter, RedisAdapterAsync
from splitio.models.impressions import Impression
from splitio.models.telemetry import MethodExceptionsAndLatencies
from splitio.optional.loaders import asyncio

class StandardRecorderTests(object):
    """StandardRecorderTests test cases."""

    def test_standard_recorder(self, mocker):
        impressions = [
            Impression('k1', 'f1', 'on', 'l1', 123, None, None, None, None),
            Impression('k1', 'f2', 'on', 'l1', 123, None, None, None, None)
        ]
        impmanager = mocker.Mock(spec=ImpressionsManager)
        impmanager.process_impressions.return_value = impressions, 0, [
            (Impression('k1', 'f1', 'on', 'l1', 123, None, None, None, None), None),
            (Impression('k1', 'f2', 'on', 'l1', 123, None, None, None, None), None)], \
            [{"f": "f1", "ks": ["l1"]}, {"f": "f2", "ks": ["l1"]}], [('k1', 'f1'), ('k1', 'f2')]
        event = mocker.Mock(spec=EventStorage)
        impression = mocker.Mock(spec=ImpressionStorage)
        telemetry_storage = mocker.Mock(spec=InMemoryTelemetryStorage)
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        listener = mocker.Mock(spec=ImpressionListenerWrapper)

        def record_latency(*args, **kwargs):
            self.passed_args = args

        telemetry_storage.record_latency.side_effect = record_latency

        imp_counter = mocker.Mock(spec=ImpressionsCounter())
        unique_keys_tracker = mocker.Mock(spec=UniqueKeysTracker())
        recorder = StandardRecorder(impmanager, event, impression, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer(),
                                    listener=listener, unique_keys_tracker=unique_keys_tracker, imp_counter=imp_counter)
        recorder.record_treatment_stats(impressions, 1, MethodExceptionsAndLatencies.TREATMENT, 'get_treatment')

        assert recorder._impression_storage.put.mock_calls[0][1][0] == impressions
        assert(self.passed_args[0] == MethodExceptionsAndLatencies.TREATMENT)
        assert(self.passed_args[1] == 1)
        assert listener.log_impression.mock_calls == [
            mocker.call(Impression('k1', 'f1', 'on', 'l1', 123, None, None, None, None), None),
            mocker.call(Impression('k1', 'f2', 'on', 'l1', 123, None, None, None, None), None)
        ]
        assert recorder._imp_counter.track.mock_calls == [mocker.call([{"f": "f1", "ks": ["l1"]}, {"f": "f2", "ks": ["l1"]}])]
        assert recorder._unique_keys_tracker.track.mock_calls == [mocker.call('k1', 'f1'), mocker.call('k1', 'f2')]

    def test_pipelined_recorder(self, mocker):
        impressions = [
            Impression('k1', 'f1', 'on', 'l1', 123, None, None, None, None),
            Impression('k1', 'f2', 'on', 'l1', 123, None, None, None, None)
        ]
        redis = mocker.Mock(spec=RedisAdapter)
        def execute():
            return []
        redis().execute = execute

        impmanager = mocker.Mock(spec=ImpressionsManager)
        impmanager.process_impressions.return_value = impressions, 0, [
            (Impression('k1', 'f1', 'on', 'l1', 123, None, None, None, None), None),
            (Impression('k1', 'f2', 'on', 'l1', 123, None, None, None, None), None)], \
            [{"f": "f1", "ks": ["l1"]}, {"f": "f2", "ks": ["l1"]}], [('k1', 'f1'), ('k1', 'f2')]
        event = mocker.Mock(spec=RedisEventsStorage)
        impression = mocker.Mock(spec=RedisImpressionsStorage)
        listener = mocker.Mock(spec=ImpressionListenerWrapper)
        imp_counter = mocker.Mock(spec=ImpressionsCounter())
        unique_keys_tracker = mocker.Mock(spec=UniqueKeysTracker())
        recorder = PipelinedRecorder(redis, impmanager, event, impression, mocker.Mock(),
                                    listener=listener, unique_keys_tracker=unique_keys_tracker, imp_counter=imp_counter)
        recorder.record_treatment_stats(impressions, 1, MethodExceptionsAndLatencies.TREATMENT, 'get_treatment')

        assert recorder._impression_storage.add_impressions_to_pipe.mock_calls[0][1][0] == impressions
        assert recorder._telemetry_redis_storage.add_latency_to_pipe.mock_calls[0][1][0] == MethodExceptionsAndLatencies.TREATMENT
        assert recorder._telemetry_redis_storage.add_latency_to_pipe.mock_calls[0][1][1] == 1
        assert listener.log_impression.mock_calls == [
            mocker.call(Impression('k1', 'f1', 'on', 'l1', 123, None, None, None, None), None),
            mocker.call(Impression('k1', 'f2', 'on', 'l1', 123, None, None, None, None), None)
        ]
        assert recorder._imp_counter.track.mock_calls == [mocker.call([{"f": "f1", "ks": ["l1"]}, {"f": "f2", "ks": ["l1"]}])]
        assert recorder._unique_keys_tracker.track.mock_calls == [mocker.call('k1', 'f1'), mocker.call('k1', 'f2')]

    def test_sampled_recorder(self, mocker):
        impressions = [
            Impression('k1', 'f1', 'on', 'l1', 123, None, None, None, None),
            Impression('k1', 'f2', 'on', 'l1', 123, None, None, None, None)
        ]
        redis = mocker.Mock(spec=RedisAdapter)
        impmanager = mocker.Mock(spec=ImpressionsManager)
        impmanager.process_impressions.return_value = impressions, 0, [
            (Impression('k1', 'f1', 'on', 'l1', 123, None, None, None, None), None),
            (Impression('k1', 'f2', 'on', 'l1', 123, None, None, None, None), None)
        ], [], []

        event = mocker.Mock(spec=EventStorage)
        impression = mocker.Mock(spec=ImpressionStorage)
        imp_counter = mocker.Mock(spec=ImpressionsCounter())
        unique_keys_tracker = mocker.Mock(spec=UniqueKeysTracker())
        recorder = PipelinedRecorder(redis, impmanager, event, impression, 0.5, mocker.Mock(), imp_counter=imp_counter, unique_keys_tracker=unique_keys_tracker)

        def put(x):
            return
        recorder._impression_storage.put.side_effect = put

        for _ in range(100):
            recorder.record_treatment_stats(impressions, 1, 'some', 'get_treatment')
        print(recorder._impression_storage.put.call_count)
        assert recorder._impression_storage.put.call_count < 80
        assert recorder._imp_counter.track.mock_calls == []
        assert recorder._unique_keys_tracker.track.mock_calls == []

class StandardRecorderAsyncTests(object):
    """StandardRecorder async test cases."""

    @pytest.mark.asyncio
    async def test_standard_recorder(self, mocker):
        impressions = [
            Impression('k1', 'f1', 'on', 'l1', 123, None, None, None, None),
            Impression('k1', 'f2', 'on', 'l1', 123, None, None, None, None)
        ]
        impmanager = mocker.Mock(spec=ImpressionsManager)
        impmanager.process_impressions.return_value = impressions, 0, [
            (Impression('k1', 'f1', 'on', 'l1', 123, None, None, None, None), {'att1': 'val'}),
            (Impression('k1', 'f2', 'on', 'l1', 123, None, None, None, None), None)], \
            [{"f": "f1", "ks": ["l1"]}, {"f": "f2", "ks": ["l1"]}], [('k1', 'f1'), ('k1', 'f2')]
        event = mocker.Mock(spec=InMemoryEventStorageAsync)
        impression = mocker.Mock(spec=InMemoryImpressionStorageAsync)
        telemetry_storage = mocker.Mock(spec=InMemoryTelemetryStorage)
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        listener = mocker.Mock(spec=ImpressionListenerWrapperAsync)
        self.listener_impressions = []
        self.listener_attributes = []
        async def log_impression(impressions, attributes):
            self.listener_impressions.append(impressions)
            self.listener_attributes.append(attributes)
        listener.log_impression = log_impression

        async def record_latency(*args, **kwargs):
            self.passed_args = args
        telemetry_storage.record_latency.side_effect = record_latency

        imp_counter = mocker.Mock(spec=ImpressionsCounter())
        unique_keys_tracker = mocker.Mock(spec=UniqueKeysTrackerAsync())
        recorder = StandardRecorderAsync(impmanager, event, impression, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer(),
                                    listener=listener, unique_keys_tracker=unique_keys_tracker, imp_counter=imp_counter)
        self.impressions = []
        async def put(x):
            self.impressions = x
            return
        recorder._impression_storage.put = put

        self.count = []
        def track(x):
            self.count = x
        recorder._imp_counter.track = track

        self.unique_keys = []
        async def track2(x, y):
            self.unique_keys.append((x, y))
        recorder._unique_keys_tracker.track = track2

        await recorder.record_treatment_stats(impressions, 1, MethodExceptionsAndLatencies.TREATMENT, 'get_treatment')
        await asyncio.sleep(1)

        assert self.impressions == impressions
        assert(self.passed_args[0] == MethodExceptionsAndLatencies.TREATMENT)
        assert(self.passed_args[1] == 1)
        assert self.listener_impressions == [
            Impression('k1', 'f1', 'on', 'l1', 123, None, None, None, None),
            Impression('k1', 'f2', 'on', 'l1', 123, None, None, None, None),
        ]
        assert self.listener_attributes == [{'att1': 'val'}, None]
        assert self.count == [{"f": "f1", "ks": ["l1"]}, {"f": "f2", "ks": ["l1"]}]
        assert self.unique_keys == [('k1', 'f1'), ('k1', 'f2')]

    @pytest.mark.asyncio
    async def test_pipelined_recorder(self, mocker):
        impressions = [
            Impression('k1', 'f1', 'on', 'l1', 123, None, None, None, None),
            Impression('k1', 'f2', 'on', 'l1', 123, None, None, None, None)
        ]
        redis = mocker.Mock(spec=RedisAdapterAsync)
        async def execute():
            return []
        redis().execute = execute
        impmanager = mocker.Mock(spec=ImpressionsManager)
        impmanager.process_impressions.return_value = impressions, 0, [
            (Impression('k1', 'f1', 'on', 'l1', 123, None, None, None, None), {'att1': 'val'}),
            (Impression('k1', 'f2', 'on', 'l1', 123, None, None, None, None), None)], \
            [{"f": "f1", "ks": ["l1"]}, {"f": "f2", "ks": ["l1"]}], [('k1', 'f1'), ('k1', 'f2')]
        event = mocker.Mock(spec=RedisEventsStorageAsync)
        impression = mocker.Mock(spec=RedisImpressionsStorageAsync)
        listener = mocker.Mock(spec=ImpressionListenerWrapperAsync)
        self.listener_impressions = []
        self.listener_attributes = []
        async def log_impression(impressions, attributes):
            self.listener_impressions.append(impressions)
            self.listener_attributes.append(attributes)
        listener.log_impression = log_impression

        imp_counter = mocker.Mock(spec=ImpressionsCounter())
        unique_keys_tracker = mocker.Mock(spec=UniqueKeysTrackerAsync())
        recorder = PipelinedRecorderAsync(redis, impmanager, event, impression, mocker.Mock(),
                                    listener=listener, unique_keys_tracker=unique_keys_tracker, imp_counter=imp_counter)
        self.count = []
        def track(x):
            self.count = x
        recorder._imp_counter.track = track

        self.unique_keys = []
        async def track2(x, y):
            self.unique_keys.append((x, y))
        recorder._unique_keys_tracker.track = track2

        await recorder.record_treatment_stats(impressions, 1, MethodExceptionsAndLatencies.TREATMENT, 'get_treatment')
        await asyncio.sleep(.2)
        assert recorder._impression_storage.add_impressions_to_pipe.mock_calls[0][1][0] == impressions
        assert recorder._telemetry_redis_storage.add_latency_to_pipe.mock_calls[0][1][0] == MethodExceptionsAndLatencies.TREATMENT
        assert recorder._telemetry_redis_storage.add_latency_to_pipe.mock_calls[0][1][1] == 1
        assert self.listener_impressions == [
            Impression('k1', 'f1', 'on', 'l1', 123, None, None, None, None),
            Impression('k1', 'f2', 'on', 'l1', 123, None, None, None, None),
        ]
        assert self.listener_attributes == [{'att1': 'val'}, None]
        assert self.count == [{"f": "f1", "ks": ["l1"]}, {"f": "f2", "ks": ["l1"]}]
        assert self.unique_keys == [('k1', 'f1'), ('k1', 'f2')]

    @pytest.mark.asyncio
    async def test_sampled_recorder(self, mocker):
        impressions = [
            Impression('k1', 'f1', 'on', 'l1', 123, None, None, None, None),
            Impression('k1', 'f2', 'on', 'l1', 123, None, None, None, None)
        ]
        redis = mocker.Mock(spec=RedisAdapterAsync)
        impmanager = mocker.Mock(spec=ImpressionsManager)
        impmanager.process_impressions.return_value = impressions, 0, [
            (Impression('k1', 'f1', 'on', 'l1', 123, None, None, None, None), None),
            (Impression('k1', 'f2', 'on', 'l1', 123, None, None, None, None), None)
        ], [], []
        event = mocker.Mock(spec=RedisEventsStorageAsync)
        impression = mocker.Mock(spec=RedisImpressionsStorageAsync)
        imp_counter = mocker.Mock(spec=ImpressionsCounter())
        unique_keys_tracker = mocker.Mock(spec=UniqueKeysTrackerAsync())
        recorder = PipelinedRecorderAsync(redis, impmanager, event, impression, 0.5, mocker.Mock(),
                                    unique_keys_tracker=unique_keys_tracker, imp_counter=imp_counter)
        self.count = []
        async def track(x):
            self.count = x
        recorder._imp_counter.track = track

        self.unique_keys = []
        async def track2(x, y):
            self.unique_keys.append((x, y))
        recorder._unique_keys_tracker.track = track2

        async def put(x):
            return

        recorder._impression_storage.put.side_effect = put

        for _ in range(100):
            await recorder.record_treatment_stats(impressions, 1, 'some', 'get_treatment')
        print(recorder._impression_storage.put.call_count)
        assert recorder._impression_storage.put.call_count < 80
        assert self.count == []
        assert self.unique_keys == []
