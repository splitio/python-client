"""Telemetry model test module."""
import os
import random
import pytest

from splitio.models.telemetry import StorageType, OperationMode, MethodLatencies, MethodExceptions, \
    HTTPLatencies, HTTPErrors, LastSynchronization, TelemetryCounters, TelemetryConfig, \
    StreamingEvent, StreamingEvents, MethodExceptionsAsync, HTTPLatenciesAsync, HTTPErrorsAsync, LastSynchronizationAsync, \
    TelemetryCountersAsync, TelemetryConfigAsync, StreamingEventsAsync, MethodLatenciesAsync, UpdateFromSSE

import splitio.models.telemetry as ModelTelemetry

class TelemetryModelTests(object):
    """Telemetry model test cases."""

    def test_latency_bucket_index(self):
        for i in range(50000):
            latency = random.randint(10, 9987885)
            old_bucket = 0
            result_bucket = 0
            counter = -1
            for j in ModelTelemetry.BUCKETS:
                counter += 1
                if old_bucket == 0:
                    if latency < j:
                        old_bucket = 0
                        break
                    old_bucket = j
                    continue
                if counter == ModelTelemetry.MAX_LATENCY_BUCKET_COUNT - 1:
                    result_bucket = 22
                    break
                if latency > old_bucket and latency <= j:
                    result_bucket = counter
                    break
                old_bucket = j
            print(latency, old_bucket, j)
            assert(result_bucket == ModelTelemetry.get_latency_bucket_index(latency))

    def test_storage_type_and_operation_mode(self, mocker):
        assert(StorageType.MEMORY.value == 'memory')
        assert(StorageType.REDIS.value == 'redis')
        assert(OperationMode.STANDALONE.value == 'standalone')
        assert(OperationMode.CONSUMER.value == 'consumer')

    def test_method_latencies(self, mocker):
        method_latencies = MethodLatencies()

        method_latencies.pop_all() # should not raise exception
        for method in ModelTelemetry.MethodExceptionsAndLatencies:
            method_latencies.add_latency(method, 50)
            if method.value == 'treatment':
                assert(method_latencies._treatment[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            elif method.value == 'treatments':
                assert(method_latencies._treatments[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            elif method.value == 'treatment_with_config':
                assert(method_latencies._treatment_with_config[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            elif method.value == 'treatments_with_config':
                assert(method_latencies._treatments_with_config[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            elif method.value == 'treatments_by_flag_set':
                assert(method_latencies._treatments_by_flag_set[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            elif method.value == 'treatments_by_flag_sets':
                assert(method_latencies._treatments_by_flag_sets[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            elif method.value == 'treatments_with_config_by_flag_set':
                assert(method_latencies._treatments_with_config_by_flag_set[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            elif method.value == 'treatments_with_config_by_flag_sets':
                assert(method_latencies._treatments_with_config_by_flag_sets[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            elif method.value == 'track':
                assert(method_latencies._track[ModelTelemetry.get_latency_bucket_index(50)] == 1)

            method_latencies.add_latency(method, 50000000)
            if method.value == 'treatment':
                assert(method_latencies._treatment[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            if method.value == 'treatments':
                assert(method_latencies._treatments[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            if method.value == 'treatment_with_config':
                assert(method_latencies._treatment_with_config[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            if method.value == 'treatments_with_config':
                assert(method_latencies._treatments_with_config[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            elif method.value == 'treatments_by_flag_set':
                assert(method_latencies._treatments_by_flag_set[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            elif method.value == 'treatments_by_flag_sets':
                assert(method_latencies._treatments_by_flag_sets[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            elif method.value == 'treatments_with_config_by_flag_set':
                assert(method_latencies._treatments_with_config_by_flag_set[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            elif method.value == 'treatments_with_config_by_flag_sets':
                assert(method_latencies._treatments_with_config_by_flag_sets[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            if method.value == 'track':
                assert(method_latencies._track[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)

        method_latencies.pop_all()
        assert(method_latencies._track == [0] * 23)
        assert(method_latencies._treatment == [0] * 23)
        assert(method_latencies._treatments == [0] * 23)
        assert(method_latencies._treatment_with_config == [0] * 23)
        assert(method_latencies._treatments_with_config == [0] * 23)
        assert(method_latencies._treatments_by_flag_set == [0] * 23)
        assert(method_latencies._treatments_by_flag_sets == [0] * 23)
        assert(method_latencies._treatments_with_config_by_flag_set == [0] * 23)
        assert(method_latencies._treatments_with_config_by_flag_sets == [0] * 23)

        method_latencies.add_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT, 10)
        [method_latencies.add_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS, 20) for i in range(2)]
        method_latencies.add_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG, 50)
        method_latencies.add_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG, 20)
        [method_latencies.add_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET, 20) for i in range(3)]
        [method_latencies.add_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS, 20) for i in range(4)]
        [method_latencies.add_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET, 20) for i in range(5)]
        [method_latencies.add_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS, 20) for i in range(6)]
        method_latencies.add_latency(ModelTelemetry.MethodExceptionsAndLatencies.TRACK, 20)
        latencies = method_latencies.pop_all()
        assert(latencies == {'methodLatencies': {'treatment': [1] + [0] * 22,
                                                 'treatments': [2] + [0] * 22,
                                                 'treatment_with_config': [1] + [0] * 22,
                                                 'treatments_with_config': [1] + [0] * 22,
                                                 'treatments_by_flag_set': [3] + [0] * 22,
                                                 'treatments_by_flag_sets': [4] + [0] * 22,
                                                 'treatments_with_config_by_flag_set': [5] + [0] * 22,
                                                 'treatments_with_config_by_flag_sets': [6] + [0] * 22,
                                                 'track': [1] + [0] * 22}})

    def test_http_latencies(self, mocker):
        http_latencies = HTTPLatencies()

        http_latencies.pop_all() # should not raise exception
        for resource in ModelTelemetry.HTTPExceptionsAndLatencies:
            if self._get_http_latency(resource, http_latencies) == None:
                continue
            http_latencies.add_latency(resource, 50)
            assert(self._get_http_latency(resource, http_latencies)[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            http_latencies.add_latency(resource, 50000000)
            assert(self._get_http_latency(resource, http_latencies)[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            for j in range(10):
                latency = random.randint(1001, 4987885)
                current_count = self._get_http_latency(resource, http_latencies)[ModelTelemetry.get_latency_bucket_index(latency)]
                [http_latencies.add_latency(resource, latency) for i in range(2)]
                assert(self._get_http_latency(resource, http_latencies)[ModelTelemetry.get_latency_bucket_index(latency)] == 2 + current_count)

        http_latencies.pop_all()
        assert(http_latencies._event == [0] * 23)
        assert(http_latencies._impression == [0] * 23)
        assert(http_latencies._impression_count == [0] * 23)
        assert(http_latencies._segment == [0] * 23)
        assert(http_latencies._split == [0] * 23)
        assert(http_latencies._telemetry == [0] * 23)
        assert(http_latencies._token == [0] * 23)

        http_latencies.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.SPLIT, 10)
        [http_latencies.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION, i) for i in [10, 20]]
        http_latencies.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.SEGMENT, 40)
        http_latencies.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION_COUNT, 60)
        http_latencies.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.EVENT, 90)
        http_latencies.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.TELEMETRY, 70)
        [http_latencies.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.TOKEN, i) for i in [10, 15]]
        latencies = http_latencies.pop_all()
        assert(latencies == {'httpLatencies': {'split': [1] + [0] * 22, 'segment': [1] + [0] * 22, 'impression': [2] + [0] * 22, 'impressionCount': [1] + [0] * 22, 'event': [1] + [0] * 22, 'telemetry': [1] + [0] * 22, 'token': [2] + [0] * 22}})

    def _get_http_latency(self, resource, storage):
        if resource == ModelTelemetry.HTTPExceptionsAndLatencies.SPLIT:
            return storage._split
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.SEGMENT:
            return storage._segment
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION:
            return storage._impression
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION_COUNT:
            return storage._impression_count
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.EVENT:
            return storage._event
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.TELEMETRY:
            return storage._telemetry
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.TOKEN:
            return storage._token
        else:
            return

    def test_method_exceptions(self, mocker):
        method_exception = MethodExceptions()

        exceptions = method_exception.pop_all() # should not raise exception
        [method_exception.add_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT) for i in range(2)]
        method_exception.add_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS)
        method_exception.add_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG)
        [method_exception.add_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG) for i in range(5)]
        [method_exception.add_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET) for i in range(6)]
        [method_exception.add_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS) for i in range(7)]
        [method_exception.add_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET) for i in range(8)]
        [method_exception.add_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS) for i in range(9)]
        [method_exception.add_exception(ModelTelemetry.MethodExceptionsAndLatencies.TRACK) for i in range(3)]
        exceptions = method_exception.pop_all()

        assert(method_exception._treatment == 0)
        assert(method_exception._treatments == 0)
        assert(method_exception._treatment_with_config == 0)
        assert(method_exception._treatments_with_config == 0)
        assert(method_exception._treatments_by_flag_set == 0)
        assert(method_exception._treatments_by_flag_sets == 0)
        assert(method_exception._treatments_with_config_by_flag_set == 0)
        assert(method_exception._treatments_with_config_by_flag_sets == 0)
        assert(method_exception._track == 0)
        assert(exceptions == {'methodExceptions': {'treatment': 2,
                                                   'treatments': 1,
                                                   'treatment_with_config': 1,
                                                   'treatments_with_config': 5,
                                                   'treatments_by_flag_set': 6,
                                                   'treatments_by_flag_sets': 7,
                                                   'treatments_with_config_by_flag_set': 8,
                                                   'treatments_with_config_by_flag_sets': 9,
                                                   'track': 3}})

    def test_http_errors(self, mocker):
        http_error = HTTPErrors()
        errors = http_error.pop_all() # should not raise exception
        [http_error.add_error(ModelTelemetry.HTTPExceptionsAndLatencies.SEGMENT, str(i)) for i in [500, 501, 502]]
        [http_error.add_error(ModelTelemetry.HTTPExceptionsAndLatencies.SPLIT, str(i)) for i in [400, 401, 402]]
        http_error.add_error(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION, '502')
        [http_error.add_error(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION_COUNT, str(i)) for i in [501, 502]]
        http_error.add_error(ModelTelemetry.HTTPExceptionsAndLatencies.EVENT, '501')
        http_error.add_error(ModelTelemetry.HTTPExceptionsAndLatencies.TELEMETRY, '505')
        [http_error.add_error(ModelTelemetry.HTTPExceptionsAndLatencies.TOKEN, '502') for i in range(5)]
        errors = http_error.pop_all()
        assert(errors == {'httpErrors': {'split': {'400': 1, '401': 1, '402': 1}, 'segment': {'500': 1, '501': 1, '502': 1},
                                        'impression': {'502': 1}, 'impressionCount': {'501': 1, '502': 1},
                                        'event': {'501': 1}, 'telemetry': {'505': 1}, 'token': {'502': 5}}})
        assert(http_error._split == {})
        assert(http_error._segment == {})
        assert(http_error._impression == {})
        assert(http_error._impression_count == {})
        assert(http_error._event == {})
        assert(http_error._telemetry == {})

    def test_last_synchronization(self, mocker):
        last_synchronization = LastSynchronization()
        last_synchronization.get_all() # should not raise exception
        last_synchronization.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.SPLIT, 10)
        last_synchronization.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION, 20)
        last_synchronization.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.SEGMENT, 40)
        last_synchronization.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION_COUNT, 60)
        last_synchronization.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.EVENT, 90)
        last_synchronization.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.TELEMETRY, 70)
        last_synchronization.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.TOKEN, 15)
        assert(last_synchronization.get_all() == {'lastSynchronizations': {'split': 10, 'segment': 40, 'impression': 20, 'impressionCount': 60, 'event': 90, 'telemetry': 70, 'token': 15}})

    def test_telemetry_counters(self):
        telemetry_counter = TelemetryCounters()
        assert(telemetry_counter._impressions_queued == 0)
        assert(telemetry_counter._impressions_deduped == 0)
        assert(telemetry_counter._impressions_dropped == 0)
        assert(telemetry_counter._events_dropped == 0)
        assert(telemetry_counter._events_queued == 0)
        assert(telemetry_counter._auth_rejections == 0)
        assert(telemetry_counter._token_refreshes == 0)
        assert(telemetry_counter._update_from_sse == {})

        assert(telemetry_counter.get_session_length() == 0)
        telemetry_counter.record_session_length(20)
        assert(telemetry_counter.get_session_length() == 20)

        assert(telemetry_counter.pop_auth_rejections() == 0)
        [telemetry_counter.record_auth_rejections() for i in range(5)]
        auth_rejections = telemetry_counter.pop_auth_rejections()
        assert(telemetry_counter._auth_rejections == 0)
        assert(auth_rejections == 5)

        assert(telemetry_counter.pop_token_refreshes() == 0)
        [telemetry_counter.record_token_refreshes() for i in range(3)]
        token_refreshes = telemetry_counter.pop_token_refreshes()
        assert(telemetry_counter._token_refreshes == 0)
        assert(token_refreshes == 3)

        assert(telemetry_counter.get_counter_stats(ModelTelemetry.CounterConstants.IMPRESSIONS_QUEUED) == 0)
        assert(telemetry_counter.get_counter_stats(ModelTelemetry.CounterConstants.IMPRESSIONS_DEDUPED) == 0)
        assert(telemetry_counter.get_counter_stats(ModelTelemetry.CounterConstants.IMPRESSIONS_DROPPED) == 0)
        assert(telemetry_counter.get_counter_stats(ModelTelemetry.CounterConstants.EVENTS_QUEUED) == 0)
        assert(telemetry_counter.get_counter_stats(ModelTelemetry.CounterConstants.EVENTS_DROPPED) == 0)
        telemetry_counter.record_impressions_value(ModelTelemetry.CounterConstants.IMPRESSIONS_QUEUED, 10)
        assert(telemetry_counter._impressions_queued == 10)
        telemetry_counter.record_impressions_value(ModelTelemetry.CounterConstants.IMPRESSIONS_DEDUPED, 14)
        assert(telemetry_counter._impressions_deduped == 14)
        telemetry_counter.record_impressions_value(ModelTelemetry.CounterConstants.IMPRESSIONS_DROPPED, 2)
        assert(telemetry_counter._impressions_dropped == 2)
        telemetry_counter.record_events_value(ModelTelemetry.CounterConstants.EVENTS_QUEUED, 30)
        assert(telemetry_counter._events_queued == 30)
        telemetry_counter.record_events_value(ModelTelemetry.CounterConstants.EVENTS_DROPPED, 1)
        assert(telemetry_counter._events_dropped == 1)
        telemetry_counter.record_update_from_sse(UpdateFromSSE.SPLIT_UPDATE)
        assert(telemetry_counter._update_from_sse[UpdateFromSSE.SPLIT_UPDATE.value] == 1)
        updates = telemetry_counter.pop_update_from_sse(UpdateFromSSE.SPLIT_UPDATE)
        assert(telemetry_counter._update_from_sse[UpdateFromSSE.SPLIT_UPDATE.value] == 0)
        assert(updates == 1)

    def test_streaming_event(self, mocker):
        streaming_event = StreamingEvent((ModelTelemetry.StreamingEventTypes.CONNECTION_ESTABLISHED, 'split', 1234))
        assert(streaming_event.type == ModelTelemetry.StreamingEventTypes.CONNECTION_ESTABLISHED.value)
        assert(streaming_event.data == 'split')
        assert(streaming_event.time == 1234)

    def test_streaming_events(self, mocker):
        streaming_events = StreamingEvents()
        events = streaming_events.pop_streaming_events()  # should not raise exception
        streaming_events.record_streaming_event((ModelTelemetry.StreamingEventTypes.CONNECTION_ESTABLISHED, 'split', 1234))
        streaming_events.record_streaming_event((ModelTelemetry.StreamingEventTypes.STREAMING_STATUS, 'split', 1234))
        events = streaming_events.pop_streaming_events()
        assert(streaming_events._streaming_events == [])
        assert(events == {'streamingEvents': [{'e': ModelTelemetry.StreamingEventTypes.CONNECTION_ESTABLISHED.value, 'd': 'split', 't': 1234},
                                    {'e': ModelTelemetry.StreamingEventTypes.STREAMING_STATUS.value, 'd': 'split', 't': 1234}]})

    def test_telemetry_config(self):
        telemetry_config = TelemetryConfig()
        stats = telemetry_config.get_stats() # should not raise exception
        config = {'operationMode': 'standalone',
                  'streamingEnabled': True,
                  'impressionsQueueSize': 100,
                  'eventsQueueSize': 200,
                  'impressionsMode': 'DEBUG',''
                  'impressionListener': None,
                  'featuresRefreshRate': 30,
                  'segmentsRefreshRate': 30,
                  'impressionsRefreshRate': 60,
                  'eventsPushRate': 60,
                  'metricsRefreshRate': 10,
                  'storageType': None,
                  'flagSetsFilter': None
                }
        telemetry_config.record_config(config, {}, 5, 2)
        assert(telemetry_config.get_stats() == {'oM': 0,
            'sT': telemetry_config._get_storage_type(config['operationMode'], config['storageType']),
            'sE': config['streamingEnabled'],
            'rR': {'sp': 30, 'se': 30, 'im': 60, 'ev': 60, 'te': 10},
            'uO':  {'s': False, 'e': False, 'a': False, 'st': False, 't': False},
            'iQ': config['impressionsQueueSize'],
            'eQ': config['eventsQueueSize'],
            'iM': telemetry_config._get_impressions_mode(config['impressionsMode']),
            'iL': True if config['impressionListener'] is not None else False,
            'hp': telemetry_config._check_if_proxy_detected(),
            'tR': 0,
            'nR': 0,
            'bT': 0,
            'aF': 0,
            'rF': 0,
            'fsT': 5,
            'fsI': 2}
            )

        telemetry_config.record_ready_time(10)
        assert(telemetry_config._time_until_ready == 10)

        assert(telemetry_config.get_bur_time_outs() == 0)
        [telemetry_config.record_bur_time_out() for i in range(2)]
        assert(telemetry_config.get_bur_time_outs() == 2)

        assert(telemetry_config.get_non_ready_usage() == 0)
        [telemetry_config.record_not_ready_usage() for i in range(5)]
        assert(telemetry_config.get_non_ready_usage() == 5)

        os.environ["https_proxy"] = "some_host_ip"
        assert(telemetry_config._check_if_proxy_detected() == True)

        del os.environ["https_proxy"]
        assert(telemetry_config._check_if_proxy_detected() == False)

        os.environ["HTTPS_proxy"] = "some_host_ip"
        assert(telemetry_config._check_if_proxy_detected() == True)

        del os.environ["HTTPS_proxy"]
        assert(telemetry_config._check_if_proxy_detected() == False)

class TelemetryModelAsyncTests(object):
    """Telemetry model async test cases."""

    @pytest.mark.asyncio
    async def test_method_latencies(self, mocker):
        method_latencies = await MethodLatenciesAsync.create()

        for method in ModelTelemetry.MethodExceptionsAndLatencies:
            await method_latencies.add_latency(method, 50)
            if method.value == 'treatment':
                assert(method_latencies._treatment[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            elif method.value == 'treatments':
                assert(method_latencies._treatments[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            elif method.value == 'treatment_with_config':
                assert(method_latencies._treatment_with_config[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            elif method.value == 'treatments_with_config':
                assert(method_latencies._treatments_with_config[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            elif method.value == 'treatments_by_flag_set':
                assert(method_latencies._treatments_by_flag_set[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            elif method.value == 'treatments_by_flag_sets':
                assert(method_latencies._treatments_by_flag_sets[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            elif method.value == 'treatments_with_config_by_flag_set':
                assert(method_latencies._treatments_with_config_by_flag_set[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            elif method.value == 'treatments_with_config_by_flag_sets':
                assert(method_latencies._treatments_with_config_by_flag_sets[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            elif method.value == 'track':
                assert(method_latencies._track[ModelTelemetry.get_latency_bucket_index(50)] == 1)

            await method_latencies.add_latency(method, 50000000)
            if method.value == 'treatment':
                assert(method_latencies._treatment[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            if method.value == 'treatments':
                assert(method_latencies._treatments[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            if method.value == 'treatment_with_config':
                assert(method_latencies._treatment_with_config[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            if method.value == 'treatments_with_config':
                assert(method_latencies._treatments_with_config[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            elif method.value == 'treatments_by_flag_set':
                assert(method_latencies._treatments_by_flag_set[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            elif method.value == 'treatments_by_flag_sets':
                assert(method_latencies._treatments_by_flag_sets[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            elif method.value == 'treatments_with_config_by_flag_set':
                assert(method_latencies._treatments_with_config_by_flag_set[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            elif method.value == 'treatments_with_config_by_flag_sets':
                assert(method_latencies._treatments_with_config_by_flag_sets[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            if method.value == 'track':
                assert(method_latencies._track[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)

        await method_latencies.pop_all()
        assert(method_latencies._track == [0] * 23)
        assert(method_latencies._treatment == [0] * 23)
        assert(method_latencies._treatments == [0] * 23)
        assert(method_latencies._treatment_with_config == [0] * 23)
        assert(method_latencies._treatments_with_config == [0] * 23)
        assert(method_latencies._treatments_by_flag_set == [0] * 23)
        assert(method_latencies._treatments_by_flag_sets == [0] * 23)
        assert(method_latencies._treatments_with_config_by_flag_set == [0] * 23)
        assert(method_latencies._treatments_with_config_by_flag_sets == [0] * 23)

        await method_latencies.add_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT, 10)
        [await method_latencies.add_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS, 20) for i in range(2)]
        await method_latencies.add_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG, 50)
        await method_latencies.add_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG, 20)
        [await method_latencies.add_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET, 20) for i in range(3)]
        [await method_latencies.add_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS, 20) for i in range(4)]
        [await method_latencies.add_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET, 20) for i in range(5)]
        [await method_latencies.add_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS, 20) for i in range(6)]
        await method_latencies.add_latency(ModelTelemetry.MethodExceptionsAndLatencies.TRACK, 20)
        latencies = await method_latencies.pop_all()
        assert(latencies == {'methodLatencies': {'treatment': [1] + [0] * 22,
                                                 'treatments': [2] + [0] * 22,
                                                 'treatment_with_config': [1] + [0] * 22,
                                                 'treatments_with_config': [1] + [0] * 22,
                                                 'treatments_by_flag_set': [3] + [0] * 22,
                                                 'treatments_by_flag_sets': [4] + [0] * 22,
                                                 'treatments_with_config_by_flag_set': [5] + [0] * 22,
                                                 'treatments_with_config_by_flag_sets': [6] + [0] * 22,
                                                 'track': [1] + [0] * 22}})

    @pytest.mark.asyncio
    async def test_http_latencies(self, mocker):
        http_latencies = await HTTPLatenciesAsync.create()

        for resource in ModelTelemetry.HTTPExceptionsAndLatencies:
            if self._get_http_latency(resource, http_latencies) == None:
                continue
            await http_latencies.add_latency(resource, 50)
            assert(self._get_http_latency(resource, http_latencies)[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            await http_latencies.add_latency(resource, 50000000)
            assert(self._get_http_latency(resource, http_latencies)[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            for j in range(10):
                latency = random.randint(1001, 4987885)
                current_count = self._get_http_latency(resource, http_latencies)[ModelTelemetry.get_latency_bucket_index(latency)]
                [await http_latencies.add_latency(resource, latency) for i in range(2)]
                assert(self._get_http_latency(resource, http_latencies)[ModelTelemetry.get_latency_bucket_index(latency)] == 2 + current_count)

        await http_latencies.pop_all()
        assert(http_latencies._event == [0] * 23)
        assert(http_latencies._impression == [0] * 23)
        assert(http_latencies._impression_count == [0] * 23)
        assert(http_latencies._segment == [0] * 23)
        assert(http_latencies._split == [0] * 23)
        assert(http_latencies._telemetry == [0] * 23)
        assert(http_latencies._token == [0] * 23)

        await http_latencies.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.SPLIT, 10)
        [await http_latencies.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION, i) for i in [10, 20]]
        await http_latencies.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.SEGMENT, 40)
        await http_latencies.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION_COUNT, 60)
        await http_latencies.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.EVENT, 90)
        await http_latencies.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.TELEMETRY, 70)
        [await http_latencies.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.TOKEN, i) for i in [10, 15]]
        latencies = await http_latencies.pop_all()
        assert(latencies == {'httpLatencies': {'split': [1] + [0] * 22, 'segment': [1] + [0] * 22, 'impression': [2] + [0] * 22, 'impressionCount': [1] + [0] * 22, 'event': [1] + [0] * 22, 'telemetry': [1] + [0] * 22, 'token': [2] + [0] * 22}})

    def _get_http_latency(self, resource, storage):
        if resource == ModelTelemetry.HTTPExceptionsAndLatencies.SPLIT:
            return storage._split
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.SEGMENT:
            return storage._segment
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION:
            return storage._impression
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION_COUNT:
            return storage._impression_count
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.EVENT:
            return storage._event
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.TELEMETRY:
            return storage._telemetry
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.TOKEN:
            return storage._token
        else:
            return

    @pytest.mark.asyncio
    async def test_method_exceptions(self, mocker):
        method_exception = await MethodExceptionsAsync.create()

        [await method_exception.add_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT) for i in range(2)]
        await method_exception.add_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS)
        await method_exception.add_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG)
        [await method_exception.add_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG) for i in range(5)]
        [await method_exception.add_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET) for i in range(6)]
        [await method_exception.add_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS) for i in range(7)]
        [await method_exception.add_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET) for i in range(8)]
        [await method_exception.add_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS) for i in range(9)]
        [await method_exception.add_exception(ModelTelemetry.MethodExceptionsAndLatencies.TRACK) for i in range(3)]
        exceptions = await method_exception.pop_all()

        assert(method_exception._treatment == 0)
        assert(method_exception._treatments == 0)
        assert(method_exception._treatment_with_config == 0)
        assert(method_exception._treatments_with_config == 0)
        assert(method_exception._treatments_by_flag_set == 0)
        assert(method_exception._treatments_by_flag_sets == 0)
        assert(method_exception._treatments_with_config_by_flag_set == 0)
        assert(method_exception._treatments_with_config_by_flag_sets == 0)
        assert(method_exception._track == 0)
        assert(exceptions == {'methodExceptions': {'treatment': 2,
                                                   'treatments': 1,
                                                   'treatment_with_config': 1,
                                                   'treatments_with_config': 5,
                                                   'treatments_by_flag_set': 6,
                                                   'treatments_by_flag_sets': 7,
                                                   'treatments_with_config_by_flag_set': 8,
                                                   'treatments_with_config_by_flag_sets': 9,
                                                   'track': 3}})

    @pytest.mark.asyncio
    async def test_http_errors(self, mocker):
        http_error = await HTTPErrorsAsync.create()
        [await http_error.add_error(ModelTelemetry.HTTPExceptionsAndLatencies.SEGMENT, str(i)) for i in [500, 501, 502]]
        [await http_error.add_error(ModelTelemetry.HTTPExceptionsAndLatencies.SPLIT, str(i)) for i in [400, 401, 402]]
        await http_error.add_error(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION, '502')
        [await http_error.add_error(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION_COUNT, str(i)) for i in [501, 502]]
        await http_error.add_error(ModelTelemetry.HTTPExceptionsAndLatencies.EVENT, '501')
        await http_error.add_error(ModelTelemetry.HTTPExceptionsAndLatencies.TELEMETRY, '505')
        [await http_error.add_error(ModelTelemetry.HTTPExceptionsAndLatencies.TOKEN, '502') for i in range(5)]
        errors = await http_error.pop_all()
        assert(errors == {'httpErrors': {'split': {'400': 1, '401': 1, '402': 1}, 'segment': {'500': 1, '501': 1, '502': 1},
                                        'impression': {'502': 1}, 'impressionCount': {'501': 1, '502': 1},
                                        'event': {'501': 1}, 'telemetry': {'505': 1}, 'token': {'502': 5}}})
        assert(http_error._split == {})
        assert(http_error._segment == {})
        assert(http_error._impression == {})
        assert(http_error._impression_count == {})
        assert(http_error._event == {})
        assert(http_error._telemetry == {})

    @pytest.mark.asyncio
    async def test_last_synchronization(self, mocker):
        last_synchronization = await LastSynchronizationAsync.create()
        await last_synchronization.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.SPLIT, 10)
        await last_synchronization.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION, 20)
        await last_synchronization.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.SEGMENT, 40)
        await last_synchronization.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION_COUNT, 60)
        await last_synchronization.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.EVENT, 90)
        await last_synchronization.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.TELEMETRY, 70)
        await last_synchronization.add_latency(ModelTelemetry.HTTPExceptionsAndLatencies.TOKEN, 15)
        assert(await last_synchronization.get_all() == {'lastSynchronizations': {'split': 10, 'segment': 40, 'impression': 20, 'impressionCount': 60, 'event': 90, 'telemetry': 70, 'token': 15}})

    @pytest.mark.asyncio
    async def test_telemetry_counters(self):
        telemetry_counter = await TelemetryCountersAsync.create()
        assert(telemetry_counter._impressions_queued == 0)
        assert(telemetry_counter._impressions_deduped == 0)
        assert(telemetry_counter._impressions_dropped == 0)
        assert(telemetry_counter._events_dropped == 0)
        assert(telemetry_counter._events_queued == 0)
        assert(telemetry_counter._auth_rejections == 0)
        assert(telemetry_counter._token_refreshes == 0)
        assert(telemetry_counter._update_from_sse == {})

        await telemetry_counter.record_session_length(20)
        assert(await telemetry_counter.get_session_length() == 20)

        [await telemetry_counter.record_auth_rejections() for i in range(5)]
        auth_rejections = await telemetry_counter.pop_auth_rejections()
        assert(telemetry_counter._auth_rejections == 0)
        assert(auth_rejections == 5)

        [await telemetry_counter.record_token_refreshes() for i in range(3)]
        token_refreshes = await telemetry_counter.pop_token_refreshes()
        assert(telemetry_counter._token_refreshes == 0)
        assert(token_refreshes == 3)

        await telemetry_counter.record_impressions_value(ModelTelemetry.CounterConstants.IMPRESSIONS_QUEUED, 10)
        assert(telemetry_counter._impressions_queued == 10)
        await telemetry_counter.record_impressions_value(ModelTelemetry.CounterConstants.IMPRESSIONS_DEDUPED, 14)
        assert(telemetry_counter._impressions_deduped == 14)
        await telemetry_counter.record_impressions_value(ModelTelemetry.CounterConstants.IMPRESSIONS_DROPPED, 2)
        assert(telemetry_counter._impressions_dropped == 2)
        await telemetry_counter.record_events_value(ModelTelemetry.CounterConstants.EVENTS_QUEUED, 30)
        assert(telemetry_counter._events_queued == 30)
        await telemetry_counter.record_events_value(ModelTelemetry.CounterConstants.EVENTS_DROPPED, 1)
        assert(telemetry_counter._events_dropped == 1)
        await telemetry_counter.record_update_from_sse(UpdateFromSSE.SPLIT_UPDATE)
        assert(telemetry_counter._update_from_sse[UpdateFromSSE.SPLIT_UPDATE.value] == 1)
        updates = await telemetry_counter.pop_update_from_sse(UpdateFromSSE.SPLIT_UPDATE)
        assert(telemetry_counter._update_from_sse[UpdateFromSSE.SPLIT_UPDATE.value] == 0)
        assert(updates == 1)

    @pytest.mark.asyncio
    async def test_streaming_events(self, mocker):
        streaming_events = await StreamingEventsAsync.create()
        await streaming_events.record_streaming_event((ModelTelemetry.StreamingEventTypes.CONNECTION_ESTABLISHED, 'split', 1234))
        await streaming_events.record_streaming_event((ModelTelemetry.StreamingEventTypes.STREAMING_STATUS, 'split', 1234))
        events = await streaming_events.pop_streaming_events()
        assert(streaming_events._streaming_events == [])
        assert(events == {'streamingEvents': [{'e': ModelTelemetry.StreamingEventTypes.CONNECTION_ESTABLISHED.value, 'd': 'split', 't': 1234},
                                    {'e': ModelTelemetry.StreamingEventTypes.STREAMING_STATUS.value, 'd': 'split', 't': 1234}]})

    @pytest.mark.asyncio
    async def test_telemetry_config(self):
        telemetry_config = await TelemetryConfigAsync.create()
        config = {'operationMode': 'standalone',
                  'streamingEnabled': True,
                  'impressionsQueueSize': 100,
                  'eventsQueueSize': 200,
                  'impressionsMode': 'DEBUG',''
                  'impressionListener': None,
                  'featuresRefreshRate': 30,
                  'segmentsRefreshRate': 30,
                  'impressionsRefreshRate': 60,
                  'eventsPushRate': 60,
                  'metricsRefreshRate': 10,
                  'storageType': None,
                  'flagSetsFilter': None
                }
        await telemetry_config.record_config(config, {}, 5, 2)
        assert(await telemetry_config.get_stats() == {'oM': 0,
            'sT': telemetry_config._get_storage_type(config['operationMode'], config['storageType']),
            'sE': config['streamingEnabled'],
            'rR': {'sp': 30, 'se': 30, 'im': 60, 'ev': 60, 'te': 10},
            'uO':  {'s': False, 'e': False, 'a': False, 'st': False, 't': False},
            'iQ': config['impressionsQueueSize'],
            'eQ': config['eventsQueueSize'],
            'iM': telemetry_config._get_impressions_mode(config['impressionsMode']),
            'iL': True if config['impressionListener'] is not None else False,
            'hp': telemetry_config._check_if_proxy_detected(),
            'tR': 0,
            'nR': 0,
            'bT': 0,
            'aF': 0,
            'rF': 0,
            'fsT': 5,
            'fsI': 2}
            )

        await telemetry_config.record_ready_time(10)
        assert(telemetry_config._time_until_ready == 10)

        [await telemetry_config.record_bur_time_out() for i in range(2)]
        assert(await telemetry_config.get_bur_time_outs() == 2)

        [await telemetry_config.record_not_ready_usage() for i in range(5)]
        assert(await telemetry_config.get_non_ready_usage() == 5)
