"""Telemetry model test module."""
import os

from splitio.models.telemetry import StorageType, OperationMode, MethodLatencies, MethodExceptions, \
    HTTPLatencies, HTTPErrors, LastSynchronization, TelemetryCounters, TelemetryConfig, \
    StreamingEvent, StreamingEvents, RefreshRates, URLOverrides

class TelemetryModelTests(object):
    """Telemetry model test cases."""

    def test_storage_type_and_operation_mode(self, mocker):
        assert(StorageType.LOCALHOST == 'localhost')
        assert(StorageType.MEMEORY == 'memory')
        assert(StorageType.REDIS == 'redis')
        assert(OperationMode.MEMEORY == 'in-memory')
        assert(OperationMode.REDIS == 'redis-consumer')

    def test_nethod_latencies(self, mocker):
        method_latencies = MethodLatencies()
        method_latencies.add_latency('treatment', 10)
        assert(method_latencies._treatment == [10])
        [method_latencies.add_latency('treatment', 10) for i in range(25)]
        assert(len(method_latencies._treatment) == 23)

        [method_latencies.add_latency('treatments', i) for i in [20, 30]]
        assert(method_latencies._treatments == [20, 30])
        [method_latencies.add_latency('treatments', 10) for i in range(25)]
        assert(len(method_latencies._treatments) == 23)

        method_latencies.add_latency('treatmentWithConfig', 50)
        assert(method_latencies._treatment_with_config == [50])
        [method_latencies.add_latency('treatmentWithConfig', 10) for i in range(25)]
        assert(len(method_latencies._treatment_with_config) == 23)

        method_latencies.add_latency('treatmentsWithConfig', 20)
        assert(method_latencies._treatments_with_config == [20])
        [method_latencies.add_latency('treatmentsWithConfig', 10) for i in range(25)]
        assert(len(method_latencies._treatments_with_config) == 23)

        method_latencies.add_latency('track', 20)
        assert(method_latencies._track == [20])
        [method_latencies.add_latency('track', 10) for i in range(25)]
        assert(len(method_latencies._track) == 23)

        method_latencies.pop_all()
        assert(method_latencies._track == [])
        assert(method_latencies._treatment == [])
        assert(method_latencies._treatments == [])
        assert(method_latencies._treatment_with_config == [])
        assert(method_latencies._treatments_with_config == [])

        method_latencies.add_latency('treatment', 10)
        method_latencies.add_latency('treatments', 20)
        method_latencies.add_latency('treatments', 30)
        method_latencies.add_latency('treatmentWithConfig', 50)
        method_latencies.add_latency('treatmentsWithConfig', 20)
        method_latencies.add_latency('track', 20)
        method_latencies.add_latency('track', 60)
        latencies = method_latencies.pop_all()
        assert(latencies == {'methodLatencies': {'treatment': [10], 'treatments': [20, 30], 'treatmentWithConfig': [50], 'treatmentsWithConfig': [20], 'track': [20, 60]}})

    def test_http_latencies(self, mocker):
        http_latencies = HTTPLatencies()

        http_latencies.add_latency('split', 10)
        assert(http_latencies._split == [10])
        [http_latencies.add_latency('split', 10) for i in range(25)]
        assert(len(http_latencies._split) == 23)

        http_latencies.add_latency('segment', 10)
        assert(http_latencies._segment == [10])
        [http_latencies.add_latency('segment', 10) for i in range(25)]
        assert(len(http_latencies._segment) == 23)

        http_latencies.add_latency('impression', 10)
        assert(http_latencies._impression == [10])
        [http_latencies.add_latency('impression', 10) for i in range(25)]
        assert(len(http_latencies._impression) == 23)

        http_latencies.add_latency('impressionCount', 10)
        assert(http_latencies._impression_count == [10])
        [http_latencies.add_latency('impressionCount', 10) for i in range(25)]
        assert(len(http_latencies._impression_count) == 23)

        http_latencies.add_latency('telemetry', 10)
        assert(http_latencies._telemetry == [10])
        [http_latencies.add_latency('telemetry', 10) for i in range(25)]
        assert(len(http_latencies._telemetry) == 23)

        http_latencies.add_latency('token', 10)
        assert(http_latencies._token == [10])
        [http_latencies.add_latency('token', 10) for i in range(25)]
        assert(len(http_latencies._token) == 23)

        http_latencies.pop_all()
        assert(http_latencies._event == [])
        assert(http_latencies._impression == [])
        assert(http_latencies._impression_count == [])
        assert(http_latencies._segment == [])
        assert(http_latencies._split == [])
        assert(http_latencies._telemetry == [])
        assert(http_latencies._token == [])


        http_latencies.add_latency('split', 10)
        [http_latencies.add_latency('impression', i) for i in [10, 20]]
        http_latencies.add_latency('segment', 40)
        http_latencies.add_latency('impressionCount', 60)
        http_latencies.add_latency('event', 90)
        http_latencies.add_latency('telemetry', 70)
        [http_latencies.add_latency('token', i) for i in [10, 15]]
        latencies = http_latencies.pop_all()
        assert(latencies == {'httpLatencies': {'split': [10], 'segment': [40], 'impression': [10, 20], 'impressionCount': [60], 'event': [90], 'telemetry': [70], 'token': [10, 15]}})

    def test_method_exceptions(self, mocker):
        method_exception = MethodExceptions()

        [method_exception.add_exception('treatment') for i in range(2)]
        method_exception.add_exception('treatments')
        method_exception.add_exception('treatmentWithConfig')
        [method_exception.add_exception('treatmentsWithConfig') for i in range(5)]
        [method_exception.add_exception('track') for i in range(3)]
        exceptions = method_exception.pop_all()

        assert(method_exception._treatment == 0)
        assert(method_exception._treatments == 0)
        assert(method_exception._treatment_with_config == 0)
        assert(method_exception._treatments_with_config == 0)
        assert(method_exception._track == 0)
        assert(exceptions == {'methodExceptions': {'treatment': 2, 'treatments': 1, 'treatmentWithConfig': 1, 'treatmentsWithConfig': 5, 'track': 3}})

    def test_http_errors(self, mocker):
        http_error = HTTPErrors()
        [http_error.add_error('segment', str(i)) for i in [500, 501, 502]]
        [http_error.add_error('split', str(i)) for i in [400, 401, 402]]
        http_error.add_error('impression', '502')
        [http_error.add_error('impressionCount', str(i)) for i in [501, 502]]
        http_error.add_error('event', '501')
        http_error.add_error('telemetry', '505')
        [http_error.add_error('token', '502') for i in range(5)]
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
        last_synchronization.add_latency('split', 10)
        last_synchronization.add_latency('impression', 20)
        last_synchronization.add_latency('segment', 40)
        last_synchronization.add_latency('impressionCount', 60)
        last_synchronization.add_latency('event', 90)
        last_synchronization.add_latency('telemetry', 70)
        last_synchronization.add_latency('token', 15)
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

        telemetry_counter.set_session_length(20)
        assert(telemetry_counter.get_session_length() == 20)

        [telemetry_counter.append_auth_rejections() for i in range(5)]
        auth_rejections = telemetry_counter.pop_auth_rejections()
        assert(telemetry_counter._auth_rejections == 0)
        assert(auth_rejections == 5)

        [telemetry_counter.append_token_refreshes() for i in range(3)]
        token_refreshes = telemetry_counter.pop_token_refreshes()
        assert(telemetry_counter._token_refreshes == 0)
        assert(token_refreshes == 3)

        telemetry_counter.set_value('impressionsQueued', 10)
        assert(telemetry_counter._impressions_queued == 10)
        telemetry_counter.set_value('impressionsDeduped', 14)
        assert(telemetry_counter._impressions_deduped == 14)
        telemetry_counter.set_value('impressionsDropped', 2)
        assert(telemetry_counter._impressions_dropped == 2)
        telemetry_counter.set_value('eventsQueued', 30)
        assert(telemetry_counter._events_queued == 30)
        telemetry_counter.set_value('eventsDropped', 1)
        assert(telemetry_counter._events_dropped == 1)

    def test_streaming_event(self, mocker):
        streaming_event = StreamingEvent({'type': 'update', 'data': 'split', 'time': 1234})
        assert(streaming_event.type == 'update')
        assert(streaming_event.data == 'split')
        assert(streaming_event.time == 1234)

    def test_streaming_events(self, mocker):
        streaming_events = StreamingEvents()
        streaming_events.record_streaming_event({'type': 'update', 'data': 'split', 'time': 1234})
        streaming_events.record_streaming_event({'type': 'delete', 'data': 'split', 'time': 1234})
        events = streaming_events.pop_streaming_events()
        assert(streaming_events._streaming_events == [])
        assert(events == {'streamingEvents': [{'e': 'update', 'd': 'split', 't': 1234},
                                    {'e': 'delete', 'd': 'split', 't': 1234}]})

    def test_refresh_rates(self):
        refresh_rates = RefreshRates(30, 60, 40, 100, 120)
        assert(refresh_rates.splits == 30)
        assert(refresh_rates.segments == 60)
        assert(refresh_rates.impressions == 40)
        assert(refresh_rates.events == 100)
        assert(refresh_rates.telemetry == 120)

    def test_url_overrides(self):
        url_overrides = URLOverrides(True, True, False, False, True)
        assert(url_overrides.sdk == True)
        assert(url_overrides.events == True)
        assert(url_overrides.auth == False)
        assert(url_overrides.streaming == False)
        assert(url_overrides.telemetry == True)

    def test_telemetry_config(self):
        telemetry_config = TelemetryConfig()
        config = {'operationMode': 'inmemory',
                  'streamingEnabled': True,
                  'impressionsQueueSize': 100,
                  'eventsQueueSize': 200,
                  'impressionsMode': 'DEBUG',''
                  'impressionListener': None,
                  'featuresRefreshRate': 30,
                  'segmentsRefreshRate': 30,
                  'impressionsRefreshRate': 60,
                  'eventsPushRate': 60,
                  'metrcsRefreshRate': 10,
                  'activeFactoryCount': 1,
                  'redundantFactoryCount': 0
                  }
        telemetry_config.record_config(config)
        assert(telemetry_config.get_stats() == {'operationMode': 2,
            'storageType': telemetry_config._get_storage_type(config['operationMode']),
            'streamingEnabled': config['streamingEnabled'],
            'refreshRate': {'sp': 30, 'se': 30, 'im': 60, 'ev': 60, 'te': 10},
            'urlOverride':  {'s': False, 'e': False, 'a': False, 'st': False, 't': False},
            'impressionsQueueSize': config['impressionsQueueSize'],
            'eventsQueueSize': config['eventsQueueSize'],
            'impressionsMode': telemetry_config._get_impressions_mode(config['impressionsMode']),
            'impressionListener': True if config['impressionListener'] is not None else False,
            'httpProxy': telemetry_config._check_if_proxy_detected(),
            'blockUntilReadyTimeout': 0,
            'timeUntilReady': 0,
            'notReady': 0,
            'activeFactoryCount': 1,
            'redundantFactoryCount': 0}
            )

        telemetry_config.record_ready_time(10)
        assert(telemetry_config._time_until_ready == 10)

        [telemetry_config.record_bur_time_out() for i in range(2)]
        assert(telemetry_config.get_bur_time_outs() == 2)

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