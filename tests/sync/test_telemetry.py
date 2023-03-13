"""Telemetry Worker tests."""
import unittest.mock as mock
import json
from splitio.sync.telemetry import TelemetrySynchronizer, InMemoryTelemetrySubmitter
from splitio.engine.telemetry import TelemetryEvaluationConsumer, TelemetryInitConsumer, TelemetryRuntimeConsumer, TelemetryStorageConsumer
from splitio.storage.inmemmory import InMemoryTelemetryStorage, InMemorySegmentStorage, InMemorySplitStorage
from splitio.models.splits import Split, Status
from splitio.models.segments import Segment
from splitio.models.telemetry import StreamingEvents
from splitio.api.telemetry import TelemetryAPI

class TelemetrySynchronizerTests(object):
    """Telemetry synchronizer test cases."""

    @mock.patch('splitio.sync.telemetry.InMemoryTelemetrySubmitter.synchronize_config')
    def test_synchronize_config(self, mocker):
        telemetry_synchronizer = TelemetrySynchronizer(InMemoryTelemetrySubmitter(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock()))
        telemetry_synchronizer.synchronize_config()
        assert(mocker.called)

    @mock.patch('splitio.sync.telemetry.InMemoryTelemetrySubmitter.synchronize_stats')
    def test_synchronize_stats(self, mocker):
        telemetry_synchronizer = TelemetrySynchronizer(InMemoryTelemetrySubmitter(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock()))
        telemetry_synchronizer.synchronize_stats()
        assert(mocker.called)

class TelemetrySubmitterTests(object):
    """Telemetry submitter test cases."""

    def test_synchronize_telemetry(self, mocker):
        api = mocker.Mock(spec=TelemetryAPI)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        split_storage = InMemorySplitStorage()
        split_storage.put(Split('split1', 1234, 1, False, 'user', Status.ACTIVE, 123))
        segment_storage = InMemorySegmentStorage()
        segment_storage.put(Segment('segment1', [], 123))
        telemetry_submitter = InMemoryTelemetrySubmitter(telemetry_consumer, split_storage, segment_storage, api)

        telemetry_storage._counters._impressions_queued = 100
        telemetry_storage._counters._impressions_deduped = 30
        telemetry_storage._counters._impressions_dropped = 0
        telemetry_storage._counters._events_queued = 20
        telemetry_storage._counters._events_dropped = 10
        telemetry_storage._counters._auth_rejections = 1
        telemetry_storage._counters._token_refreshes = 3
        telemetry_storage._counters._session_length = 3

        telemetry_storage._method_exceptions._treatment =  10
        telemetry_storage._method_exceptions._treatments = 1
        telemetry_storage._method_exceptions._treatment_with_config = 5
        telemetry_storage._method_exceptions._treatments_with_config = 1
        telemetry_storage._method_exceptions._track = 3

        telemetry_storage._last_synchronization._split = 5
        telemetry_storage._last_synchronization._segment = 3
        telemetry_storage._last_synchronization._impression = 10
        telemetry_storage._last_synchronization._impression_count = 0
        telemetry_storage._last_synchronization._event = 4
        telemetry_storage._last_synchronization._telemetry = 0
        telemetry_storage._last_synchronization._token = 3

        telemetry_storage._http_sync_errors._split = {'500': 3, '501': 2}
        telemetry_storage._http_sync_errors._segment = {'401': 1}
        telemetry_storage._http_sync_errors._impression = {'500': 1}
        telemetry_storage._http_sync_errors._impression_count = {'401': 5}
        telemetry_storage._http_sync_errors._event = {'404': 10}
        telemetry_storage._http_sync_errors._telemetry = {'501': 3}
        telemetry_storage._http_sync_errors._token = {'505': 11}

        telemetry_storage._streaming_events = StreamingEvents()
        telemetry_storage._tags = ['tag1']

        telemetry_storage._method_latencies._treatment = [1] + [0] * 22
        telemetry_storage._method_latencies._treatments = [0] * 23
        telemetry_storage._method_latencies._treatment_with_config = [0] * 23
        telemetry_storage._method_latencies._treatments_with_config = [0] * 23
        telemetry_storage._method_latencies._track = [0] * 23

        telemetry_storage._http_latencies._split = [1] + [0] * 22
        telemetry_storage._http_latencies._segment = [0] * 23
        telemetry_storage._http_latencies._impression = [0] * 23
        telemetry_storage._http_latencies._impression_count = [0] * 23
        telemetry_storage._http_latencies._event = [0] * 23
        telemetry_storage._http_latencies._telemetry = [0] * 23
        telemetry_storage._http_latencies._token =  [0] * 23

        telemetry_storage.record_config({'operationMode': 'inmemory',
                                         'storageType': None,
                                        'streamingEnabled': True,
                                        'impressionsQueueSize': 100,
                                        'eventsQueueSize': 200,
                                        'impressionsMode': 'DEBUG',
                                        'impressionListener': None,
                                        'featuresRefreshRate': 30,
                                        'segmentsRefreshRate': 30,
                                        'impressionsRefreshRate': 60,
                                        'eventsPushRate': 60,
                                        'metricsRefreshRate': 10,
                                        'activeFactoryCount': 1,
                                        'notReady': 0,
                                        'timeUntilReady': 1
                                       }, {}
        )
        self.formatted_config = ""
        def record_init(*args, **kwargs):
            self.formatted_config = args[0]

        api.record_init.side_effect = record_init
        telemetry_submitter.synchronize_config()
        assert(self.formatted_config == telemetry_submitter._telemetry_init_consumer.get_config_stats())

        def record_stats(*args, **kwargs):
            self.formatted_stats = args[0]

        api.record_stats.side_effect = record_stats
        telemetry_submitter.synchronize_stats()
        assert(self.formatted_stats == {
            "iQ": 100,
            "iDe": 30,
            "iDr": 0,
            "eQ": 20,
            "eD": 10,
            "lS": {"sp": 5, "se": 3, "im": 10, "ic": 0, "ev": 4, "te": 0, "to": 3},
            "t": ["tag1"],
            "hE": {"sp": {"500": 3, "501": 2}, "se": {"401": 1}, "im": {"500": 1}, "ic": {"401": 5}, "ev": {"404": 10}, "te": {"501": 3}, "to": {"505": 11}},
            "hL": {"sp": [1] + [0] * 22, "se": [0] * 23, "im": [0] * 23, "ic": [0] * 23, "ev": [0] * 23, "te": [0] * 23, "to": [0] * 23},
            "aR": 1,
            "tR": 3,
            "sE": [],
            "sL": 3,
            "mE": {"t": 10, "ts": 1, "tc": 5, "tcs": 1, "tr": 3},
            "mL": {"t": [1] + [0] * 22, "ts": [0] * 23, "tc": [0] * 23, "tcs": [0] * 23, "tr": [0] * 23},
            "spC": 1,
            "seC": 1,
            "skC": 0,
            "t": ['tag1']
        })
