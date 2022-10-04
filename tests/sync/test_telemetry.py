"""Telemetry Worker tests."""
import unittest.mock as mock
import json

from splitio.sync.telemetry import TelemetrySynchronizer, TelemetrySubmitter
from splitio.engine.telemetry import TelemetryEvaluationConsumer, TelemetryInitConsumer, TelemetryRuntimeConsumer, TelemetryStorageConsumer
from splitio.storage.inmemmory import InMemoryTelemetryStorage, InMemorySegmentStorage, InMemorySplitStorage
from splitio.models.splits import Split, Status
from splitio.models.segments import Segment

class TelemetrySynchronizerTests(object):
    """Telemetry synchronizer test cases."""

    @mock.patch('splitio.sync.telemetry.TelemetrySubmitter.synchronize_config')
    def test_synchronize_config(self, mocker):
        telemetry_synchronizer = TelemetrySynchronizer(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())
        telemetry_synchronizer.synchronize_config()
        assert(mocker.called)

    @mock.patch('splitio.sync.telemetry.TelemetrySubmitter.synchronize_stats')
    def test_synchronize_stats(self, mocker):
        telemetry_synchronizer = TelemetrySynchronizer(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())
        telemetry_synchronizer.synchronize_stats()
        assert(mocker.called)

class TelemetrySubmitterTests(object):
    """Telemetry submitter test cases."""

    def test_synchronize_telemetry(self, mocker):
        api = mocker.Mock()
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        split_storage = InMemorySplitStorage()
        split_storage.put(Split('split1', 1234, 1, False, 'user', Status.ACTIVE, 123))
        segment_storage = InMemorySegmentStorage()
        segment_storage.put(Segment('segment1', [], 123))
        telemetry_submitter = TelemetrySubmitter(telemetry_consumer, split_storage, segment_storage, api)

        telemetry_storage._counters = {'impressionsQueued': 1, 'impressionsDeduped': 0, 'impressionsDropped': 3,
                                        'eventsQueued': 0, 'eventsDropped': 10,
                                        'authRejections': 1, 'tokenRefreshes': 3}
        telemetry_storage._exceptions = {'methodExceptions': {'treatment': 1, 'treatments': 0,
                                        'treatmentWithConfig': 5, 'treatmentsWithConfig': 0, 'track': 3}}
        telemetry_storage._records = {'lastSynchronizations': {'split': 5, 'segment': 3,
                                        'impression': 10, 'impressionCount': 0, 'event': 4,
                                        'telemetry': 0, 'token': 3},'sessionLength': 3}
        telemetry_storage._http_errors = {'split': {'500': 3}, 'segment': {}, 'impression': {}, 'impressionCount': {}, 'event': {}, 'telemetry': {}, 'token': {}}
        telemetry_storage._config = {'blockUntilReadyTimeout': 10, 'notReady': 0, 'timeUntilReady': 1}
        telemetry_storage._streaming_events = []
        telemetry_storage._tags = ['tag1']
        telemetry_storage._integrations = {}
        telemetry_storage._latencies = {'methodLatencies': {'treatment': [10, 20], 'treatments': [50], 'treatmentWithConfig': [], 'treatmentsWithConfig': [], 'track': []},
                           'httpLatencies': {'split': [200, 300], 'segment': [400], 'impression': [], 'impressionCount': [200], 'event': [], 'telemetry': [], 'token': []}}

        telemetry_storage.record_config({'operationMode': 'inmemory',
                                        'streamingEnabled': True,
                                        'impressionsQueueSize': 100,
                                        'eventsQueueSize': 200,
                                        'impressionsMode': 'DEBUG',
                                        'impressionListener': None,
                                        'featuresRefreshRate': 30,
                                        'segmentsRefreshRate': 30,
                                        'impressionsRefreshRate': 60,
                                        'eventsPushRate': 60,
                                        'metrcsRefreshRate': 10,
                                        'activeFactoryCount': 1,
                                        'redundantFactoryCount': 0
                                       }
        )
        def record_init(*args, **kwargs):
            self.formatted_config = args[0]

        api.record_init.side_effect = record_init
        telemetry_submitter.synchronize_config()
        assert(self.formatted_config == telemetry_submitter._telemetry_init_consumer.get_config_stats_to_json())

        def record_stats(*args, **kwargs):
            self.formatted_stats = args[0]

        api.record_stats.side_effect = record_stats
        telemetry_submitter.synchronize_stats()
        assert(self.formatted_stats == json.dumps({
            "iQ": 1,
            "iDe": 0,
            "iDr": 3,
            "eQ": 0,
            "eD": 10,
            "lS": {"sp": 5, "se": 3, "im": 10, "ic": 0, "ev": 4, "te": 0, "to": 3},
            "t": ['tag1'],
            "hE": {"sp": {'500': 3}, "se": {}, "im": {}, "ic": {}, "ev": {}, "te": {}, "to": {}},
            "hL": {"sp": [200, 300], "se": [400], "im": [], "ic": [200], "ev": [], "te": [], "to": []},
            "aR": 1,
            "tR": 3,
            "sE": [],
            "sL": 3,
            "mE": {"t": 1, "ts": 0, "tc": 5, "tcs": 0, "tr": 3},
            "mL": {"t": [10, 20], "ts": [50], "tc": [], "tcs": [], "tr": []},
            "spC": 1,
            "seC": 1,
            "skC": 0
        }))
