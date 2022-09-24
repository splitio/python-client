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
        telemetry_storage._counters = {'iQ': 1, 'iDe': 0, 'iDr': 3, 'eQ': 0, 'eD': 10, 'sL': 0,
                        'aR': 0, 'tR': 3}
        telemetry_storage._exceptions = {'mE': {'t': 1, 'ts': 0, 'tc': 5, 'tcs': 0, 'tr': 3}}
        telemetry_storage._records = {'IS': {'sp': 5, 'se': 3, 'ms': 0, 'im': 10, 'ic': 0, 'ev': 4, 'te': 0, 'to': 0},
                         'sL': 3}
        telemetry_storage._http_errors = {'sp': {'500': 3}, 'se': {}, 'ms': {}, 'im': {}, 'ic': {}, 'ev': {}, 'te': {}, 'to': {}}
        telemetry_storage._config = {'bT':0, 'nR':0, 'uC': 0}
        telemetry_storage._streaming_events = []
        telemetry_storage._tags = ['tag1']
        telemetry_storage._integrations = {}
        telemetry_storage._latencies = {'mL': {'t': [10, 20], 'ts': [50], 'tc': [], 'tcs': [], 'tr': []},
                           'hL': {'sp': [200, 300], 'se': [], 'ms': [400], 'im': [], 'ic': [200], 'ev': [], 'te': [], 'to': []}}

        def record_init(*args, **kwargs):
            self.formatted_config = args[0]

        api.record_init.side_effect = record_init
        telemetry_submitter.synchronize_config()
        assert(self.formatted_config == json.dumps(telemetry_submitter._telemetry_init_consumer.get_config_stats()))

        def record_stats(*args, **kwargs):
            self.formatted_stats = args[0]

        api.record_stats.side_effect = record_stats
        telemetry_submitter.synchronize_stats()
        assert(self.formatted_stats == json.dumps({
            **{'iQ': telemetry_submitter._telemetry_runtime_consumer.get_impressions_stats('iQ')},
            **{'iDe': telemetry_submitter._telemetry_runtime_consumer.get_impressions_stats('iDe')},
            **{'iDr': telemetry_submitter._telemetry_runtime_consumer.get_impressions_stats('iDr')},
            **{'eQ': telemetry_submitter._telemetry_runtime_consumer.get_events_stats('eQ')},
            **{'eD': telemetry_submitter._telemetry_runtime_consumer.get_events_stats('eD')},
            **{'IS': telemetry_submitter._telemetry_runtime_consumer.get_last_synchronization()},
            **{'t': ['tag1']},
            **{'hE': {'sp': {'500': 3}, 'se': {}, 'ms': {}, 'im': {}, 'ic': {}, 'ev': {}, 'te': {}, 'to': {}}},
            **{'hL': {'sp': [200, 300], 'se': [], 'ms': [400], 'im': [], 'ic': [200], 'ev': [], 'te': [], 'to': []}},
            **{'aR': 0},
            **{'tR': 3},
            **{'sE': []},
            **{'sL': 3},
            **{'mE': {'t': 1, 'ts': 0, 'tc': 5, 'tcs': 0, 'tr': 3}},
            **{'mL': {'t': [10, 20], 'ts': [50], 'tc': [], 'tcs': [], 'tr': []}},
            **{'spC': telemetry_submitter._split_storage.get_splits_count()},
            **{'seC': telemetry_submitter._segment_storage.get_segments_count()},
            **{'skC': telemetry_submitter._segment_storage.get_segments_keys_count()}
        }))
