"""Recorder unit tests."""

import pytest

from splitio.recorder.recorder import StandardRecorder, PipelinedRecorder
from splitio.engine.impressions import Manager as ImpressionsManager
from splitio.storage.inmemmory import TelemetryStorage, EventStorage, ImpressionStorage
from splitio.storage.redis import TelemetryPipelinedStorage, ImpressionPipelinedStorage, EventStorage
from splitio.storage.adapters.redis import RedisAdapter
from splitio.models.impressions import Impression


class StandardRecorderTests(object):
    """StandardRecorderTests test cases."""

    def test_standard_recorder(self, mocker):
        impressions = [
            Impression('k1', 'f1', 'on', 'l1', 123, None, None),
            Impression('k1', 'f2', 'on', 'l1', 123, None, None)
        ]
        impmanager = mocker.Mock(spec=ImpressionsManager)
        impmanager.process_impressions.return_value = impressions
        telemetry = mocker.Mock(spec=TelemetryStorage)
        event = mocker.Mock(spec=EventStorage)
        impression = mocker.Mock(spec=ImpressionStorage)
        recorder = StandardRecorder(impmanager, telemetry, event, impression)
        recorder.record_treatment_stats(impressions, 1, 'some')

        assert recorder._impression_storage.put.mock_calls[0][1][0] == impressions
        assert recorder._telemetry_storage.inc_latency.mock_calls == [mocker.call('some', 1)]

    def test_pipelined_recorder(self, mocker):
        impressions = [
            Impression('k1', 'f1', 'on', 'l1', 123, None, None),
            Impression('k1', 'f2', 'on', 'l1', 123, None, None)
        ]
        redis = mocker.Mock(spec=RedisAdapter)
        impmanager = mocker.Mock(spec=ImpressionsManager)
        impmanager.process_impressions.return_value = impressions
        telemetry = mocker.Mock(spec=TelemetryPipelinedStorage)
        event = mocker.Mock(spec=EventStorage)
        impression = mocker.Mock(spec=ImpressionPipelinedStorage)
        recorder = PipelinedRecorder(redis, impmanager, telemetry, event, impression)
        recorder.record_treatment_stats(impressions, 1, 'some')

        assert recorder._impression_storage.add_impressions_to_pipe.mock_calls[0][1][0] == impressions
        assert recorder._telemetry_storage.add_latency_to_pipe.mock_calls[0][1][0] == 'some'
        assert recorder._telemetry_storage.add_latency_to_pipe.mock_calls[0][1][1] == 1
