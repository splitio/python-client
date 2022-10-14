"""Recorder unit tests."""

import pytest

from splitio.recorder.recorder import StandardRecorder, PipelinedRecorder
from splitio.engine.impressions.impressions import Manager as ImpressionsManager
from splitio.storage.inmemmory import EventStorage, ImpressionStorage
from splitio.storage.redis import ImpressionPipelinedStorage, EventStorage
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
        event = mocker.Mock(spec=EventStorage)
        impression = mocker.Mock(spec=ImpressionStorage)
        recorder = StandardRecorder(impmanager, event, impression)
        recorder.record_treatment_stats(impressions, 1, 'some')

        assert recorder._impression_storage.put.mock_calls[0][1][0] == impressions

    def test_pipelined_recorder(self, mocker):
        impressions = [
            Impression('k1', 'f1', 'on', 'l1', 123, None, None),
            Impression('k1', 'f2', 'on', 'l1', 123, None, None)
        ]
        redis = mocker.Mock(spec=RedisAdapter)
        impmanager = mocker.Mock(spec=ImpressionsManager)
        impmanager.process_impressions.return_value = impressions
        event = mocker.Mock(spec=EventStorage)
        impression = mocker.Mock(spec=ImpressionStorage)
        recorder = PipelinedRecorder(redis, impmanager, event, impression)
        recorder.record_treatment_stats(impressions, 1, 'some')
        assert recorder._impression_storage.put.mock_calls[0][1][0] == impressions

        # TODO @matias.melograno Commented until we implement TelemetryV2
        # assert recorder._impression_storage.add_impressions_to_pipe.mock_calls[0][1][0] == impressions
        # assert recorder._telemetry_storage.add_latency_to_pipe.mock_calls[0][1][0] == 'some'
        # assert recorder._telemetry_storage.add_latency_to_pipe.mock_calls[0][1][1] == 1


    def test_sampled_recorder(self, mocker):
        impressions = [
            Impression('k1', 'f1', 'on', 'l1', 123, None, None),
            Impression('k1', 'f2', 'on', 'l1', 123, None, None)
        ]
        redis = mocker.Mock(spec=RedisAdapter)
        impmanager = mocker.Mock(spec=ImpressionsManager)
        impmanager.process_impressions.return_value = impressions
        event = mocker.Mock(spec=EventStorage)
        impression = mocker.Mock(spec=ImpressionStorage)
        recorder = PipelinedRecorder(redis, impmanager, event, impression, 0.5)

        def put(x):
            return

        recorder._impression_storage.put.side_effect = put

        for _ in range(100):
            recorder.record_treatment_stats(impressions, 1, 'some')
        print(recorder._impression_storage.put.call_count)
        assert recorder._impression_storage.put.call_count < 80
