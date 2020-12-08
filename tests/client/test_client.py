"""SDK main client test module."""
# pylint: disable=no-self-use,protected-access

import json
import os
from splitio.client.client import Client, _LOGGER as _logger, CONTROL
from splitio.client.factory import SplitFactory
from splitio.engine.evaluator import Evaluator
from splitio.models.impressions import Impression, Label
from splitio.models.events import Event, EventWrapper
from splitio.storage import EventStorage, ImpressionStorage, SegmentStorage, SplitStorage, \
    TelemetryStorage
from splitio.storage.inmemmory import InMemorySplitStorage, InMemorySegmentStorage, \
    InMemoryImpressionStorage, InMemoryTelemetryStorage, InMemoryEventStorage
from splitio.models import splits, segments
from splitio.engine.impressions import Manager as ImpressionManager

# Recorder
from splitio.recorder.recorder import StandardRecorder


class ClientTests(object):  # pylint: disable=too-few-public-methods
    """Split client test cases."""

    def test_get_treatment(self, mocker):
        """Test get_treatment execution paths."""
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)
        telemetry_storage = mocker.Mock(spec=TelemetryStorage)

        def _get_storage_mock(name):
            return {
                'splits': split_storage,
                'segments': segment_storage,
                'impressions': impression_storage,
                'events': event_storage,
                'telemetry': telemetry_storage
            }[name]

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        factory = mocker.Mock(spec=SplitFactory)
        factory._get_storage.side_effect = _get_storage_mock
        factory._waiting_fork.return_value = False
        type(factory).destroyed = destroyed_property

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        impmanager = mocker.Mock(spec=ImpressionManager)
        recorder = StandardRecorder(impmanager, telemetry_storage, event_storage,
                                    impression_storage)
        client = Client(factory, recorder, True)
        client._evaluator = mocker.Mock(spec=Evaluator)
        client._evaluator.evaluate_feature.return_value = {
            'treatment': 'on',
            'configurations': None,
            'impression': {
                'label': 'some_label',
                'change_number': 123
            },
        }
        _logger = mocker.Mock()

        assert client.get_treatment('some_key', 'some_feature') == 'on'
        assert mocker.call(
            [(Impression('some_key', 'some_feature', 'on', 'some_label', 123, None, 1000), None)]
        ) in impmanager.process_impressions.mock_calls
        assert mocker.call('sdk.getTreatment', 5) in telemetry_storage.inc_latency.mock_calls
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        impmanager.process_impressions.reset_mock()
        assert client.get_treatment('some_key', 'some_feature', {'some_attribute': 1}) == 'control'
        assert mocker.call(
            [(Impression('some_key', 'some_feature', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY), {'some_attribute': 1})]
        ) in impmanager.process_impressions.mock_calls

        # Test with exception:
        ready_property.return_value = True
        split_storage.get_change_number.return_value = -1

        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_feature.side_effect = _raise
        assert client.get_treatment('some_key', 'some_feature') == 'control'
        assert mocker.call(
            [(Impression('some_key', 'some_feature', 'control', 'exception', -1, None, 1000), None)]
        ) in impmanager.process_impressions.mock_calls
        assert len(telemetry_storage.inc_latency.mock_calls) == 3

    def test_get_treatment_with_config(self, mocker):
        """Test get_treatment execution paths."""
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)
        telemetry_storage = mocker.Mock(spec=TelemetryStorage)

        def _get_storage_mock(name):
            return {
                'splits': split_storage,
                'segments': segment_storage,
                'impressions': impression_storage,
                'events': event_storage,
                'telemetry': telemetry_storage
            }[name]

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        factory = mocker.Mock(spec=SplitFactory)
        factory._get_storage.side_effect = _get_storage_mock
        factory._waiting_fork.return_value = False
        type(factory).destroyed = destroyed_property

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        impmanager = mocker.Mock(spec=ImpressionManager)
        recorder = StandardRecorder(impmanager, telemetry_storage, event_storage,
                                    impression_storage)
        client = Client(factory, recorder, True)
        client._evaluator = mocker.Mock(spec=Evaluator)
        client._evaluator.evaluate_feature.return_value = {
            'treatment': 'on',
            'configurations': '{"some_config": True}',
            'impression': {
                'label': 'some_label',
                'change_number': 123
            }
        }
        _logger = mocker.Mock()
        client._send_impression_to_listener = mocker.Mock()

        assert client.get_treatment_with_config(
            'some_key',
            'some_feature'
        ) == ('on', '{"some_config": True}')
        assert mocker.call(
            [(Impression('some_key', 'some_feature', 'on', 'some_label', 123, None, 1000), None)]
        ) in impmanager.process_impressions.mock_calls
        assert mocker.call('sdk.getTreatmentWithConfig', 5) in telemetry_storage.inc_latency.mock_calls
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        impmanager.process_impressions.reset_mock()
        assert client.get_treatment_with_config('some_key', 'some_feature', {'some_attribute': 1}) == ('control', None)
        assert mocker.call(
            [(Impression('some_key', 'some_feature', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY),
              {'some_attribute': 1})]
        ) in impmanager.process_impressions.mock_calls

        # Test with exception:
        ready_property.return_value = True
        split_storage.get_change_number.return_value = -1

        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_feature.side_effect = _raise
        assert client.get_treatment_with_config('some_key', 'some_feature') == ('control', None)
        assert mocker.call(
            [(Impression('some_key', 'some_feature', 'control', 'exception', -1, None, 1000), None)]
        ) in impmanager.process_impressions.mock_calls
        assert len(telemetry_storage.inc_latency.mock_calls) == 3

    def test_get_treatments(self, mocker):
        """Test get_treatment execution paths."""
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)
        telemetry_storage = mocker.Mock(spec=TelemetryStorage)

        def _get_storage_mock(name):
            return {
                'splits': split_storage,
                'segments': segment_storage,
                'impressions': impression_storage,
                'events': event_storage,
                'telemetry': telemetry_storage
            }[name]

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        factory = mocker.Mock(spec=SplitFactory)
        factory._get_storage.side_effect = _get_storage_mock
        factory._waiting_fork.return_value = False
        type(factory).destroyed = destroyed_property

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        impmanager = mocker.Mock(spec=ImpressionManager)
        recorder = StandardRecorder(impmanager, telemetry_storage, event_storage,
                                    impression_storage)
        client = Client(factory, recorder, True)
        client._evaluator = mocker.Mock(spec=Evaluator)
        evaluation = {
            'treatment': 'on',
            'configurations': '{"color": "red"}',
            'impression': {
                'label': 'some_label',
                'change_number': 123
            }
        }
        client._evaluator.evaluate_features.return_value = {
            'f1': evaluation,
            'f2': evaluation
        }
        _logger = mocker.Mock()
        client._send_impression_to_listener = mocker.Mock()
        assert client.get_treatments('key', ['f1', 'f2']) == {'f1': 'on', 'f2': 'on'}

        impressions_called = impmanager.process_impressions.mock_calls[0][1][0]
        assert (Impression('key', 'f1', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert (Impression('key', 'f2', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert mocker.call('sdk.getTreatments', 5) in telemetry_storage.inc_latency.mock_calls
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        impmanager.process_impressions.reset_mock()
        assert client.get_treatments('some_key', ['some_feature'], {'some_attribute': 1}) == {'some_feature': 'control'}
        assert mocker.call(
            [(Impression('some_key', 'some_feature', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY), {'some_attribute': 1})]
        ) in impmanager.process_impressions.mock_calls

        # Test with exception:
        ready_property.return_value = True
        split_storage.get_change_number.return_value = -1

        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_features.side_effect = _raise
        assert client.get_treatments('key', ['f1', 'f2']) == {'f1': 'control', 'f2': 'control'}
        assert len(telemetry_storage.inc_latency.mock_calls) == 2

    def test_get_treatments_with_config(self, mocker):
        """Test get_treatment execution paths."""
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)
        telemetry_storage = mocker.Mock(spec=TelemetryStorage)

        def _get_storage_mock(name):
            return {
                'splits': split_storage,
                'segments': segment_storage,
                'impressions': impression_storage,
                'events': event_storage,
                'telemetry': telemetry_storage
            }[name]

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        factory = mocker.Mock(spec=SplitFactory)
        factory._get_storage.side_effect = _get_storage_mock
        factory._waiting_fork.return_value = False
        type(factory).destroyed = destroyed_property

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        impmanager = mocker.Mock(spec=ImpressionManager)
        recorder = StandardRecorder(impmanager, telemetry_storage, event_storage,
                                    impression_storage)
        client = Client(factory, recorder, True)
        client._evaluator = mocker.Mock(spec=Evaluator)
        evaluation = {
            'treatment': 'on',
            'configurations': '{"color": "red"}',
            'impression': {
                'label': 'some_label',
                'change_number': 123
            }
        }
        client._evaluator.evaluate_features.return_value = {
            'f1': evaluation,
            'f2': evaluation
        }
        _logger = mocker.Mock()
        assert client.get_treatments_with_config('key', ['f1', 'f2']) == {
            'f1': ('on', '{"color": "red"}'),
            'f2': ('on', '{"color": "red"}')
        }

        impressions_called = impmanager.process_impressions.mock_calls[0][1][0]
        assert (Impression('key', 'f1', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert (Impression('key', 'f2', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert mocker.call('sdk.getTreatmentsWithConfig', 5) in telemetry_storage.inc_latency.mock_calls
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        impmanager.process_impressions.reset_mock()
        assert client.get_treatments_with_config('some_key', ['some_feature'], {'some_attribute': 1}) == {'some_feature': ('control', None)}
        assert mocker.call(
            [(Impression('some_key', 'some_feature', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY), {'some_attribute': 1})]
        ) in impmanager.process_impressions.mock_calls

        # Test with exception:
        ready_property.return_value = True
        split_storage.get_change_number.return_value = -1

        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_features.side_effect = _raise
        assert client.get_treatments_with_config('key', ['f1', 'f2']) == {
            'f1': ('control', None),
            'f2': ('control', None)
        }
        assert len(telemetry_storage.inc_latency.mock_calls) == 2

    def test_destroy(self, mocker):
        """Test that destroy/destroyed calls are forwarded to the factory."""
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)
        telemetry_storage = mocker.Mock(spec=TelemetryStorage)

        def _get_storage_mock(name):
            return {
                'splits': split_storage,
                'segments': segment_storage,
                'impressions': impression_storage,
                'events': event_storage,
                'telemetry': telemetry_storage
            }[name]
        factory = mocker.Mock(spec=SplitFactory)
        destroyed_mock = mocker.PropertyMock()
        type(factory).destroyed = destroyed_mock

        impmanager = mocker.Mock(spec=ImpressionManager)
        recorder = StandardRecorder(impmanager, telemetry_storage, event_storage,
                                    impression_storage)
        client = Client(factory, recorder, True)
        client.destroy()
        assert factory.destroy.mock_calls == [mocker.call()]
        assert client.destroyed is not None
        assert destroyed_mock.mock_calls == [mocker.call()]

    def test_evaluations_before_running_post_fork(self, mocker):
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        factory = mocker.Mock(spec=SplitFactory)
        factory._waiting_fork.return_value = True
        type(factory).destroyed = destroyed_property

        expected_msg = [
            mocker.call('Client is not ready - no calls possible')
        ]

        client = Client(factory, mocker.Mock())
        _logger = mocker.Mock()
        mocker.patch('splitio.client.client._LOGGER', new=_logger)

        assert client.get_treatment('some_key', 'some_feature') == CONTROL
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert client.get_treatment_with_config('some_key', 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert client.track("some_key", "traffic_type", "event_type", None) is False
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert client.get_treatments(None, ['some_feature']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert client.get_treatments_with_config('some_key', ['some_feature']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()
