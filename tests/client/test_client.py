"""SDK main client test module."""
# pylint: disable=no-self-use,protected-access

import json
import os
import unittest.mock as mock
import pytest

from splitio.client.client import Client, _LOGGER as _logger, CONTROL
from splitio.client.factory import SplitFactory, Status as FactoryStatus
from splitio.engine.evaluator import Evaluator
from splitio.models.impressions import Impression, Label
from splitio.models.events import Event, EventWrapper
from splitio.storage import EventStorage, ImpressionStorage, SegmentStorage, SplitStorage
from splitio.storage.inmemmory import InMemorySplitStorage, InMemorySegmentStorage, \
    InMemoryImpressionStorage, InMemoryEventStorage, InMemoryTelemetryStorage
from splitio.models.splits import Split, Status
from splitio.engine.impressions.impressions import Manager as ImpressionManager
from splitio.engine.telemetry import TelemetryStorageConsumer, TelemetryStorageProducer

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

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock(),
        )

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

    def test_get_treatment_with_config(self, mocker):
        """Test get_treatment with config execution paths."""
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

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

    def test_get_treatments(self, mocker):
        """Test get_treatments execution paths."""
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

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

    def test_get_treatments_with_config(self, mocker):
        """Test get_treatments with config execution paths."""
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False
        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

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

    def test_get_treatments_by_flag_set(self, mocker):
        """Test get_treatments by flagset execution paths."""
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)
        def get_feature_flags_by_set(flag_sets):
            if flag_sets == 'set1':
                return ['f1', 'f2']
            if flag_sets == 'set2':
                return ['f3', 'f4']
            if flag_sets == 'set3':
                return ['some_feature']
        split_storage.get_feature_flags_by_set = get_feature_flags_by_set

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
        def evaluate_features(feature_flag_names, matching_key, bucketing_key, attributes=None):
            return {feature_flag_name: evaluation for feature_flag_name in feature_flag_names}
        client._evaluator.evaluate_features = evaluate_features

        _logger = mocker.Mock()
        client._send_impression_to_listener = mocker.Mock()
        assert client.get_treatments_by_flag_set('key', 'set1') == {'f1': 'on', 'f2': 'on'}

        impressions_called = impmanager.process_impressions.mock_calls[0][1][0]
        assert (Impression('key', 'f1', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert (Impression('key', 'f2', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert _logger.mock_calls == []

        assert client.get_treatments_by_flag_set('key', 'set2') == {'f3': 'on', 'f4': 'on'}
        impressions_called = impmanager.process_impressions.mock_calls[1][1][0]
        assert (Impression('key', 'f3', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert (Impression('key', 'f4', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        impmanager.process_impressions.reset_mock()
        assert client.get_treatments_by_flag_set('some_key', 'set3', {'some_attribute': 1}) == {'some_feature': 'control'}
        assert mocker.call(
            [(Impression('some_key', 'some_feature', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY), {'some_attribute': 1})]
        ) in impmanager.process_impressions.mock_calls

        # Test with exception:
        ready_property.return_value = True
        split_storage.get_change_number.return_value = -1

        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_features = _raise
        assert client.get_treatments_by_flag_set('key', 'set1') == {'f1': 'control', 'f2': 'control'}

    def test_get_treatments_by_flag_sets(self, mocker):
        """Test get_treatments by flagsets execution paths."""
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)
        def get_feature_flags_by_set(flag_sets):
            if flag_sets == 'set1':
                return ['f1']
            if flag_sets == 'set2':
                return ['f2']
            if flag_sets == 'set3':
                return ['f3', 'f4']
            if flag_sets == 'set4':
                return []
            if flag_sets == 'set5':
                return ['some_feature']
        split_storage.get_feature_flags_by_set = get_feature_flags_by_set

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
        def evaluate_features(feature_flag_names, matching_key, bucketing_key, attributes=None):
            return {feature_flag_name: evaluation for feature_flag_name in feature_flag_names}

        client._evaluator.evaluate_features = evaluate_features
        _logger = mocker.Mock()
        client._send_impression_to_listener = mocker.Mock()
        assert client.get_treatments_by_flag_sets('key', ['set1', 'set2']) == {'f1': 'on', 'f2': 'on'}

        impressions_called = impmanager.process_impressions.mock_calls[0][1][0]
        assert (Impression('key', 'f1', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert (Impression('key', 'f2', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert _logger.mock_calls == []

        assert client.get_treatments_by_flag_sets('key', ['set3', 'set4']) == {'f3': 'on', 'f4': 'on'}
        impressions_called = impmanager.process_impressions.mock_calls[1][1][0]
        assert (Impression('key', 'f3', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert (Impression('key', 'f4', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        impmanager.process_impressions.reset_mock()
        assert client.get_treatments_by_flag_sets('some_key', ['set5'], {'some_attribute': 1}) == {'some_feature': 'control'}
        assert mocker.call(
            [(Impression('some_key', 'some_feature', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY), {'some_attribute': 1})]
        ) in impmanager.process_impressions.mock_calls

        # Test with exception:
        ready_property.return_value = True
        split_storage.get_change_number.return_value = -1

        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_features = _raise
        assert client.get_treatments_by_flag_sets('key', ['set1', 'set2']) == {'f1': 'control', 'f2': 'control'}

    def test_get_treatments_with_config_by_flag_set(self, mocker):
        """Test get_treatments with config by flagset execution paths."""
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False
        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)
        def get_feature_flags_by_set(flag_sets):
            if flag_sets == 'set1':
                return ['f1', 'f2']
            if flag_sets == 'set2':
                return ['f3', 'f4']
            if flag_sets == 'set3':
                return ['some_feature']
        split_storage.get_feature_flags_by_set = get_feature_flags_by_set

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
        def evaluate_features(feature_flag_names, matching_key, bucketing_key, attributes=None):
            return {feature_flag_name: evaluation for feature_flag_name in feature_flag_names}
        client._evaluator.evaluate_features = evaluate_features

        _logger = mocker.Mock()
        assert client.get_treatments_with_config_by_flag_set('key', 'set1') == {
            'f1': ('on', '{"color": "red"}'),
            'f2': ('on', '{"color": "red"}')
        }

        impressions_called = impmanager.process_impressions.mock_calls[0][1][0]
        assert (Impression('key', 'f1', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert (Impression('key', 'f2', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert _logger.mock_calls == []

        _logger = mocker.Mock()
        assert client.get_treatments_with_config_by_flag_set('key', 'set2') == {
            'f3': ('on', '{"color": "red"}'),
            'f4': ('on', '{"color": "red"}')
        }

        impressions_called = impmanager.process_impressions.mock_calls[1][1][0]
        assert (Impression('key', 'f3', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert (Impression('key', 'f4', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        impmanager.process_impressions.reset_mock()
        assert client.get_treatments_with_config_by_flag_set('some_key', 'set3', {'some_attribute': 1}) == {'some_feature': ('control', None)}
        assert mocker.call(
            [(Impression('some_key', 'some_feature', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY), {'some_attribute': 1})]
        ) in impmanager.process_impressions.mock_calls

        # Test with exception:
        ready_property.return_value = True
        split_storage.get_change_number.return_value = -1

        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_features = _raise
        assert client.get_treatments_with_config_by_flag_set('key', 'set1') == {
            'f1': ('control', None),
            'f2': ('control', None)
        }

    def test_get_treatments_with_config_by_flag_sets(self, mocker):
        """Test get_treatments with config by flagsets execution paths."""
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False
        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)
        def get_feature_flags_by_set(flag_sets):
            if flag_sets == 'set1':
                return ['f1']
            if flag_sets == 'set2':
                return ['f2']
            if flag_sets == 'set3':
                return ['f3', 'f4']
            if flag_sets == 'set4':
                return []
            if flag_sets == 'set5':
                return ['some_feature']
        split_storage.get_feature_flags_by_set = get_feature_flags_by_set

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
        def evaluate_features(feature_flag_names, matching_key, bucketing_key, attributes=None):
            return {feature_flag_name: evaluation for feature_flag_name in feature_flag_names}
        client._evaluator.evaluate_features = evaluate_features

        _logger = mocker.Mock()
        assert client.get_treatments_with_config_by_flag_sets('key', ['set1', 'set2']) == {
            'f1': ('on', '{"color": "red"}'),
            'f2': ('on', '{"color": "red"}')
        }

        impressions_called = impmanager.process_impressions.mock_calls[0][1][0]
        assert (Impression('key', 'f1', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert (Impression('key', 'f2', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert _logger.mock_calls == []

        _logger = mocker.Mock()
        assert client.get_treatments_with_config_by_flag_sets('key', ['set3', 'set4']) == {
            'f3': ('on', '{"color": "red"}'),
            'f4': ('on', '{"color": "red"}')
        }

        impressions_called = impmanager.process_impressions.mock_calls[1][1][0]
        assert (Impression('key', 'f3', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert (Impression('key', 'f4', 'on', 'some_label', 123, None, 1000), None) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        impmanager.process_impressions.reset_mock()
        assert client.get_treatments_with_config_by_flag_sets('some_key', ['set5'], {'some_attribute': 1}) == {'some_feature': ('control', None)}
        assert mocker.call(
            [(Impression('some_key', 'some_feature', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY), {'some_attribute': 1})]
        ) in impmanager.process_impressions.mock_calls

        # Test with exception:
        ready_property.return_value = True
        split_storage.get_change_number.return_value = -1

        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_features = _raise
        assert client.get_treatments_with_config_by_flag_sets('key', ['set1', 'set2']) == {
            'f1': ('control', None),
            'f2': ('control', None)
        }

    @mock.patch('splitio.client.factory.SplitFactory.destroy')
    def test_destroy(self, mocker):
        """Test that destroy/destroyed calls are forwarded to the factory."""
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )

        client = Client(factory, recorder, True)
        client.destroy()
        assert client.destroyed is not None
        assert(mocker.called)

    def test_track(self, mocker):
        """Test that destroy/destroyed calls are forwarded to the factory."""
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)
        event_storage.put.return_value = True

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )

        destroyed_mock = mocker.PropertyMock()
        destroyed_mock.return_value = False
        factory._apikey = 'test'
        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)

        client = Client(factory, recorder, True)
        assert client.track('key', 'user', 'purchase', 12) is True
        assert mocker.call([
            EventWrapper(
                event=Event('key', 'user', 'purchase', 12, 1000, None),
                size=1024
            )
        ]) in event_storage.put.mock_calls

    def test_evaluations_before_running_post_fork(self, mocker):
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        recorder = StandardRecorder(impmanager, mocker.Mock(), mocker.Mock(), telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': mocker.Mock(),
            'segments': mocker.Mock(),
            'impressions': mocker.Mock(),
            'events': mocker.Mock()},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock(),
            True
        )

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

    @mock.patch('splitio.client.client.Client.ready', side_effect=None)
    def test_telemetry_not_ready(self, mocker):
        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        recorder = StandardRecorder(impmanager, mocker.Mock(), mocker.Mock(), telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory('localhost',
            {'splits': mocker.Mock(),
            'segments': mocker.Mock(),
            'impressions': mocker.Mock(),
            'events': mocker.Mock()},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        client = Client(factory, mocker.Mock())
        client.ready = False
        client._evaluate_if_ready('matching_key','matching_key', 'feature')
        assert(telemetry_storage._tel_config._not_ready == 1)
        client.track('key', 'tt', 'ev')
        assert(telemetry_storage._tel_config._not_ready == 2)

    @mock.patch('splitio.client.client.Client._evaluate_if_ready', side_effect=Exception())
    def test_telemetry_record_treatment_exception(self, mocker):
        split_storage = InMemorySplitStorage()
        split_storage.update([Split('split1', 1234, 1, False, 'user', Status.ACTIVE, 123)], [], 123)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        client = Client(factory, recorder, True)
        try:
            client.get_treatment('key', 'split1')
        except:
            pass
        assert(telemetry_storage._method_exceptions._treatment == 1)

        try:
            client.get_treatment_with_config('key', 'split1')
        except:
            pass
        assert(telemetry_storage._method_exceptions._treatment_with_config == 1)

    @mock.patch('splitio.client.client.Client._evaluate_features_if_ready', side_effect=Exception())
    def test_telemetry_record_treatments_exception(self, mocker):
        split_storage = InMemorySplitStorage()
        split_storage.update([Split('split1', 1234, 1, False, 'user', Status.ACTIVE, 123)], [], 123)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        client = Client(factory, recorder, True)
        try:
            client.get_treatments('key', ['split1'])
        except:
            pass
        assert(telemetry_storage._method_exceptions._treatments == 1)

        try:
            client.get_treatments_with_config('key', ['split1'])
        except:
            pass
        assert(telemetry_storage._method_exceptions._treatments_with_config == 1)

    def test_telemetry_method_latency(self, mocker):
        split_storage = InMemorySplitStorage()
        split_storage.update([Split('split1', 1234, 1, False, 'user', Status.ACTIVE, 123)], [], 123)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        client = Client(factory, recorder, True)
        client.get_treatment('key', 'split1')
        assert(telemetry_storage._method_latencies._treatment[0] == 1)
        client.get_treatment_with_config('key', 'split1')
        assert(telemetry_storage._method_latencies._treatment_with_config[0] == 1)
        client.get_treatments('key', ['split1'])
        assert(telemetry_storage._method_latencies._treatments[0] == 1)
        client.get_treatments_with_config('key', ['split1'])
        assert(telemetry_storage._method_latencies._treatments_with_config[0] == 1)
        client.track('key', 'tt', 'ev')
        assert(telemetry_storage._method_latencies._track[0] == 1)

    @mock.patch('splitio.recorder.recorder.StandardRecorder.record_track_stats', side_effect=Exception())
    def test_telemetry_track_exception(self, mocker):
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        client = Client(factory, recorder, True)
        try:
            client.track('key', 'tt', 'ev')
        except:
            pass
        assert(telemetry_storage._method_exceptions._track == 1)
