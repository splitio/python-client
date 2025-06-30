"""SDK main client test module."""
# pylint: disable=no-self-use,protected-access

import json
import os
import unittest.mock as mock
import time
import pytest

from splitio.client.client import Client, _LOGGER as _logger, CONTROL, ClientAsync
from splitio.client.factory import SplitFactory, Status as FactoryStatus, SplitFactoryAsync
from splitio.models.impressions import Impression, Label
from splitio.models.events import Event, EventWrapper
from splitio.storage import EventStorage, ImpressionStorage, SegmentStorage, SplitStorage, RuleBasedSegmentsStorage
from splitio.storage.inmemmory import InMemorySplitStorage, InMemorySegmentStorage, \
    InMemoryImpressionStorage, InMemoryTelemetryStorage, InMemorySplitStorageAsync, \
    InMemoryImpressionStorageAsync, InMemorySegmentStorageAsync, InMemoryTelemetryStorageAsync, InMemoryEventStorageAsync, \
    InMemoryRuleBasedSegmentStorage, InMemoryRuleBasedSegmentStorageAsync
from splitio.models.splits import Split, Status, from_raw
from splitio.engine.impressions.impressions import Manager as ImpressionManager
from splitio.engine.impressions.manager import Counter as ImpressionsCounter
from splitio.engine.impressions.unique_keys_tracker import UniqueKeysTracker, UniqueKeysTrackerAsync
from splitio.engine.telemetry import TelemetryStorageConsumer, TelemetryStorageProducer, TelemetryStorageProducerAsync
from splitio.engine.evaluator import Evaluator
from splitio.recorder.recorder import StandardRecorder, StandardRecorderAsync
from splitio.engine.impressions.strategies import StrategyDebugMode, StrategyNoneMode, StrategyOptimizedMode
from tests.integration import splits_json


class ClientTests(object):  # pylint: disable=too-few-public-methods
    """Split client test cases."""

    def test_get_treatment(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        rb_segment_storage = InMemoryRuleBasedSegmentStorage()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer(),
                                    unique_keys_tracker=UniqueKeysTracker(),
                                    imp_counter=ImpressionsCounter())
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            TelemetrySubmitterMock(),
        )
        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(factory).ready = ready_property
        factory.block_until_ready(5)

        split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0])], [], -1)
        client = Client(factory, recorder, True)
        client._evaluator = mocker.Mock(spec=Evaluator)
        client._evaluator.eval_with_context.return_value = {
            'treatment': 'on',
            'configurations': None,
            'impression': {
                'label': 'some_label',
                'change_number': 123
            },
            'impressions_disabled': False
        }
        _logger = mocker.Mock()
        assert client.get_treatment('some_key', 'SPLIT_2') == 'on'
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, None)]
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
       # pytest.set_trace()
        assert client.get_treatment('some_key', 'SPLIT_2', {'some_attribute': 1}) == 'control'
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', Label.NOT_READY, None, None, 1000, None, None)]

        # Test with exception:
        ready_property.return_value = True
        def _raise(*_):
            raise RuntimeError('something')
        client._evaluator.eval_with_context.side_effect = _raise
        assert client.get_treatment('some_key', 'SPLIT_2') == 'control'
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', 'exception', None, None, 1000, None, None)]
        factory.destroy()

    def test_get_treatment_with_config(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        rb_segment_storage = InMemoryRuleBasedSegmentStorage()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'impressions': impression_storage,
            'rule_based_segments': rb_segment_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0])], [], -1)
        client = Client(factory, recorder, True)
        client._evaluator = mocker.Mock(spec=Evaluator)
        client._evaluator.eval_with_context.return_value = {
            'treatment': 'on',
            'configurations': '{"some_config": True}',
            'impression': {
                'label': 'some_label',
                'change_number': 123
            },
            'impressions_disabled': False
        }
        _logger = mocker.Mock()
        client._send_impression_to_listener = mocker.Mock()

        assert client.get_treatment_with_config(
            'some_key',
            'SPLIT_2'
        ) == ('on', '{"some_config": True}')
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, None)]
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert client.get_treatment_with_config('some_key', 'SPLIT_2', {'some_attribute': 1}) == ('control', None)
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise RuntimeError('something')
        client._evaluator.eval_with_context.side_effect = _raise
        assert client.get_treatment_with_config('some_key', 'SPLIT_2') == ('control', None)
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', 'exception', None, None, 1000, None, None)]
        factory.destroy()

    def test_get_treatments(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        rb_segment_storage = InMemoryRuleBasedSegmentStorage()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0]), from_raw(splits_json['splitChange1_1']['ff']['d'][1])], [], -1)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,            
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
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

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
            },
            'impressions_disabled': False
        }
        client._evaluator.eval_many_with_context.return_value = {
            'SPLIT_2': evaluation,
            'SPLIT_1': evaluation
        }
        _logger = mocker.Mock()
        client._send_impression_to_listener = mocker.Mock()
        treatments = client.get_treatments('key', ['SPLIT_2', 'SPLIT_1'])
        assert treatments == {'SPLIT_2': 'on', 'SPLIT_1': 'on'}

        impressions_called = impression_storage.pop_many(100)
        assert Impression('key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert Impression('key', 'SPLIT_1', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert client.get_treatments('some_key', ['SPLIT_2'], {'some_attribute': 1}) == {'SPLIT_2': 'control'}
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise RuntimeError('something')
        client._evaluator.eval_many_with_context.side_effect = _raise
        assert client.get_treatments('key', ['SPLIT_2', 'SPLIT_1']) == {'SPLIT_2': 'control', 'SPLIT_1': 'control'}
        factory.destroy()

    def test_get_treatments_by_flag_set(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        rb_segment_storage = InMemoryRuleBasedSegmentStorage()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0]), from_raw(splits_json['splitChange1_1']['ff']['d'][1])], [], -1)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,            
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
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

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
            },
            'impressions_disabled': False
        }
        client._evaluator.eval_many_with_context.return_value = {
            'SPLIT_2': evaluation,
            'SPLIT_1': evaluation
        }
        _logger = mocker.Mock()
        client._send_impression_to_listener = mocker.Mock()
        assert client.get_treatments_by_flag_set('key', 'set_1') == {'SPLIT_2': 'on', 'SPLIT_1': 'on'}

        impressions_called = impression_storage.pop_many(100)
        assert Impression('key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert Impression('key', 'SPLIT_1', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert client.get_treatments_by_flag_set('some_key', 'set_2', {'some_attribute': 1}) == {'SPLIT_1': 'control'}
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_1', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise RuntimeError('something')
        client._evaluator.eval_many_with_context.side_effect = _raise
        assert client.get_treatments_by_flag_set('key', 'set_1') == {'SPLIT_2': 'control', 'SPLIT_1': 'control'}
        factory.destroy()

    def test_get_treatments_by_flag_sets(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        rb_segment_storage = InMemoryRuleBasedSegmentStorage()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0]), from_raw(splits_json['splitChange1_1']['ff']['d'][1])], [], -1)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
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
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

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
            },
            'impressions_disabled': False
        }
        client._evaluator.eval_many_with_context.return_value = {
            'SPLIT_2': evaluation,
            'SPLIT_1': evaluation
        }
        _logger = mocker.Mock()
        client._send_impression_to_listener = mocker.Mock()
        assert client.get_treatments_by_flag_sets('key', ['set_1']) == {'SPLIT_2': 'on', 'SPLIT_1': 'on'}

        impressions_called = impression_storage.pop_many(100)
        assert Impression('key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert Impression('key', 'SPLIT_1', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert client.get_treatments_by_flag_sets('some_key', ['set_2'], {'some_attribute': 1}) == {'SPLIT_1': 'control'}
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_1', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise RuntimeError('something')
        client._evaluator.eval_many_with_context.side_effect = _raise
        assert client.get_treatments_by_flag_sets('key', ['set_1']) == {'SPLIT_2': 'control', 'SPLIT_1': 'control'}
        factory.destroy()

    def test_get_treatments_with_config(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        rb_segment_storage = InMemoryRuleBasedSegmentStorage()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0]), from_raw(splits_json['splitChange1_1']['ff']['d'][1])], [], -1)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
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
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

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
            },
            'impressions_disabled': False
        }
        client._evaluator.eval_many_with_context.return_value = {
            'SPLIT_1': evaluation,
            'SPLIT_2': evaluation
        }
        _logger = mocker.Mock()
        assert client.get_treatments_with_config('key', ['SPLIT_1', 'SPLIT_2']) == {
            'SPLIT_1': ('on', '{"color": "red"}'),
            'SPLIT_2': ('on', '{"color": "red"}')
        }

        impressions_called = impression_storage.pop_many(100)
        assert Impression('key', 'SPLIT_1', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert Impression('key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert client.get_treatments_with_config('some_key', ['SPLIT_1'], {'some_attribute': 1}) == {'SPLIT_1': ('control', None)}
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_1', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise RuntimeError('something')
        client._evaluator.eval_many_with_context.side_effect = _raise
        assert client.get_treatments_with_config('key', ['SPLIT_1', 'SPLIT_2']) == {
            'SPLIT_1': ('control', None),
            'SPLIT_2': ('control', None)
        }
        factory.destroy()

    def test_get_treatments_with_config_by_flag_set(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        rb_segment_storage = InMemoryRuleBasedSegmentStorage()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0]), from_raw(splits_json['splitChange1_1']['ff']['d'][1])], [], -1)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
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
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

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
            },
            'impressions_disabled': False
        }
        client._evaluator.eval_many_with_context.return_value = {
            'SPLIT_1': evaluation,
            'SPLIT_2': evaluation
        }
        _logger = mocker.Mock()
        assert client.get_treatments_with_config_by_flag_set('key', 'set_1') == {
            'SPLIT_1': ('on', '{"color": "red"}'),
            'SPLIT_2': ('on', '{"color": "red"}')
        }

        impressions_called = impression_storage.pop_many(100)
        assert Impression('key', 'SPLIT_1', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert Impression('key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert client.get_treatments_with_config_by_flag_set('some_key', 'set_2', {'some_attribute': 1}) == {'SPLIT_1': ('control', None)}
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_1', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise RuntimeError('something')
        client._evaluator.eval_many_with_context.side_effect = _raise
        assert client.get_treatments_with_config_by_flag_set('key', 'set_1') == {'SPLIT_1': ('control', None), 'SPLIT_2': ('control', None)}
        factory.destroy()

    def test_get_treatments_with_config_by_flag_sets(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        rb_segment_storage = InMemoryRuleBasedSegmentStorage()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0]), from_raw(splits_json['splitChange1_1']['ff']['d'][1])], [], -1)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
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
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

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
            },
            'impressions_disabled': False
        }
        client._evaluator.eval_many_with_context.return_value = {
            'SPLIT_1': evaluation,
            'SPLIT_2': evaluation
        }
        _logger = mocker.Mock()
        assert client.get_treatments_with_config_by_flag_sets('key', ['set_1']) == {
            'SPLIT_1': ('on', '{"color": "red"}'),
            'SPLIT_2': ('on', '{"color": "red"}')
        }

        impressions_called = impression_storage.pop_many(100)
        assert Impression('key', 'SPLIT_1', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert Impression('key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert client.get_treatments_with_config_by_flag_sets('some_key', ['set_2'], {'some_attribute': 1}) == {'SPLIT_1': ('control', None)}
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_1', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise RuntimeError('something')
        client._evaluator.eval_many_with_context.side_effect = _raise
        assert client.get_treatments_with_config_by_flag_sets('key', ['set_1']) == {'SPLIT_1': ('control', None), 'SPLIT_2': ('control', None)}
        factory.destroy()

    def test_impression_toggle_optimized(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        rb_segment_storage = InMemoryRuleBasedSegmentStorage()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        impmanager = ImpressionManager(StrategyOptimizedMode(), StrategyNoneMode(), telemetry_runtime_producer)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass

        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            TelemetrySubmitterMock(),
        )

        factory.block_until_ready(5)

        split_storage.update([
            from_raw(splits_json['splitChange1_1']['ff']['d'][0]),
            from_raw(splits_json['splitChange1_1']['ff']['d'][1]),
            from_raw(splits_json['splitChange1_1']['ff']['d'][2])
            ], [], -1)
        client = Client(factory, recorder, True)
        assert client.get_treatment('some_key', 'SPLIT_1') == 'off'
        assert client.get_treatment('some_key', 'SPLIT_2') == 'on'
        assert client.get_treatment('some_key', 'SPLIT_3') == 'on'

        impressions = impression_storage.pop_many(100)
        assert len(impressions) == 2

        found1 = False
        found2 = False
        for impression in impressions:
            if impression[1] == 'SPLIT_1':
                found1 = True
            if impression[1] == 'SPLIT_2':
                found2 = True
        assert found1
        assert found2
        factory.destroy()

    def test_impression_toggle_debug(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        rb_segment_storage = InMemoryRuleBasedSegmentStorage()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass

        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            TelemetrySubmitterMock(),
        )

        factory.block_until_ready(5)

        split_storage.update([
            from_raw(splits_json['splitChange1_1']['ff']['d'][0]),
            from_raw(splits_json['splitChange1_1']['ff']['d'][1]),
            from_raw(splits_json['splitChange1_1']['ff']['d'][2])
            ], [], -1)
        client = Client(factory, recorder, True)
        assert client.get_treatment('some_key', 'SPLIT_1') == 'off'
        assert client.get_treatment('some_key', 'SPLIT_2') == 'on'
        assert client.get_treatment('some_key', 'SPLIT_3') == 'on'

        impressions = impression_storage.pop_many(100)
        assert len(impressions) == 2

        found1 = False
        found2 = False
        for impression in impressions:
            if impression[1] == 'SPLIT_1':
                found1 = True
            if impression[1] == 'SPLIT_2':
                found2 = True
        assert found1
        assert found2
        factory.destroy()

    def test_impression_toggle_none(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        rb_segment_storage = InMemoryRuleBasedSegmentStorage()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)
        non_strategy = StrategyNoneMode()
        impmanager = ImpressionManager(non_strategy, non_strategy, telemetry_runtime_producer)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass

        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            TelemetrySubmitterMock(),
        )

        factory.block_until_ready(5)

        split_storage.update([
            from_raw(splits_json['splitChange1_1']['ff']['d'][0]),
            from_raw(splits_json['splitChange1_1']['ff']['d'][1]),
            from_raw(splits_json['splitChange1_1']['ff']['d'][2])
            ], [], -1)
        client = Client(factory, recorder, True)
        assert client.get_treatment('some_key', 'SPLIT_1') == 'off'
        assert client.get_treatment('some_key', 'SPLIT_2') == 'on'
        assert client.get_treatment('some_key', 'SPLIT_3') == 'on'

        impressions = impression_storage.pop_many(100)
        assert len(impressions) == 0
        factory.destroy()

    @mock.patch('splitio.client.factory.SplitFactory.destroy')
    def test_destroy(self, mocker):
        """Test that destroy/destroyed calls are forwarded to the factory."""
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        rb_segment_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
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
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        client = Client(factory, recorder, True)
        client.destroy()
        assert client.destroyed is not None
        assert(mocker.called)

    def test_track(self, mocker):
        """Test that destroy/destroyed calls are forwarded to the factory."""
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        rb_segment_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)        
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)
        event_storage.put.return_value = True

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
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
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

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
        factory.destroy()

    def test_evaluations_before_running_post_fork(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        rb_segment_storage = InMemoryRuleBasedSegmentStorage()        
        split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0])], [], -1)
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        impmanager = mocker.Mock(spec=ImpressionManager)
        recorder = StandardRecorder(impmanager, mocker.Mock(), impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
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
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        expected_msg = [
            mocker.call('Client is not ready - no calls possible')
        ]

        client = Client(factory, mocker.Mock())
        _logger = mocker.Mock()
        mocker.patch('splitio.client.client._LOGGER', new=_logger)

        assert client.get_treatment('some_key', 'SPLIT_2') == CONTROL
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert client.get_treatment_with_config('some_key', 'SPLIT_2') == (CONTROL, None)
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert client.track("some_key", "traffic_type", "event_type", None) is False
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert client.get_treatments(None, ['SPLIT_2']) == {'SPLIT_2': CONTROL}
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert client.get_treatments_by_flag_set(None, 'set_1') == {'SPLIT_2': CONTROL}
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert client.get_treatments_by_flag_sets(None, ['set_1']) == {'SPLIT_2': CONTROL}
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert client.get_treatments_with_config('some_key', ['SPLIT_2']) == {'SPLIT_2': (CONTROL, None)}
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert client.get_treatments_with_config_by_flag_set('some_key', 'set_1') == {'SPLIT_2': (CONTROL, None)}
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert client.get_treatments_with_config_by_flag_sets('some_key', ['set_1']) == {'SPLIT_2': (CONTROL, None)}
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()
        factory.destroy()

    @mock.patch('splitio.client.client.Client.ready', side_effect=None)
    def test_telemetry_not_ready(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        rb_segment_storage = InMemoryRuleBasedSegmentStorage()        
        split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0])], [], -1)
        recorder = StandardRecorder(impmanager, mocker.Mock(), mocker.Mock(), telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory('localhost',
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': mocker.Mock()},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        client = Client(factory, mocker.Mock())
        client.ready = False
        assert client.get_treatment('some_key', 'SPLIT_2') == CONTROL
        assert(telemetry_storage._tel_config._not_ready == 1)
        client.track('key', 'tt', 'ev')
        assert(telemetry_storage._tel_config._not_ready == 2)
        factory.destroy()

    def test_telemetry_record_treatment_exception(self, mocker):
        split_storage = InMemorySplitStorage()
        split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0])], [], -1)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        rb_segment_storage = InMemoryRuleBasedSegmentStorage()
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory('localhost',
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
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
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        class SyncManagerMock():
            def stop(*_):
                pass
        factory._sync_manager = SyncManagerMock()

        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(factory).ready = ready_property
        client = Client(factory, recorder, True)
        def _raise(*_):
            raise RuntimeError('something')
        client._evaluator.eval_many_with_context = _raise
        client._evaluator.eval_with_context = _raise


        try:
            client.get_treatment('key', 'SPLIT_2')
        except:
            pass
        assert(telemetry_storage._method_exceptions._treatment == 1)
        try:
            client.get_treatment_with_config('key', 'SPLIT_2')
        except:
            pass
        assert(telemetry_storage._method_exceptions._treatment_with_config == 1)

        try:
            client.get_treatments('key', ['SPLIT_2'])
        except:
            pass
        assert(telemetry_storage._method_exceptions._treatments == 1)

        try:
            client.get_treatments_by_flag_set('key', 'set_1')
        except:
            pass
        assert(telemetry_storage._method_exceptions._treatments_by_flag_set == 1)

        try:
            client.get_treatments_by_flag_sets('key', ['set_1'])
        except:
            pass
        assert(telemetry_storage._method_exceptions._treatments_by_flag_sets == 1)

        try:
            client.get_treatments_with_config('key', ['SPLIT_2'])
        except:
            pass
        assert(telemetry_storage._method_exceptions._treatments_with_config == 1)

        try:
            client.get_treatments_with_config_by_flag_set('key', 'set_1')
        except:
            pass
        assert(telemetry_storage._method_exceptions._treatments_with_config_by_flag_set == 1)

        try:
            client.get_treatments_with_config_by_flag_sets('key', ['set_1'])
        except:
            pass
        assert(telemetry_storage._method_exceptions._treatments_with_config_by_flag_sets == 1)
        factory.destroy()

    def test_telemetry_method_latency(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        rb_segment_storage = InMemoryRuleBasedSegmentStorage()        
        split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0])], [], -1)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda:1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
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
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        def stop(*_):
            pass
        factory._sync_manager.stop = stop

        client = Client(factory, recorder, True)
        assert client.get_treatment('key', 'SPLIT_2') == 'on'
        assert(telemetry_storage._method_latencies._treatment[0] == 1)

        client.get_treatment_with_config('key', 'SPLIT_2')
        assert(telemetry_storage._method_latencies._treatment_with_config[0] == 1)

        client.get_treatments('key', ['SPLIT_2'])
        assert(telemetry_storage._method_latencies._treatments[0] == 1)

        client.get_treatments_by_flag_set('key', 'set_1')
        assert(telemetry_storage._method_latencies._treatments_by_flag_set[0] == 1)

        client.get_treatments_by_flag_sets('key', ['set_1'])
        assert(telemetry_storage._method_latencies._treatments_by_flag_sets[0] == 1)

        client.get_treatments_with_config('key', ['SPLIT_2'])
        assert(telemetry_storage._method_latencies._treatments_with_config[0] == 1)

        client.get_treatments_with_config_by_flag_set('key', 'set_1')
        assert(telemetry_storage._method_latencies._treatments_with_config_by_flag_set[0] == 1)

        client.get_treatments_with_config_by_flag_sets('key', ['set_1'])
        assert(telemetry_storage._method_latencies._treatments_with_config_by_flag_sets[0] == 1)

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        client.track('key', 'tt', 'ev')
        assert(telemetry_storage._method_latencies._track[0] == 1)
        factory.destroy()

    @mock.patch('splitio.recorder.recorder.StandardRecorder.record_track_stats', side_effect=Exception())
    def test_telemetry_track_exception(self, mocker):
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        rb_segment_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)        
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
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
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        client = Client(factory, recorder, True)
        try:
            client.track('key', 'tt', 'ev')
        except:
            pass
        assert(telemetry_storage._method_exceptions._track == 1)
        factory.destroy()

    def test_impressions_properties(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        rb_segment_storage = InMemoryRuleBasedSegmentStorage()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer(),
                                    unique_keys_tracker=UniqueKeysTracker(),
                                    imp_counter=ImpressionsCounter())
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
            
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            TelemetrySubmitterMock(),
        )
        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(factory).ready = ready_property
        factory.block_until_ready(5)

        split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0])], [], -1)
        client = Client(factory, recorder, True)
        client._evaluator = mocker.Mock(spec=Evaluator)
        evaluation = {
            'treatment': 'on',
            'configurations': None,
            'impression': {
                'label': 'some_label',
                'change_number': 123
            },
            'impressions_disabled': False
        }
        client._evaluator.eval_with_context.return_value = evaluation
        client._evaluator.eval_many_with_context.return_value = {
            'SPLIT_2': evaluation
        }

        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)
        assert client.get_treatment('some_key', 'SPLIT_2', impressions_properties={"prop": "value"}) == 'on'
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, '{"prop": "value"}')]

        assert client.get_treatment('some_key', 'SPLIT_2', impressions_properties=12) == 'on'
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, None)]
        assert _logger.error.mock_calls == [mocker.call('%s: properties must be of type dictionary.', 'get_treatment')]
        
        _logger.reset_mock()
        assert client.get_treatment('some_key', 'SPLIT_2', impressions_properties='12') == 'on'
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, 1000, None)]
        assert _logger.error.mock_calls == [mocker.call('%s: properties must be of type dictionary.', 'get_treatment')]

        assert client.get_treatment_with_config('some_key', 'SPLIT_2', impressions_properties={"prop": "value"}) == ('on', None)
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, '{"prop": "value"}')]

        assert client.get_treatments('some_key', ['SPLIT_2'], impressions_properties={"prop": "value"}) ==  {'SPLIT_2': 'on'}
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, '{"prop": "value"}')]

        _logger.reset_mock()
        assert client.get_treatments('some_key', ['SPLIT_2'], impressions_properties="prop") ==  {'SPLIT_2': 'on'}
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, 1000, None)]
        assert _logger.error.mock_calls == [mocker.call('%s: properties must be of type dictionary.', 'get_treatments')]

        _logger.reset_mock()
        assert client.get_treatments('some_key', ['SPLIT_2'], impressions_properties=123) ==  {'SPLIT_2': 'on'}
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, 1000, None)]
        assert _logger.error.mock_calls == [mocker.call('%s: properties must be of type dictionary.', 'get_treatments')]

        assert client.get_treatments_with_config('some_key', ['SPLIT_2'], impressions_properties={"prop": "value"}) ==  {'SPLIT_2': ('on', None)}
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, '{"prop": "value"}')]

        assert client.get_treatments_by_flag_set('some_key', 'set_1', impressions_properties={"prop": "value"}) == {'SPLIT_2': 'on'}
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, '{"prop": "value"}')]

        assert client.get_treatments_by_flag_sets('some_key', ['set_1'], impressions_properties={"prop": "value"}) == {'SPLIT_2': 'on'}
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, '{"prop": "value"}')]

        assert client.get_treatments_with_config_by_flag_set('some_key', 'set_1', impressions_properties={"prop": "value"}) == {'SPLIT_2': ('on', None)}
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, '{"prop": "value"}')]

        assert client.get_treatments_with_config_by_flag_sets('some_key', ['set_1'], impressions_properties={"prop": "value"}) == {'SPLIT_2': ('on', None)}
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, '{"prop": "value"}')]

class ClientAsyncTests(object):  # pylint: disable=too-few-public-methods
    """Split client async test cases."""

    @pytest.mark.asyncio
    async def test_get_treatment_async(self, mocker):
        """Test get_treatment_async execution paths."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        rb_segment_storage = InMemoryRuleBasedSegmentStorageAsync()        
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0])], [], -1)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        class TelemetrySubmitterMock():
            async def synchronize_config(*_):
                pass
        factory = SplitFactoryAsync(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            TelemetrySubmitterMock(),
        )

        await factory.block_until_ready(1)
        client = ClientAsync(factory, recorder, True)
        client._evaluator = mocker.Mock(spec=Evaluator)
        client._evaluator.eval_with_context.return_value = {
            'treatment': 'on',
            'configurations': None,
            'impression': {
                'label': 'some_label',
                'change_number': 123
            },
            'impressions_disabled': False
        }
        _logger = mocker.Mock()
        assert await client.get_treatment('some_key', 'SPLIT_2') == 'on'
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, None)]
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert await client.get_treatment('some_key', 'SPLIT_2', {'some_attribute': 1}) == 'control'
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', Label.NOT_READY, None, None, 1000, None, None)]

        # Test with exception:
        ready_property.return_value = True
        def _raise(*_):
            raise RuntimeError('something')
        client._evaluator.eval_with_context.side_effect = _raise
        assert await client.get_treatment('some_key', 'SPLIT_2') == 'control'
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', 'exception', None, None, 1000, None, None)]
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatment_with_config_async(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        rb_segment_storage = InMemoryRuleBasedSegmentStorageAsync()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0])], [], -1)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        factory = SplitFactoryAsync(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        class TelemetrySubmitterMock():
            async def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        await factory.block_until_ready(1)
        client = ClientAsync(factory, recorder, True)
        client._evaluator = mocker.Mock(spec=Evaluator)
        client._evaluator.eval_with_context.return_value = {
            'treatment': 'on',
            'configurations': '{"some_config": True}',
            'impression': {
                'label': 'some_label',
                'change_number': 123
            },
            'impressions_disabled': False
        }
        _logger = mocker.Mock()
        client._send_impression_to_listener = mocker.Mock()
        assert await client.get_treatment_with_config(
            'some_key',
            'SPLIT_2'
        ) == ('on', '{"some_config": True}')
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, None)]
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert await client.get_treatment_with_config('some_key', 'SPLIT_2', {'some_attribute': 1}) == ('control', None)
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise RuntimeError('something')
        client._evaluator.eval_with_context.side_effect = _raise
        assert await client.get_treatment_with_config('some_key', 'SPLIT_2') == ('control', None)
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', 'exception', None, None, 1000, None, None)]
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_async(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        rb_segment_storage = InMemoryRuleBasedSegmentStorageAsync()        
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0]), from_raw(splits_json['splitChange1_1']['ff']['d'][1])], [], -1)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        factory = SplitFactoryAsync(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        class TelemetrySubmitterMock():
            async def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        await factory.block_until_ready(1)
        client = ClientAsync(factory, recorder, True)
        client._evaluator = mocker.Mock(spec=Evaluator)
        evaluation = {
            'treatment': 'on',
            'configurations': '{"color": "red"}',
            'impression': {
                'label': 'some_label',
                'change_number': 123
            },
            'impressions_disabled': False
        }
        client._evaluator.eval_many_with_context.return_value = {
            'SPLIT_2': evaluation,
            'SPLIT_1': evaluation
        }
        _logger = mocker.Mock()
        client._send_impression_to_listener = mocker.Mock()
        assert await client.get_treatments('key', ['SPLIT_2', 'SPLIT_1']) == {'SPLIT_2': 'on', 'SPLIT_1': 'on'}

        impressions_called = await impression_storage.pop_many(100)
        assert Impression('key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert Impression('key', 'SPLIT_1', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert await client.get_treatments('some_key', ['SPLIT_2'], {'some_attribute': 1}) == {'SPLIT_2': 'control'}
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise RuntimeError('something')
        client._evaluator.eval_many_with_context.side_effect = _raise
        assert await client.get_treatments('key', ['SPLIT_2', 'SPLIT_1']) == {'SPLIT_2': 'control', 'SPLIT_1': 'control'}
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_by_flag_set_async(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        rb_segment_storage = InMemoryRuleBasedSegmentStorageAsync()        
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0]), from_raw(splits_json['splitChange1_1']['ff']['d'][1])], [], -1)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        factory = SplitFactoryAsync(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        class TelemetrySubmitterMock():
            async def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        await factory.block_until_ready(1)
        client = ClientAsync(factory, recorder, True)
        client._evaluator = mocker.Mock(spec=Evaluator)
        evaluation = {
            'treatment': 'on',
            'configurations': '{"color": "red"}',
            'impression': {
                'label': 'some_label',
                'change_number': 123
            },
            'impressions_disabled': False
        }
        client._evaluator.eval_many_with_context.return_value = {
            'SPLIT_2': evaluation,
            'SPLIT_1': evaluation
        }
        _logger = mocker.Mock()
        client._send_impression_to_listener = mocker.Mock()
        assert await client.get_treatments_by_flag_set('key', 'set_1') == {'SPLIT_2': 'on', 'SPLIT_1': 'on'}

        impressions_called = await impression_storage.pop_many(100)
        assert Impression('key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert Impression('key', 'SPLIT_1', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert await client.get_treatments_by_flag_set('some_key', 'set_2', {'some_attribute': 1}) == {'SPLIT_1': 'control'}
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_1', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise RuntimeError('something')
        client._evaluator.eval_many_with_context.side_effect = _raise
        assert await client.get_treatments_by_flag_set('key', 'set_1') == {'SPLIT_2': 'control', 'SPLIT_1': 'control'}
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_by_flag_sets_async(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        rb_segment_storage = InMemoryRuleBasedSegmentStorageAsync()        
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0]), from_raw(splits_json['splitChange1_1']['ff']['d'][1])], [], -1)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        factory = SplitFactoryAsync(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        class TelemetrySubmitterMock():
            async def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        await factory.block_until_ready(1)
        client = ClientAsync(factory, recorder, True)
        client._evaluator = mocker.Mock(spec=Evaluator)
        evaluation = {
            'treatment': 'on',
            'configurations': '{"color": "red"}',
            'impression': {
                'label': 'some_label',
                'change_number': 123
            },
            'impressions_disabled': False
        }
        client._evaluator.eval_many_with_context.return_value = {
            'SPLIT_2': evaluation,
            'SPLIT_1': evaluation
        }
        _logger = mocker.Mock()
        client._send_impression_to_listener = mocker.Mock()
        assert await client.get_treatments_by_flag_sets('key', ['set_1']) == {'SPLIT_2': 'on', 'SPLIT_1': 'on'}

        impressions_called = await impression_storage.pop_many(100)
        assert Impression('key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert Impression('key', 'SPLIT_1', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert await client.get_treatments_by_flag_sets('some_key', ['set_2'], {'some_attribute': 1}) == {'SPLIT_1': 'control'}
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_1', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise RuntimeError('something')
        client._evaluator.eval_many_with_context.side_effect = _raise
        assert await client.get_treatments_by_flag_sets('key', ['set_1']) == {'SPLIT_2': 'control', 'SPLIT_1': 'control'}
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_with_config(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        rb_segment_storage = InMemoryRuleBasedSegmentStorageAsync()        
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0]), from_raw(splits_json['splitChange1_1']['ff']['d'][1])], [], -1)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False
        factory = SplitFactoryAsync(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        class TelemetrySubmitterMock():
            async def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        await factory.block_until_ready(1)
        client = ClientAsync(factory, recorder, True)
        client._evaluator = mocker.Mock(spec=Evaluator)
        evaluation = {
            'treatment': 'on',
            'configurations': '{"color": "red"}',
            'impression': {
                'label': 'some_label',
                'change_number': 123
            },
            'impressions_disabled': False
        }
        client._evaluator.eval_many_with_context.return_value = {
            'SPLIT_1': evaluation,
            'SPLIT_2': evaluation
        }
        _logger = mocker.Mock()
        assert await client.get_treatments_with_config('key', ['SPLIT_1', 'SPLIT_2']) == {
            'SPLIT_1': ('on', '{"color": "red"}'),
            'SPLIT_2': ('on', '{"color": "red"}')
        }

        impressions_called = await impression_storage.pop_many(100)
        assert Impression('key', 'SPLIT_1', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert Impression('key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert await client.get_treatments_with_config('some_key', ['SPLIT_1'], {'some_attribute': 1}) == {'SPLIT_1': ('control', None)}
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_1', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise RuntimeError('something')
        client._evaluator.eval_many_with_context.side_effect = _raise
        assert await client.get_treatments_with_config('key', ['SPLIT_1', 'SPLIT_2']) == {
            'SPLIT_1': ('control', None),
            'SPLIT_2': ('control', None)
        }
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_with_config_by_flag_set(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        rb_segment_storage = InMemoryRuleBasedSegmentStorageAsync()        
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0]), from_raw(splits_json['splitChange1_1']['ff']['d'][1])], [], -1)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False
        factory = SplitFactoryAsync(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        class TelemetrySubmitterMock():
            async def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        await factory.block_until_ready(1)
        client = ClientAsync(factory, recorder, True)
        client._evaluator = mocker.Mock(spec=Evaluator)
        evaluation = {
            'treatment': 'on',
            'configurations': '{"color": "red"}',
            'impression': {
                'label': 'some_label',
                'change_number': 123
            },
            'impressions_disabled': False
        }
        client._evaluator.eval_many_with_context.return_value = {
            'SPLIT_1': evaluation,
            'SPLIT_2': evaluation
        }
        _logger = mocker.Mock()
        assert await client.get_treatments_with_config_by_flag_set('key', 'set_1') == {
            'SPLIT_1': ('on', '{"color": "red"}'),
            'SPLIT_2': ('on', '{"color": "red"}')
        }

        impressions_called = await impression_storage.pop_many(100)
        assert Impression('key', 'SPLIT_1', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert Impression('key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert await client.get_treatments_with_config_by_flag_set('some_key', 'set_2', {'some_attribute': 1}) == {'SPLIT_1': ('control', None)}
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_1', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise RuntimeError('something')
        client._evaluator.eval_many_with_context.side_effect = _raise
        assert await client.get_treatments_with_config_by_flag_set('key', 'set_1') == {
            'SPLIT_1': ('control', None),
            'SPLIT_2': ('control', None)
        }
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_with_config_by_flag_sets(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        rb_segment_storage = InMemoryRuleBasedSegmentStorageAsync()        
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0]), from_raw(splits_json['splitChange1_1']['ff']['d'][1])], [], -1)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False
        factory = SplitFactoryAsync(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        class TelemetrySubmitterMock():
            async def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        await factory.block_until_ready(1)
        client = ClientAsync(factory, recorder, True)
        client._evaluator = mocker.Mock(spec=Evaluator)
        evaluation = {
            'treatment': 'on',
            'configurations': '{"color": "red"}',
            'impression': {
                'label': 'some_label',
                'change_number': 123
            },
            'impressions_disabled': False
        }
        client._evaluator.eval_many_with_context.return_value = {
            'SPLIT_1': evaluation,
            'SPLIT_2': evaluation
        }
        _logger = mocker.Mock()
        assert await client.get_treatments_with_config_by_flag_sets('key', ['set_1']) == {
            'SPLIT_1': ('on', '{"color": "red"}'),
            'SPLIT_2': ('on', '{"color": "red"}')
        }

        impressions_called = await impression_storage.pop_many(100)
        assert Impression('key', 'SPLIT_1', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert Impression('key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, None) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert await client.get_treatments_with_config_by_flag_sets('some_key', ['set_2'], {'some_attribute': 1}) == {'SPLIT_1': ('control', None)}
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_1', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise RuntimeError('something')
        client._evaluator.eval_many_with_context.side_effect = _raise
        assert await client.get_treatments_with_config_by_flag_sets('key', ['set_1']) == {
            'SPLIT_1': ('control', None),
            'SPLIT_2': ('control', None)
        }
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_impression_toggle_optimized(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        rb_segment_storage = InMemoryRuleBasedSegmentStorageAsync()        
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        impmanager = ImpressionManager(StrategyOptimizedMode(), StrategyNoneMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactoryAsync(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )

        await factory.block_until_ready(5)

        await split_storage.update([
            from_raw(splits_json['splitChange1_1']['ff']['d'][0]),
            from_raw(splits_json['splitChange1_1']['ff']['d'][1]),
            from_raw(splits_json['splitChange1_1']['ff']['d'][2])
            ], [], -1)
        client = ClientAsync(factory, recorder, True)
        treatment = await client.get_treatment('some_key', 'SPLIT_1')
        assert  treatment == 'off'
        treatment = await client.get_treatment('some_key', 'SPLIT_2')
        assert treatment == 'on'
        treatment = await client.get_treatment('some_key', 'SPLIT_3')
        assert treatment == 'on'

        impressions = await impression_storage.pop_many(100)
        assert len(impressions) == 2

        found1 = False
        found2 = False
        for impression in impressions:
            if impression[1] == 'SPLIT_1':
                found1 = True
            if impression[1] == 'SPLIT_2':
                found2 = True
        assert found1
        assert found2
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_impression_toggle_debug(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        rb_segment_storage = InMemoryRuleBasedSegmentStorageAsync()        
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactoryAsync(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )

        await factory.block_until_ready(5)

        await split_storage.update([
            from_raw(splits_json['splitChange1_1']['ff']['d'][0]),
            from_raw(splits_json['splitChange1_1']['ff']['d'][1]),
            from_raw(splits_json['splitChange1_1']['ff']['d'][2])
            ], [], -1)
        client = ClientAsync(factory, recorder, True)
        assert await client.get_treatment('some_key', 'SPLIT_1') == 'off'
        assert await client.get_treatment('some_key', 'SPLIT_2') == 'on'
        assert await client.get_treatment('some_key', 'SPLIT_3') == 'on'

        impressions = await impression_storage.pop_many(100)
        assert len(impressions) == 2

        found1 = False
        found2 = False
        for impression in impressions:
            if impression[1] == 'SPLIT_1':
                found1 = True
            if impression[1] == 'SPLIT_2':
                found2 = True
        assert found1
        assert found2
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_impression_toggle_none(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        rb_segment_storage = InMemoryRuleBasedSegmentStorageAsync()        
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)
        non_strategy = StrategyNoneMode()
        impmanager = ImpressionManager(non_strategy, non_strategy, telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactoryAsync(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )

        await factory.block_until_ready(5)

        await split_storage.update([
            from_raw(splits_json['splitChange1_1']['ff']['d'][0]),
            from_raw(splits_json['splitChange1_1']['ff']['d'][1]),
            from_raw(splits_json['splitChange1_1']['ff']['d'][2])
            ], [], -1)
        client = ClientAsync(factory, recorder, True)
        assert await client.get_treatment('some_key', 'SPLIT_1') == 'off'
        assert await client.get_treatment('some_key', 'SPLIT_2') == 'on'
        assert await client.get_treatment('some_key', 'SPLIT_3') == 'on'

        impressions = await impression_storage.pop_many(100)
        assert len(impressions) == 0
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_track_async(self, mocker):
        """Test that destroy/destroyed calls are forwarded to the factory."""
        split_storage = InMemorySplitStorageAsync()
        segment_storage = mocker.Mock(spec=SegmentStorage)
        rb_segment_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)
        self.events = []
        async def put(event):
            self.events.append(event)
            return True
        event_storage.put = put

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactoryAsync(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        class TelemetrySubmitterMock():
            async def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        destroyed_mock = mocker.PropertyMock()
        destroyed_mock.return_value = False
        factory._apikey = 'test'
        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)

        await factory.block_until_ready(1)
        client = ClientAsync(factory, recorder, True)
        assert await client.track('key', 'user', 'purchase', 12) is True
        assert self.events[0] == [EventWrapper(
                event=Event('key', 'user', 'purchase', 12, 1000, None),
                size=1024
            )]
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_telemetry_not_ready_async(self, mocker):
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        rb_segment_storage = InMemoryRuleBasedSegmentStorageAsync()        
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = InMemoryEventStorageAsync(10, telemetry_runtime_producer)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0])], [], -1)
        factory = SplitFactoryAsync('localhost',
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': mocker.Mock()},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        class TelemetrySubmitterMock():
            async def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property

        await factory.block_until_ready(1)
        client = ClientAsync(factory, recorder)
        assert await client.get_treatment('some_key', 'SPLIT_2') == CONTROL
        assert(telemetry_storage._tel_config._not_ready == 1)
        await client.track('key', 'tt', 'ev')
        assert(telemetry_storage._tel_config._not_ready == 2)
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_telemetry_record_treatment_exception_async(self, mocker):
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        rb_segment_storage = InMemoryRuleBasedSegmentStorageAsync()        
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = InMemoryEventStorageAsync(10, telemetry_runtime_producer)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0])], [], -1)
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        factory = SplitFactoryAsync(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        class TelemetrySubmitterMock():
            async def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(factory).ready = ready_property

        client = ClientAsync(factory, recorder, True)
        client._evaluator = mocker.Mock()
        def _raise(*_):
            raise RuntimeError('something')
        client._evaluator.eval_with_context.side_effect = _raise
        client._evaluator.eval_many_with_context.side_effect = _raise

        await client.get_treatment('key', 'SPLIT_2')
        assert(telemetry_storage._method_exceptions._treatment == 1)

        await client.get_treatment_with_config('key', 'SPLIT_2')
        assert(telemetry_storage._method_exceptions._treatment_with_config == 1)

        await client.get_treatments('key', ['SPLIT_2'])
        assert(telemetry_storage._method_exceptions._treatments == 1)

        await client.get_treatments_by_flag_set('key', 'set_1')
        assert(telemetry_storage._method_exceptions._treatments_by_flag_set == 1)

        await client.get_treatments_by_flag_sets('key', ['set_1'])
        assert(telemetry_storage._method_exceptions._treatments_by_flag_sets == 1)

        await client.get_treatments_with_config('key', ['SPLIT_2'])
        assert(telemetry_storage._method_exceptions._treatments_with_config == 1)

        await client.get_treatments_with_config_by_flag_set('key', 'set_1')
        assert(telemetry_storage._method_exceptions._treatments_with_config_by_flag_set == 1)

        await client.get_treatments_with_config_by_flag_sets('key', ['set_1'])
        assert(telemetry_storage._method_exceptions._treatments_with_config_by_flag_sets == 1)

        await factory.destroy()

    @pytest.mark.asyncio
    async def test_telemetry_method_latency_async(self, mocker):
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        rb_segment_storage = InMemoryRuleBasedSegmentStorageAsync()        
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = InMemoryEventStorageAsync(10, telemetry_runtime_producer)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0])], [], -1)
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        factory = SplitFactoryAsync(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        class TelemetrySubmitterMock():
            async def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(factory).ready = ready_property

        try:
            await factory.block_until_ready(1)
        except:
            pass
        client = ClientAsync(factory, recorder, True)
        assert await client.get_treatment('key', 'SPLIT_2') == 'on'
        assert(telemetry_storage._method_latencies._treatment[0] == 1)

        await client.get_treatment_with_config('key', 'SPLIT_2')
        assert(telemetry_storage._method_latencies._treatment_with_config[0] == 1)

        await client.get_treatments('key', ['SPLIT_2'])
        assert(telemetry_storage._method_latencies._treatments[0] == 1)

        await client.get_treatments_by_flag_set('key', 'set_1')
        assert(telemetry_storage._method_latencies._treatments_by_flag_set[0] == 1)

        await client.get_treatments_by_flag_sets('key', ['set_1'])
        assert(telemetry_storage._method_latencies._treatments_by_flag_sets[0] == 1)

        await client.get_treatments_with_config('key', ['SPLIT_2'])
        assert(telemetry_storage._method_latencies._treatments_with_config[0] == 1)

        await client.get_treatments_with_config_by_flag_set('key', 'set_1')
        assert(telemetry_storage._method_latencies._treatments_with_config_by_flag_set[0] == 1)

        await client.get_treatments_with_config_by_flag_sets('key', ['set_1'])
        assert(telemetry_storage._method_latencies._treatments_with_config_by_flag_sets[0] == 1)

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        await client.track('key', 'tt', 'ev')
        assert(telemetry_storage._method_latencies._track[0] == 1)
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_telemetry_track_exception_async(self, mocker):
        split_storage = InMemorySplitStorageAsync()
        segment_storage = mocker.Mock(spec=SegmentStorage)
        rb_segment_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        event_storage = InMemoryEventStorageAsync(10, telemetry_producer.get_telemetry_runtime_producer())
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactoryAsync(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        class TelemetrySubmitterMock():
            async def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        async def exc(*_):
            raise RuntimeError("something")
        recorder.record_track_stats = exc

        await factory.block_until_ready(1)
        client = ClientAsync(factory, recorder, True)
        try:
            await client.track('key', 'tt', 'ev')
        except:
            pass
        assert(telemetry_storage._method_exceptions._track == 1)
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_impressions_properties_async(self, mocker):
        """Test get_treatment_async execution paths."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        rb_segment_storage = InMemoryRuleBasedSegmentStorageAsync()        
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        impmanager = ImpressionManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer(), imp_counter=ImpressionsCounter())
        await split_storage.update([from_raw(splits_json['splitChange1_1']['ff']['d'][0])], [], -1)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        class TelemetrySubmitterMock():
            async def synchronize_config(*_):
                pass
        factory = SplitFactoryAsync(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
            'rule_based_segments': rb_segment_storage,
            'impressions': impression_storage,
            'events': event_storage},
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            TelemetrySubmitterMock(),
        )

        await factory.block_until_ready(1)
        client = ClientAsync(factory, recorder, True)
        client._evaluator = mocker.Mock(spec=Evaluator)
        evaluation = {
            'treatment': 'on',
            'configurations': None,
            'impression': {
                'label': 'some_label',
                'change_number': 123
            },
            'impressions_disabled': False
        }
        client._evaluator.eval_with_context.return_value = evaluation
        client._evaluator.eval_many_with_context.return_value = {
            'SPLIT_2': evaluation
        }

        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)
        assert await client.get_treatment('some_key', 'SPLIT_2', impressions_properties={"prop": "value"}) == 'on'
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, '{"prop": "value"}')]

        assert await client.get_treatment('some_key', 'SPLIT_2', impressions_properties=12) == 'on'
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, None)]
        assert _logger.error.mock_calls == [mocker.call('%s: properties must be of type dictionary.', 'get_treatment')]
        
        _logger.reset_mock()
        assert await client.get_treatment('some_key', 'SPLIT_2', impressions_properties='12') == 'on'
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, 1000, None)]
        assert _logger.error.mock_calls == [mocker.call('%s: properties must be of type dictionary.', 'get_treatment')]

        assert await client.get_treatment_with_config('some_key', 'SPLIT_2', impressions_properties={"prop": "value"}) == ('on', None)
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, '{"prop": "value"}')]

        assert await client.get_treatments('some_key', ['SPLIT_2'], impressions_properties={"prop": "value"}) ==  {'SPLIT_2': 'on'}
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, '{"prop": "value"}')]

        _logger.reset_mock()
        assert await client.get_treatments('some_key', ['SPLIT_2'], impressions_properties="prop") ==  {'SPLIT_2': 'on'}
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, 1000, None)]
        assert _logger.error.mock_calls == [mocker.call('%s: properties must be of type dictionary.', 'get_treatments')]

        _logger.reset_mock()
        assert await client.get_treatments('some_key', ['SPLIT_2'], impressions_properties=123) ==  {'SPLIT_2': 'on'}
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, 1000, None)]
        assert _logger.error.mock_calls == [mocker.call('%s: properties must be of type dictionary.', 'get_treatments')]

        assert await client.get_treatments_with_config('some_key', ['SPLIT_2'], impressions_properties={"prop": "value"}) ==  {'SPLIT_2': ('on', None)}
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, '{"prop": "value"}')]

        assert await client.get_treatments_by_flag_set('some_key', 'set_1', impressions_properties={"prop": "value"}) == {'SPLIT_2': 'on'}
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, '{"prop": "value"}')]

        assert await client.get_treatments_by_flag_sets('some_key', ['set_1'], impressions_properties={"prop": "value"}) == {'SPLIT_2': 'on'}
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, '{"prop": "value"}')]

        assert await client.get_treatments_with_config_by_flag_set('some_key', 'set_1', impressions_properties={"prop": "value"}) == {'SPLIT_2': ('on', None)}
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, '{"prop": "value"}')]

        assert await client.get_treatments_with_config_by_flag_sets('some_key', ['set_1'], impressions_properties={"prop": "value"}) == {'SPLIT_2': ('on', None)}
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, None, 1000, None, '{"prop": "value"}')]
