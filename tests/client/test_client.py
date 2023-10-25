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
from splitio.storage import EventStorage, ImpressionStorage, SegmentStorage, SplitStorage
from splitio.storage.inmemmory import InMemorySplitStorage, InMemorySegmentStorage, \
    InMemoryImpressionStorage, InMemoryTelemetryStorage, InMemorySplitStorageAsync, \
    InMemoryImpressionStorageAsync, InMemorySegmentStorageAsync, InMemoryTelemetryStorageAsync, InMemoryEventStorageAsync
from splitio.models.splits import Split, Status, from_raw
from splitio.engine.impressions.impressions import Manager as ImpressionManager
from splitio.engine.telemetry import TelemetryStorageConsumer, TelemetryStorageProducer, TelemetryStorageProducerAsync
from splitio.engine.evaluator import Evaluator
from splitio.recorder.recorder import StandardRecorder, StandardRecorderAsync
from splitio.engine.impressions.strategies import StrategyDebugMode
from tests.integration import splits_json


class ClientTests(object):  # pylint: disable=too-few-public-methods
    """Split client test cases."""

    def test_get_treatment(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        impmanager = ImpressionManager(StrategyDebugMode(), telemetry_runtime_producer)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
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
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][0]))
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
        assert client.get_treatment('some_key', 'SPLIT_2') == 'on'
#        pytest.set_trace()
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, 'some_key', 1000)]
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert client.get_treatment('some_key', 'SPLIT_2', {'some_attribute': 1}) == 'control'
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', Label.NOT_READY, None, 'some_key', 1000)]

        # Test with exception:
        ready_property.return_value = True
        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_feature.side_effect = _raise
        assert client.get_treatment('some_key', 'SPLIT_2') == 'control'
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', 'exception', -1, 'some_key', 1000)]
        factory.destroy()

    def test_get_treatment_with_config(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        impmanager = ImpressionManager(StrategyDebugMode(), telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
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
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][0]))
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
            'SPLIT_2'
        ) == ('on', '{"some_config": True}')
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, 'some_key', 1000)]
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert client.get_treatment_with_config('some_key', 'SPLIT_2', {'some_attribute': 1}) == ('control', None)
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_feature.side_effect = _raise
        assert client.get_treatment_with_config('some_key', 'SPLIT_2') == ('control', None)
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', 'exception', -1, 'some_key', 1000)]
        factory.destroy()

    def test_get_treatments(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        impmanager = ImpressionManager(StrategyDebugMode(), telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][0]))
        split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][1]))

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
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
            }
        }
        client._evaluator.evaluate_features.return_value = {
            'SPLIT_2': evaluation,
            'SPLIT_1': evaluation
        }
        _logger = mocker.Mock()
        client._send_impression_to_listener = mocker.Mock()
        assert client.get_treatments('key', ['SPLIT_2', 'SPLIT_1']) == {'SPLIT_2': 'on', 'SPLIT_1': 'on'}

        impressions_called = impression_storage.pop_many(100)
        assert Impression('key', 'SPLIT_2', 'on', 'some_label', 123, 'key', 1000) in impressions_called
        assert Impression('key', 'SPLIT_1', 'on', 'some_label', 123, 'key', 1000) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert client.get_treatments('some_key', ['SPLIT_2'], {'some_attribute': 1}) == {'SPLIT_2': 'control'}
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_features.side_effect = _raise
        assert client.get_treatments('key', ['SPLIT_2', 'SPLIT_1']) == {'SPLIT_2': 'control', 'SPLIT_1': 'control'}
        factory.destroy()

    def test_get_treatments_with_config(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        impmanager = ImpressionManager(StrategyDebugMode(), telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][0]))
        split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][1]))

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
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
            }
        }
        client._evaluator.evaluate_features.return_value = {
            'SPLIT_1': evaluation,
            'SPLIT_2': evaluation
        }
        _logger = mocker.Mock()
        assert client.get_treatments_with_config('key', ['SPLIT_1', 'SPLIT_2']) == {
            'SPLIT_1': ('on', '{"color": "red"}'),
            'SPLIT_2': ('on', '{"color": "red"}')
        }

        impressions_called = impression_storage.pop_many(100)
        assert Impression('key', 'SPLIT_1', 'on', 'some_label', 123, 'key', 1000) in impressions_called
        assert Impression('key', 'SPLIT_2', 'on', 'some_label', 123, 'key', 1000) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert client.get_treatments_with_config('some_key', ['SPLIT_1'], {'some_attribute': 1}) == {'SPLIT_1': ('control', None)}
        assert impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_1', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_features.side_effect = _raise
        assert client.get_treatments_with_config('key', ['SPLIT_1', 'SPLIT_2']) == {
            'SPLIT_1': ('control', None),
            'SPLIT_2': ('control', None)
        }
        factory.destroy()

    @mock.patch('splitio.client.factory.SplitFactory.destroy')
    def test_destroy(self, mocker):
        """Test that destroy/destroyed calls are forwarded to the factory."""
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
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
        impmanager = ImpressionManager(StrategyDebugMode(), telemetry_runtime_producer)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][0]))
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        impmanager = mocker.Mock(spec=ImpressionManager)
        recorder = StandardRecorder(impmanager, mocker.Mock(), impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
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

        assert client.get_treatments_with_config('some_key', ['SPLIT_2']) == {'SPLIT_2': (CONTROL, None)}
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()
        factory.destroy()

    @mock.patch('splitio.client.client.Client.ready', side_effect=None)
    def test_telemetry_not_ready(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        impmanager = ImpressionManager(StrategyDebugMode(), telemetry_runtime_producer)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][0]))
        recorder = StandardRecorder(impmanager, mocker.Mock(), mocker.Mock(), telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory('localhost',
            {'splits': split_storage,
            'segments': segment_storage,
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

    @mock.patch('splitio.client.client.Client._evaluate_if_ready', side_effect=Exception())
    def test_telemetry_record_treatment_exception(self, mocker):
        split_storage = InMemorySplitStorage()
        split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][0]))
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
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
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
        class TelemetrySubmitterMock():
            def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(factory).ready = ready_property
        client = Client(factory, recorder, True)
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

        def exc(*_):
            raise Exception("something")
        client._evaluate_features_if_ready = exc
        try:
            client.get_treatments('key', ['SPLIT_2'])
        except:
            pass
        assert(telemetry_storage._method_exceptions._treatments == 1)

        try:
            client.get_treatments_with_config('key', ['SPLIT_2'])
        except:
            pass
        assert(telemetry_storage._method_exceptions._treatments_with_config == 1)
        factory.destroy()

    def test_telemetry_method_latency(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorage(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        impmanager = ImpressionManager(StrategyDebugMode(), telemetry_runtime_producer)
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()
        split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][0]))
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

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
        client.get_treatments_with_config('key', ['SPLIT_2'])
        assert(telemetry_storage._method_latencies._treatments_with_config[0] == 1)
        client.track('key', 'tt', 'ev')
        assert(telemetry_storage._method_latencies._track[0] == 1)
        factory.destroy()

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
        recorder = StandardRecorder(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
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


class ClientAsyncTests(object):  # pylint: disable=too-few-public-methods
    """Split client async test cases."""

    @pytest.mark.asyncio
    async def test_get_treatment_async(self, mocker):
        """Test get_treatment_async execution paths."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        impmanager = ImpressionManager(StrategyDebugMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][0]))

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        factory = SplitFactoryAsync(mocker.Mock(),
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
        class TelemetrySubmitterMock():
            async def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        await factory.block_until_ready(1)
        client = ClientAsync(factory, recorder, True)
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
        assert await client.get_treatment('some_key', 'SPLIT_2') == 'on'
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, 'some_key', 1000)]
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert await client.get_treatment('some_key', 'SPLIT_2', {'some_attribute': 1}) == 'control'
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', Label.NOT_READY, None, 'some_key', 1000)]

        # Test with exception:
        ready_property.return_value = True
        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_feature.side_effect = _raise
        assert await client.get_treatment('some_key', 'SPLIT_2') == 'control'
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', 'exception', -1, 'some_key', 1000)]
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatment_with_config_async(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        impmanager = ImpressionManager(StrategyDebugMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][0]))

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        factory = SplitFactoryAsync(mocker.Mock(),
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
        class TelemetrySubmitterMock():
            async def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        await factory.block_until_ready(1)
        client = ClientAsync(factory, recorder, True)
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
        assert await client.get_treatment_with_config(
            'some_key',
            'SPLIT_2'
        ) == ('on', '{"some_config": True}')
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'on', 'some_label', 123, 'some_key', 1000)]
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert await client.get_treatment_with_config('some_key', 'SPLIT_2', {'some_attribute': 1}) == ('control', None)
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_feature.side_effect = _raise
        assert await client.get_treatment_with_config('some_key', 'SPLIT_2') == ('control', None)
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', 'exception', -1, 'some_key', 1000)]
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_async(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        impmanager = ImpressionManager(StrategyDebugMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][0]))
        await split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][1]))

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        factory = SplitFactoryAsync(mocker.Mock(),
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
            }
        }
        client._evaluator.evaluate_features.return_value = {
            'SPLIT_2': evaluation,
            'SPLIT_1': evaluation
        }
        _logger = mocker.Mock()
        client._send_impression_to_listener = mocker.Mock()
        assert await client.get_treatments('key', ['SPLIT_2', 'SPLIT_1']) == {'SPLIT_2': 'on', 'SPLIT_1': 'on'}

        impressions_called = await impression_storage.pop_many(100)
        assert Impression('key', 'SPLIT_2', 'on', 'some_label', 123, 'key', 1000) in impressions_called
        assert Impression('key', 'SPLIT_1', 'on', 'some_label', 123, 'key', 1000) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert await client.get_treatments('some_key', ['SPLIT_2'], {'some_attribute': 1}) == {'SPLIT_2': 'control'}
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_2', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_features.side_effect = _raise
        assert await client.get_treatments('key', ['SPLIT_2', 'SPLIT_1']) == {'SPLIT_2': 'control', 'SPLIT_1': 'control'}
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_with_config(self, mocker):
        """Test get_treatment execution paths."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        impmanager = ImpressionManager(StrategyDebugMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][0]))
        await split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][1]))

        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False
        factory = SplitFactoryAsync(mocker.Mock(),
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
            }
        }
        client._evaluator.evaluate_features.return_value = {
            'SPLIT_1': evaluation,
            'SPLIT_2': evaluation
        }
        _logger = mocker.Mock()
        assert await client.get_treatments_with_config('key', ['SPLIT_1', 'SPLIT_2']) == {
            'SPLIT_1': ('on', '{"color": "red"}'),
            'SPLIT_2': ('on', '{"color": "red"}')
        }

        impressions_called = await impression_storage.pop_many(100)
        assert Impression('key', 'SPLIT_1', 'on', 'some_label', 123, 'key', 1000) in impressions_called
        assert Impression('key', 'SPLIT_2', 'on', 'some_label', 123, 'key', 1000) in impressions_called
        assert _logger.mock_calls == []

        # Test with client not ready
        ready_property = mocker.PropertyMock()
        ready_property.return_value = False
        type(factory).ready = ready_property
        assert await client.get_treatments_with_config('some_key', ['SPLIT_1'], {'some_attribute': 1}) == {'SPLIT_1': ('control', None)}
        assert await impression_storage.pop_many(100) == [Impression('some_key', 'SPLIT_1', 'control', Label.NOT_READY, mocker.ANY, mocker.ANY, mocker.ANY)]

        # Test with exception:
        ready_property.return_value = True

        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_features.side_effect = _raise
        assert await client.get_treatments_with_config('key', ['SPLIT_1', 'SPLIT_2']) == {
            'SPLIT_1': ('control', None),
            'SPLIT_2': ('control', None)
        }
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_track_async(self, mocker):
        """Test that destroy/destroyed calls are forwarded to the factory."""
        split_storage = InMemorySplitStorageAsync()
        segment_storage = mocker.Mock(spec=SegmentStorage)
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
    async def test_evaluations_before_running_post_fork_async(self, mocker):
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = mocker.Mock(spec=EventStorage)
        impmanager = ImpressionManager(StrategyDebugMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][0]))
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        impmanager = mocker.Mock(spec=ImpressionManager)
        factory = SplitFactoryAsync(mocker.Mock(),
            {'splits': split_storage,
            'segments': segment_storage,
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
            async def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        expected_msg = [
            mocker.call('Client is not ready - no calls possible')
        ]
        try:
            await factory.block_until_ready(1)
        except:
            pass
        client = ClientAsync(factory, mocker.Mock())

        async def _record_stats_async(impressions, start, operation):
            pass
        client._record_stats_async = _record_stats_async

        _logger = mocker.Mock()
        mocker.patch('splitio.client.client._LOGGER', new=_logger)

        assert await client.get_treatment('some_key', 'SPLIT_2') == CONTROL
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert await client.get_treatment_with_config('some_key', 'SPLIT_2') == (CONTROL, None)
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert await client.track("some_key", "traffic_type", "event_type", None) is False
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert await client.get_treatments(None, ['SPLIT_2']) == {'SPLIT_2': CONTROL}
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert await client.get_treatments_with_config('some_key', ['SPLIT_2']) == {'SPLIT_2': (CONTROL, None)}
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_telemetry_not_ready_async(self, mocker):
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = InMemoryEventStorageAsync(10, telemetry_runtime_producer)
        impmanager = ImpressionManager(StrategyDebugMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][0]))
        factory = SplitFactoryAsync('localhost',
            {'splits': split_storage,
            'segments': segment_storage,
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
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = InMemoryEventStorageAsync(10, telemetry_runtime_producer)
        impmanager = ImpressionManager(StrategyDebugMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][0]))
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        factory = SplitFactoryAsync(mocker.Mock(),
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
        class TelemetrySubmitterMock():
            async def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        await factory.block_until_ready(1)
        client = ClientAsync(factory, recorder, True)
        def _raise(*_):
            raise Exception('something')
        client._evaluate_if_ready = _raise
        try:
            await client.get_treatment('key', 'SPLIT_2')
        except:
            pass
        assert(telemetry_storage._method_exceptions._treatment == 1)
        try:
            await client.get_treatment_with_config('key', 'SPLIT_2')
        except:
            pass
        assert(telemetry_storage._method_exceptions._treatment_with_config == 1)
        client._evaluate_features_if_ready = _raise
        try:
            await client.get_treatments('key', ['SPLIT_2'])
        except:
            pass
        assert(telemetry_storage._method_exceptions._treatments == 1)
        try:
            await client.get_treatments_with_config('key', ['SPLIT_2'])
        except:
            pass
        assert(telemetry_storage._method_exceptions._treatments_with_config == 1)
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_telemetry_method_latency_async(self, mocker):
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        impression_storage = InMemoryImpressionStorageAsync(10, telemetry_runtime_producer)
        event_storage = InMemoryEventStorageAsync(10, telemetry_runtime_producer)
        impmanager = ImpressionManager(StrategyDebugMode(), telemetry_runtime_producer)
        recorder = StandardRecorderAsync(impmanager, event_storage, impression_storage, telemetry_producer.get_telemetry_evaluation_producer(), telemetry_producer.get_telemetry_runtime_producer())
        await split_storage.put(from_raw(splits_json['splitChange1_1']['splits'][0]))
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        mocker.patch('splitio.client.client.utctime_ms', new=lambda: 1000)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        factory = SplitFactoryAsync(mocker.Mock(),
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
#        pytest.set_trace()
        assert await client.get_treatment('key', 'SPLIT_2') == 'on'
        assert(telemetry_storage._method_latencies._treatment[0] == 1)
        await client.get_treatment_with_config('key', 'SPLIT_2')
        assert(telemetry_storage._method_latencies._treatment_with_config[0] == 1)
        await client.get_treatments('key', ['SPLIT_2'])
        assert(telemetry_storage._method_latencies._treatments[0] == 1)
        await client.get_treatments_with_config('key', ['SPLIT_2'])
        assert(telemetry_storage._method_latencies._treatments_with_config[0] == 1)
        await client.track('key', 'tt', 'ev')
        assert(telemetry_storage._method_latencies._track[0] == 1)
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_telemetry_track_exception_async(self, mocker):
        split_storage = InMemorySplitStorageAsync()
        segment_storage = mocker.Mock(spec=SegmentStorage)
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
            async def synchronize_config(*_):
                pass
        factory._telemetry_submitter = TelemetrySubmitterMock()

        async def exc(*_):
            raise Exception("something")
        recorder.record_track_stats = exc

        await factory.block_until_ready(1)
        client = ClientAsync(factory, recorder, True)
        try:
            await client.track('key', 'tt', 'ev')
        except:
            pass
        assert(telemetry_storage._method_exceptions._track == 1)
        await factory.destroy()
