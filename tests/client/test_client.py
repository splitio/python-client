"""SDK main client test module."""
#pylint: disable=no-self-use,protected-access

import json
import os
from splitio.client.client import Client
from splitio.client.factory import SplitFactory
from splitio.engine.evaluator import Evaluator
from splitio.models.impressions import Impression
from splitio.models.events import Event
from splitio.storage import EventStorage, ImpressionStorage, SegmentStorage, SplitStorage, \
    TelemetryStorage
from splitio.storage.inmemmory import InMemorySplitStorage, InMemorySegmentStorage, \
    InMemoryImpressionStorage, InMemoryTelemetryStorage, InMemoryEventStorage
from splitio.models import splits, segments

class ClientTests(object):  #pylint: disable=too-few-public-methods
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
        type(factory).destroyed = destroyed_property

        mocker.patch('splitio.client.client.time.time', new=lambda: 1)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        client = Client(factory, True, None)
        client._evaluator = mocker.Mock(spec=Evaluator)
        client._evaluator.evaluate_treatment.return_value = {
            'treatment': 'on',
            'configurations': None,
            'impression': {
                'label': 'some_label',
                'change_number': 123
            }
        }
        client._logger = mocker.Mock()
        client._send_impression_to_listener = mocker.Mock()

        assert client.get_treatment('some_key', 'some_feature') == 'on'
        assert mocker.call(
            [Impression('some_key', 'some_feature', 'on', 'some_label', 123, None, 1000)]
        ) in impression_storage.put.mock_calls
        assert mocker.call('sdk.getTreatment', 5) in telemetry_storage.inc_latency.mock_calls
        assert client._logger.mock_calls == []
        assert mocker.call(
            Impression('some_key', 'some_feature', 'on', 'some_label', 123, None, 1000),
            None
        ) in client._send_impression_to_listener.mock_calls

        # Test with exception:
        split_storage.get_change_number.return_value = -1
        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_treatment.side_effect = _raise
        assert client.get_treatment('some_key', 'some_feature') == 'control'
        assert mocker.call(
            [Impression('some_key', 'some_feature', 'control', 'exception', -1, None, 1000)]
        ) in impression_storage.put.mock_calls
        assert len(telemetry_storage.inc_latency.mock_calls) == 2

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
        type(factory).destroyed = destroyed_property

        mocker.patch('splitio.client.client.time.time', new=lambda: 1)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        client = Client(factory, True, None)
        client._evaluator = mocker.Mock(spec=Evaluator)
        client._evaluator.evaluate_treatment.return_value = {
            'treatment': 'on',
            'configurations': '{"some_config": True}',
            'impression': {
                'label': 'some_label',
                'change_number': 123
            }
        }
        client._logger = mocker.Mock()
        client._send_impression_to_listener = mocker.Mock()

        assert client.get_treatment_with_config(
            'some_key',
            'some_feature'
        ) == ('on', '{"some_config": True}')
        assert mocker.call(
            [Impression('some_key', 'some_feature', 'on', 'some_label', 123, None, 1000)]
        ) in impression_storage.put.mock_calls
        assert mocker.call('sdk.getTreatment', 5) in telemetry_storage.inc_latency.mock_calls
        assert client._logger.mock_calls == []
        assert mocker.call(
            Impression('some_key', 'some_feature', 'on', 'some_label', 123, None, 1000),
            None
        ) in client._send_impression_to_listener.mock_calls

        # Test with exception:
        split_storage.get_change_number.return_value = -1
        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_treatment.side_effect = _raise
        assert client.get_treatment_with_config('some_key', 'some_feature') == ('control', None)
        assert mocker.call(
            [Impression('some_key', 'some_feature', 'control', 'exception', -1, None, 1000)]
        ) in impression_storage.put.mock_calls
        assert len(telemetry_storage.inc_latency.mock_calls) == 2

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
        type(factory).destroyed = destroyed_property

        mocker.patch('splitio.client.client.time.time', new=lambda: 1)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        client = Client(factory, True, None)
        client._evaluator = mocker.Mock(spec=Evaluator)
        client._evaluator.evaluate_treatment.return_value = {
            'treatment': 'on',
            'configurations': '{"color": "red"}',
            'impression': {
                'label': 'some_label',
                'change_number': 123
            }
        }
        client._logger = mocker.Mock()
        client._send_impression_to_listener = mocker.Mock()
        assert client.get_treatments('key', ['f1', 'f2']) == {'f1': 'on', 'f2': 'on'}

        impressions_called = impression_storage.put.mock_calls[0][1][0]
        assert Impression('key', 'f1', 'on', 'some_label', 123, None, 1000) in impressions_called
        assert Impression('key', 'f2', 'on', 'some_label', 123, None, 1000) in impressions_called
        assert mocker.call('sdk.getTreatments', 5) in telemetry_storage.inc_latency.mock_calls
        assert client._logger.mock_calls == []
        assert mocker.call(
            Impression('key', 'f1', 'on', 'some_label', 123, None, 1000),
            None
        ) in client._send_impression_to_listener.mock_calls
        assert mocker.call(
            Impression('key', 'f2', 'on', 'some_label', 123, None, 1000),
            None
        ) in client._send_impression_to_listener.mock_calls

        # Test with exception:
        split_storage.get_change_number.return_value = -1
        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_treatment.side_effect = _raise
        assert client.get_treatments('key', ['f1', 'f2']) == {'f1': 'control', 'f2': 'control'}
        assert len(telemetry_storage.inc_latency.mock_calls) == 1

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
        type(factory).destroyed = destroyed_property

        mocker.patch('splitio.client.client.time.time', new=lambda: 1)
        mocker.patch('splitio.client.client.get_latency_bucket_index', new=lambda x: 5)

        client = Client(factory, True, None)
        client._evaluator = mocker.Mock(spec=Evaluator)
        client._evaluator.evaluate_treatment.return_value = {
            'treatment': 'on',
            'configurations': '{"color": "red"}',
            'impression': {
                'label': 'some_label',
                'change_number': 123
            }
        }
        client._logger = mocker.Mock()
        client._send_impression_to_listener = mocker.Mock()
        assert client.get_treatments_with_config('key', ['f1', 'f2']) == {
            'f1': ('on', '{"color": "red"}'),
            'f2': ('on', '{"color": "red"}')
        }

        impressions_called = impression_storage.put.mock_calls[0][1][0]
        assert Impression('key', 'f1', 'on', 'some_label', 123, None, 1000) in impressions_called
        assert Impression('key', 'f2', 'on', 'some_label', 123, None, 1000) in impressions_called
        assert mocker.call('sdk.getTreatments', 5) in telemetry_storage.inc_latency.mock_calls
        assert client._logger.mock_calls == []
        assert mocker.call(
            Impression('key', 'f1', 'on', 'some_label', 123, None, 1000),
            None
        ) in client._send_impression_to_listener.mock_calls
        assert mocker.call(
            Impression('key', 'f2', 'on', 'some_label', 123, None, 1000),
            None
        ) in client._send_impression_to_listener.mock_calls

        # Test with exception:
        split_storage.get_change_number.return_value = -1
        def _raise(*_):
            raise Exception('something')
        client._evaluator.evaluate_treatment.side_effect = _raise
        assert client.get_treatments_with_config('key', ['f1', 'f2']) == {
            'f1': ('control', None),
            'f2': ('control', None)
        }
        assert len(telemetry_storage.inc_latency.mock_calls) == 1

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

        client = Client(factory)
        client.destroy()
        assert factory.destroy.mock_calls == [mocker.call()]
        assert client.destroyed is not None
        assert destroyed_mock.mock_calls == [mocker.call()]

    def test_track(self, mocker):
        """Test that destroy/destroyed calls are forwarded to the factory."""
        split_storage = mocker.Mock(spec=SplitStorage)
        segment_storage = mocker.Mock(spec=SegmentStorage)
        impression_storage = mocker.Mock(spec=ImpressionStorage)
        event_storage = mocker.Mock(spec=EventStorage)
        event_storage.put.return_value = True
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
        factory._get_storage = _get_storage_mock
        destroyed_mock = mocker.PropertyMock()
        destroyed_mock.return_value = False
        type(factory).destroyed = destroyed_mock
        factory._apikey = 'test'
        mocker.patch('splitio.client.client.time.time', new=lambda: 1)

        client = Client(factory)
        assert client.track('key', 'user', 'purchase', 12) is True
        assert mocker.call([
            Event('key', 'user', 'purchase', 12, 1000)
        ]) in event_storage.put.mock_calls
