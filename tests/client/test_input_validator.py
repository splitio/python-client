"""Unit tests for the input_validator module."""
import logging
import pytest

from splitio.client.factory import SplitFactory, get_factory
from splitio.client.client import CONTROL, Client, _LOGGER as _logger
from splitio.client.manager import SplitManager
from splitio.client.key import Key
from splitio.storage import SplitStorage, EventStorage, ImpressionStorage, SegmentStorage
from splitio.storage.inmemmory import InMemoryTelemetryStorage
from splitio.models.splits import Split
from splitio.client import input_validator
from splitio.recorder.recorder import StandardRecorder
from splitio.engine.telemetry import TelemetryStorageProducer, TelemetryStorageConsumer
from splitio.engine.impressions.impressions import Manager as ImpressionManager

class ClientInputValidationTests(object):
    """Input validation test cases."""

    def test_get_treatment(self, mocker):
        """Test get_treatment validation."""
        split_mock = mocker.Mock(spec=Split)
        default_treatment_mock = mocker.PropertyMock()
        default_treatment_mock.return_value = 'default_treatment'
        type(split_mock).default_treatment = default_treatment_mock
        conditions_mock = mocker.PropertyMock()
        conditions_mock.return_value = []
        type(split_mock).conditions = conditions_mock
        storage_mock = mocker.Mock(spec=SplitStorage)
        storage_mock.get.return_value = split_mock

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        recorder = StandardRecorder(impmanager, mocker.Mock(spec=EventStorage), ImpressionStorage, telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_consumer.get_telemetry_init_consumer(),
            mocker.Mock()
        )

        client = Client(factory, mocker.Mock())
        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert client.get_treatment(None, 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null key, key must be a non-empty string.', 'get_treatment')
        ]

        _logger.reset_mock()
        assert client.get_treatment('', 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatment', 'key', 'key')
        ]

        _logger.reset_mock()
        key = ''.join('a' for _ in range(0, 255))
        assert client.get_treatment(key, 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatment', 'key', 250)
        ]

        _logger.reset_mock()
        assert client.get_treatment(12345, 'some_feature') == 'default_treatment'
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatment', 'key', 12345)
        ]

        _logger.reset_mock()
        assert client.get_treatment(float('nan'), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatment(float('inf'), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatment(True, 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatment([], 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatment('some_key', None) == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatment', 'feature_name', 'feature_name')
        ]

        _logger.reset_mock()
        assert client.get_treatment('some_key', 123) == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'feature_name', 'feature_name')
        ]

        _logger.reset_mock()
        assert client.get_treatment('some_key', True) == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'feature_name', 'feature_name')
        ]

        _logger.reset_mock()
        assert client.get_treatment('some_key', []) == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'feature_name', 'feature_name')
        ]

        _logger.reset_mock()
        assert client.get_treatment('some_key', '') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatment', 'feature_name', 'feature_name')
        ]

        _logger.reset_mock()
        assert client.get_treatment('some_key', 'some_feature') == 'default_treatment'
        assert _logger.error.mock_calls == []
        assert _logger.warning.mock_calls == []

        _logger.reset_mock()
        assert client.get_treatment(Key(None, 'bucketing_key'), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatment', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment(Key('', 'bucketing_key'), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatment', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment(Key(float('nan'), 'bucketing_key'), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment(Key(float('inf'), 'bucketing_key'), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment(Key(True, 'bucketing_key'), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment(Key([], 'bucketing_key'), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment(Key(12345, 'bucketing_key'), 'some_feature') == 'default_treatment'
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatment', 'matching_key', 12345)
        ]

        _logger.reset_mock()
        key = ''.join('a' for _ in range(0, 255))
        assert client.get_treatment(Key(key, 'bucketing_key'), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatment', 'matching_key', 250)
        ]

        _logger.reset_mock()
        assert client.get_treatment(Key('matching_key', None), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatment', 'bucketing_key', 'bucketing_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment(Key('matching_key', True), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'bucketing_key', 'bucketing_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment(Key('matching_key', []), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'bucketing_key', 'bucketing_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment(Key('matching_key', ''), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatment', 'bucketing_key', 'bucketing_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment(Key('matching_key', 12345), 'some_feature') == 'default_treatment'
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatment', 'bucketing_key', 12345)
        ]

        _logger.reset_mock()
        assert client.get_treatment('matching_key', 'some_feature', True) == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: attributes must be of type dictionary.', 'get_treatment')
        ]

        _logger.reset_mock()
        assert client.get_treatment('matching_key', 'some_feature', {'test': 'test'}) == 'default_treatment'
        assert _logger.error.mock_calls == []

        _logger.reset_mock()
        assert client.get_treatment('matching_key', 'some_feature', None) == 'default_treatment'
        assert _logger.error.mock_calls == []

        _logger.reset_mock()
        assert client.get_treatment('matching_key', '  some_feature   ', None) == 'default_treatment'
        assert _logger.warning.mock_calls == [
            mocker.call('%s: feature_name \'%s\' has extra whitespace, trimming.', 'get_treatment', '  some_feature   ')
        ]

        _logger.reset_mock()
        storage_mock.get.return_value = None
        assert client.get_treatment('matching_key', 'some_feature', None) == CONTROL
        assert _logger.warning.mock_calls == [
            mocker.call(
                "%s: you passed \"%s\" that does not exist in this environment, "
                "please double check what Splits exist in the web console.",
                'get_treatment',
                'some_feature'
            )
        ]

    def test_get_treatment_with_config(self, mocker):
        """Test get_treatment validation."""
        split_mock = mocker.Mock(spec=Split)
        default_treatment_mock = mocker.PropertyMock()
        default_treatment_mock.return_value = 'default_treatment'
        type(split_mock).default_treatment = default_treatment_mock
        conditions_mock = mocker.PropertyMock()
        conditions_mock.return_value = []
        type(split_mock).conditions = conditions_mock

        def _configs(treatment):
            return '{"some": "property"}' if treatment == 'default_treatment' else None
        split_mock.get_configurations_for.side_effect = _configs
        storage_mock = mocker.Mock(spec=SplitStorage)
        storage_mock.get.return_value = split_mock

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        recorder = StandardRecorder(impmanager, mocker.Mock(spec=EventStorage), ImpressionStorage, telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_consumer.get_telemetry_init_consumer(),
            mocker.Mock()
        )

        client = Client(factory, mocker.Mock())
        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert client.get_treatment_with_config(None, 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null key, key must be a non-empty string.', 'get_treatment_with_config')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config('', 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatment_with_config', 'key', 'key')
        ]

        _logger.reset_mock()
        key = ''.join('a' for _ in range(0, 255))
        assert client.get_treatment_with_config(key, 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatment_with_config', 'key', 250)
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config(12345, 'some_feature') == ('default_treatment', '{"some": "property"}')
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatment_with_config', 'key', 12345)
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config(float('nan'), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config(float('inf'), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config(True, 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config([], 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config('some_key', None) == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatment_with_config', 'feature_name', 'feature_name')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config('some_key', 123) == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'feature_name', 'feature_name')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config('some_key', True) == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'feature_name', 'feature_name')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config('some_key', []) == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'feature_name', 'feature_name')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config('some_key', '') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatment_with_config', 'feature_name', 'feature_name')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config('some_key', 'some_feature') == ('default_treatment', '{"some": "property"}')
        assert _logger.error.mock_calls == []
        assert _logger.warning.mock_calls == []

        _logger.reset_mock()
        assert client.get_treatment_with_config(Key(None, 'bucketing_key'), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatment_with_config', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config(Key('', 'bucketing_key'), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatment_with_config', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config(Key(float('nan'), 'bucketing_key'), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config(Key(float('inf'), 'bucketing_key'), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config(Key(True, 'bucketing_key'), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config(Key([], 'bucketing_key'), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config(Key(12345, 'bucketing_key'), 'some_feature') == ('default_treatment', '{"some": "property"}')
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatment_with_config', 'matching_key', 12345)
        ]

        _logger.reset_mock()
        key = ''.join('a' for _ in range(0, 255))
        assert client.get_treatment_with_config(Key(key, 'bucketing_key'), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatment_with_config', 'matching_key', 250)
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config(Key('matching_key', None), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatment_with_config', 'bucketing_key', 'bucketing_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config(Key('matching_key', True), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'bucketing_key', 'bucketing_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config(Key('matching_key', []), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'bucketing_key', 'bucketing_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config(Key('matching_key', ''), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatment_with_config', 'bucketing_key', 'bucketing_key')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config(Key('matching_key', 12345), 'some_feature') == ('default_treatment', '{"some": "property"}')
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatment_with_config', 'bucketing_key', 12345)
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config('matching_key', 'some_feature', True) == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: attributes must be of type dictionary.', 'get_treatment_with_config')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config('matching_key', 'some_feature', {'test': 'test'}) == ('default_treatment', '{"some": "property"}')
        assert _logger.error.mock_calls == []

        _logger.reset_mock()
        assert client.get_treatment_with_config('matching_key', 'some_feature', None) == ('default_treatment', '{"some": "property"}')
        assert _logger.error.mock_calls == []

        _logger.reset_mock()
        assert client.get_treatment_with_config('matching_key', '  some_feature   ', None) == ('default_treatment', '{"some": "property"}')
        assert _logger.warning.mock_calls == [
            mocker.call('%s: feature_name \'%s\' has extra whitespace, trimming.', 'get_treatment_with_config', '  some_feature   ')
        ]

        _logger.reset_mock()
        storage_mock.get.return_value = None
        assert client.get_treatment_with_config('matching_key', 'some_feature', None) == (CONTROL, None)
        assert _logger.warning.mock_calls == [
            mocker.call(
                "%s: you passed \"%s\" that does not exist in this environment, "
                "please double check what Splits exist in the web console.",
                'get_treatment_with_config',
                'some_feature'
            )
        ]

    def test_valid_properties(self, mocker):
        """Test valid_properties() method."""
        assert input_validator.valid_properties(None) == (True, None, 1024)
        assert input_validator.valid_properties([]) == (False, None, 0)
        assert input_validator.valid_properties(True) == (False, None, 0)
        assert input_validator.valid_properties(dict()) == (True, None, 1024)
        assert input_validator.valid_properties({2: 123}) == (True, None, 1024)

        class Test:
            pass
        assert input_validator.valid_properties({
            "test": Test()
        }) == (True, {"test": None}, 1028)

        props1 = {
            "test1": "test",
            "test2": 1,
            "test3": True,
            "test4": None,
            "test5": [],
            2: "t",
        }
        r1, r2, r3 = input_validator.valid_properties(props1)
        assert r1 is True
        assert len(r2.keys()) == 5
        assert r2["test1"] == "test"
        assert r2["test2"] == 1
        assert r2["test3"] is True
        assert r2["test4"] is None
        assert r2["test5"] is None
        assert r3 == 1053

        props2 = dict()
        for i in range(301):
            props2[str(i)] = i
        assert input_validator.valid_properties(props2) == (True, props2, 1817)

        props3 = dict()
        for i in range(100, 210):
            props3["prop" + str(i)] = "a" * 300
        r1, r2, r3 = input_validator.valid_properties(props3)
        assert r1 is False
        assert r3 == 32952

    def test_track(self, mocker):
        """Test track method()."""
        events_storage_mock = mocker.Mock(spec=EventStorage)
        events_storage_mock.put.return_value = True
        event_storage = mocker.Mock(spec=EventStorage)
        event_storage.put.return_value = True
        split_storage_mock = mocker.Mock(spec=SplitStorage)
        split_storage_mock.is_valid_traffic_type.return_value = True

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        recorder = StandardRecorder(impmanager, events_storage_mock, ImpressionStorage, telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {
                'splits': split_storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': events_storage_mock,
            },
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_consumer.get_telemetry_init_consumer(),
            mocker.Mock()
        )
        factory._apikey = 'some-test'

        client = Client(factory, recorder)
        client._event_storage = event_storage
        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert client.track(None, "traffic_type", "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.", 'track', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.track("", "traffic_type", "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an empty %s, %s must be a non-empty string.", 'track', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.track(12345, "traffic_type", "event_type", 1) is True
        assert _logger.warning.mock_calls == [
            mocker.call("%s: %s %s is not of type string, converting.", 'track', 'key', 12345)
        ]

        _logger.reset_mock()
        assert client.track(True, "traffic_type", "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'track', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.track([], "traffic_type", "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'track', 'key', 'key')
        ]

        _logger.reset_mock()
        key = ''.join('a' for _ in range(0, 255))
        assert client.track(key, "traffic_type", "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: %s too long - must be %s characters or less.", 'track', 'key', 250)
        ]

        _logger.reset_mock()
        assert client.track("some_key", None, "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.", 'track', 'traffic_type', 'traffic_type')
        ]

        _logger.reset_mock()
        assert client.track("some_key", "", "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an empty %s, %s must be a non-empty string.", 'track', 'traffic_type', 'traffic_type')
        ]

        _logger.reset_mock()
        assert client.track("some_key", 12345, "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'track', 'traffic_type', 'traffic_type')
        ]

        _logger.reset_mock()
        assert client.track("some_key", True, "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'track', 'traffic_type', 'traffic_type')
        ]

        _logger.reset_mock()
        assert client.track("some_key", [], "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'track', 'traffic_type', 'traffic_type')
        ]

        _logger.reset_mock()
        assert client.track("some_key", "TRAFFIC_type", "event_type", 1) is True
        assert _logger.warning.mock_calls == [
            mocker.call("track: %s should be all lowercase - converting string to lowercase.", 'TRAFFIC_type')
        ]

        assert client.track("some_key", "traffic_type", None, 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.", 'track', 'event_type', 'event_type')
        ]

        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", "", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an empty %s, %s must be a non-empty string.", 'track', 'event_type', 'event_type')
        ]

        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", True, 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'track', 'event_type', 'event_type')
        ]

        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", [], 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'track', 'event_type', 'event_type')
        ]

        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", 12345, 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'track', 'event_type', 'event_type')
        ]

        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", "@@", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed %s, event_type must adhere to the regular "
                        "expression %s. This means "
                        "an event name must be alphanumeric, cannot be more than 80 "
                        "characters long, and can only include a dash, underscore, "
                        "period, or colon as separators of alphanumeric characters.",
                        'track', '@@', '^[a-zA-Z0-9][-_.:a-zA-Z0-9]{0,79}$')
        ]

        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", None) is True
        assert _logger.error.mock_calls == []

        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", 1) is True
        assert _logger.error.mock_calls == []

        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", 1.23) is True
        assert _logger.error.mock_calls == []

        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", "test") is False
        assert _logger.error.mock_calls == [
            mocker.call("track: value must be a number.")
        ]

        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", True) is False
        assert _logger.error.mock_calls == [
            mocker.call("track: value must be a number.")
        ]

        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", []) is False
        assert _logger.error.mock_calls == [
            mocker.call("track: value must be a number.")
        ]

        # Test traffic type existance
        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(factory).ready = ready_property

#        factory._get_storage.return_value = split_storage_mock

        # Test that it doesn't warn if tt is cached, not in localhost mode and sdk is ready
        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", None) is True
        assert _logger.error.mock_calls == []
        assert _logger.warning.mock_calls == []

        # Test that it does warn if tt is cached, not in localhost mode and sdk is ready
        split_storage_mock.is_valid_traffic_type.return_value = False
        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", None) is True
        assert _logger.error.mock_calls == []
        assert _logger.warning.mock_calls == [mocker.call(
            'track: Traffic Type %s does not have any corresponding Splits in this environment, '
            'make sure you\'re tracking your events to a valid traffic type defined '
            'in the Split console.',
            'traffic_type'
        )]

        # Test that it does not warn when in localhost mode.
        factory._apikey = 'localhost'
        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", None) is True
        assert _logger.error.mock_calls == []
        assert _logger.warning.mock_calls == []

        # Test that it does not warn when not in localhost mode and not ready
        factory._apikey = 'not-localhost'
        ready_property.return_value = False
        type(factory).ready = ready_property
        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", None) is True
        assert _logger.error.mock_calls == []
        assert _logger.warning.mock_calls == []

        # Test track with invalid properties
        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", 1, []) is False
        assert _logger.error.mock_calls == [
            mocker.call("track: properties must be of type dictionary.")
        ]

        # Test track with invalid properties
        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", 1, True) is False
        assert _logger.error.mock_calls == [
            mocker.call("track: properties must be of type dictionary.")
        ]

        # Test track with properties
        props1 = {
            "test1": "test",
            "test2": 1,
            "test3": True,
            "test4": None,
            "test5": [],
            2: "t",
        }
        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", 1, props1) is True
        assert _logger.warning.mock_calls == [
            mocker.call("Property %s is of invalid type. Setting value to None", [])
        ]

        # Test track with more than 300 properties
        props2 = dict()
        for i in range(301):
            props2[str(i)] = i
        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", 1, props2) is True
        assert _logger.warning.mock_calls == [
            mocker.call("Event has more than 300 properties. Some of them will be trimmed when processed")
        ]

        # Test track with properties higher than 32kb
        _logger.reset_mock()
        props3 = dict()
        for i in range(100, 210):
            props3["prop" + str(i)] = "a" * 300
        assert client.track("some_key", "traffic_type", "event_type", 1, props3) is False
        assert _logger.error.mock_calls == [
            mocker.call("The maximum size allowed for the properties is 32768 bytes. Current one is 32952 bytes. Event not queued")
        ]

    def test_get_treatments(self, mocker):
        """Test getTreatments() method."""
        split_mock = mocker.Mock(spec=Split)
        default_treatment_mock = mocker.PropertyMock()
        default_treatment_mock.return_value = 'default_treatment'
        type(split_mock).default_treatment = default_treatment_mock
        conditions_mock = mocker.PropertyMock()
        conditions_mock.return_value = []
        type(split_mock).conditions = conditions_mock
        storage_mock = mocker.Mock(spec=SplitStorage)
        storage_mock.get.return_value = split_mock
        storage_mock.fetch_many.return_value = {
            'some_feature': split_mock,
            'some': split_mock,
        }

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        recorder = StandardRecorder(impmanager, mocker.Mock(spec=EventStorage), mocker.Mock(spec=ImpressionStorage), telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_consumer.get_telemetry_init_consumer(),
            mocker.Mock()
        )
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock

        client = Client(factory, recorder)
        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert client.get_treatments(None, ['some_feature']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null key, key must be a non-empty string.', 'get_treatments')
        ]

        _logger.reset_mock()
        assert client.get_treatments("", ['some_feature']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatments', 'key', 'key')
        ]

        key = ''.join('a' for _ in range(0, 255))
        _logger.reset_mock()
        assert client.get_treatments(key, ['some_feature']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatments', 'key', 250)
        ]

        _logger.reset_mock()
        assert client.get_treatments(12345, ['some_feature']) == {'some_feature': 'default_treatment'}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatments', 'key', 12345)
        ]

        _logger.reset_mock()
        assert client.get_treatments(True, ['some_feature']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatments([], ['some_feature']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatments('some_key', None) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature_names must be a non-empty array.', 'get_treatments')
        ]

        _logger.reset_mock()
        assert client.get_treatments('some_key', True) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature_names must be a non-empty array.', 'get_treatments')
        ]

        _logger.reset_mock()
        assert client.get_treatments('some_key', 'some_string') == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature_names must be a non-empty array.', 'get_treatments')
        ]

        _logger.reset_mock()
        assert client.get_treatments('some_key', []) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature_names must be a non-empty array.', 'get_treatments')
        ]

        _logger.reset_mock()
        assert client.get_treatments('some_key', [None, None]) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature_names must be a non-empty array.', 'get_treatments')
        ]

        _logger.reset_mock()
        assert client.get_treatments('some_key', [True]) == {}
        assert mocker.call('%s: feature_names must be a non-empty array.', 'get_treatments') in _logger.error.mock_calls

        _logger.reset_mock()
        assert client.get_treatments('some_key', ['', '']) == {}
        assert mocker.call('%s: feature_names must be a non-empty array.', 'get_treatments') in _logger.error.mock_calls

        _logger.reset_mock()
        assert client.get_treatments('some_key', ['some   ']) == {'some': 'default_treatment'}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: feature_name \'%s\' has extra whitespace, trimming.', 'get_treatments', 'some   ')
        ]

        _logger.reset_mock()
        storage_mock.fetch_many.return_value = {
            'some_feature': None
        }
        storage_mock.get.return_value = None
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock
        assert client.get_treatments('matching_key', ['some_feature'], None) == {'some_feature': CONTROL}
        assert _logger.warning.mock_calls == [
            mocker.call(
                "%s: you passed \"%s\" that does not exist in this environment, "
                "please double check what Splits exist in the web console.",
                'get_treatments',
                'some_feature'
            )
        ]

    def test_get_treatments_with_config(self, mocker):
        """Test getTreatments() method."""
        split_mock = mocker.Mock(spec=Split)
        default_treatment_mock = mocker.PropertyMock()
        default_treatment_mock.return_value = 'default_treatment'
        type(split_mock).default_treatment = default_treatment_mock
        conditions_mock = mocker.PropertyMock()
        conditions_mock.return_value = []
        type(split_mock).conditions = conditions_mock

        storage_mock = mocker.Mock(spec=SplitStorage)
        storage_mock.fetch_many.return_value = {
            'some_feature': split_mock
        }

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        recorder = StandardRecorder(impmanager, mocker.Mock(spec=EventStorage), mocker.Mock(spec=ImpressionStorage), telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_consumer.get_telemetry_init_consumer(),
            mocker.Mock()
        )

        def _configs(treatment):
            return '{"some": "property"}' if treatment == 'default_treatment' else None
        split_mock.get_configurations_for.side_effect = _configs

        client = Client(factory, mocker.Mock())
        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert client.get_treatments_with_config(None, ['some_feature']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null key, key must be a non-empty string.', 'get_treatments_with_config')
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config("", ['some_feature']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatments_with_config', 'key', 'key')
        ]

        key = ''.join('a' for _ in range(0, 255))
        _logger.reset_mock()
        assert client.get_treatments_with_config(key, ['some_feature']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatments_with_config', 'key', 250)
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config(12345, ['some_feature']) == {'some_feature': ('default_treatment', '{"some": "property"}')}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatments_with_config', 'key', 12345)
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config(True, ['some_feature']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_with_config', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config([], ['some_feature']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_with_config', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config('some_key', None) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature_names must be a non-empty array.', 'get_treatments_with_config')
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config('some_key', True) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature_names must be a non-empty array.', 'get_treatments_with_config')
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config('some_key', 'some_string') == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature_names must be a non-empty array.', 'get_treatments_with_config')
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config('some_key', []) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature_names must be a non-empty array.', 'get_treatments_with_config')
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config('some_key', [None, None]) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature_names must be a non-empty array.', 'get_treatments_with_config')
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config('some_key', [True]) == {}
        assert mocker.call('%s: feature_names must be a non-empty array.', 'get_treatments_with_config') in _logger.error.mock_calls

        _logger.reset_mock()
        assert client.get_treatments_with_config('some_key', ['', '']) == {}
        assert mocker.call('%s: feature_names must be a non-empty array.', 'get_treatments_with_config') in _logger.error.mock_calls

        _logger.reset_mock()
        assert client.get_treatments_with_config('some_key', ['some_feature   ']) == {'some_feature': ('default_treatment', '{"some": "property"}')}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: feature_name \'%s\' has extra whitespace, trimming.', 'get_treatments_with_config', 'some_feature   ')
        ]

        _logger.reset_mock()
        storage_mock.fetch_many.return_value = {
            'some_feature': None
        }
        storage_mock.get.return_value = None
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock
        assert client.get_treatments('matching_key', ['some_feature'], None) == {'some_feature': CONTROL}
        assert _logger.warning.mock_calls == [
            mocker.call(
                "%s: you passed \"%s\" that does not exist in this environment, "
                "please double check what Splits exist in the web console.",
                'get_treatments',
                'some_feature'
            )
        ]

class ManagerInputValidationTests(object):  #pylint: disable=too-few-public-methods
    """Manager input validation test cases."""

    def test_split_(self, mocker):
        """Test split input validation."""
        storage_mock = mocker.Mock(spec=SplitStorage)
        split_mock = mocker.Mock(spec=Split)
        storage_mock.get.return_value = split_mock

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        recorder = StandardRecorder(impmanager, mocker.Mock(spec=EventStorage), mocker.Mock(spec=ImpressionStorage), telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_consumer.get_telemetry_init_consumer(),
            mocker.Mock()
        )

        manager = SplitManager(factory)
        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert manager.split(None) is None
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.", 'split', 'feature_name', 'feature_name')
        ]

        _logger.reset_mock()
        assert manager.split("") is None
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an empty %s, %s must be a non-empty string.", 'split', 'feature_name', 'feature_name')
        ]

        _logger.reset_mock()
        assert manager.split(True) is None
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'split', 'feature_name', 'feature_name')
        ]

        _logger.reset_mock()
        assert manager.split([]) is None
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'split', 'feature_name', 'feature_name')
        ]

        _logger.reset_mock()
        manager.split('some_split')
        assert split_mock.to_split_view.mock_calls == [mocker.call()]
        assert _logger.error.mock_calls == []

        _logger.reset_mock()
        split_mock.reset_mock()
        storage_mock.get.return_value = None
        manager.split('nonexistant-split')
        assert split_mock.to_split_view.mock_calls == []
        assert _logger.warning.mock_calls == [mocker.call(
            "split: you passed \"%s\" that does not exist in this environment, "
            "please double check what Splits exist in the web console.",
            'nonexistant-split'
        )]

class FactoryInputValidationTests(object):  #pylint: disable=too-few-public-methods
    """Factory instantiation input validation test cases."""

    def test_input_validation_factory(self, mocker):
        """Test the input validators for factory instantiation."""
        logger = mocker.Mock(spec=logging.Logger)
        mocker.patch('splitio.client.input_validator._LOGGER', new=logger)

        assert get_factory(None) is None
        assert logger.error.mock_calls == [
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.", 'factory_instantiation', 'apikey', 'apikey')
        ]

        logger.reset_mock()
        assert get_factory('') is None
        assert logger.error.mock_calls == [
            mocker.call("%s: you passed an empty %s, %s must be a non-empty string.", 'factory_instantiation', 'apikey', 'apikey')
        ]

        logger.reset_mock()
        assert get_factory(True) is None
        assert logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'factory_instantiation', 'apikey', 'apikey')
        ]

        logger.reset_mock()
        try:
            f = get_factory(True, config={'redisHost': 'localhost'})
        except:
            pass
        assert logger.error.mock_calls == []
        f.destroy()