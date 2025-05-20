"""Unit tests for the input_validator module."""
import logging
import pytest

from splitio.client.factory import SplitFactory, get_factory, SplitFactoryAsync, get_factory_async
from splitio.client.client import CONTROL, Client, _LOGGER as _logger, ClientAsync
from splitio.client.manager import SplitManager, SplitManagerAsync
from splitio.client.key import Key
from splitio.storage import SplitStorage, EventStorage, ImpressionStorage, SegmentStorage, RuleBasedSegmentsStorage
from splitio.storage.inmemmory import InMemoryTelemetryStorage, InMemoryTelemetryStorageAsync, \
    InMemorySplitStorage, InMemorySplitStorageAsync, InMemoryRuleBasedSegmentStorage, InMemoryRuleBasedSegmentStorageAsync
from splitio.models.splits import Split
from splitio.client import input_validator
from splitio.recorder.recorder import StandardRecorder, StandardRecorderAsync
from splitio.engine.telemetry import TelemetryStorageProducer, TelemetryStorageProducerAsync
from splitio.engine.impressions.impressions import Manager as ImpressionManager
from splitio.engine.evaluator import EvaluationDataFactory

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
        storage_mock.fetch_many.return_value = {'some_feature': split_mock}
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorage)
        rbs_storage.fetch_many.return_value = {}

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, mocker.Mock(spec=EventStorage), ImpressionStorage, telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': rbs_storage,
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )

        client = Client(factory, mocker.Mock())
        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert client.get_treatment(None, 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatment', 'key', 'key')
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
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatment', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert client.get_treatment('some_key', 123) == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert client.get_treatment('some_key', True) == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert client.get_treatment('some_key', []) == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert client.get_treatment('some_key', '') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatment', 'feature_flag_name', 'feature_flag_name')
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
            mocker.call('%s: %s \'%s\' has extra whitespace, trimming.', 'get_treatment', 'feature flag name', '  some_feature   ')
        ]

        _logger.reset_mock()
        storage_mock.fetch_many.return_value = {'some_feature': None}
        mocker.patch('splitio.client.client._LOGGER', new=_logger)
        assert client.get_treatment('matching_key', 'some_feature', None) == CONTROL
        assert _logger.warning.mock_calls == [
            mocker.call(
                "%s: you passed \"%s\" that does not exist in this environment, "
                "please double check what Feature flags exist in the Split user interface.",
                'get_treatment',
                'some_feature'
            )
        ]
        factory.destroy

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
        storage_mock.fetch_many.return_value = {'some_feature': split_mock}
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorage)
        rbs_storage.fetch_many.return_value = {}

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, mocker.Mock(spec=EventStorage), ImpressionStorage, telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': rbs_storage,
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )

        client = Client(factory, mocker.Mock())
        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert client.get_treatment_with_config(None, 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatment_with_config', 'key', 'key')
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
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatment_with_config', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config('some_key', 123) == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config('some_key', True) == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config('some_key', []) == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert client.get_treatment_with_config('some_key', '') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatment_with_config', 'feature_flag_name', 'feature_flag_name')
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
            mocker.call('%s: %s \'%s\' has extra whitespace, trimming.', 'get_treatment_with_config', 'feature flag name', '  some_feature   ')
        ]

        _logger.reset_mock()
        storage_mock.fetch_many.return_value = {'some_feature': None}
        mocker.patch('splitio.client.client._LOGGER', new=_logger)
        assert client.get_treatment_with_config('matching_key', 'some_feature', None) == (CONTROL, None)
        assert _logger.warning.mock_calls == [
            mocker.call(
                "%s: you passed \"%s\" that does not exist in this environment, "
                "please double check what Feature flags exist in the Split user interface.",
                'get_treatment_with_config',
                'some_feature'
            )
        ]
        factory.destroy

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
        recorder = StandardRecorder(impmanager, events_storage_mock, ImpressionStorage, telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {
                'splits': split_storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': mocker.Mock(spec=RuleBasedSegmentsStorage),
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': events_storage_mock,
            },
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        factory._sdk_key = 'some-test'

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
            mocker.call("%s: %s '%s' should be all lowercase - converting string to lowercase", 'track', 'traffic type', 'TRAFFIC_type')
        ]

        _logger.reset_mock()
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
            mocker.call("%s: you passed %s, %s must adhere to the regular "
                        "expression %s. This means "
                        "%s must be alphanumeric, cannot be more than %s "
                        "characters long, and can only include a dash, underscore, "
                        "period, or colon as separators of alphanumeric characters.",
                        'track', '@@', 'an event name', '^[a-zA-Z0-9][-_.:a-zA-Z0-9]{0,79}$', 'an event name', 80)
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
            'track: Traffic Type %s does not have any corresponding Feature flags in this environment, '
            'make sure you\'re tracking your events to a valid traffic type defined '
            'in the Split user interface.',
            'traffic_type'
        )]

        # Test that it does not warn when in localhost mode.
        factory._sdk_key = 'localhost'
        _logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", None) is True
        assert _logger.error.mock_calls == []
        assert _logger.warning.mock_calls == []

        # Test that it does not warn when not in localhost mode and not ready
        factory._sdk_key = 'not-localhost'
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
        factory.destroy

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
        storage_mock.fetch_many.return_value = {
            'some_feature': split_mock
        }
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorage)
        rbs_storage.fetch_many.return_value = {}

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, mocker.Mock(spec=EventStorage), mocker.Mock(spec=ImpressionStorage), telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': rbs_storage,
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
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
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatments', 'key', 'key')
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

        split_mock.name = 'some_feature'
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
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments')
        ]

        _logger.reset_mock()
        assert client.get_treatments('some_key', True) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments')
        ]

        _logger.reset_mock()
        assert client.get_treatments('some_key', 'some_string') == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments')
        ]

        _logger.reset_mock()
        assert client.get_treatments('some_key', []) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments')
        ]

        _logger.reset_mock()
        assert client.get_treatments('some_key', [None, None]) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments')
        ]

        _logger.reset_mock()
        assert client.get_treatments('some_key', [True]) == {}
        assert mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments') in _logger.error.mock_calls

        _logger.reset_mock()
        assert client.get_treatments('some_key', ['', '']) == {}
        assert mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments') in _logger.error.mock_calls

        _logger.reset_mock()
        assert client.get_treatments('some_key', ['some_feature   ']) == {'some_feature': 'default_treatment'}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s \'%s\' has extra whitespace, trimming.', 'get_treatments', 'feature flag name', 'some_feature   ')
        ]

        _logger.reset_mock()
        storage_mock.fetch_many.return_value = {
            'some_feature': None
        }
        storage_mock.fetch_many.return_value = {'some_feature': None}
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock
        mocker.patch('splitio.client.client._LOGGER', new=_logger)
        assert client.get_treatments('matching_key', ['some_feature'], None) == {'some_feature': CONTROL}
        assert _logger.warning.mock_calls == [
            mocker.call(
                "%s: you passed \"%s\" that does not exist in this environment, "
                "please double check what Feature flags exist in the Split user interface.",
                'get_treatments',
                'some_feature'
            )
        ]
        factory.destroy

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
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorage)
        rbs_storage.fetch_many.return_value = {}

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, mocker.Mock(spec=EventStorage), mocker.Mock(spec=ImpressionStorage), telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': rbs_storage,
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        split_mock.name = 'some_feature'

        def _configs(treatment):
            return '{"some": "property"}' if treatment == 'default_treatment' else None
        split_mock.get_configurations_for.side_effect = _configs

        client = Client(factory, mocker.Mock())
        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert client.get_treatments_with_config(None, ['some_feature']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatments_with_config', 'key', 'key')
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
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments_with_config')
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config('some_key', True) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments_with_config')
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config('some_key', 'some_string') == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments_with_config')
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config('some_key', []) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments_with_config')
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config('some_key', [None, None]) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments_with_config')
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config('some_key', [True]) == {}
        assert mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments_with_config') in _logger.error.mock_calls

        _logger.reset_mock()
        assert client.get_treatments_with_config('some_key', ['', '']) == {}
        assert mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments_with_config') in _logger.error.mock_calls

        _logger.reset_mock()
        assert client.get_treatments_with_config('some_key', ['some_feature   ']) == {'some_feature': ('default_treatment', '{"some": "property"}')}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s \'%s\' has extra whitespace, trimming.', 'get_treatments_with_config', 'feature flag name', 'some_feature   ')
        ]

        _logger.reset_mock()
        storage_mock.fetch_many.return_value = {
            'some_feature': None
        }
        storage_mock.get.return_value = None
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock
        mocker.patch('splitio.client.client._LOGGER', new=_logger)
        assert client.get_treatments('matching_key', ['some_feature'], None) == {'some_feature': CONTROL}
        assert _logger.warning.mock_calls == [
            mocker.call(
                "%s: you passed \"%s\" that does not exist in this environment, "
                "please double check what Feature flags exist in the Split user interface.",
                'get_treatments',
                'some_feature'
            )
        ]
        factory.destroy

    def test_get_treatments_by_flag_set(self, mocker):
        """Test getTreatments() method."""
        split_mock = mocker.Mock(spec=Split)
        default_treatment_mock = mocker.PropertyMock()
        default_treatment_mock.return_value = 'default_treatment'
        type(split_mock).default_treatment = default_treatment_mock
        conditions_mock = mocker.PropertyMock()
        conditions_mock.return_value = []
        type(split_mock).conditions = conditions_mock
        storage_mock = mocker.Mock(spec=InMemorySplitStorage)
        storage_mock.fetch_many.return_value = {
            'some_feature': split_mock
        }
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorage)
        rbs_storage.fetch_many.return_value = {}
        storage_mock.get_feature_flags_by_sets.return_value = ['some_feature']
        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, mocker.Mock(spec=EventStorage), mocker.Mock(spec=ImpressionStorage), telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': rbs_storage,
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock

        client = Client(factory, recorder)
        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert client.get_treatments_by_flag_set(None, 'some_set') == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatments_by_flag_set', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatments_by_flag_set("", 'some_set') == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatments_by_flag_set', 'key', 'key')
        ]

        key = ''.join('a' for _ in range(0, 255))
        _logger.reset_mock()
        assert client.get_treatments_by_flag_set(key, 'some_set') == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatments_by_flag_set', 'key', 250)
        ]

        split_mock.name = 'some_feature'
        _logger.reset_mock()
        assert client.get_treatments_by_flag_set(12345, 'some_set') == {'some_feature': 'default_treatment'}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatments_by_flag_set', 'key', 12345)
        ]

        _logger.reset_mock()
        assert client.get_treatments_by_flag_set(True, 'some_set') == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_by_flag_set', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatments_by_flag_set([], 'some_set') == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_by_flag_set', 'key', 'key')
        ]

        _logger.reset_mock()
        client.get_treatments_by_flag_set('some_key', None)
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.",
                        'get_treatments_by_flag_set', 'flag set', 'flag set')
        ]

        _logger.reset_mock()
        client.get_treatments_by_flag_set('some_key', '$$')
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed %s, %s must adhere to the regular "
                        "expression %s. This means "
                        "%s must be alphanumeric, cannot be more than %s "
                        "characters long, and can only include a dash, underscore, "
                        "period, or colon as separators of alphanumeric characters.",
                        'get_treatments_by_flag_set', '$$', 'a flag set', '^[a-z0-9][_a-z0-9]{0,49}$', 'a flag set', 50)
        ]

        _logger.reset_mock()
        assert client.get_treatments_by_flag_set('some_key', 'some_set   ') == {'some_feature': 'default_treatment'}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s \'%s\' has extra whitespace, trimming.', 'get_treatments_by_flag_set', 'flag set', 'some_set   ')
        ]

        _logger.reset_mock()
        storage_mock.get_feature_flags_by_sets.return_value = []
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock
        mocker.patch('splitio.client.client._LOGGER', new=_logger)
        assert client.get_treatments_by_flag_set('matching_key', 'some_set') == {}
        assert _logger.warning.mock_calls == [
            mocker.call("%s: No valid Flag set or no feature flags found for evaluating treatments", "get_treatments_by_flag_set")
        ]
        factory.destroy

    def test_get_treatments_by_flag_sets(self, mocker):
        """Test getTreatments() method."""
        split_mock = mocker.Mock(spec=Split)
        default_treatment_mock = mocker.PropertyMock()
        default_treatment_mock.return_value = 'default_treatment'
        type(split_mock).default_treatment = default_treatment_mock
        conditions_mock = mocker.PropertyMock()
        conditions_mock.return_value = []
        type(split_mock).conditions = conditions_mock
        storage_mock = mocker.Mock(spec=InMemorySplitStorage)
        storage_mock.fetch_many.return_value = {
            'some_feature': split_mock
        }
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorage)
        rbs_storage.fetch_many.return_value = {}
        storage_mock.get_feature_flags_by_sets.return_value = ['some_feature']
        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, mocker.Mock(spec=EventStorage), mocker.Mock(spec=ImpressionStorage), telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': rbs_storage,
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock

        client = Client(factory, recorder)
        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert client.get_treatments_by_flag_sets(None, ['some_set']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatments_by_flag_sets', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatments_by_flag_sets("", ['some_set']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatments_by_flag_sets', 'key', 'key')
        ]

        key = ''.join('a' for _ in range(0, 255))
        _logger.reset_mock()
        assert client.get_treatments_by_flag_sets(key, ['some_set']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatments_by_flag_sets', 'key', 250)
        ]

        split_mock.name = 'some_feature'
        _logger.reset_mock()
        assert client.get_treatments_by_flag_sets(12345, ['some_set']) == {'some_feature': 'default_treatment'}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatments_by_flag_sets', 'key', 12345)
        ]

        _logger.reset_mock()
        assert client.get_treatments_by_flag_sets(True, ['some_set']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_by_flag_sets', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatments_by_flag_sets([], ['some_set']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_by_flag_sets', 'key', 'key')
        ]

        _logger.reset_mock()
        client.get_treatments_by_flag_sets('some_key', None)
        assert _logger.warning.mock_calls == [
            mocker.call("%s: flag sets parameter type should be list object, parameter is discarded", "get_treatments_by_flag_sets")
        ]

        _logger.reset_mock()
        client.get_treatments_by_flag_sets('some_key', [None])
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.",
                        'get_treatments_by_flag_sets', 'flag set', 'flag set')
        ]

        _logger.reset_mock()
        client.get_treatments_by_flag_sets('some_key', ['$$'])
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed %s, %s must adhere to the regular "
                        "expression %s. This means "
                        "%s must be alphanumeric, cannot be more than %s "
                        "characters long, and can only include a dash, underscore, "
                        "period, or colon as separators of alphanumeric characters.",
                        'get_treatments_by_flag_sets', '$$', 'a flag set', '^[a-z0-9][_a-z0-9]{0,49}$', 'a flag set', 50)
        ]

        _logger.reset_mock()
        assert client.get_treatments_by_flag_sets('some_key', ['some_set   ']) == {'some_feature': 'default_treatment'}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s \'%s\' has extra whitespace, trimming.', 'get_treatments_by_flag_sets', 'flag set', 'some_set   ')
        ]

        _logger.reset_mock()
        storage_mock.get_feature_flags_by_sets.return_value = []
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock
        mocker.patch('splitio.client.client._LOGGER', new=_logger)
        assert client.get_treatments_by_flag_sets('matching_key', ['some_set']) == {}
        assert _logger.warning.mock_calls == [
            mocker.call("%s: No valid Flag set or no feature flags found for evaluating treatments", "get_treatments_by_flag_sets")
        ]
        factory.destroy

    def test_get_treatments_with_config_by_flag_set(self, mocker):
        split_mock = mocker.Mock(spec=Split)
        def _configs(treatment):
            return '{"some": "property"}' if treatment == 'default_treatment' else None
        split_mock.get_configurations_for.side_effect = _configs
        split_mock.name = 'some_feature'
        default_treatment_mock = mocker.PropertyMock()
        default_treatment_mock.return_value = 'default_treatment'
        type(split_mock).default_treatment = default_treatment_mock
        conditions_mock = mocker.PropertyMock()
        conditions_mock.return_value = []
        type(split_mock).conditions = conditions_mock
        storage_mock = mocker.Mock(spec=InMemorySplitStorage)
        storage_mock.fetch_many.return_value = {
            'some_feature': split_mock
        }
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorage)
        rbs_storage.fetch_many.return_value = {}
        
        storage_mock.get_feature_flags_by_sets.return_value = ['some_feature']

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, mocker.Mock(spec=EventStorage), mocker.Mock(spec=ImpressionStorage), telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': rbs_storage,
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock

        client = Client(factory, recorder)
        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert client.get_treatments_with_config_by_flag_set(None, 'some_set') == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatments_with_config_by_flag_set', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config_by_flag_set("", 'some_set') == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatments_with_config_by_flag_set', 'key', 'key')
        ]

        key = ''.join('a' for _ in range(0, 255))
        _logger.reset_mock()
        assert client.get_treatments_with_config_by_flag_set(key, 'some_set') == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatments_with_config_by_flag_set', 'key', 250)
        ]

        split_mock.name = 'some_feature'
        _logger.reset_mock()
        assert client.get_treatments_with_config_by_flag_set(12345, 'some_set') == {'some_feature': ('default_treatment', '{"some": "property"}')}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatments_with_config_by_flag_set', 'key', 12345)
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config_by_flag_set(True, 'some_set') == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_with_config_by_flag_set', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config_by_flag_set([], 'some_set') == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_with_config_by_flag_set', 'key', 'key')
        ]

        _logger.reset_mock()
        client.get_treatments_with_config_by_flag_set('some_key', None)
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.",
                        'get_treatments_with_config_by_flag_set', 'flag set', 'flag set')
        ]

        _logger.reset_mock()
        client.get_treatments_with_config_by_flag_set('some_key', '$$')
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed %s, %s must adhere to the regular "
                        "expression %s. This means "
                        "%s must be alphanumeric, cannot be more than %s "
                        "characters long, and can only include a dash, underscore, "
                        "period, or colon as separators of alphanumeric characters.",
                        'get_treatments_with_config_by_flag_set', '$$', 'a flag set', '^[a-z0-9][_a-z0-9]{0,49}$', 'a flag set', 50)
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config_by_flag_set('some_key', 'some_set   ') == {'some_feature': ('default_treatment', '{"some": "property"}')}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s \'%s\' has extra whitespace, trimming.', 'get_treatments_with_config_by_flag_set', 'flag set', 'some_set   ')
        ]

        _logger.reset_mock()
        storage_mock.get_feature_flags_by_sets.return_value = []
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock
        mocker.patch('splitio.client.client._LOGGER', new=_logger)
        assert client.get_treatments_with_config_by_flag_set('matching_key', 'some_set') == {}
        assert _logger.warning.mock_calls == [
            mocker.call("%s: No valid Flag set or no feature flags found for evaluating treatments", "get_treatments_with_config_by_flag_set")
        ]
        factory.destroy

    def test_get_treatments_with_config_by_flag_sets(self, mocker):
        split_mock = mocker.Mock(spec=Split)
        def _configs(treatment):
            return '{"some": "property"}' if treatment == 'default_treatment' else None
        split_mock.get_configurations_for.side_effect = _configs
        split_mock.name = 'some_feature'
        default_treatment_mock = mocker.PropertyMock()
        default_treatment_mock.return_value = 'default_treatment'
        type(split_mock).default_treatment = default_treatment_mock
        conditions_mock = mocker.PropertyMock()
        conditions_mock.return_value = []
        type(split_mock).conditions = conditions_mock
        storage_mock = mocker.Mock(spec=InMemorySplitStorage)
        storage_mock.fetch_many.return_value = {
            'some_feature': split_mock
        }
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorage)
        rbs_storage.fetch_many.return_value = {}
        
        storage_mock.get_feature_flags_by_sets.return_value = ['some_feature']

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, mocker.Mock(spec=EventStorage), mocker.Mock(spec=ImpressionStorage), telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': rbs_storage,
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock

        client = Client(factory, recorder)
        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert client.get_treatments_with_config_by_flag_sets(None, ['some_set']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatments_with_config_by_flag_sets', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config_by_flag_sets("", ['some_set']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatments_with_config_by_flag_sets', 'key', 'key')
        ]

        key = ''.join('a' for _ in range(0, 255))
        _logger.reset_mock()
        assert client.get_treatments_with_config_by_flag_sets(key, ['some_set']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatments_with_config_by_flag_sets', 'key', 250)
        ]

        split_mock.name = 'some_feature'
        _logger.reset_mock()
        assert client.get_treatments_with_config_by_flag_sets(12345, ['some_set']) == {'some_feature': ('default_treatment', '{"some": "property"}')}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatments_with_config_by_flag_sets', 'key', 12345)
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config_by_flag_sets(True, ['some_set']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_with_config_by_flag_sets', 'key', 'key')
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config_by_flag_sets([], ['some_set']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_with_config_by_flag_sets', 'key', 'key')
        ]

        _logger.reset_mock()
        client.get_treatments_with_config_by_flag_sets('some_key', [None])
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.",
                        'get_treatments_with_config_by_flag_sets', 'flag set', 'flag set')
        ]

        _logger.reset_mock()
        client.get_treatments_with_config_by_flag_sets('some_key', ['$$'])
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed %s, %s must adhere to the regular "
                        "expression %s. This means "
                        "%s must be alphanumeric, cannot be more than %s "
                        "characters long, and can only include a dash, underscore, "
                        "period, or colon as separators of alphanumeric characters.",
                        'get_treatments_with_config_by_flag_sets', '$$', 'a flag set', '^[a-z0-9][_a-z0-9]{0,49}$', 'a flag set', 50)
        ]

        _logger.reset_mock()
        assert client.get_treatments_with_config_by_flag_sets('some_key', ['some_set   ']) == {'some_feature': ('default_treatment', '{"some": "property"}')}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s \'%s\' has extra whitespace, trimming.', 'get_treatments_with_config_by_flag_sets', 'flag set', 'some_set   ')
        ]

        _logger.reset_mock()
        storage_mock.get_feature_flags_by_sets.return_value = []
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock
        mocker.patch('splitio.client.client._LOGGER', new=_logger)
        assert client.get_treatments_with_config_by_flag_sets('matching_key', ['some_set']) == {}
        assert _logger.warning.mock_calls == [
            mocker.call("%s: No valid Flag set or no feature flags found for evaluating treatments", "get_treatments_with_config_by_flag_sets")
        ]
        factory.destroy

    def test_flag_sets_validation(self):
        """Test sanitization for flag sets."""
        flag_sets = input_validator.validate_flag_sets([' set1', 'set2 ', 'set3'], 'method')
        assert sorted(flag_sets) == ['set1', 'set2', 'set3']

        flag_sets = input_validator.validate_flag_sets(['1set', '_set2'], 'method')
        assert flag_sets == ['1set']

        flag_sets = input_validator.validate_flag_sets(['Set1', 'SET2'], 'method')
        assert sorted(flag_sets) == ['set1', 'set2']

        flag_sets = input_validator.validate_flag_sets(['se\t1', 's/et2', 's*et3', 's!et4', 'se@t5', 'se#t5', 'se$t5', 'se^t5', 'se%t5', 'se&t5'], 'method')
        assert flag_sets == []

        flag_sets = input_validator.validate_flag_sets(['set4', 'set1', 'set3', 'set1'], 'method')
        assert sorted(flag_sets) == ['set1', 'set3', 'set4']

        flag_sets = input_validator.validate_flag_sets(['w' * 50, 's' * 51], 'method')
        assert flag_sets == ['w' * 50]

        flag_sets = input_validator.validate_flag_sets('set1', 'method')
        assert flag_sets == []

        flag_sets = input_validator.validate_flag_sets([12, 33], 'method')
        assert flag_sets == []


class ClientInputValidationAsyncTests(object):
    """Input validation test cases."""

    @pytest.mark.asyncio
    async def test_get_treatment(self, mocker):
        """Test get_treatment validation."""
        split_mock = mocker.Mock(spec=Split)
        default_treatment_mock = mocker.PropertyMock()
        default_treatment_mock.return_value = 'default_treatment'
        type(split_mock).default_treatment = default_treatment_mock
        conditions_mock = mocker.PropertyMock()
        conditions_mock.return_value = []
        type(split_mock).conditions = conditions_mock
        storage_mock = mocker.Mock(spec=SplitStorage)
        async def fetch_many(*_):
            return {
            'some_feature': split_mock
            }
        storage_mock.fetch_many = fetch_many
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorageAsync)
        async def fetch_many_rbs(*_):
            return {}
        rbs_storage.fetch_many = fetch_many_rbs

        async def get_change_number(*_):
            return 1
        storage_mock.get_change_number = get_change_number

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        recorder = StandardRecorderAsync(impmanager, mocker.Mock(spec=EventStorage), ImpressionStorage, telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactoryAsync(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': rbs_storage,
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            impmanager,
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock

        client = ClientAsync(factory, mocker.Mock())

        async def record_treatment_stats(*_):
            pass
        client._recorder.record_treatment_stats = record_treatment_stats

        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert await client.get_treatment(None, 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatment', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment('', 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatment', 'key', 'key')
        ]

        _logger.reset_mock()
        key = ''.join('a' for _ in range(0, 255))
        assert await client.get_treatment(key, 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatment', 'key', 250)
        ]

        _logger.reset_mock()
        assert await client.get_treatment(12345, 'some_feature') == 'default_treatment'
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatment', 'key', 12345)
        ]

        _logger.reset_mock()
        assert await client.get_treatment(float('nan'), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment(float('inf'), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment(True, 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment([], 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment('some_key', None) == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatment', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert await client.get_treatment('some_key', 123) == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert await client.get_treatment('some_key', True) == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert await client.get_treatment('some_key', []) == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert await client.get_treatment('some_key', '') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatment', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert await client.get_treatment('some_key', 'some_feature') == 'default_treatment'
        assert _logger.error.mock_calls == []
        assert _logger.warning.mock_calls == []

        _logger.reset_mock()
        assert await client.get_treatment(Key(None, 'bucketing_key'), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatment', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment(Key('', 'bucketing_key'), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatment', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment(Key(float('nan'), 'bucketing_key'), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment(Key(float('inf'), 'bucketing_key'), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment(Key(True, 'bucketing_key'), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment(Key([], 'bucketing_key'), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment(Key(12345, 'bucketing_key'), 'some_feature') == 'default_treatment'
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatment', 'matching_key', 12345)
        ]

        _logger.reset_mock()
        key = ''.join('a' for _ in range(0, 255))
        assert await client.get_treatment(Key(key, 'bucketing_key'), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatment', 'matching_key', 250)
        ]

        _logger.reset_mock()
        assert await client.get_treatment(Key('matching_key', None), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatment', 'bucketing_key', 'bucketing_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment(Key('matching_key', True), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'bucketing_key', 'bucketing_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment(Key('matching_key', []), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment', 'bucketing_key', 'bucketing_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment(Key('matching_key', ''), 'some_feature') == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatment', 'bucketing_key', 'bucketing_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment(Key('matching_key', 12345), 'some_feature') == 'default_treatment'
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatment', 'bucketing_key', 12345)
        ]

        _logger.reset_mock()
        assert await client.get_treatment('matching_key', 'some_feature', True) == CONTROL
        assert _logger.error.mock_calls == [
            mocker.call('%s: attributes must be of type dictionary.', 'get_treatment')
        ]

        _logger.reset_mock()
        assert await client.get_treatment('matching_key', 'some_feature', {'test': 'test'}) == 'default_treatment'
        assert _logger.error.mock_calls == []

        _logger.reset_mock()
        assert await client.get_treatment('matching_key', 'some_feature', None) == 'default_treatment'
        assert _logger.error.mock_calls == []

        _logger.reset_mock()
        assert await client.get_treatment('matching_key', '  some_feature   ', None) == 'default_treatment'
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s \'%s\' has extra whitespace, trimming.', 'get_treatment', 'feature flag name', '  some_feature   ')
        ]

        _logger.reset_mock()
        async def fetch_many(*_):
            return {'some_feature': None}
        storage_mock.fetch_many = fetch_many

        mocker.patch('splitio.client.client._LOGGER', new=_logger)
        assert await client.get_treatment('matching_key', 'some_feature', None) == CONTROL
        assert _logger.warning.mock_calls == [
            mocker.call(
                "%s: you passed \"%s\" that does not exist in this environment, "
                "please double check what Feature flags exist in the Split user interface.",
                'get_treatment',
                'some_feature'
            )
        ]
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatment_with_config(self, mocker):
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
        async def fetch_many(*_):
            return {
            'some_feature': split_mock
            }
        storage_mock.fetch_many = fetch_many
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorageAsync)
        async def fetch_many_rbs(*_):
            return {}
        rbs_storage.fetch_many = fetch_many_rbs

        async def get_change_number(*_):
            return 1
        storage_mock.get_change_number = get_change_number

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        recorder = StandardRecorderAsync(impmanager, mocker.Mock(spec=EventStorage), ImpressionStorage, telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactoryAsync(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': rbs_storage,
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            impmanager,
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock

        client = ClientAsync(factory, mocker.Mock())
        async def record_treatment_stats(*_):
            pass
        client._recorder.record_treatment_stats = record_treatment_stats

        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert await client.get_treatment_with_config(None, 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatment_with_config', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config('', 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatment_with_config', 'key', 'key')
        ]

        _logger.reset_mock()
        key = ''.join('a' for _ in range(0, 255))
        assert await client.get_treatment_with_config(key, 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatment_with_config', 'key', 250)
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config(12345, 'some_feature') == ('default_treatment', '{"some": "property"}')
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatment_with_config', 'key', 12345)
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config(float('nan'), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config(float('inf'), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config(True, 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config([], 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config('some_key', None) == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatment_with_config', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config('some_key', 123) == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config('some_key', True) == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config('some_key', []) == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config('some_key', '') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatment_with_config', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config('some_key', 'some_feature') == ('default_treatment', '{"some": "property"}')
        assert _logger.error.mock_calls == []
        assert _logger.warning.mock_calls == []

        _logger.reset_mock()
        assert await client.get_treatment_with_config(Key(None, 'bucketing_key'), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatment_with_config', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config(Key('', 'bucketing_key'), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatment_with_config', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config(Key(float('nan'), 'bucketing_key'), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config(Key(float('inf'), 'bucketing_key'), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config(Key(True, 'bucketing_key'), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config(Key([], 'bucketing_key'), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'matching_key', 'matching_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config(Key(12345, 'bucketing_key'), 'some_feature') == ('default_treatment', '{"some": "property"}')
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatment_with_config', 'matching_key', 12345)
        ]

        _logger.reset_mock()
        key = ''.join('a' for _ in range(0, 255))
        assert await client.get_treatment_with_config(Key(key, 'bucketing_key'), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatment_with_config', 'matching_key', 250)
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config(Key('matching_key', None), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatment_with_config', 'bucketing_key', 'bucketing_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config(Key('matching_key', True), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'bucketing_key', 'bucketing_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config(Key('matching_key', []), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatment_with_config', 'bucketing_key', 'bucketing_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config(Key('matching_key', ''), 'some_feature') == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatment_with_config', 'bucketing_key', 'bucketing_key')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config(Key('matching_key', 12345), 'some_feature') == ('default_treatment', '{"some": "property"}')
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatment_with_config', 'bucketing_key', 12345)
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config('matching_key', 'some_feature', True) == (CONTROL, None)
        assert _logger.error.mock_calls == [
            mocker.call('%s: attributes must be of type dictionary.', 'get_treatment_with_config')
        ]

        _logger.reset_mock()
        assert await client.get_treatment_with_config('matching_key', 'some_feature', {'test': 'test'}) == ('default_treatment', '{"some": "property"}')
        assert _logger.error.mock_calls == []

        _logger.reset_mock()
        assert await client.get_treatment_with_config('matching_key', 'some_feature', None) == ('default_treatment', '{"some": "property"}')
        assert _logger.error.mock_calls == []

        _logger.reset_mock()
        assert await client.get_treatment_with_config('matching_key', '  some_feature   ', None) == ('default_treatment', '{"some": "property"}')
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s \'%s\' has extra whitespace, trimming.', 'get_treatment_with_config', 'feature flag name', '  some_feature   ')
        ]

        _logger.reset_mock()
        async def fetch_many(*_):
            return {'some_feature': None}
        storage_mock.fetch_many = fetch_many

        mocker.patch('splitio.client.client._LOGGER', new=_logger)
        assert await client.get_treatment_with_config('matching_key', 'some_feature', None) == (CONTROL, None)
        assert _logger.warning.mock_calls == [
            mocker.call(
                "%s: you passed \"%s\" that does not exist in this environment, "
                "please double check what Feature flags exist in the Split user interface.",
                'get_treatment_with_config',
                'some_feature'
            )
        ]
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_track(self, mocker):
        """Test track method()."""
        events_storage_mock = mocker.Mock(spec=EventStorage)
        async def put(*_):
            return True
        events_storage_mock.put = put

        event_storage = mocker.Mock(spec=EventStorage)
        event_storage.put = put
        split_storage_mock = mocker.Mock(spec=SplitStorage)
        split_storage_mock.is_valid_traffic_type = put

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        recorder = StandardRecorderAsync(impmanager, events_storage_mock, ImpressionStorage, telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactoryAsync(mocker.Mock(),
            {
                'splits': split_storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': mocker.Mock(spec=RuleBasedSegmentsStorage),
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': events_storage_mock,
            },
            mocker.Mock(),
            recorder,
            impmanager,
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        factory._sdk_key = 'some-test'

        client = ClientAsync(factory, recorder)
        client._event_storage = event_storage
        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert await client.track(None, "traffic_type", "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.", 'track', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.track("", "traffic_type", "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an empty %s, %s must be a non-empty string.", 'track', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.track(12345, "traffic_type", "event_type", 1) is True
        assert _logger.warning.mock_calls == [
            mocker.call("%s: %s %s is not of type string, converting.", 'track', 'key', 12345)
        ]

        _logger.reset_mock()
        assert await client.track(True, "traffic_type", "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'track', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.track([], "traffic_type", "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'track', 'key', 'key')
        ]

        _logger.reset_mock()
        key = ''.join('a' for _ in range(0, 255))
        assert await client.track(key, "traffic_type", "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: %s too long - must be %s characters or less.", 'track', 'key', 250)
        ]

        _logger.reset_mock()
        assert await client.track("some_key", None, "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.", 'track', 'traffic_type', 'traffic_type')
        ]

        _logger.reset_mock()
        assert await client.track("some_key", "", "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an empty %s, %s must be a non-empty string.", 'track', 'traffic_type', 'traffic_type')
        ]

        _logger.reset_mock()
        assert await client.track("some_key", 12345, "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'track', 'traffic_type', 'traffic_type')
        ]

        _logger.reset_mock()
        assert await client.track("some_key", True, "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'track', 'traffic_type', 'traffic_type')
        ]

        _logger.reset_mock()
        assert await client.track("some_key", [], "event_type", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'track', 'traffic_type', 'traffic_type')
        ]

        _logger.reset_mock()
        assert await client.track("some_key", "TRAFFIC_type", "event_type", 1) is True
        assert _logger.warning.mock_calls == [
            mocker.call("%s: %s '%s' should be all lowercase - converting string to lowercase", 'track', 'traffic type', 'TRAFFIC_type')
        ]

        assert await client.track("some_key", "traffic_type", None, 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.", 'track', 'event_type', 'event_type')
        ]

        _logger.reset_mock()
        assert await client.track("some_key", "traffic_type", "", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an empty %s, %s must be a non-empty string.", 'track', 'event_type', 'event_type')
        ]

        _logger.reset_mock()
        assert await client.track("some_key", "traffic_type", True, 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'track', 'event_type', 'event_type')
        ]

        _logger.reset_mock()
        assert await client.track("some_key", "traffic_type", [], 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'track', 'event_type', 'event_type')
        ]

        _logger.reset_mock()
        assert await client.track("some_key", "traffic_type", 12345, 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'track', 'event_type', 'event_type')
        ]

        _logger.reset_mock()
        assert await client.track("some_key", "traffic_type", "@@", 1) is False
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed %s, %s must adhere to the regular "
                        "expression %s. This means "
                        "%s must be alphanumeric, cannot be more than %s "
                        "characters long, and can only include a dash, underscore, "
                        "period, or colon as separators of alphanumeric characters.",
                        'track', '@@', 'an event name', '^[a-zA-Z0-9][-_.:a-zA-Z0-9]{0,79}$', 'an event name', 80)
        ]

        _logger.reset_mock()
        assert await client.track("some_key", "traffic_type", "event_type", None) is True
        assert _logger.error.mock_calls == []

        _logger.reset_mock()
        assert await client.track("some_key", "traffic_type", "event_type", 1) is True
        assert _logger.error.mock_calls == []

        _logger.reset_mock()
        assert await client.track("some_key", "traffic_type", "event_type", 1.23) is True
        assert _logger.error.mock_calls == []

        _logger.reset_mock()
        assert await client.track("some_key", "traffic_type", "event_type", "test") is False
        assert _logger.error.mock_calls == [
            mocker.call("track: value must be a number.")
        ]

        _logger.reset_mock()
        assert await client.track("some_key", "traffic_type", "event_type", True) is False
        assert _logger.error.mock_calls == [
            mocker.call("track: value must be a number.")
        ]

        _logger.reset_mock()
        assert await client.track("some_key", "traffic_type", "event_type", []) is False
        assert _logger.error.mock_calls == [
            mocker.call("track: value must be a number.")
        ]

        # Test traffic type existance
        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(factory).ready = ready_property

        # Test that it doesn't warn if tt is cached, not in localhost mode and sdk is ready
        _logger.reset_mock()
        assert await client.track("some_key", "traffic_type", "event_type", None) is True
        assert _logger.error.mock_calls == []
        assert _logger.warning.mock_calls == []

        # Test that it does warn if tt is cached, not in localhost mode and sdk is ready
        async def is_valid_traffic_type(*_):
            return False
        split_storage_mock.is_valid_traffic_type = is_valid_traffic_type

        _logger.reset_mock()
        assert await client.track("some_key", "traffic_type", "event_type", None) is True
        assert _logger.error.mock_calls == []
        assert _logger.warning.mock_calls == [mocker.call(
            'track: Traffic Type %s does not have any corresponding Feature flags in this environment, '
            'make sure you\'re tracking your events to a valid traffic type defined '
            'in the Split user interface.',
            'traffic_type'
        )]

        # Test that it does not warn when in localhost mode.
        factory._sdk_key = 'localhost'
        _logger.reset_mock()
        assert await client.track("some_key", "traffic_type", "event_type", None) is True
        assert _logger.error.mock_calls == []
        assert _logger.warning.mock_calls == []

        # Test that it does not warn when not in localhost mode and not ready
        factory._sdk_key = 'not-localhost'
        ready_property.return_value = False
        type(factory).ready = ready_property
        _logger.reset_mock()
        assert await client.track("some_key", "traffic_type", "event_type", None) is True
        assert _logger.error.mock_calls == []
        assert _logger.warning.mock_calls == []

        # Test track with invalid properties
        _logger.reset_mock()
        assert await client.track("some_key", "traffic_type", "event_type", 1, []) is False
        assert _logger.error.mock_calls == [
            mocker.call("track: properties must be of type dictionary.")
        ]

        # Test track with invalid properties
        _logger.reset_mock()
        assert await client.track("some_key", "traffic_type", "event_type", 1, True) is False
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
        assert await client.track("some_key", "traffic_type", "event_type", 1, props1) is True
        assert _logger.warning.mock_calls == [
            mocker.call("Property %s is of invalid type. Setting value to None", [])
        ]

        # Test track with more than 300 properties
        props2 = dict()
        for i in range(301):
            props2[str(i)] = i
        _logger.reset_mock()
        assert await client.track("some_key", "traffic_type", "event_type", 1, props2) is True
        assert _logger.warning.mock_calls == [
            mocker.call("Event has more than 300 properties. Some of them will be trimmed when processed")
        ]

        # Test track with properties higher than 32kb
        _logger.reset_mock()
        props3 = dict()
        for i in range(100, 210):
            props3["prop" + str(i)] = "a" * 300
        assert await client.track("some_key", "traffic_type", "event_type", 1, props3) is False
        assert _logger.error.mock_calls == [
            mocker.call("The maximum size allowed for the properties is 32768 bytes. Current one is 32952 bytes. Event not queued")
        ]
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments(self, mocker):
        """Test getTreatments() method."""
        split_mock = mocker.Mock(spec=Split)
        default_treatment_mock = mocker.PropertyMock()
        default_treatment_mock.return_value = 'default_treatment'
        type(split_mock).default_treatment = default_treatment_mock
        conditions_mock = mocker.PropertyMock()
        conditions_mock.return_value = []
        type(split_mock).conditions = conditions_mock
        storage_mock = mocker.Mock(spec=SplitStorage)
        async def get(*_):
            return split_mock
        storage_mock.get = get
        async def get_change_number(*_):
            return 1
        storage_mock.get_change_number = get_change_number
        async def fetch_many(*_):
            return {
            'some_feature': split_mock,
            'some': split_mock,
        }
        storage_mock.fetch_many = fetch_many
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorageAsync)
        async def fetch_many_rbs(*_):
            return {}
        rbs_storage.fetch_many = fetch_many_rbs

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        recorder = StandardRecorderAsync(impmanager, mocker.Mock(spec=EventStorage), mocker.Mock(spec=ImpressionStorage), telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactoryAsync(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': rbs_storage,
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            impmanager,
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock

        client = ClientAsync(factory, recorder)
        async def record_treatment_stats(*_):
            pass
        client._recorder.record_treatment_stats = record_treatment_stats

        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert await client.get_treatments(None, ['some_feature']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatments', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatments("", ['some_feature']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatments', 'key', 'key')
        ]

        key = ''.join('a' for _ in range(0, 255))
        _logger.reset_mock()
        assert await client.get_treatments(key, ['some_feature']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatments', 'key', 250)
        ]

        split_mock.name = 'some_feature'
        _logger.reset_mock()
        assert await client.get_treatments(12345, ['some_feature']) == {'some_feature': 'default_treatment'}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatments', 'key', 12345)
        ]

        _logger.reset_mock()
        assert await client.get_treatments(True, ['some_feature']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatments([], ['some_feature']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatments('some_key', None) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments')
        ]

        _logger.reset_mock()
        assert await client.get_treatments('some_key', True) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments')
        ]

        _logger.reset_mock()
        assert await client.get_treatments('some_key', 'some_string') == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments')
        ]

        _logger.reset_mock()
        assert await client.get_treatments('some_key', []) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments')
        ]

        _logger.reset_mock()
        assert await client.get_treatments('some_key', [None, None]) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments')
        ]

        _logger.reset_mock()
        assert await client.get_treatments('some_key', [True]) == {}
        assert mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments') in _logger.error.mock_calls

        _logger.reset_mock()
        assert await client.get_treatments('some_key', ['', '']) == {}
        assert mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments') in _logger.error.mock_calls

        _logger.reset_mock()
        assert await client.get_treatments('some_key', ['some_feature   ']) == {'some_feature': 'default_treatment'}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s \'%s\' has extra whitespace, trimming.', 'get_treatments', 'feature flag name', 'some_feature   ')
        ]

        _logger.reset_mock()
        async def fetch_many(*_):
            return {
            'some_feature': None
        }
        storage_mock.fetch_many = fetch_many

        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock
        mocker.patch('splitio.client.client._LOGGER', new=_logger)
        assert await client.get_treatments('matching_key', ['some_feature'], None) == {'some_feature': CONTROL}
        assert _logger.warning.mock_calls == [
            mocker.call(
                "%s: you passed \"%s\" that does not exist in this environment, "
                "please double check what Feature flags exist in the Split user interface.",
                'get_treatments',
                'some_feature'
            )
        ]
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_with_config(self, mocker):
        """Test getTreatments() method."""
        split_mock = mocker.Mock(spec=Split)
        default_treatment_mock = mocker.PropertyMock()
        default_treatment_mock.return_value = 'default_treatment'
        type(split_mock).default_treatment = default_treatment_mock
        conditions_mock = mocker.PropertyMock()
        conditions_mock.return_value = []
        type(split_mock).conditions = conditions_mock

        storage_mock = mocker.Mock(spec=SplitStorage)
        async def get(*_):
            return split_mock
        storage_mock.get = get
        async def get_change_number(*_):
            return 1
        storage_mock.get_change_number = get_change_number
        async def fetch_many(*_):
            return {
            'some_feature': split_mock
        }
        storage_mock.fetch_many = fetch_many
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorageAsync)
        async def fetch_many_rbs(*_):
            return {}
        rbs_storage.fetch_many = fetch_many_rbs

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        recorder = StandardRecorderAsync(impmanager, mocker.Mock(spec=EventStorage), mocker.Mock(spec=ImpressionStorage), telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactoryAsync(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': rbs_storage,
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            impmanager,
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        split_mock.name = 'some_feature'

        def _configs(treatment):
            return '{"some": "property"}' if treatment == 'default_treatment' else None
        split_mock.get_configurations_for.side_effect = _configs

        client = ClientAsync(factory, mocker.Mock())
        async def record_treatment_stats(*_):
            pass
        client._recorder.record_treatment_stats = record_treatment_stats

        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert await client.get_treatments_with_config(None, ['some_feature']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatments_with_config', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatments_with_config("", ['some_feature']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatments_with_config', 'key', 'key')
        ]

        key = ''.join('a' for _ in range(0, 255))
        _logger.reset_mock()
        assert await client.get_treatments_with_config(key, ['some_feature']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatments_with_config', 'key', 250)
        ]

        _logger.reset_mock()
        assert await client.get_treatments_with_config(12345, ['some_feature']) == {'some_feature': ('default_treatment', '{"some": "property"}')}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatments_with_config', 'key', 12345)
        ]

        _logger.reset_mock()
        assert await client.get_treatments_with_config(True, ['some_feature']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_with_config', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatments_with_config([], ['some_feature']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_with_config', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatments_with_config('some_key', None) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments_with_config')
        ]

        _logger.reset_mock()
        assert await client.get_treatments_with_config('some_key', True) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments_with_config')
        ]

        _logger.reset_mock()
        assert await client.get_treatments_with_config('some_key', 'some_string') == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments_with_config')
        ]

        _logger.reset_mock()
        assert await client.get_treatments_with_config('some_key', []) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments_with_config')
        ]

        _logger.reset_mock()
        assert await client.get_treatments_with_config('some_key', [None, None]) == {}
        assert _logger.error.mock_calls == [
            mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments_with_config')
        ]

        _logger.reset_mock()
        assert await client.get_treatments_with_config('some_key', [True]) == {}
        assert mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments_with_config') in _logger.error.mock_calls

        _logger.reset_mock()
        assert await client.get_treatments_with_config('some_key', ['', '']) == {}
        assert mocker.call('%s: feature flag names must be a non-empty array.', 'get_treatments_with_config') in _logger.error.mock_calls

        _logger.reset_mock()
        assert await client.get_treatments_with_config('some_key', ['some_feature   ']) == {'some_feature': ('default_treatment', '{"some": "property"}')}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s \'%s\' has extra whitespace, trimming.', 'get_treatments_with_config', 'feature flag name', 'some_feature   ')
        ]

        _logger.reset_mock()
        async def fetch_many(*_):
            return {
            'some_feature': None
        }
        storage_mock.fetch_many = fetch_many

        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock
        mocker.patch('splitio.client.client._LOGGER', new=_logger)
        assert await client.get_treatments('matching_key', ['some_feature'], None) == {'some_feature': CONTROL}
        assert _logger.warning.mock_calls == [
            mocker.call(
                "%s: you passed \"%s\" that does not exist in this environment, "
                "please double check what Feature flags exist in the Split user interface.",
                'get_treatments',
                'some_feature'
            )
        ]
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_by_flag_set(self, mocker):
        split_mock = mocker.Mock(spec=Split)
        default_treatment_mock = mocker.PropertyMock()
        default_treatment_mock.return_value = 'default_treatment'
        type(split_mock).default_treatment = default_treatment_mock
        conditions_mock = mocker.PropertyMock()
        conditions_mock.return_value = []
        type(split_mock).conditions = conditions_mock
        storage_mock = mocker.Mock(spec=SplitStorage)
        async def get(*_):
            return split_mock
        storage_mock.get = get
        async def get_change_number(*_):
            return 1
        storage_mock.get_change_number = get_change_number
        async def fetch_many(*_):
            return {
            'some_feature': split_mock,
            'some': split_mock,
        }
        storage_mock.fetch_many = fetch_many
        async def get_feature_flags_by_sets(*_):
            return ['some_feature']
        storage_mock.get_feature_flags_by_sets = get_feature_flags_by_sets
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorageAsync)
        async def fetch_many_rbs(*_):
            return {}
        rbs_storage.fetch_many = fetch_many_rbs

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        recorder = StandardRecorderAsync(impmanager, mocker.Mock(spec=EventStorage), mocker.Mock(spec=ImpressionStorage), telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactoryAsync(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': rbs_storage,
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock

        client = ClientAsync(factory, recorder)
        async def record_treatment_stats(*_):
            pass
        client._recorder.record_treatment_stats = record_treatment_stats

        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert await client.get_treatments_by_flag_set(None, 'some_flag') == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatments_by_flag_set', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatments_by_flag_set("", 'some_flag') == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatments_by_flag_set', 'key', 'key')
        ]

        key = ''.join('a' for _ in range(0, 255))
        _logger.reset_mock()
        assert await client.get_treatments_by_flag_set(key, 'some_flag') == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatments_by_flag_set', 'key', 250)
        ]

        split_mock.name = 'some_feature'
        _logger.reset_mock()
        assert await client.get_treatments_by_flag_set(12345, 'some_flag') == {'some_feature': 'default_treatment'}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatments_by_flag_set', 'key', 12345)
        ]

        _logger.reset_mock()
        assert await client.get_treatments_by_flag_set(True, 'some_flag') == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_by_flag_set', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatments_by_flag_set([], 'some_flag') == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_by_flag_set', 'key', 'key')
        ]

        _logger.reset_mock()
        await client.get_treatments_by_flag_set('some_key', None)
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.",
                        'get_treatments_by_flag_set', 'flag set', 'flag set')
        ]

        _logger.reset_mock()
        await client.get_treatments_by_flag_set('some_key', "$$")
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed %s, %s must adhere to the regular "
                        "expression %s. This means "
                        "%s must be alphanumeric, cannot be more than %s "
                        "characters long, and can only include a dash, underscore, "
                        "period, or colon as separators of alphanumeric characters.",
                        'get_treatments_by_flag_set', '$$', 'a flag set', '^[a-z0-9][_a-z0-9]{0,49}$', 'a flag set', 50)
        ]

        _logger.reset_mock()
        assert await client.get_treatments_by_flag_set('some_key', 'some_flag   ') == {'some_feature': 'default_treatment'}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s \'%s\' has extra whitespace, trimming.', 'get_treatments_by_flag_set', 'flag set', 'some_flag   ')
        ]

        _logger.reset_mock()
        async def fetch_many(*_):
            return {
            'some_feature': None
        }
        storage_mock.fetch_many = fetch_many

        async def get_feature_flags_by_sets(*_):
            return []
        storage_mock.get_feature_flags_by_sets = get_feature_flags_by_sets

        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock
        mocker.patch('splitio.client.client._LOGGER', new=_logger)
        assert await client.get_treatments_by_flag_set('matching_key', 'some_flag', None) == {}
        assert _logger.warning.mock_calls == [
            mocker.call("%s: No valid Flag set or no feature flags found for evaluating treatments", "get_treatments_by_flag_set")
        ]
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_by_flag_sets(self, mocker):
        split_mock = mocker.Mock(spec=Split)
        default_treatment_mock = mocker.PropertyMock()
        default_treatment_mock.return_value = 'default_treatment'
        type(split_mock).default_treatment = default_treatment_mock
        conditions_mock = mocker.PropertyMock()
        conditions_mock.return_value = []
        type(split_mock).conditions = conditions_mock
        storage_mock = mocker.Mock(spec=SplitStorage)
        async def get(*_):
            return split_mock
        storage_mock.get = get
        async def get_change_number(*_):
            return 1
        storage_mock.get_change_number = get_change_number
        async def fetch_many(*_):
            return {
            'some_feature': split_mock,
            'some': split_mock,
        }
        storage_mock.fetch_many = fetch_many
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorageAsync)
        async def fetch_many_rbs(*_):
            return {}
        rbs_storage.fetch_many = fetch_many_rbs
        
        async def get_feature_flags_by_sets(*_):
            return ['some_feature']
        storage_mock.get_feature_flags_by_sets = get_feature_flags_by_sets

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        recorder = StandardRecorderAsync(impmanager, mocker.Mock(spec=EventStorage), mocker.Mock(spec=ImpressionStorage), telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactoryAsync(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': rbs_storage,
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock

        client = ClientAsync(factory, recorder)
        async def record_treatment_stats(*_):
            pass
        client._recorder.record_treatment_stats = record_treatment_stats

        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert await client.get_treatments_by_flag_sets(None, ['some_flag']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatments_by_flag_sets', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatments_by_flag_sets("", ['some_flag']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatments_by_flag_sets', 'key', 'key')
        ]

        key = ''.join('a' for _ in range(0, 255))
        _logger.reset_mock()
        assert await client.get_treatments_by_flag_sets(key, ['some_flag']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatments_by_flag_sets', 'key', 250)
        ]

        split_mock.name = 'some_feature'
        _logger.reset_mock()
        assert await client.get_treatments_by_flag_sets(12345, ['some_flag']) == {'some_feature': 'default_treatment'}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatments_by_flag_sets', 'key', 12345)
        ]

        _logger.reset_mock()
        assert await client.get_treatments_by_flag_sets(True, ['some_flag']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_by_flag_sets', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatments_by_flag_sets([], ['some_flag']) == {'some_feature': CONTROL}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_by_flag_sets', 'key', 'key')
        ]

        _logger.reset_mock()
        await client.get_treatments_by_flag_sets('some_key', None)
        assert _logger.warning.mock_calls == [
            mocker.call("%s: flag sets parameter type should be list object, parameter is discarded", "get_treatments_by_flag_sets")
        ]

        _logger.reset_mock()
        await client.get_treatments_by_flag_sets('some_key', [None])
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.",
                        'get_treatments_by_flag_sets', 'flag set', 'flag set')
        ]

        _logger.reset_mock()
        await client.get_treatments_by_flag_sets('some_key', ["$$"])
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed %s, %s must adhere to the regular "
                        "expression %s. This means "
                        "%s must be alphanumeric, cannot be more than %s "
                        "characters long, and can only include a dash, underscore, "
                        "period, or colon as separators of alphanumeric characters.",
                        'get_treatments_by_flag_sets', '$$', 'a flag set', '^[a-z0-9][_a-z0-9]{0,49}$', 'a flag set', 50)
        ]

        _logger.reset_mock()
        assert await client.get_treatments_by_flag_sets('some_key', ['some_flag   ']) == {'some_feature': 'default_treatment'}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s \'%s\' has extra whitespace, trimming.', 'get_treatments_by_flag_sets', 'flag set', 'some_flag   ')
        ]

        _logger.reset_mock()
        async def fetch_many(*_):
            return {
            'some_feature': None
        }
        storage_mock.fetch_many = fetch_many

        async def get_feature_flags_by_sets(*_):
            return []
        storage_mock.get_feature_flags_by_sets = get_feature_flags_by_sets

        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock
        mocker.patch('splitio.client.client._LOGGER', new=_logger)
        assert await client.get_treatments_by_flag_sets('matching_key', ['some_flag'], None) == {}
        assert _logger.warning.mock_calls == [
            mocker.call("%s: No valid Flag set or no feature flags found for evaluating treatments", "get_treatments_by_flag_sets")
        ]
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_with_config_by_flag_set(self, mocker):
        split_mock = mocker.Mock(spec=Split)
        def _configs(treatment):
            return '{"some": "property"}' if treatment == 'default_treatment' else None
        split_mock.get_configurations_for.side_effect = _configs
        split_mock.name = 'some_feature'
        default_treatment_mock = mocker.PropertyMock()
        default_treatment_mock.return_value = 'default_treatment'
        type(split_mock).default_treatment = default_treatment_mock
        conditions_mock = mocker.PropertyMock()
        conditions_mock.return_value = []
        type(split_mock).conditions = conditions_mock
        storage_mock = mocker.Mock(spec=SplitStorage)
        async def get(*_):
            return split_mock
        storage_mock.get = get
        async def get_change_number(*_):
            return 1
        storage_mock.get_change_number = get_change_number
        async def fetch_many(*_):
            return {
            'some_feature': split_mock,
            'some': split_mock,
        }
        storage_mock.fetch_many = fetch_many
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorageAsync)
        async def fetch_many_rbs(*_):
            return {}
        rbs_storage.fetch_many = fetch_many_rbs
        async def get_feature_flags_by_sets(*_):
            return ['some_feature']
        storage_mock.get_feature_flags_by_sets = get_feature_flags_by_sets

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        recorder = StandardRecorderAsync(impmanager, mocker.Mock(spec=EventStorage), mocker.Mock(spec=ImpressionStorage), telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactoryAsync(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': rbs_storage,
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock

        client = ClientAsync(factory, recorder)
        async def record_treatment_stats(*_):
            pass
        client._recorder.record_treatment_stats = record_treatment_stats

        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert await client.get_treatments_with_config_by_flag_set(None, 'some_flag') == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatments_with_config_by_flag_set', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatments_with_config_by_flag_set("", 'some_flag') == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatments_with_config_by_flag_set', 'key', 'key')
        ]

        key = ''.join('a' for _ in range(0, 255))
        _logger.reset_mock()
        assert await client.get_treatments_with_config_by_flag_set(key, 'some_flag') == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatments_with_config_by_flag_set', 'key', 250)
        ]

        split_mock.name = 'some_feature'
        _logger.reset_mock()
        assert await client.get_treatments_with_config_by_flag_set(12345, 'some_flag') == {'some_feature': ('default_treatment', '{"some": "property"}')}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatments_with_config_by_flag_set', 'key', 12345)
        ]

        _logger.reset_mock()
        assert await client.get_treatments_with_config_by_flag_set(True, 'some_flag') == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_with_config_by_flag_set', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatments_with_config_by_flag_set([], 'some_flag') == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_with_config_by_flag_set', 'key', 'key')
        ]

        _logger.reset_mock()
        await client.get_treatments_with_config_by_flag_set('some_key', None)
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.",
                        'get_treatments_with_config_by_flag_set', 'flag set', 'flag set')
        ]

        _logger.reset_mock()
        await client.get_treatments_with_config_by_flag_set('some_key', "$$")
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed %s, %s must adhere to the regular "
                        "expression %s. This means "
                        "%s must be alphanumeric, cannot be more than %s "
                        "characters long, and can only include a dash, underscore, "
                        "period, or colon as separators of alphanumeric characters.",
                        'get_treatments_with_config_by_flag_set', '$$', 'a flag set', '^[a-z0-9][_a-z0-9]{0,49}$', 'a flag set', 50)
        ]

        _logger.reset_mock()
        assert await client.get_treatments_with_config_by_flag_set('some_key', 'some_flag   ') == {'some_feature': ('default_treatment', '{"some": "property"}')}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s \'%s\' has extra whitespace, trimming.', 'get_treatments_with_config_by_flag_set', 'flag set', 'some_flag   ')
        ]

        _logger.reset_mock()
        async def fetch_many(*_):
            return {
            'some_feature': None
        }
        storage_mock.fetch_many = fetch_many

        async def get_feature_flags_by_sets(*_):
            return []
        storage_mock.get_feature_flags_by_sets = get_feature_flags_by_sets

        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock
        mocker.patch('splitio.client.client._LOGGER', new=_logger)
        assert await client.get_treatments_with_config_by_flag_set('matching_key', 'some_flag', None) == {}
        assert _logger.warning.mock_calls == [
            mocker.call("%s: No valid Flag set or no feature flags found for evaluating treatments", "get_treatments_with_config_by_flag_set")
        ]
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_with_config_by_flag_sets(self, mocker):
        split_mock = mocker.Mock(spec=Split)
        def _configs(treatment):
            return '{"some": "property"}' if treatment == 'default_treatment' else None
        split_mock.get_configurations_for.side_effect = _configs
        default_treatment_mock = mocker.PropertyMock()
        default_treatment_mock.return_value = 'default_treatment'
        type(split_mock).default_treatment = default_treatment_mock
        conditions_mock = mocker.PropertyMock()
        conditions_mock.return_value = []
        type(split_mock).conditions = conditions_mock
        storage_mock = mocker.Mock(spec=SplitStorage)
        async def get(*_):
            return split_mock
        storage_mock.get = get
        async def get_change_number(*_):
            return 1
        storage_mock.get_change_number = get_change_number
        async def fetch_many(*_):
            return {
            'some_feature': split_mock,
            'some': split_mock,
        }
        storage_mock.fetch_many = fetch_many
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorageAsync)
        async def fetch_many_rbs(*_):
            return {}
        rbs_storage.fetch_many = fetch_many_rbs
        
        async def get_feature_flags_by_sets(*_):
            return ['some_feature']
        storage_mock.get_feature_flags_by_sets = get_feature_flags_by_sets

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        recorder = StandardRecorderAsync(impmanager, mocker.Mock(spec=EventStorage), mocker.Mock(spec=ImpressionStorage), telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactoryAsync(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': rbs_storage,
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )
        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock

        client = ClientAsync(factory, recorder)
        async def record_treatment_stats(*_):
            pass
        client._recorder.record_treatment_stats = record_treatment_stats

        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert await client.get_treatments_with_config_by_flag_sets(None, ['some_flag']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed a null %s, %s must be a non-empty string.', 'get_treatments_with_config_by_flag_sets', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatments_with_config_by_flag_sets("", ['some_flag']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an empty %s, %s must be a non-empty string.', 'get_treatments_with_config_by_flag_sets', 'key', 'key')
        ]

        key = ''.join('a' for _ in range(0, 255))
        _logger.reset_mock()
        assert await client.get_treatments_with_config_by_flag_sets(key, ['some_flag']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: %s too long - must be %s characters or less.', 'get_treatments_with_config_by_flag_sets', 'key', 250)
        ]

        split_mock.name = 'some_feature'
        _logger.reset_mock()
        assert await client.get_treatments_with_config_by_flag_sets(12345, ['some_flag']) == {'some_feature': ('default_treatment', '{"some": "property"}')}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s %s is not of type string, converting.', 'get_treatments_with_config_by_flag_sets', 'key', 12345)
        ]

        _logger.reset_mock()
        assert await client.get_treatments_with_config_by_flag_sets(True, ['some_flag']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_with_config_by_flag_sets', 'key', 'key')
        ]

        _logger.reset_mock()
        assert await client.get_treatments_with_config_by_flag_sets([], ['some_flag']) == {'some_feature': (CONTROL, None)}
        assert _logger.error.mock_calls == [
            mocker.call('%s: you passed an invalid %s, %s must be a non-empty string.', 'get_treatments_with_config_by_flag_sets', 'key', 'key')
        ]

        _logger.reset_mock()
        await client.get_treatments_with_config_by_flag_sets('some_key', None)
        assert _logger.warning.mock_calls == [
            mocker.call("%s: flag sets parameter type should be list object, parameter is discarded", "get_treatments_with_config_by_flag_sets")
        ]

        _logger.reset_mock()
        await client.get_treatments_with_config_by_flag_sets('some_key', [None])
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.",
                        'get_treatments_with_config_by_flag_sets', 'flag set', 'flag set')
        ]

        _logger.reset_mock()
        await client.get_treatments_with_config_by_flag_sets('some_key', ["$$"])
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed %s, %s must adhere to the regular "
                        "expression %s. This means "
                        "%s must be alphanumeric, cannot be more than %s "
                        "characters long, and can only include a dash, underscore, "
                        "period, or colon as separators of alphanumeric characters.",
                        'get_treatments_with_config_by_flag_sets', '$$', 'a flag set', '^[a-z0-9][_a-z0-9]{0,49}$', 'a flag set', 50)
        ]

        _logger.reset_mock()
        assert await client.get_treatments_with_config_by_flag_sets('some_key', ['some_flag   ']) == {'some_feature': ('default_treatment', '{"some": "property"}')}
        assert _logger.warning.mock_calls == [
            mocker.call('%s: %s \'%s\' has extra whitespace, trimming.', 'get_treatments_with_config_by_flag_sets', 'flag set', 'some_flag   ')
        ]

        _logger.reset_mock()
        async def fetch_many(*_):
            return {
            'some_feature': None
        }
        storage_mock.fetch_many = fetch_many

        async def get_feature_flags_by_sets(*_):
            return []
        storage_mock.get_feature_flags_by_sets = get_feature_flags_by_sets

        ready_mock = mocker.PropertyMock()
        ready_mock.return_value = True
        type(factory).ready = ready_mock
        mocker.patch('splitio.client.client._LOGGER', new=_logger)
        assert await client.get_treatments_with_config_by_flag_sets('matching_key', ['some_flag'], None) == {}
        assert _logger.warning.mock_calls == [
            mocker.call("%s: No valid Flag set or no feature flags found for evaluating treatments", "get_treatments_with_config_by_flag_sets")
        ]
        await factory.destroy()


    def test_flag_sets_validation(self):
        """Test sanitization for flag sets."""
        flag_sets = input_validator.validate_flag_sets([' set1', 'set2 ', 'set3'], 'method')
        assert sorted(flag_sets) == ['set1', 'set2', 'set3']

        flag_sets = input_validator.validate_flag_sets(['1set', '_set2'], 'method')
        assert flag_sets == ['1set']

        flag_sets = input_validator.validate_flag_sets(['Set1', 'SET2'], 'method')
        assert sorted(flag_sets) == ['set1', 'set2']

        flag_sets = input_validator.validate_flag_sets(['se\t1', 's/et2', 's*et3', 's!et4', 'se@t5', 'se#t5', 'se$t5', 'se^t5', 'se%t5', 'se&t5'], 'method')
        assert flag_sets == []

        flag_sets = input_validator.validate_flag_sets(['set4', 'set1', 'set3', 'set1'], 'method')
        assert sorted(flag_sets) == ['set1', 'set3', 'set4']

        flag_sets = input_validator.validate_flag_sets(['w' * 50, 's' * 51], 'method')
        assert flag_sets == ['w' * 50]

        flag_sets = input_validator.validate_flag_sets('set1', 'method')
        assert flag_sets == []

        flag_sets = input_validator.validate_flag_sets([12, 33], 'method')
        assert flag_sets == []


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
        recorder = StandardRecorder(impmanager, mocker.Mock(spec=EventStorage), mocker.Mock(spec=ImpressionStorage), telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactory(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': mocker.Mock(spec=RuleBasedSegmentsStorage),
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )

        manager = SplitManager(factory)
        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert manager.split(None) is None
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.", 'split', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert manager.split("") is None
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an empty %s, %s must be a non-empty string.", 'split', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert manager.split(True) is None
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'split', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert manager.split([]) is None
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'split', 'feature_flag_name', 'feature_flag_name')
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
            "please double check what Feature flags exist in the Split user interface.",
            'nonexistant-split'
        )]

class ManagerInputValidationAsyncTests(object):  #pylint: disable=too-few-public-methods
    """Manager input validation test cases."""

    @pytest.mark.asyncio
    async def test_split_(self, mocker):
        """Test split input validation."""
        storage_mock = mocker.Mock(spec=SplitStorage)
        split_mock = mocker.Mock(spec=Split)
        async def get(*_):
            return split_mock
        storage_mock.get = get

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        recorder = StandardRecorderAsync(impmanager, mocker.Mock(spec=EventStorage), mocker.Mock(spec=ImpressionStorage), telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
        factory = SplitFactoryAsync(mocker.Mock(),
            {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'rule_based_segments': mocker.Mock(spec=RuleBasedSegmentsStorage),
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
            },
            mocker.Mock(),
            recorder,
            mocker.Mock(),
            telemetry_producer,
            telemetry_producer.get_telemetry_init_producer(),
            mocker.Mock()
        )

        manager = SplitManagerAsync(factory)
        _logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=_logger)

        assert await manager.split(None) is None
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.", 'split', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert await manager.split("") is None
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an empty %s, %s must be a non-empty string.", 'split', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert await manager.split(True) is None
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'split', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        assert await manager.split([]) is None
        assert _logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'split', 'feature_flag_name', 'feature_flag_name')
        ]

        _logger.reset_mock()
        await manager.split('some_split')
        assert split_mock.to_split_view.mock_calls == [mocker.call()]
        assert _logger.error.mock_calls == []

        _logger.reset_mock()
        split_mock.reset_mock()
        async def get(*_):
            return None
        storage_mock.get = get

        await manager.split('nonexistant-split')
        assert split_mock.to_split_view.mock_calls == []
        assert _logger.warning.mock_calls == [mocker.call(
            "split: you passed \"%s\" that does not exist in this environment, "
            "please double check what Feature flags exist in the Split user interface.",
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
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.", 'factory_instantiation', 'sdk_key', 'sdk_key')
        ]

        logger.reset_mock()
        assert get_factory('') is None
        assert logger.error.mock_calls == [
            mocker.call("%s: you passed an empty %s, %s must be a non-empty string.", 'factory_instantiation', 'sdk_key', 'sdk_key')
        ]

        logger.reset_mock()
        assert get_factory(True) is None
        assert logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'factory_instantiation', 'sdk_key', 'sdk_key')
        ]

        logger.reset_mock()
        try:
            f = get_factory(True, config={'redisHost': 'localhost'})
        except:
            pass
        assert logger.error.mock_calls == []
        f.destroy()


class FactoryInputValidationAsyncTests(object):  #pylint: disable=too-few-public-methods
    """Factory instantiation input validation test cases."""

    @pytest.mark.asyncio
    async def test_input_validation_factory(self, mocker):
        """Test the input validators for factory instantiation."""
        logger = mocker.Mock(spec=logging.Logger)
        mocker.patch('splitio.client.input_validator._LOGGER', new=logger)

        assert await get_factory_async(None) is None
        assert logger.error.mock_calls == [
            mocker.call("%s: you passed a null %s, %s must be a non-empty string.", 'factory_instantiation', 'sdk_key', 'sdk_key')
        ]

        logger.reset_mock()
        assert await get_factory_async('') is None
        assert logger.error.mock_calls == [
            mocker.call("%s: you passed an empty %s, %s must be a non-empty string.", 'factory_instantiation', 'sdk_key', 'sdk_key')
        ]

        logger.reset_mock()
        assert await get_factory_async(True) is None
        assert logger.error.mock_calls == [
            mocker.call("%s: you passed an invalid %s, %s must be a non-empty string.", 'factory_instantiation', 'sdk_key', 'sdk_key')
        ]

        logger.reset_mock()
        try:
            f = await get_factory_async(True, config={'redisHost': 'localhost'})
        except:
            pass
        assert logger.error.mock_calls == []
        await f.destroy()

class PluggableInputValidationTests(object):  #pylint: disable=too-few-public-methods
    """Pluggable adapter instance validation test cases."""

    class mock_adapter0():
        def set(self, key, value):
            print(key)

    class mock_adapter1(object):
        def set(self, key, value):
            print(key)

    class mock_adapter2(mock_adapter1):
        def get(self, key):
            print(key)

        def get_items(self, key):
            print(key)

        def get_many(self, keys):
            print(keys)

        def push_items(self, key, *value):
            print(key)

        def delete(self, key):
            print(key)

        def increment(self, key, value):
            print(key)

        def decrement(self, key, value):
            print(key)

        def get_keys_by_prefix(self, prefix):
            print(prefix)

        def get_many(self, keys):
            print(keys)

        def add_items(self, key, added_items):
            print(key)

        def remove_items(self, key, removed_items):
            print(key)

        def item_contains(self, key, item):
            print(key)

        def get_items_count(self, key):
            print(key)

    class mock_adapter3(mock_adapter2):
        def expire(self, key):
            print(key)

    class mock_adapter4(mock_adapter2):
        def expire(self, key, value, till):
            print(key)

    def test_validate_pluggable_adapter(self):
        # missing storageWrapper config parameter
        assert(not input_validator.validate_pluggable_adapter({'storageType': 'pluggable'}))

        # ignore if storage type is not pluggable
        assert(input_validator.validate_pluggable_adapter({'storageType': 'memory'}))

        # mock adapter is not derived from object class
        assert(not input_validator.validate_pluggable_adapter({'storageType': 'pluggable', 'pe': self.mock_adapter0()}))

        # mock adapter missing many functions
        assert(not input_validator.validate_pluggable_adapter({'storageType': 'pluggable', 'storageWrapper': self.mock_adapter1()}))

        # mock adapter missing expire function
        assert(not input_validator.validate_pluggable_adapter({'storageType': 'pluggable', 'storageWrapper': self.mock_adapter2()}))

        # mock adapter expire function has incrrect args count
        assert(not input_validator.validate_pluggable_adapter({'storageType': 'pluggable', 'storageWrapper': self.mock_adapter3()}))

        # expected mock adapter should pass
        assert(input_validator.validate_pluggable_adapter({'storageType': 'pluggable', 'storageWrapper': self.mock_adapter4()}))

        # using string type prefix should pass
        assert(input_validator.validate_pluggable_adapter({'storageType': 'pluggable', 'storagePrefix': 'myprefix', 'storageWrapper': self.mock_adapter4()}))

        # using non-string type prefix should not pass
        assert(not input_validator.validate_pluggable_adapter({'storageType': 'pluggable', 'storagePrefix': 'myprefix', 123: self.mock_adapter4()}))

    def test_sanitize_flag_sets(self):
        """Test sanitization for flag sets."""
        flag_sets = input_validator.validate_flag_sets([' set1', 'set2 ', 'set3'], 'm')
        assert sorted(flag_sets) == ['set1', 'set2', 'set3']

        flag_sets = input_validator.validate_flag_sets(['1set', '_set2'], 'm')
        assert flag_sets == ['1set']

        flag_sets = input_validator.validate_flag_sets(['Set1', 'SET2'], 'm')
        assert sorted(flag_sets) == ['set1', 'set2']

        flag_sets = input_validator.validate_flag_sets(['se\t1', 's/et2', 's*et3', 's!et4', 'se@t5', 'se#t5', 'se$t5', 'se^t5', 'se%t5', 'se&t5'], 'm')
        assert flag_sets == []

        flag_sets = input_validator.validate_flag_sets(['set4', 'set1', 'set3', 'set1'], 'm')
        assert sorted(flag_sets) == ['set1', 'set3', 'set4']

        flag_sets = input_validator.validate_flag_sets(['w' * 50, 's' * 51], 'm')
        assert flag_sets == ['w' * 50]

        flag_sets = input_validator.validate_flag_sets('set1', 'm')
        assert flag_sets == []

        flag_sets = input_validator.validate_flag_sets([12, 33], 'm')

        assert flag_sets == []
