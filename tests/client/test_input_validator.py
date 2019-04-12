"""Unit tests for the input_validator module."""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

from splitio.client.factory import SplitFactory
from splitio.client.client import CONTROL, Client
from splitio.client.manager import SplitManager
from splitio.client.key import Key
from splitio.storage import SplitStorage, EventStorage, ImpressionStorage, TelemetryStorage, \
    SegmentStorage
from splitio.models.splits import Split, SplitView
from splitio.models.grammar.condition import Condition
from splitio.models.grammar.partitions import Partition
from splitio.client import input_validator


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

        def _get_storage_mock(storage):
            return {
                'splits': storage_mock,
                'segments': mocker.Mock(spec=SegmentStorage),
                'impressions': mocker.Mock(spec=ImpressionStorage),
                'events': mocker.Mock(spec=EventStorage),
                'telemetry': mocker.Mock(spec=TelemetryStorage)
            }[storage]
        factory_mock = mocker.Mock(spec=SplitFactory)
        factory_mock._get_storage.side_effect = _get_storage_mock
        factory_destroyed = mocker.PropertyMock()
        factory_destroyed.return_value = False
        type(factory_mock).destroyed = factory_destroyed

        client = Client(factory_mock)
        client._logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=client._logger)

        assert client.get_treatment(None, 'some_feature') == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: you passed a null key, key must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment('', 'some_feature') == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: you passed an empty key, key must be a non-empty string.')
        ]

        client._logger.reset_mock()
        key = ''.join('a' for _ in range(0,255))
        assert client.get_treatment(key, 'some_feature') == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: key too long - must be 250 characters or less.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment(12345, 'some_feature') == 'default_treatment'
        assert client._logger.warning.mock_calls == [
            mocker.call('get_treatment: key 12345 is not of type string, converting.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment(float('nan'), 'some_feature') == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: you passed an invalid key, key must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment(float('inf'), 'some_feature') == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: you passed an invalid key, key must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment(True, 'some_feature') == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: you passed an invalid key, key must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment([], 'some_feature') == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: you passed an invalid key, key must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment('some_key', None) == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: you passed a null feature_name, feature_name must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment('some_key', 123) == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: you passed an invalid feature_name, feature_name must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment('some_key',  True) == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: you passed an invalid feature_name, feature_name must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment('some_key',  []) == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: you passed an invalid feature_name, feature_name must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment('some_key',  '') == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: you passed an empty feature_name, feature_name must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment('some_key',  'some_feature') == 'default_treatment'
        assert client._logger.error.mock_calls == []
        assert client._logger.warning.mock_calls == []

        client._logger.reset_mock()
        assert client.get_treatment(Key(None, 'bucketing_key'),  'some_feature') == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: you passed a null matching_key, matching_key must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment(Key('', 'bucketing_key'),  'some_feature') == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: you passed an empty matching_key, matching_key must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment(Key(float('nan'), 'bucketing_key'),  'some_feature') == CONTROL
        assert client._logger.error.mock_calls == [
                mocker.call('get_treatment: you passed an invalid matching_key, matching_key must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment(Key(float('inf'), 'bucketing_key'),  'some_feature') == CONTROL
        assert client._logger.error.mock_calls == [
                mocker.call('get_treatment: you passed an invalid matching_key, matching_key must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment(Key(True, 'bucketing_key'),  'some_feature') == CONTROL
        assert client._logger.error.mock_calls == [
                mocker.call('get_treatment: you passed an invalid matching_key, matching_key must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment(Key([], 'bucketing_key'),  'some_feature') == CONTROL
        assert client._logger.error.mock_calls == [
                mocker.call('get_treatment: you passed an invalid matching_key, matching_key must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment(Key(12345, 'bucketing_key'),  'some_feature') == 'default_treatment'
        assert client._logger.warning.mock_calls == [
                mocker.call('get_treatment: matching_key 12345 is not of type string, ' 'converting.')
        ]

        client._logger.reset_mock()
        key = ''.join('a' for _ in range(0,255))
        assert client.get_treatment(Key(key, 'bucketing_key'), 'some_feature') == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: matching_key too long - must be 250 characters or less.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment(Key('mathcing_key', None), 'some_feature') == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: you passed a null bucketing_key, bucketing_key must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment(Key('mathcing_key', True), 'some_feature') == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: you passed an invalid bucketing_key, bucketing_key must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment(Key('mathcing_key', []), 'some_feature') == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: you passed an invalid bucketing_key, bucketing_key must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment(Key('mathcing_key', ''), 'some_feature') == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: you passed an empty bucketing_key, bucketing_key must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment(Key('mathcing_key', 12345), 'some_feature') == 'default_treatment'
        assert client._logger.warning.mock_calls == [
            mocker.call('get_treatment: bucketing_key 12345 is not of type string, converting.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment('mathcing_key', 'some_feature', True) == CONTROL
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatment: attributes must be of type dictionary.')
        ]

        client._logger.reset_mock()
        assert client.get_treatment('mathcing_key', 'some_feature', {'test': 'test'}) =='default_treatment'
        assert client._logger.error.mock_calls == []

        client._logger.reset_mock()
        assert client.get_treatment('mathcing_key', 'some_feature', None) =='default_treatment'
        assert client._logger.error.mock_calls == []

        client._logger.reset_mock()
        assert client.get_treatment('mathcing_key', '  some_feature   ', None) =='default_treatment'
        assert client._logger.warning.mock_calls == [
            mocker.call('get_treatment: feature_name \'  some_feature   \' has extra whitespace, trimming.')
        ]

    def test_track(self, mocker):
        """Test track method()."""
        events_storage_mock = mocker.Mock(spec=EventStorage)
        events_storage_mock.put.return_value = True
        factory_mock = mocker.Mock(spec=SplitFactory)
        factory_destroyed = mocker.PropertyMock()
        factory_destroyed.return_value = False
        type(factory_mock).destroyed = factory_destroyed

        client = Client(factory_mock)
        client._events_storage = mocker.Mock(spec=EventStorage)
        client._events_storage.put.return_value = True
        client._logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=client._logger)

        assert client.track(None, "traffic_type", "event_type", 1) == False
        assert client._logger.error.mock_calls == [
            mocker.call("track: you passed a null key, key must be a non-empty string.")
        ]

        client._logger.reset_mock()
        assert client.track("", "traffic_type", "event_type", 1) == False
        assert client._logger.error.mock_calls == [
            mocker.call("track: you passed an empty key, key must be a non-empty string.")
        ]

        client._logger.reset_mock()
        assert client.track(12345, "traffic_type", "event_type", 1) == True
        assert client._logger.warning.mock_calls == [
            mocker.call("track: key 12345 is not of type string, converting.")
        ]

        client._logger.reset_mock()
        assert client.track(True, "traffic_type", "event_type", 1) == False
        assert client._logger.error.mock_calls == [
            mocker.call("track: you passed an invalid key, key must be a non-empty string.")
        ]

        client._logger.reset_mock()
        assert client.track([], "traffic_type", "event_type", 1) == False
        assert client._logger.error.mock_calls == [
            mocker.call("track: you passed an invalid key, key must be a non-empty string.")
        ]

        client._logger.reset_mock()
        key = ''.join('a' for _ in range(0,255))
        assert client.track(key, "traffic_type", "event_type", 1) == False
        assert client._logger.error.mock_calls == [
            mocker.call("track: key too long - must be 250 characters or less.")
        ]

        client._logger.reset_mock()
        assert client.track("some_key", None, "event_type", 1) == False
        assert client._logger.error.mock_calls == [
            mocker.call("track: you passed a null traffic_type, traffic_type must be a non-empty string.")
        ]

        client._logger.reset_mock()
        assert client.track("some_key", "", "event_type", 1) == False
        assert client._logger.error.mock_calls == [
            mocker.call("track: you passed an empty traffic_type, traffic_type must be a non-empty string.")
        ]

        client._logger.reset_mock()
        assert client.track("some_key", 12345, "event_type", 1) == False
        assert client._logger.error.mock_calls == [
            mocker.call("track: you passed an invalid traffic_type, traffic_type must be a non-empty string.")
        ]

        client._logger.reset_mock()
        assert client.track("some_key", True, "event_type", 1) == False
        assert client._logger.error.mock_calls == [
            mocker.call("track: you passed an invalid traffic_type, traffic_type must be a non-empty string.")
        ]

        client._logger.reset_mock()
        assert client.track("some_key", [], "event_type", 1) == False
        assert client._logger.error.mock_calls == [
            mocker.call("track: you passed an invalid traffic_type, traffic_type must be a non-empty string.")
        ]

        client._logger.reset_mock()
        assert client.track("some_key", "TRAFFIC_type", "event_type", 1) == True
        assert client._logger.warning.mock_calls == [
            mocker.call("track: TRAFFIC_type should be all lowercase - converting string to lowercase.")
        ]

        assert client.track("some_key", "traffic_type", None, 1) == False
        assert client._logger.error.mock_calls == [
            mocker.call("track: you passed a null event_type, event_type must be a non-empty string.")
        ]

        client._logger.reset_mock()
        assert client.track("some_key", "traffic_type", "", 1) == False
        assert client._logger.error.mock_calls == [
            mocker.call("track: you passed an empty event_type, event_type must be a non-empty string.")
        ]

        client._logger.reset_mock()
        assert client.track("some_key", "traffic_type", True, 1) == False
        assert client._logger.error.mock_calls == [
            mocker.call("track: you passed an invalid event_type, event_type must be a non-empty string.")
        ]

        client._logger.reset_mock()
        assert client.track("some_key", "traffic_type", [], 1) == False
        assert client._logger.error.mock_calls == [
            mocker.call("track: you passed an invalid event_type, event_type must be a non-empty string.")
        ]

        client._logger.reset_mock()
        assert client.track("some_key", "traffic_type", 12345, 1) == False
        assert client._logger.error.mock_calls == [
            mocker.call("track: you passed an invalid event_type, event_type must be a non-empty string.")
        ]

        client._logger.reset_mock()
        assert client.track("some_key", "traffic_type", "@@", 1) == False
        assert client._logger.error.mock_calls == [
            mocker.call("track: you passed @@, event_type must adhere to the regular "
                        "expression ^[a-zA-Z0-9][-_.:a-zA-Z0-9]{0,79}$. This means "
                        "an event name must be alphanumeric, cannot be more than 80 "
                        "characters long, and can only include a dash, underscore, "
                        "period, or colon as separators of alphanumeric characters.")
        ]

        client._logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", None) == True
        assert client._logger.error.mock_calls == []

        client._logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", 1) == True
        assert client._logger.error.mock_calls == []

        client._logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", 1.23) == True
        assert client._logger.error.mock_calls == []

        client._logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", "test") == False
        assert client._logger.error.mock_calls == [
            mocker.call("track: value must be a number.")
        ]

        client._logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", True) == False
        assert client._logger.error.mock_calls == [
            mocker.call("track: value must be a number.")
        ]

        client._logger.reset_mock()
        assert client.track("some_key", "traffic_type", "event_type", []) == False
        assert client._logger.error.mock_calls == [
            mocker.call("track: value must be a number.")
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

        factory_mock = mocker.Mock(spec=SplitFactory)
        factory_mock._get_storage.return_value = storage_mock
        factory_destroyed = mocker.PropertyMock()
        factory_destroyed.return_value = False
        type(factory_mock).destroyed = factory_destroyed

        client = Client(factory_mock)
        client._logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=client._logger)

        assert client.get_treatments(None, ['some_feature']) == {'some_feature': CONTROL}
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatments: you passed a null key, key must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatments("", ['some_feature']) == {'some_feature': CONTROL}
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatments: you passed an empty key, key must be a non-empty string.')
        ]

        key = ''.join('a' for _ in range(0,255))
        client._logger.reset_mock()
        assert client.get_treatments(key, ['some_feature']) == {'some_feature': CONTROL}
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatments: key too long - must be 250 characters or less.')
        ]

        client._logger.reset_mock()
        assert client.get_treatments(12345, ['some_feature']) == {'some_feature': 'default_treatment'}
        assert client._logger.warning.mock_calls == [
            mocker.call('get_treatments: key 12345 is not of type string, converting.')
        ]

        client._logger.reset_mock()
        assert client.get_treatments(True, ['some_feature']) == {'some_feature': CONTROL}
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatments: you passed an invalid key, key must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatments([], ['some_feature']) == {'some_feature': CONTROL}
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatments: you passed an invalid key, key must be a non-empty string.')
        ]

        client._logger.reset_mock()
        assert client.get_treatments('some_key', None) == {}
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatments: feature_names must be a non-empty array.')
        ]

        client._logger.reset_mock()
        assert client.get_treatments('some_key', True) == {}
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatments: feature_names must be a non-empty array.')
        ]

        client._logger.reset_mock()
        assert client.get_treatments('some_key', 'some_string') == {}
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatments: feature_names must be a non-empty array.')
        ]

        client._logger.reset_mock()
        assert client.get_treatments('some_key', []) == {}
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatments: feature_names must be a non-empty array.')
        ]

        client._logger.reset_mock()
        assert client.get_treatments('some_key', [None, None]) == {}
        assert client._logger.error.mock_calls == [
            mocker.call('get_treatments: feature_names must be a non-empty array.')
        ]

        client._logger.reset_mock()
        assert client.get_treatments('some_key', [True]) == {}
        assert mocker.call('get_treatments: feature_names must be a non-empty array.') in client._logger.error.mock_calls

        client._logger.reset_mock()
        assert client.get_treatments('some_key', ['', '']) == {}
        assert mocker.call('get_treatments: feature_names must be a non-empty array.') in client._logger.error.mock_calls

        client._logger.reset_mock()
        assert client.get_treatments('some_key', ['some   ']) == {'some': 'default_treatment'}
        assert client._logger.warning.mock_calls == [
            mocker.call('get_treatments: feature_name \'some   \' has extra whitespace, trimming.')
        ]


class ManagerInputValidationTests(object):
    """Manager input validation test cases."""

    def test_split_(self, mocker):
        """Test split input validation."""
        storage_mock = mocker.Mock(spec=SplitStorage)
        split_mock = mocker.Mock(spec=Split)
        storage_mock.get.return_value = split_mock
        factory_mock = mocker.Mock(spec=SplitFactory)
        factory_mock._get_storage.return_value = storage_mock
        factory_destroyed = mocker.PropertyMock()
        factory_destroyed.return_value = False
        type(factory_mock).destroyed = factory_destroyed

        manager = SplitManager(factory_mock)
        manager._logger = mocker.Mock()
        mocker.patch('splitio.client.input_validator._LOGGER', new=manager._logger)

        assert manager.split(None) == None
        assert manager._logger.error.mock_calls == [
            mocker.call("split: you passed a null feature_name, feature_name must be a non-empty string.")
        ]

        manager._logger.reset_mock()
        assert manager.split("") == None
        assert manager._logger.error.mock_calls == [
            mocker.call("split: you passed an empty feature_name, feature_name must be a non-empty string.")
        ]

        manager._logger.reset_mock()
        assert manager.split(True) == None
        assert manager._logger.error.mock_calls == [
            mocker.call("split: you passed an invalid feature_name, feature_name must be a non-empty string.")
        ]

        manager._logger.reset_mock()
        assert manager.split([]) == None
        assert manager._logger.error.mock_calls == [
            mocker.call("split: you passed an invalid feature_name, feature_name must be a non-empty string.")
        ]

        manager._logger.reset_mock()
        manager.split('some_split')
        assert split_mock.to_split_view.mock_calls == [mocker.call()]
        assert manager._logger.error.mock_calls == []



#class TestInputSanitizationFactory(TestCase):
#
#    def setUp(self):
#        input_validator._LOGGER.error = mock.MagicMock()
#        self.logger_error = input_validator._LOGGER.error
#
#    def test_factory_with_null_apikey(self):
#        self.assertEqual(None, get_factory(None))
#        self.logger_error \
#            .assert_called_once_with("factory_instantiation: you passed a null apikey, apikey" +
#                                     " must be a non-empty string.")
#
#    def test_factory_with_empty_apikey(self):
#        self.assertEqual(None, get_factory(''))
#        self.logger_error \
#            .assert_called_once_with("factory_instantiation: you passed an empty apikey, apikey" +
#                                     " must be a non-empty string.")
#
#    def test_factory_with_invalid_apikey(self):
#        self.assertEqual(None, get_factory(True))
#        self.logger_error \
#            .assert_called_once_with("factory_instantiation: you passed an invalid apikey, apikey" +
#                                     " must be a non-empty string.")
#
#    def test_factory_with_invalid_apikey_redis(self):
#        config = {
#            'redisDb': 0,
#            'redisHost': 'localhost'
#        }
#        self.assertNotEqual(None, get_factory(True, config=config))
#        self.logger_error.assert_not_called()
#
#    def test_factory_with_invalid_config(self):
#        config = {
#            'some': 0
#        }
#        self.assertEqual(None, get_factory("apikey", config=config))
#        self.logger_error \
#            .assert_called_once_with('no ready parameter has been set - incorrect control '
#                                     + 'treatments could be logged')
#
#    def test_factory_with_invalid_null_ready(self):
#        config = {
#            'ready': None
#        }
#        self.assertEqual(None, get_factory("apikey", config=config))
#        self.logger_error \
#            .assert_called_once_with('no ready parameter has been set - incorrect control '
#                                     + 'treatments could be logged')
#
#    def test_factory_with_invalid_ready(self):
#        config = {
#            'ready': True
#        }
#        self.assertEqual(None, get_factory("apikey", config=config))
#        self.logger_error \
#            .assert_called_once_with('no ready parameter has been set - incorrect control '
#                                     + 'treatments could be logged')
