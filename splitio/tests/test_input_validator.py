"""Unit tests for the input_validator module"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase
from splitio.brokers import RedisBroker, SelfRefreshingBroker, UWSGIBroker
from splitio.clients import Client
from splitio.treatments import CONTROL
from splitio.redis_support import get_redis
from splitio.splits import Split
from splitio import input_validator
from splitio.managers import RedisSplitManager, SelfRefreshingSplitManager, UWSGISplitManager
from splitio.key import Key
from splitio.uwsgi import UWSGICacheEmulator
from splitio import get_factory


class TestInputSanitizationGetTreatment(TestCase):

    def setUp(self):
        self.some_config = mock.MagicMock()
        self.some_api_key = mock.MagicMock()
        self.redis = get_redis({'redisPrefix': 'test'})
        self.client = Client(RedisBroker(self.redis, self.some_config))
        self.client._broker.fetch_feature = mock.MagicMock(return_value=Split(
            "some_feature",
            0,
            False,
            "default_treatment",
            "user",
            "ACTIVE",
            123
        ))

        input_validator._LOGGER.error = mock.MagicMock()
        self.logger_error = input_validator._LOGGER.error
        input_validator._LOGGER.warning = mock.MagicMock()
        self.logger_warning = input_validator._LOGGER.warning

    def test_get_treatment_with_null_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            None, "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed a null key, key must be a" +
                                     " non-empty string.")

    def test_get_treatment_with_empty_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            "", "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed an empty key, key must be a" +
                                     " non-empty string.")

    def test_get_treatment_with_length_key(self):
        key = ""
        for x in range(0, 255):
            key = key + "a"
        self.assertEqual(CONTROL, self.client.get_treatment(key, "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: key too long - must be 250 characters or " +
                                     "less.")

    def test_get_treatment_with_number_key(self):
        self.assertEqual("default_treatment", self.client.get_treatment(
            12345, "some_feature"))
        self.logger_warning \
            .assert_called_once_with("get_treatment: key 12345 is not of type string, converting.")

    def test_get_treatment_with_nan_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            float("nan"), "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed an invalid key, key must be a" +
                                     " non-empty string.")

    def test_get_treatment_with_inf_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            float("inf"), "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed an invalid key, key must be a" +
                                     " non-empty string.")

    def test_get_treatment_with_bool_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            True, "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed an invalid key, key must be a" +
                                     " non-empty string.")

    def test_get_treatment_with_array_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            [], "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed an invalid key, key must be a" +
                                     " non-empty string.")

    def test_get_treatment_with_null_feature_name(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            "some_key", None))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed a null feature_name, " +
                                     "feature_name must be a non-empty string.")

    def test_get_treatment_with_numeric_feature_name(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            "some_key", 12345))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed an invalid feature_name, " +
                                     "feature_name must be a non-empty string.")

    def test_get_treatment_with_bool_feature_name(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            "some_key", True))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed an invalid feature_name, " +
                                     "feature_name must be a non-empty string.")

    def test_get_treatment_with_array_feature_name(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            "some_key", []))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed an invalid feature_name, " +
                                     "feature_name must be a non-empty string.")

    def test_get_treatment_with_empty_feature_name(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            "some_key", ""))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed an empty feature_name, " +
                                     "feature_name must be a non-empty string.")

    def test_get_treatment_with_valid_inputs(self):
        self.assertEqual("default_treatment", self.client.get_treatment(
            "some_key", "some_feature"))
        self.logger_error.assert_not_called()
        self.logger_warning.assert_not_called()

    def test_get_treatment_with_null_matching_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            Key(None, "bucketing_key"), "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed a null matching_key, " +
                                     "matching_key must be a non-empty string.")

    def test_get_treatment_with_empty_matching_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            Key("", "bucketing_key"), "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed an empty matching_key, " +
                                     "matching_key must be a non-empty string.")

    def test_get_treatment_with_nan_matching_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            Key(float("nan"), "bucketing_key"), "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed an invalid matching_key, " +
                                     "matching_key must be a non-empty string.")

    def test_get_treatment_with_inf_matching_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            Key(float("inf"), "bucketing_key"), "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed an invalid matching_key, " +
                                     "matching_key must be a non-empty string.")

    def test_get_treatment_with_bool_matching_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            Key(True, "bucketing_key"), "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed an invalid matching_key, " +
                                     "matching_key must be a non-empty string.")

    def test_get_treatment_with_array_matching_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            Key([], "bucketing_key"), "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed an invalid matching_key, " +
                                     "matching_key must be a non-empty string.")

    def test_get_treatment_with_numeric_matching_key(self):
        self.assertEqual("default_treatment", self.client.get_treatment(
            Key(12345, "bucketing_key"), "some_feature"))
        self.logger_warning \
            .assert_called_once_with("get_treatment: matching_key 12345 is not of type string, "
                                     "converting.")

    def test_get_treatment_with_length_matching_key(self):
        key = ""
        for x in range(0, 255):
            key = key + "a"
        self.assertEqual(CONTROL, self.client.get_treatment(Key(key, "bucketing_key"),
                                                            "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: matching_key too long - must be 250 " +
                                     "characters or less.")

    def test_get_treatment_with_null_bucketing_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            Key("matching_key", None), "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed a null bucketing_key, " +
                                     "bucketing_key must be a non-empty string.")

    def test_get_treatment_with_bool_bucketing_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            Key("matching_key", True), "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed an invalid bucketing_key, " +
                                     "bucketing_key must be a non-empty string.")

    def test_get_treatment_with_array_bucketing_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            Key("matching_key", []), "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed an invalid bucketing_key, " +
                                     "bucketing_key must be a non-empty string.")

    def test_get_treatment_with_empty_bucketing_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            Key("matching_key", ""), "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: you passed an empty bucketing_key, " +
                                     "bucketing_key must be a non-empty string.")

    def test_get_treatment_with_numeric_bucketing_key(self):
        self.assertEqual("default_treatment", self.client.get_treatment(
            Key("matching_key", 12345), "some_feature"))
        self.logger_warning \
            .assert_called_once_with("get_treatment: bucketing_key 12345 is not of type string, "
                                     "converting.")

    def test_get_treatment_with_invalid_attributes(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            "some_key", "some_feature", True))
        self.logger_error \
            .assert_called_once_with("get_treatment: attributes must be of type dictionary.")

    def test_get_treatment_with_valid_attributes(self):
        attributes = {
            "test": "test"
        }
        self.assertEqual("default_treatment", self.client.get_treatment(
            "some_key", "some_feature", attributes))

    def test_get_treatment_with_none_attributes(self):
        self.assertEqual("default_treatment", self.client.get_treatment(
            "some_key", "some_feature", None))


class TestInputSanitizationTrack(TestCase):

    def setUp(self):
        self.some_config = mock.MagicMock()
        self.some_api_key = mock.MagicMock()
        self.redis = get_redis({'redisPrefix': 'test'})
        self.client = Client(RedisBroker(self.redis, self.some_config))

        input_validator._LOGGER.error = mock.MagicMock()
        self.logger_error = input_validator._LOGGER.error
        input_validator._LOGGER.warning = mock.MagicMock()
        self.logger_warning = input_validator._LOGGER.warning

    def test_track_with_null_key(self):
        self.assertEqual(False, self.client.track(
            None, "traffic_type", "event_type", 1))
        self.logger_error \
            .assert_called_once_with("track: you passed a null key, key must be a" +
                                     " non-empty string.")

    def test_track_with_empty_key(self):
        self.assertEqual(False, self.client.track(
            "", "traffic_type", "event_type", 1))
        self.logger_error \
            .assert_called_once_with("track: you passed an empty key, key must be a" +
                                     " non-empty string.")

    def test_track_with_numeric_key(self):
        self.assertEqual(True, self.client.track(
            12345, "traffic_type", "event_type", 1))
        self.logger_warning \
            .assert_called_once_with("track: key 12345 is not of type string,"
                                     " converting.")

    def test_track_with_bool_key(self):
        self.assertEqual(False, self.client.track(
            True, "traffic_type", "event_type", 1))
        self.logger_error \
            .assert_called_once_with("track: you passed an invalid key, key must be a" +
                                     " non-empty string.")

    def test_track_with_array_key(self):
        self.assertEqual(False, self.client.track(
            [], "traffic_type", "event_type", 1))
        self.logger_error \
            .assert_called_once_with("track: you passed an invalid key, key must be a" +
                                     " non-empty string.")

    def test_track_with_length_key(self):
        key = ""
        for x in range(0, 255):
            key = key + "a"
        self.assertEqual(False, self.client.track(
            key, "traffic_type", "event_type", 1))
        self.logger_error \
            .assert_called_once_with("track: key too long - must be 250 characters or " +
                                     "less.")

    def test_track_with_null_traffic_type(self):
        self.assertEqual(False, self.client.track(
            "some_key", None, "event_type", 1))
        self.logger_error \
            .assert_called_once_with("track: you passed a null traffic_type, traffic_type" +
                                     " must be a non-empty string.")

    def test_track_with_bool_traffic_type(self):
        self.assertEqual(False, self.client.track(
            "some_key", True, "event_type", 1))
        self.logger_error \
            .assert_called_once_with("track: you passed an invalid traffic_type, traffic_type" +
                                     " must be a non-empty string.")

    def test_track_with_array_traffic_type(self):
        self.assertEqual(False, self.client.track(
            "some_key", [], "event_type", 1))
        self.logger_error \
            .assert_called_once_with("track: you passed an invalid traffic_type, traffic_type" +
                                     " must be a non-empty string.")

    def test_track_with_numeric_traffic_type(self):
        self.assertEqual(False, self.client.track(
            "some_key", 12345, "event_type", 1))
        self.logger_error \
            .assert_called_once_with("track: you passed an invalid traffic_type, traffic_type" +
                                     " must be a non-empty string.")

    def test_track_with_empty_traffic_type(self):
        self.assertEqual(False, self.client.track(
            "some_key", "", "event_type", 1))
        self.logger_error \
            .assert_called_once_with("track: you passed an empty traffic_type, traffic_type" +
                                     " must be a non-empty string.")

    def test_track_with_lowercase_traffic_type(self):
        self.assertEqual(True, self.client.track(
            "some_key", "TRAFFIC_type", "event_type", 1))
        self.logger_warning \
            .assert_called_once_with("track: TRAFFIC_type should be all lowercase -" +
                                     " converting string to lowercase.")

    def test_track_with_null_event_type(self):
        self.assertEqual(False, self.client.track(
            "some_key", "traffic_type", None, 1))
        self.logger_error \
            .assert_called_once_with("track: you passed a null event_type, event_type" +
                                     " must be a non-empty string.")

    def test_track_with_empty_event_type(self):
        self.assertEqual(False, self.client.track(
            "some_key", "traffic_type", "", 1))
        self.logger_error \
            .assert_called_once_with("track: you passed an empty event_type, event_type" +
                                     " must be a non-empty string.")

    def test_track_with_bool_event_type(self):
        self.assertEqual(False, self.client.track(
            "some_key", "traffic_type", True, 1))
        self.logger_error \
            .assert_called_once_with("track: you passed an invalid event_type, event_type" +
                                     " must be a non-empty string.")

    def test_track_with_array_event_type(self):
        self.assertEqual(False, self.client.track(
            "some_key", "traffic_type", [], 1))
        self.logger_error \
            .assert_called_once_with("track: you passed an invalid event_type, event_type" +
                                     " must be a non-empty string.")

    def test_track_with_numeric_event_type(self):
        self.assertEqual(False, self.client.track(
            "some_key", "traffic_type", 12345, 1))
        self.logger_error \
            .assert_called_once_with("track: you passed an invalid event_type, event_type" +
                                     " must be a non-empty string.")

    def test_track_with_event_type_does_not_conform_reg_exp(self):
        self.assertEqual(False, self.client.track(
            "some_key", "traffic_type", "@@", 1))
        self.logger_error \
            .assert_called_once_with("track: you passed @@, event_type must adhere to the regular "
                                     "expression ^[a-zA-Z0-9][-_.:a-zA-Z0-9]{0,79}$. This means "
                                     "an event name must be alphanumeric, cannot be more than 80 "
                                     "characters long, and can only include a dash, underscore, "
                                     "period, or colon as separators of alphanumeric characters.")

    def test_track_with_null_value(self):
        self.assertEqual(True, self.client.track(
            "some_key", "traffic_type", "event_type", None))
        self.logger_error.assert_not_called()
        self.logger_warning.assert_not_called()

    def test_track_with_string_value(self):
        self.assertEqual(False, self.client.track(
            "some_key", "traffic_type", "event_type", "test"))
        self.logger_error \
            .assert_called_once_with("track: value must be a number.")

    def test_track_with_bool_value(self):
        self.assertEqual(False, self.client.track(
            "some_key", "traffic_type", "event_type", True))
        self.logger_error \
            .assert_called_once_with("track: value must be a number.")

    def test_track_with_array_value(self):
        self.assertEqual(False, self.client.track(
            "some_key", "traffic_type", "event_type", []))
        self.logger_error \
            .assert_called_once_with("track: value must be a number.")

    def test_track_with_int_value(self):
        self.assertEqual(True, self.client.track(
            "some_key", "traffic_type", "event_type", 1))
        self.logger_error.assert_not_called()
        self.logger_warning.assert_not_called()

    def test_track_with_float_value(self):
        self.assertEqual(True, self.client.track(
            "some_key", "traffic_type", "event_type", 1.3))
        self.logger_error.assert_not_called()
        self.logger_warning.assert_not_called()


class TestInputSanitizationRedisManager(TestCase):

    def setUp(self):
        self.some_config = mock.MagicMock()
        self.some_api_key = mock.MagicMock()
        self.redis = get_redis({'redisPrefix': 'test'})
        self.client = Client(RedisBroker(self.redis, self.some_config))

        self.manager = RedisSplitManager(self.client._broker)

        input_validator._LOGGER.error = mock.MagicMock()
        self.logger_error = input_validator._LOGGER.error

    def test_manager_with_null_feature_name(self):
        self.assertEqual(None, self.manager.split(None))
        self.logger_error \
            .assert_called_once_with("split: you passed a null feature_name, feature_name" +
                                     " must be a non-empty string.")

    def test_manager_with_empty_feature_name(self):
        self.assertEqual(None, self.manager.split(""))
        self.logger_error \
            .assert_called_once_with("split: you passed an empty feature_name, feature_name" +
                                     " must be a non-empty string.")

    def test_manager_with_bool_feature_name(self):
        self.assertEqual(None, self.manager.split(True))
        self.logger_error \
            .assert_called_once_with("split: you passed an invalid feature_name, feature_name" +
                                     " must be a non-empty string.")

    def test_manager_with_array_feature_name(self):
        self.assertEqual(None, self.manager.split([]))
        self.logger_error \
            .assert_called_once_with("split: you passed an invalid feature_name, feature_name" +
                                     " must be a non-empty string.")

    def test_manager_with_numeric_feature_name(self):
        self.assertEqual(None, self.manager.split(12345))
        self.logger_error \
            .assert_called_once_with("split: you passed an invalid feature_name, feature_name" +
                                     " must be a non-empty string.")

    def test_manager_with_valid_feature_name(self):
        self.assertEqual(None, self.manager.split("valid_feature_name"))
        self.logger_error.assert_not_called()


class TestInputSanitizationSelfRefreshingManager(TestCase):

    def setUp(self):
        self.some_api_key = mock.MagicMock()
        self.broker = SelfRefreshingBroker(self.some_api_key)
        self.client = Client(self.broker)
        self.manager = SelfRefreshingSplitManager(self.broker)

        input_validator._LOGGER.error = mock.MagicMock()
        self.logger_error = input_validator._LOGGER.error

    def test_manager_with_null_feature_name(self):
        self.assertEqual(None, self.manager.split(None))
        self.logger_error \
            .assert_called_once_with("split: you passed a null feature_name, feature_name" +
                                     " must be a non-empty string.")

    def test_manager_with_empty_feature_name(self):
        self.assertEqual(None, self.manager.split(""))
        self.logger_error \
            .assert_called_once_with("split: you passed an empty feature_name, feature_name" +
                                     " must be a non-empty string.")

    def test_manager_with_bool_feature_name(self):
        self.assertEqual(None, self.manager.split(True))
        self.logger_error \
            .assert_called_once_with("split: you passed an invalid feature_name, feature_name" +
                                     " must be a non-empty string.")

    def test_manager_with_array_feature_name(self):
        self.assertEqual(None, self.manager.split([]))
        self.logger_error \
            .assert_called_once_with("split: you passed an invalid feature_name, feature_name" +
                                     " must be a non-empty string.")

    def test_manager_with_numeric_feature_name(self):
        self.assertEqual(None, self.manager.split(12345))
        self.logger_error \
            .assert_called_once_with("split: you passed an invalid feature_name, feature_name" +
                                     " must be a non-empty string.")

    def test_manager_with_valid_feature_name(self):
        self.assertEqual(None, self.manager.split("valid_feature_name"))
        self.logger_error.assert_not_called()


class TestInputSanitizationUWSGIManager(TestCase):

    def setUp(self):
        self.some_api_key = mock.MagicMock()
        self.uwsgi = UWSGICacheEmulator()
        self.broker = UWSGIBroker(self.uwsgi, {'eventsQueueSize': 30})
        self.client = Client(self.broker)
        self.manager = UWSGISplitManager(self.broker)

        input_validator._LOGGER.error = mock.MagicMock()
        self.logger_error = input_validator._LOGGER.error

    def test_manager_with_null_feature_name(self):
        self.assertEqual(None, self.manager.split(None))
        self.logger_error \
            .assert_called_once_with("split: you passed a null feature_name, feature_name" +
                                     " must be a non-empty string.")

    def test_manager_with_empty_feature_name(self):
        self.assertEqual(None, self.manager.split(""))
        self.logger_error \
            .assert_called_once_with("split: you passed an empty feature_name, feature_name" +
                                     " must be a non-empty string.")

    def test_manager_with_bool_feature_name(self):
        self.assertEqual(None, self.manager.split(True))
        self.logger_error \
            .assert_called_once_with("split: you passed an invalid feature_name, feature_name" +
                                     " must be a non-empty string.")

    def test_manager_with_array_feature_name(self):
        self.assertEqual(None, self.manager.split([]))
        self.logger_error \
            .assert_called_once_with("split: you passed an invalid feature_name, feature_name" +
                                     " must be a non-empty string.")

    def test_manager_with_numeric_feature_name(self):
        self.assertEqual(None, self.manager.split(12345))
        self.logger_error \
            .assert_called_once_with("split: you passed an invalid feature_name, feature_name" +
                                     " must be a non-empty string.")

    def test_manager_with_valid_feature_name(self):
        self.assertEqual(None, self.manager.split("valid_feature_name"))
        self.logger_error.assert_not_called()


class TestInputSanitizationGetTreatments(TestCase):

    def setUp(self):
        self.some_config = mock.MagicMock()
        self.some_api_key = mock.MagicMock()
        self.redis = get_redis({'redisPrefix': 'test'})
        self.client = Client(RedisBroker(self.redis, self.some_config))

        input_validator._LOGGER.error = mock.MagicMock()
        self.logger_error = input_validator._LOGGER.error
        input_validator._LOGGER.warning = mock.MagicMock()
        self.logger_warning = input_validator._LOGGER.warning

    def test_get_treatments_with_null_key(self):
        self.assertEqual(None, self.client.get_treatments(
            None, ["some_feature"]))
        self.logger_error \
            .assert_called_once_with("get_treatments: you passed a null key, key must be a" +
                                     " non-empty string.")

    def test_get_treatments_with_empty_key(self):
        self.assertEqual(None, self.client.get_treatments(
            "", ["some_feature"]))
        self.logger_error \
            .assert_called_once_with("get_treatments: you passed an empty key, key must be a" +
                                     " non-empty string.")

    def test_get_treatments_with_length_key(self):
        key = ""
        for x in range(0, 255):
            key = key + "a"
        self.assertEqual(None, self.client.get_treatments(key, ["some_feature"]))
        self.logger_error \
            .assert_called_once_with("get_treatments: key too long - must be 250 characters or " +
                                     "less.")

    def test_get_treatments_with_number_key(self):
        self.assertEqual({"some_feature": "control"}, self.client.get_treatments(
            12345, ["some_feature"]))
        self.logger_warning \
            .assert_called_once_with("get_treatments: key 12345 is not of type string, converting.")

    def test_get_treatments_with_bool_key(self):
        self.assertEqual(None, self.client.get_treatments(
            True, ["some_feature"]))
        self.logger_error \
            .assert_called_once_with("get_treatments: you passed an invalid key, key must be a" +
                                     " non-empty string.")

    def test_get_treatments_with_array_key(self):
        self.assertEqual(None, self.client.get_treatments(
            [], ["some_feature"]))
        self.logger_error \
            .assert_called_once_with("get_treatments: you passed an invalid key, key must be a" +
                                     " non-empty string.")

    def test_get_treatments_with_null_features(self):
        self.assertEqual(None, self.client.get_treatments("some_key", None))
        self.logger_error \
            .assert_called_once_with("get_treatments: feature_names must be a non-empty array.")

    def test_get_treatments_with_bool_type_of_features(self):
        self.assertEqual(None, self.client.get_treatments("some_key", True))
        self.logger_error \
            .assert_called_once_with("get_treatments: feature_names must be a non-empty array.")

    def test_get_treatments_with_string_type_of_features(self):
        self.assertEqual(None, self.client.get_treatments("some_key", "some_string"))
        self.logger_error \
            .assert_called_once_with("get_treatments: feature_names must be a non-empty array.")

    def test_get_treatments_with_empty_features(self):
        self.assertEqual({}, self.client.get_treatments("some_key", []))
        self.logger_error \
            .assert_called_once_with("get_treatments: feature_names must be a non-empty array.")

    def test_get_treatments_with_none_features(self):
        self.assertEqual(None, self.client.get_treatments("some_key", [None, None]))
        self.logger_error \
            .assert_called_once_with("get_treatments: feature_names must be a non-empty array.")

    def test_get_treatments_with_invalid_type_of_features(self):
        self.assertEqual(None, self.client.get_treatments("some_key", [True]))
        self.logger_error \
            .assert_called_with("get_treatments: feature_names must be a non-empty array.")


class TestInputSanitizationFactory(TestCase):

    def setUp(self):

        input_validator._LOGGER.error = mock.MagicMock()
        self.logger_error = input_validator._LOGGER.error

    def test_factory_with_null_apikey(self):
        self.assertEqual(None, get_factory(None))
        self.logger_error \
            .assert_called_once_with("factory_instantiation: you passed a null apikey, apikey" +
                                     " must be a non-empty string.")

    def test_factory_with_empty_apikey(self):
        self.assertEqual(None, get_factory(''))
        self.logger_error \
            .assert_called_once_with("factory_instantiation: you passed an empty apikey, apikey" +
                                     " must be a non-empty string.")

    def test_factory_with_invalid_apikey(self):
        self.assertEqual(None, get_factory(True))
        self.logger_error \
            .assert_called_once_with("factory_instantiation: you passed an invalid apikey, apikey" +
                                     " must be a non-empty string.")

    def test_factory_with_invalid_apikey_redis(self):
        config = {
            'redisDb': 0,
            'redisHost': 'localhost'
        }
        self.assertNotEqual(None, get_factory(True, config=config))
        self.logger_error.assert_not_called()
