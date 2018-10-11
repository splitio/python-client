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
from splitio.impressions import Label
from splitio import input_validator
from splitio.managers import RedisSplitManager, SelfRefreshingSplitManager, UWSGISplitManager
from splitio.key import Key
from splitio.uwsgi import UWSGICacheEmulator


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

        self.client._build_impression = mock.MagicMock()

        input_validator._LOGGER.error = mock.MagicMock()
        self.logger_error = input_validator._LOGGER.error
        input_validator._LOGGER.warning = mock.MagicMock()
        self.logger_warning = input_validator._LOGGER.warning

    def test_get_treatment_with_null_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            None, "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: key cannot be None.")
        self.client._build_impression.assert_called_once_with(
            None, "some_feature", CONTROL, Label.EXCEPTION, 0, None, mock.ANY
        )

    def test_get_treatment_with_number_key(self):
        self.assertEqual("default_treatment", self.client.get_treatment(
            12345, "some_feature"))
        self.logger_warning \
            .assert_called_once_with("get_treatment: key 12345 is not of type string, converting.")

    def test_get_treatment_with_bool_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            True, "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: key True has to be of type string "
                                     "or object Key.")
        self.client._build_impression.assert_called_once_with(
            None, "some_feature", CONTROL, Label.EXCEPTION, 0, None, mock.ANY
        )

    def test_get_treatment_with_array_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            [], "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: key [] has to be of type string "
                                     "or object Key.")
        self.client._build_impression.assert_called_once_with(
            None, "some_feature", CONTROL, Label.EXCEPTION, 0, None, mock.ANY
        )

    def test_get_treatment_with_null_feature_name(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            "some_key", None))
        self.logger_error \
            .assert_called_once_with("get_treatment: feature_name cannot be None.")
        self.client._build_impression.assert_called_once_with(
            "some_key", None, CONTROL, Label.EXCEPTION, 0, None, mock.ANY
        )

    def test_get_treatment_with_numeric_feature_name(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            "some_key", 12345))
        self.logger_error \
            .assert_called_once_with("get_treatment: feature_name 12345 has to be of type string.")
        self.client._build_impression.assert_called_once_with(
            "some_key", None, CONTROL, Label.EXCEPTION, 0, None, mock.ANY
        )

    def test_get_treatment_with_bool_feature_name(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            "some_key", True))
        self.logger_error \
            .assert_called_once_with("get_treatment: feature_name True has to be of type string.")
        self.client._build_impression.assert_called_once_with(
            "some_key", None, CONTROL, Label.EXCEPTION, 0, None, mock.ANY
        )

    def test_get_treatment_with_array_feature_name(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            "some_key", []))
        self.logger_error \
            .assert_called_once_with("get_treatment: feature_name [] has to be of type string.")
        self.client._build_impression.assert_called_once_with(
            "some_key", None, CONTROL, Label.EXCEPTION, 0, None, mock.ANY
        )

    def test_get_treatment_with_valid_inputs(self):
        self.assertEqual("default_treatment", self.client.get_treatment(
            "some_key", "some_feature"))
        self.logger_error.assert_not_called()
        self.logger_warning.assert_not_called()

    def test_get_tratment_with_null_matching_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            Key(None, "bucketing_key"), "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: Key should be an object with "
                                     "bucketingKey and matchingKey with valid string properties.")
        self.client._build_impression.assert_called_once_with(
            None, "some_feature", CONTROL, Label.EXCEPTION, 0, None, mock.ANY
        )

    def test_get_tratment_with_empty_matching_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            Key("", "bucketing_key"), "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: matching_key must not be empty.")
        self.client._build_impression.assert_called_once_with(
            None, "some_feature", CONTROL, Label.EXCEPTION, 0, None, mock.ANY
        )

    def test_get_tratment_with_bool_matching_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            Key(True, "bucketing_key"), "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: matching_key True has to be of type string.")
        self.client._build_impression.assert_called_once_with(
            None, "some_feature", CONTROL, Label.EXCEPTION, 0, None, mock.ANY
        )

    def test_get_tratment_with_array_matching_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            Key([], "bucketing_key"), "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: matching_key [] has to be of type string.")
        self.client._build_impression.assert_called_once_with(
            None, "some_feature", CONTROL, Label.EXCEPTION, 0, None, mock.ANY
        )

    def test_get_tratment_with_numeric_matching_key(self):
        self.assertEqual("default_treatment", self.client.get_treatment(
            Key(12345, "bucketing_key"), "some_feature"))
        self.logger_warning \
            .assert_called_once_with("get_treatment: matching_key 12345 is not of type string, "
                                     "converting.")

    def test_get_tratment_with_null_bucketing_key(self):
        self.assertEqual("default_treatment", self.client.get_treatment(
            Key("matching_key", None), "some_feature"))
        self.logger_warning \
            .assert_called_once_with("get_treatment: Key object should have bucketingKey set.")

    def test_get_tratment_with_bool_bucketing_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            Key("matching_key", True), "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: bucketing_key True has to be of type string.")
        self.client._build_impression.assert_called_once_with(
            None, "some_feature", CONTROL, Label.EXCEPTION, 0, None, mock.ANY
        )

    def test_get_tratment_with_array_bucketing_key(self):
        self.assertEqual(CONTROL, self.client.get_treatment(
            Key("matching_key", []), "some_feature"))
        self.logger_error \
            .assert_called_once_with("get_treatment: bucketing_key [] has to be of type string.")
        self.client._build_impression.assert_called_once_with(
            None, "some_feature", CONTROL, Label.EXCEPTION, 0, None, mock.ANY
        )

    def test_get_tratment_with_numeric_bucketing_key(self):
        self.assertEqual("default_treatment", self.client.get_treatment(
            Key("matching_key", 12345), "some_feature"))
        self.logger_warning \
            .assert_called_once_with("get_treatment: bucketing_key 12345 is not of type string, "
                                     "converting.")


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
            .assert_called_once_with("track: key cannot be None.")

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
            .assert_called_once_with("track: key True has to be of type string.")

    def test_track_with_array_key(self):
        self.assertEqual(False, self.client.track(
            [], "traffic_type", "event_type", 1))
        self.logger_error \
            .assert_called_once_with("track: key [] has to be of type string.")

    def test_track_with_null_traffic_type(self):
        self.assertEqual(False, self.client.track(
            "some_key", None, "event_type", 1))
        self.logger_error \
            .assert_called_once_with("track: traffic_type cannot be None.")

    def test_track_with_bool_traffic_type(self):
        self.assertEqual(False, self.client.track(
            "some_key", True, "event_type", 1))
        self.logger_error \
            .assert_called_once_with("track: traffic_type True has to be of type string.")

    def test_track_with_array_traffic_type(self):
        self.assertEqual(False, self.client.track(
            "some_key", [], "event_type", 1))
        self.logger_error \
            .assert_called_once_with("track: traffic_type [] has to be of type string.")

    def test_track_with_numeric_traffic_type(self):
        self.assertEqual(False, self.client.track(
            "some_key", 12345, "event_type", 1))
        self.logger_error \
            .assert_called_once_with("track: traffic_type 12345 has to be of type string.")

    def test_track_with_empty_traffic_type(self):
        self.assertEqual(False, self.client.track(
            "some_key", "", "event_type", 1))
        self.logger_error \
            .assert_called_once_with("track: traffic_type must not be empty.")

    def test_track_with_null_event_type(self):
        self.assertEqual(False, self.client.track(
            "some_key", "traffic_type", None, 1))
        self.logger_error \
            .assert_called_once_with("track: event_type cannot be None.")

    def test_track_with_bool_event_type(self):
        self.assertEqual(False, self.client.track(
            "some_key", "traffic_type", True, 1))
        self.logger_error \
            .assert_called_once_with("track: event_type True has to be of type string.")

    def test_track_with_array_event_type(self):
        self.assertEqual(False, self.client.track(
            "some_key", "traffic_type", [], 1))
        self.logger_error \
            .assert_called_once_with("track: event_type [] has to be of type string.")

    def test_track_with_numeric_event_type(self):
        self.assertEqual(False, self.client.track(
            "some_key", "traffic_type", 12345, 1))
        self.logger_error \
            .assert_called_once_with("track: event_type 12345 has to be of type string.")

    def test_track_with_event_type_does_not_conform_reg_exp(self):
        self.assertEqual(False, self.client.track(
            "some_key", "traffic_type", "@@", 1))
        self.logger_error \
            .assert_called_once_with("track: event_type must adhere to the regular "
                                     "expression [a-zA-Z0-9][-_\\.a-zA-Z0-9]{0,62}.")

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
            .assert_called_once_with("split: feature_name cannot be None.")

    def test_manager_with_bool_feature_name(self):
        self.assertEqual(None, self.manager.split(True))
        self.logger_error \
            .assert_called_once_with("split: feature_name True has to be of type string.")

    def test_manager_with_array_feature_name(self):
        self.assertEqual(None, self.manager.split([]))
        self.logger_error \
            .assert_called_once_with("split: feature_name [] has to be of type string.")

    def test_manager_with_numeric_feature_name(self):
        self.assertEqual(None, self.manager.split(12345))
        self.logger_error \
            .assert_called_once_with("split: feature_name 12345 has to be of type string.")

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
            .assert_called_once_with("split: feature_name cannot be None.")

    def test_manager_with_bool_feature_name(self):
        self.assertEqual(None, self.manager.split(True))
        self.logger_error \
            .assert_called_once_with("split: feature_name True has to be of type string.")

    def test_manager_with_array_feature_name(self):
        self.assertEqual(None, self.manager.split([]))
        self.logger_error \
            .assert_called_once_with("split: feature_name [] has to be of type string.")

    def test_manager_with_numeric_feature_name(self):
        self.assertEqual(None, self.manager.split(12345))
        self.logger_error \
            .assert_called_once_with("split: feature_name 12345 has to be of type string.")

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
            .assert_called_once_with("split: feature_name cannot be None.")

    def test_manager_with_bool_feature_name(self):
        self.assertEqual(None, self.manager.split(True))
        self.logger_error \
            .assert_called_once_with("split: feature_name True has to be of type string.")

    def test_manager_with_array_feature_name(self):
        self.assertEqual(None, self.manager.split([]))
        self.logger_error \
            .assert_called_once_with("split: feature_name [] has to be of type string.")

    def test_manager_with_numeric_feature_name(self):
        self.assertEqual(None, self.manager.split(12345))
        self.logger_error \
            .assert_called_once_with("split: feature_name 12345 has to be of type string.")

    def test_manager_with_valid_feature_name(self):
        self.assertEqual(None, self.manager.split("valid_feature_name"))
        self.logger_error.assert_not_called()


class TestInputSanitizationGetTreatmentS(TestCase):

    def setUp(self):
        self.some_config = mock.MagicMock()
        self.some_api_key = mock.MagicMock()
        self.redis = get_redis({'redisPrefix': 'test'})
        self.client = Client(RedisBroker(self.redis, self.some_config))

        input_validator._LOGGER.error = mock.MagicMock()
        self.logger_error = input_validator._LOGGER.error
        input_validator._LOGGER.warning = mock.MagicMock()
        self.logger_warning = input_validator._LOGGER.warning

    def test_get_treatments_with_null_features(self):
        self.assertEqual(None, self.client.get_treatments("some_key", None))
        self.logger_error \
            .assert_called_once_with("get_treatments: features cannot be None.")

    def test_get_treatments_with_bool_type_of_features(self):
        self.assertEqual(None, self.client.get_treatments("some_key", True))
        self.logger_error \
            .assert_called_once_with("get_treatments: features must be a list.")

    def test_get_treatments_with_string_type_of_features(self):
        self.assertEqual(None, self.client.get_treatments("some_key", "some_string"))
        self.logger_error \
            .assert_called_once_with("get_treatments: features must be a list.")

    def test_get_treatments_with_empty_features(self):
        self.assertEqual({}, self.client.get_treatments("some_key", []))
        self.logger_warning \
            .assert_called_once_with("get_treatments: features is an empty "
                                     "list or has None values.")

    def test_get_treatments_with_none_features(self):
        self.assertEqual({}, self.client.get_treatments("some_key", [None, None]))
        self.logger_warning \
            .assert_called_once_with("get_treatments: features is an empty "
                                     "list or has None values.")
