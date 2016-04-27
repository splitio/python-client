"""Unit tests for the matchers module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase

from splitio.client import Client, SelfRefreshingClient, randomize_interval
from splitio.treatments import CONTROL
from splitio.test.utils import MockUtilsMixin


class ClientTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_key = mock.MagicMock()
        self.some_feature = mock.MagicMock()
        self.some_attributes = mock.MagicMock()

        self.some_conditions = [
            mock.MagicMock(),
            mock.MagicMock(),
            mock.MagicMock()
        ]

        self.some_split = mock.MagicMock()
        self.some_split.killed = False
        self.some_split.conditions.__iter__.return_value = self.some_conditions
        self.client = Client()
        self.get_split_fetcher_mock = self.patch_object(self.client, 'get_split_fetcher')
        self.splitter_mock = self.patch('splitio.client.Splitter')

    def test_get_splitter_returns_a_splitter(self):
        """Test that get_splitter returns a splitter"""
        self.assertEqual(self.splitter_mock.return_value, self.client.get_splitter())

    def test_get_treatment_returns_control_if_key_is_none(self):
        """Test that get_treatment returns CONTROL treatment if key is None"""
        self.assertEqual(CONTROL, self.client.get_treatment(None, self.some_feature,
                                                            self.some_attributes))

    def test_get_treatment_returns_control_if_feature_is_none(self):
        """Test that get_treatment returns CONTROL treatment if feature is None"""
        self.assertEqual(CONTROL, self.client.get_treatment(self.some_key, None,
                                                            self.some_attributes))

    def test_get_treatment_calls_get_split_fetcher(self):
        """Test that get_treatment calls get_split_fetcher"""
        self.client.get_treatment(self.some_key, self.some_feature, self.some_attributes)
        self.get_split_fetcher_mock.assert_called_once_with()

    def test_get_treatment_calls_split_fetcher_fetch(self):
        """Test that get_treatment calls split fetcher fetch"""
        self.client.get_treatment(self.some_key, self.some_feature, self.some_attributes)
        self.get_split_fetcher_mock.return_value.fetch.assert_called_once_with(self.some_feature)

    def test_get_treatment_calls_get_treatment_for_split(self):
        """Test that get_treatment calls get_treatment_for_split"""
        get_treatment_for_split_mock = self.patch_object(self.client, '_get_treatment_for_split')

        self.client.get_treatment(self.some_key, self.some_feature, self.some_attributes)
        get_treatment_for_split_mock.assert_called_once_with(
            self.get_split_fetcher_mock.return_value.fetch.return_value, self.some_key,
            self.some_attributes)

    def test_get_treatment_returns_get_treatment_for_split_result(self):
        """Test that get_treatment returns get_treatment_for_split result"""
        get_treatment_for_split_mock = self.patch_object(self.client, '_get_treatment_for_split')

        self.assertEqual(get_treatment_for_split_mock.return_value,
                         self.client.get_treatment(self.some_key, self.some_feature,
                                                   self.some_attributes))

    def test_get_treatment_returns_control_if_get_split_fetcher_raises_exception(self):
        """
        Test that get_treatment returns CONTROL treatment if get_split_fetcher raises an exception
        """
        self.get_split_fetcher_mock.side_effect = Exception()
        self.assertEqual(CONTROL, self.client.get_treatment(self.some_key, self.some_feature,
                                                            self.some_attributes))

    def test_get_treatment_returns_control_if_fetch_raises_exception(self):
        """
        Test that get_treatment returns CONTROL treatment if fetch raises an exception
        """
        self.get_split_fetcher_mock.return_value.fetch.side_effect = Exception()
        self.assertEqual(CONTROL, self.client.get_treatment(self.some_key, self.some_feature,
                                                            self.some_attributes))

    def test_get_treatment_returns_control_if_get_treatment_for_split_raises_exception(self):
        """
        Test that get_treatment returns CONTROL treatment _get_treatment_for_split raises an
        exception
        """
        self.patch_object(self.client, '_get_treatment_for_split', side_effect=Exception())
        self.assertEqual(CONTROL, self.client.get_treatment(self.some_key, self.some_feature,
                                                            self.some_attributes))

    def test_get_treatment_for_split_returns_control_if_split_is_none(self):
        """Test that _get_treatment_for_split returns CONTROL if split is None"""
        self.assertEqual(CONTROL, self.client._get_treatment_for_split(None, self.some_key,
                                                                       self.some_feature))

    def test_get_treatment_for_split_returns_default_treatment_if_feature_is_killed(self):
        """Test that _get_treatment_for_split returns CONTROL if split is None"""
        self.some_split.killed = True
        self.assertEqual(self.some_split.default_treatment,
                         self.client._get_treatment_for_split(self.some_split, self.some_key,
                                                              self.some_feature))

    def test_get_treatment_returns_default_treatment_if_no_conditions_match(self):
        """Test that _get_treatment_for_split returns CONTROL if no split conditions_match"""
        self.some_conditions[0].matcher.match.return_value = False
        self.some_conditions[1].matcher.match.return_value = False
        self.some_conditions[2].matcher.match.return_value = False
        self.assertEqual(self.some_split.default_treatment,
                         self.client._get_treatment_for_split(self.some_split, self.some_key,
                                                              self.some_feature))

    def test_get_treatment_calls_condition_matcher_match_with_short_circuit(self):
        """
        Test that _get_treatment_for_split calls the conditions matcher match method until a match
        is found
        """
        self.some_conditions[0].matcher.match.return_value = False
        self.some_conditions[1].matcher.match.return_value = True
        self.some_conditions[2].matcher.match.return_value = False
        self.client._get_treatment_for_split(self.some_split, self.some_key, self.some_attributes)
        self.some_conditions[0].matcher.match.assert_called_once_with(
            self.some_key, attributes=self.some_attributes)
        self.some_conditions[1].matcher.match.assert_called_once_with(
            self.some_key, attributes=self.some_attributes)
        self.some_conditions[2].matcher.match.assert_not_called()

    def test_get_treatment_calls_get_splitter_if_a_condition_match(self):
        """
        Test that _get_treatment_for_split calls get_treatment on splitter if a condition match
        """
        self.some_conditions[0].matcher.match.return_value = False
        self.some_conditions[1].matcher.match.return_value = True
        self.client._get_treatment_for_split(self.some_split, self.some_key, self.some_attributes)
        self.splitter_mock.return_value.get_treatment.assert_called_once_with(
            self.some_key, self.some_split.seed, self.some_conditions[1].partitions)
