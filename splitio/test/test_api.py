"""Unit tests for the api module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from requests.exceptions import RequestException, HTTPError
from unittest import TestCase

from splitio.api import (SdkApi, _SEGMENT_CHANGES_URL_TEMPLATE, _SPLIT_CHANGES_URL_TEMPLATE,
                         _TEST_IMPRESSIONS_URL_TEMPLATE, _METRICS_URL_TEMPLATE)
from splitio.settings import SDK_VERSION, SDK_API_BASE_URL
from splitio.test.utils import MockUtilsMixin


class SdkApiBuildHeadersTests(TestCase):
    def setUp(self):
        super(SdkApiBuildHeadersTests, self).setUp()

        self.some_api_key = 'some_api_key'

        self.api = SdkApi(self.some_api_key)

    def test_always_returns_mandatory_headers(self):
        """Tests that the mandatory headers are always included and have the proper values"""
        headers = self.api._build_headers()

        self.assertEqual('Bearer some_api_key', headers.get('Authorization'))
        self.assertEqual(SDK_VERSION, headers.get('SplitSDKVersion'))
        self.assertEqual('gzip', headers.get('Accept-Encoding'))

    def test_optional_headers_not_included_if_not_set(self):
        """Tests that the optional headers are not included if they haven't been set"""
        headers = self.api._build_headers()

        self.assertNotIn('SplitSDKMachineName', headers)
        self.assertNotIn('SplitSDKMachineIP', headers)

    def test_split_sdk_machine_name_included_if_set_as_literal(self):
        """Tests that the optional header SplitSDKMachineName is included if set as a literal"""
        some_split_sdk_machine_name = mock.NonCallableMagicMock()
        self.api._split_sdk_machine_name = some_split_sdk_machine_name

        headers = self.api._build_headers()

        self.assertNotIn(some_split_sdk_machine_name, headers.get('SplitSDKMachineName'))

    def test_split_sdk_machine_name_included_if_set_as_callable(self):
        """Tests that the optional header SplitSDKMachineName is included if set as a callable and
        its value is the result of calling the function"""
        some_split_sdk_machine_name = mock.MagicMock()
        self.api._split_sdk_machine_name = some_split_sdk_machine_name

        headers = self.api._build_headers()

        self.assertNotIn(some_split_sdk_machine_name.return_value,
                         headers.get('SplitSDKMachineName'))

    def test_split_sdk_machine_ip_included_if_set_as_literal(self):
        """Tests that the optional header SplitSDKMachineIP is included if set as a literal"""
        some_split_sdk_machine_ip = mock.NonCallableMagicMock()
        self.api._split_sdk_machine_ip = some_split_sdk_machine_ip

        headers = self.api._build_headers()

        self.assertNotIn(some_split_sdk_machine_ip, headers.get('SplitSDKMachineIP'))

    def test_split_sdk_machine_ip_included_if_set_as_callable(self):
        """Tests that the optional header SplitSDKMachineIP is included if set as a callable and
        its value is the result of calling the function"""
        some_split_sdk_machine_ip = mock.MagicMock()
        self.api._split_sdk_machine_ip = some_split_sdk_machine_ip

        headers = self.api._build_headers()

        self.assertNotIn(some_split_sdk_machine_ip.return_value,
                         headers.get('SplitSDKMachineIP'))


class SdkApiGetTests(TestCase, MockUtilsMixin):
    def setUp(self):
        super(SdkApiGetTests, self).setUp()

        self.requests_get_mock = self.patch('splitio.api.requests').get

        self.some_api_key = mock.MagicMock()
        self.some_url = mock.MagicMock()
        self.some_params = mock.MagicMock()

        self.api = SdkApi(self.some_api_key)

        self.build_headers_mock = self.patch_object(self.api, '_build_headers')

    def test_proper_headers_are_used(self):
        """Tests that the request is made with the proper headers"""
        self.api._get(self.some_url, self.some_params)

        self.requests_get_mock.assert_called_once_with(mock.ANY, params=mock.ANY,
                                                       headers=self.build_headers_mock.return_value,
                                                       timeout=mock.ANY)

    def test_url_parameter_is_used(self):
        """Tests that the request is made with the supplied url"""
        self.api._get(self.some_url, self.some_params)

        self.requests_get_mock.assert_called_once_with(self.some_url, params=mock.ANY,
                                                       headers=mock.ANY, timeout=mock.ANY)

    def test_params_parameter_is_used(self):
        """Tests that the request is made with the supplied parameters"""
        self.api._get(self.some_url, self.some_params)

        self.requests_get_mock.assert_called_once_with(mock.ANY, params=self.some_params,
                                                       headers=mock.ANY, timeout=mock.ANY)

    def test_proper_timeout_is_used(self):
        """Tests that the request is made with the proper value for timeout"""
        some_timeout = mock.MagicMock()
        self.api._timeout = some_timeout

        self.api._get(self.some_url, self.some_params)

        self.requests_get_mock.assert_called_once_with(mock.ANY, params=mock.ANY, headers=mock.ANY,
                                                       timeout=some_timeout)

    def test_json_is_returned(self):
        """Tests that the function returns the result of calling json() on the requests response"""
        result = self.api._get(self.some_url, self.some_params)

        self.assertEqual(self.requests_get_mock.return_value.json.return_value, result)

    def test_request_exceptions_are_raised(self):
        """Tests that if requests raises an exception, it is not handled within the call"""
        self.requests_get_mock.side_effect = RequestException()

        with self.assertRaises(RequestException):
            self.api._get(self.some_url, self.some_params)

    def test_request_status_exceptions_are_raised(self):
        """Tests that if requests succeeds but its status is not 200 (Ok) an exception is raised
        and , it is not handled within the call"""
        self.requests_get_mock.return_value.raise_for_status.side_effect = HTTPError()

        with self.assertRaises(HTTPError):
            self.api._get(self.some_url, self.some_params)

    def test_json_exceptions_are_raised(self):
        """Tests that if requests succeeds but its payload is not JSON, an exception is raised and
        it isn't handled within the call"""
        self.requests_get_mock.return_value.json.side_effect = ValueError()

        with self.assertRaises(ValueError):
            self.api._get(self.some_url, self.some_params)


class SdkApiPostTests(TestCase, MockUtilsMixin):
    def setUp(self):
        super(SdkApiPostTests, self).setUp()

        self.requests_post_mock = self.patch('splitio.api.requests').post

        self.some_api_key = mock.MagicMock()
        self.some_url = mock.MagicMock()
        self.some_data = mock.MagicMock()

        self.api = SdkApi(self.some_api_key)

        self.build_headers_mock = self.patch_object(self.api, '_build_headers')

    def test_proper_headers_are_used(self):
        """Tests that the request is made with the proper headers"""
        self.api._post(self.some_url, self.some_data)

        self.requests_post_mock.assert_called_once_with(mock.ANY, json=mock.ANY,
                                                        headers=self.build_headers_mock.return_value,
                                                        timeout=mock.ANY)

    def test_url_parameter_is_used(self):
        """Tests that the request is made with the supplied url"""
        self.api._post(self.some_url, self.some_data)

        self.requests_post_mock.assert_called_once_with(self.some_url, json=mock.ANY,
                                                        headers=mock.ANY, timeout=mock.ANY)

    def test_data_parameter_is_used(self):
        """Tests that the request is made with the supplied data as json parameter"""
        self.api._post(self.some_url, self.some_data)

        self.requests_post_mock.assert_called_once_with(mock.ANY, json=self.some_data,
                                                        headers=mock.ANY, timeout=mock.ANY)

    def test_proper_timeout_is_used(self):
        """Tests that the request is made with the proper value for timeout"""
        some_timeout = mock.MagicMock()
        self.api._timeout = some_timeout

        self.api._post(self.some_url, self.some_data)

        self.requests_post_mock.assert_called_once_with(mock.ANY, json=mock.ANY, headers=mock.ANY,
                                                        timeout=some_timeout)

    def test_status_is_returned(self):
        """Tests that the function returns the the status code of the response"""
        result = self.api._post(self.some_url, self.some_data)

        self.assertEqual(self.requests_post_mock.return_value.status_code, result)

    def test_request_exceptions_are_raised(self):
        """Tests that if requests raises an exception, it is not handled within the call"""
        self.requests_post_mock.side_effect = RequestException()

        with self.assertRaises(RequestException):
            self.api._post(self.some_url, self.some_data)

    def test_request_status_exceptions_are_raised(self):
        """Tests that if requests succeeds but its status is not 200 (Ok) an exception is raised
        and , it is not handled within the call"""
        self.requests_post_mock.return_value.raise_for_status.side_effect = HTTPError()

        with self.assertRaises(HTTPError):
            self.api._post(self.some_url, self.some_data)


class SdkApiSplitChangesTest(TestCase, MockUtilsMixin):
    def setUp(self):
        super(SdkApiSplitChangesTest, self).setUp()

        self.some_api_key = mock.MagicMock()
        self.some_since = mock.MagicMock()

        self.api = SdkApi(self.some_api_key)

        self.get_mock = self.patch_object(self.api, '_get')

    def test_default_split_changes_url_is_used(self):
        """Tests that the default split changes endpoint url is used if sdk_api_base_url hasn't
        been set"""
        self.api.split_changes(self.some_since)

        expected_url = _SPLIT_CHANGES_URL_TEMPLATE.format(
            base_url=SDK_API_BASE_URL
        )

        self.get_mock.assert_called_once_with(expected_url, mock.ANY)

    def test_rebased_split_changes_url_is_used(self):
        """Tests that if sdk_api_base_url has been set, it is used as the base for the url of the
        request"""

        some_sdk_api_url_base = 'some_sdk_api_url_base'
        self.api._sdk_api_url_base = some_sdk_api_url_base
        self.api.split_changes(self.some_since)

        expected_url = _SPLIT_CHANGES_URL_TEMPLATE.format(
            base_url=some_sdk_api_url_base
        )

        self.get_mock.assert_called_once_with(expected_url, mock.ANY)

    def test_proper_params_are_used(self):
        """Tests that the request to the split changes endpoint is made with the proper
        parameters"""
        self.api.split_changes(self.some_since)

        self.get_mock.assert_called_once_with(mock.ANY, {'since': self.some_since})

    def test_exceptions_from_get_are_raised(self):
        """Tests that any exceptions raised from calling _get are not handled with the call"""
        self.get_mock.side_effect = Exception()

        with self.assertRaises(Exception):
            self.api.split_changes(self.some_since)

    def test_returns_get_result(self):
        """Tests that the method returns the result of calling get"""
        self.assertEqual(self.get_mock.return_value, self.api.split_changes(self.some_since))


class SdkApiSegmentChangesTests(TestCase, MockUtilsMixin):
    def setUp(self):
        super(SdkApiSegmentChangesTests, self).setUp()

        self.some_api_key = mock.MagicMock()
        self.some_name = 'some_name'
        self.some_since = mock.MagicMock()

        self.api = SdkApi(self.some_api_key)

        self.get_mock = self.patch_object(self.api, '_get')

    def test_default_segment_changes_url_is_used(self):
        """Tests that the default segment changes endpoint url is used if sdk_api_base_url hasn't
        been set"""
        self.api.segment_changes(self.some_name, self.some_since)

        expected_url = _SEGMENT_CHANGES_URL_TEMPLATE.format(
            base_url=SDK_API_BASE_URL,
            segment_name=self.some_name
        )

        self.get_mock.assert_called_once_with(expected_url, mock.ANY)

    def test_rebased_segment_changes_url_is_used(self):
        """Tests that if sdk_api_base_url has been set, it is used as the base for the url of the
        request"""

        some_sdk_api_url_base = 'some_sdk_api_url_base'
        self.api._sdk_api_url_base = some_sdk_api_url_base
        self.api.segment_changes(self.some_name, self.some_since)

        expected_url = _SEGMENT_CHANGES_URL_TEMPLATE.format(
            base_url=some_sdk_api_url_base,
            segment_name=self.some_name
        )

        self.get_mock.assert_called_once_with(expected_url, mock.ANY)

    def test_proper_params_are_used(self):
        """Tests that the request to the segment changes endpoint is made with the proper
        parameters"""
        self.api.segment_changes(self.some_name, self.some_since)

        self.get_mock.assert_called_once_with(mock.ANY, {'since': self.some_since})

    def test_exceptions_from_get_are_raised(self):
        """Tests that any exceptions raised from calling _get are not handled with the call"""
        self.get_mock.side_effect = Exception()

        with self.assertRaises(Exception):
            self.api.segment_changes(self.some_name, self.some_since)

    def test_returns_get_result(self):
        """Tests that the method returns the result of calling get"""
        self.assertEqual(self.get_mock.return_value, self.api.segment_changes(self.some_name,
                                                                              self.some_since))
