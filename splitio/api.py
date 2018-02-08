"""Provides access to the Split.io SDK API"""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging
import requests
import json

from splitio.config import SDK_API_BASE_URL, EVENTS_API_BASE_URL, SDK_VERSION

_SEGMENT_CHANGES_URL_TEMPLATE = '{base_url}/segmentChanges/{segment_name}/'
_SPLIT_CHANGES_URL_TEMPLATE = '{base_url}/splitChanges/'
_TEST_IMPRESSIONS_URL_TEMPLATE = '{base_url}/testImpressions/bulk/'
_METRICS_URL_TEMPLATE = '{base_url}/metrics/{endpoint}/'
_EVENTS_URL_TEMPLATE = '{base_url}/events/bulk/'


class SdkApi(object):
    """
    SDK API Class contains all methods required to interact with
    the split backend.
    """
    def __init__(self, api_key, sdk_api_base_url=None, events_api_base_url=None,
                 split_sdk_machine_name=None, split_sdk_machine_ip=None, connect_timeout=1500,
                 read_timeout=1000):
        """Provides access to the Split.io SDK RESTful API

        :param api_key: The API key generated on the admin interface
        :type api_key: str
        :param sdk_api_base_url: An optional string used to override the default API base url.
                                 Useful for testing or to change the target environment.
        :type sdk_api_base_url: str
        :param events_api_base_url: An optional string used to override the default events API base
                                    url. Useful for testing or to change the target environment.
        :type events_api_base_url: str
        :param split_sdk_machine_name: An optional value for the SplitSDKMachineName header. It can
                                       be a function instead of a string if it has to be evaluated
                                       at request time
        :type split_sdk_machine_name: str
        :param split_sdk_machine_ip: An optional value for the SplitSDKMachineIP header. It can be
                                     a function instead of a string if it has to be evaluated at
                                     request time
        :type split_sdk_machine_ip: str
        :param connect_timeout: The TCP connection timeout. Default: 1500 (seconds)
        :type connect_timeout: float
        :param read_timeout: The HTTP read timeout. Default: 1000 (seconds)
        :type read_timeout: float
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._api_key = api_key
        self._sdk_api_url_base = sdk_api_base_url if sdk_api_base_url is not None \
            else SDK_API_BASE_URL
        self._events_api_url_base = events_api_base_url if events_api_base_url is not None \
            else EVENTS_API_BASE_URL
        self._split_sdk_machine_name = split_sdk_machine_name
        self._split_sdk_machine_ip = split_sdk_machine_ip
        self._timeout = (connect_timeout, read_timeout)

    def _build_headers(self):
        """Builds a dictionary with the standard headers used in all calls the Split.IO SDK.

        The mandatory headers are:

        * Authorization: [String] the api token bearer for the customer using the sdk.
                         Example: "Authorization: Bearer <API_KEY>".
        * SplitSDKVersion: [String] of the shape <LANGUAGE>-<VERSION>. For example, for python sdk
                           is it "python-0.0.1".
        * Accept-Encoding: gzip

        Optionally, the following headers can be included

        * SplitSDKMachineName: [String] name of the machine.
        * SplitSDKMachineIP: [String] IP address of the machine

        :return: A dictionary with the headers used in every call to the backend using the values
        set during initialization or the defaults set by the settings module.
        :rtype: dict
        """
        headers = {
            'Authorization': 'Bearer {api_key}'.format(api_key=self._api_key),
            'SplitSDKVersion': SDK_VERSION,
            'Accept-Encoding': 'gzip'
        }

        if self._split_sdk_machine_name is not None:
            headers['SplitSDKMachineName'] = self._split_sdk_machine_name() \
                if callable(self._split_sdk_machine_name) else self._split_sdk_machine_name

        if self._split_sdk_machine_ip is not None:
            headers['SplitSDKMachineIP'] = self._split_sdk_machine_ip() \
                if callable(self._split_sdk_machine_ip) else self._split_sdk_machine_ip

        return headers

    def _logHttpError(self, response):
        if response.status_code < 200 or response.status_code >= 400:
            respJson = response.json()
            if 'message' in respJson:
                self._logger.error(
                    "HTTP Error (status: %s) connecting with split servers: %s"
                    % (response.status_code, respJson['message'])
                )
            else:
                self._logger.error("HTTP Error connecting with split servers")

    def _get(self, url, params):
        headers = self._build_headers()

        response = requests.get(url, params=params, headers=headers, timeout=self._timeout)
        self._logHttpError(response)

        return response.json()

    def _post(self, url, data):
        headers = self._build_headers()

        response = requests.post(url, json=data, headers=headers, timeout=self._timeout)
        self._logHttpError(response)

        return response.status_code

    def split_changes(self, since):
        """Makes a request to the splitChanges endpoint.
        :param since: An integer that indicates when was the endpoint last called. It is usually
                      either -1, which returns the value of the split, or the value of the field
                      "till" of the response of a previous call, which will only return the changes
                      since that call.
        :type since: int
        :return: Changes seen on splits
        :rtype: dict
        """
        url = _SPLIT_CHANGES_URL_TEMPLATE.format(base_url=self._sdk_api_url_base)
        params = {
            'since': since
        }

        return self._get(url, params)

    def segment_changes(self, segment_name, since):
        """Makes a request to the segmentChanges endpoint.
        :param segment_name: Name of the segment
        :type since: str
        :param since: An integer that indicates when was the endpoint last called. It is usually
                      either -1, which returns the value of the split, or the value of the field
                      "till" of the response of a previous call, which will only return the changes
                      since that call.
        :type since: int
        :return: Changes seen on segments
        :rtype: dict
        """
        url = _SEGMENT_CHANGES_URL_TEMPLATE.format(base_url=self._sdk_api_url_base,
                                                   segment_name=segment_name)
        params = {
            'since': since
        }

        return self._get(url, params)

    def test_impressions(self, test_impressions_data):
        """Makes a request to the testImpressions endpoint. The method takes a dictionary with the
        test (feature) name and a list of impressions:

        [
            {
               "testName": str,  # name of the test (feature),
               "impressions": [
                    {
                        "keyName" : str,  # name of the key that saw this feature
                        "treatment" : str,  # the treatment e.g. "on" or "off"
                        "time" : int  # the timestamp (in ms) when this happened.
                    },
                    ...
               ]
            }
        ]

        :param test_impressions_data: Data of the impressions of a test (feature)
        :type test_impressions_data: list
        """
        url = _TEST_IMPRESSIONS_URL_TEMPLATE.format(base_url=self._events_api_url_base)
        return self._post(url, test_impressions_data)

    def metrics_times(self, times_data):
        """Makes a request to the times metrics endpoint. The method takes a list of dictionaries
        with the latencies seen for each metric:

        [
            {
                "name": str,  # name of the metric
                "latencies": [int, int, int, ...]  # latencies seen
            },
            {
                "name": str,  # name of the metric
                "latencies": [int, int, int, ...]  # latencies seen
            },
            ...
        ]
        :param times_data: Data for the metrics times
        :type times_data: list
        """
        url = _METRICS_URL_TEMPLATE.format(base_url=self._events_api_url_base, endpoint='times')
        return self._post(url, times_data)

    def metrics_counters(self, counters_data):
        """Makes a request to the counters metrics endpoint. The method takes a list of dictionaries
        with the deltas for the counts for each metric:

        [
            {
                "name": str,  # name of the metric
                "delta": int  # count delta
            },
            {
                "name": str,  # name of the metric
                "delta": int  # count delta
            },
            ...
        ]
        :param counters_data: Data for the metrics counters
        :type counters_data: list
        """
        url = _METRICS_URL_TEMPLATE.format(base_url=self._events_api_url_base, endpoint='counters')
        return self._post(url, counters_data)

    def metrics_gauge(self, gauge_data):
        """Makes a request to the gauge metrics endpoint. The method takes a list of dictionaries
        with the values for the gauge of each metric:

        [
            {
                "name": str,  # name of the metric
                "value": float  # gauge value
            },
            {
                "name": str,  # name of the metric
                "value": float  # gauge value
            },
            ...
        ]
        :param gauge_data: Data for the metrics gauge
        :type gauge_data: list
        """
        url = _METRICS_URL_TEMPLATE.format(base_url=self._events_api_url_base, endpoint='gauge')
        return self._post(url, gauge_data)

    def track_events(self, events):
        url = _EVENTS_URL_TEMPLATE.format(base_url=self._events_api_url_base)
        return self._post(url, events)


def api_factory(config):
    """Build a split.io SDK API client using a config dictionary.
    :param config: A config dictionary
    :type config: dict
    :return: SdkApi client
    :rtype: SdkApi
    """
    return SdkApi(config.get('apiKey'),
                  sdk_api_base_url=config['sdkApiBaseUrl'],
                  events_api_base_url=config['eventsApiBaseUrl'],
                  split_sdk_machine_name=config['splitSdkMachineName'],
                  split_sdk_machine_ip=config['splitSdkMachineIp'],
                  connect_timeout=config['connectionTimeout'],
                  read_timeout=config['readTimeout'])
