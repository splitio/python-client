"""Provides access to the Split.io SDK API"""
from __future__ import absolute_import, division, print_function, unicode_literals

import requests


from splitio.settings import (SDK_API_BASE_URL, SEGMENT_CHANGES_URL_TEMPLATE,
                              SPLIT_CHANGES_URL_TEMPLATE, SDK_VERSION)


class SdkApi(object):
    def __init__(self, api_key, sdk_api_base_url=None, split_sdk_machine_name=None,
                 split_sdk_machine_ip=None, connect_timeout=1500, read_timeout=1000):
        """Provides access to the Split.io SDK RESTful API

        :param api_key: The API key generated on the admin interface
        :type api_key: str
        :param sdk_api_base_url: An optional string used to override the default API base url.
                                 Useful for testing or to change the target environment.
        :type sdk_api_base_url: str
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
        self._api_key = api_key
        self._sdk_api_url_base = sdk_api_base_url if sdk_api_base_url is not None \
            else SDK_API_BASE_URL
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

    def _get(self, url, params):
        headers = self._build_headers()

        response = requests.get(url, params=params, headers=headers, timeout=self._timeout)
        response.raise_for_status()

        return response.json()

    def split_changes(self, since):
        """Makes a request to the splitChanges endpoint.
        :param since: An integer that indicates when was the endpoint last called. It is usually
                      either -1, which returns the value of the split, or the value of the field
                      "till" of the response of a previous call, which will only return the changes
                      since that call.
        :type since: int
        :return:
        :rtype: dict
        """
        url = SPLIT_CHANGES_URL_TEMPLATE.format(base_url=self._sdk_api_url_base)
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
        :return:
        :rtype: dict
        """
        url = SEGMENT_CHANGES_URL_TEMPLATE.format(base_url=self._sdk_api_url_base,
                                                  segment_name=segment_name)
        params = {
            'since': since
        }

        self._get(url, params)
