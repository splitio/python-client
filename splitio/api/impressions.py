"""Impressions API module."""

import logging
from itertools import groupby
from splitio.api import APIException
from splitio.api.client import HttpClientException


class ImpressionsAPI(object): # pylint: disable=too-few-public-methods
    """Class that uses an httpClient to communicate with the impressions API."""

    def __init__(self, client, apikey, sdk_metadata):
        """
        Class constructor.

        :param client: HTTP Client responsble for issuing calls to the backend.
        :type client: HttpClient
        :param apikey: User apikey token.
        :type apikey: string
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._client = client
        self._apikey = apikey
        self._metadata = {
            'SplitSDKVersion': sdk_metadata.sdk_version,
            'SplitSDKMachineIP': sdk_metadata.instance_ip,
            'SplitSDKMachineName': sdk_metadata.instance_name
        }

    @staticmethod
    def _build_bulk(impressions):
        """
        Build an impression bulk formatted as the API expects it.

        :param impressions: List of impressions to bundle.
        :type impressions: list(splitio.models.impressions.Impression)

        :return: Dictionary of lists of impressions.
        :rtype: dict
        """
        return [
            {
                'testName': group[0],
                'keyImpressions': [
                    {
                        'keyName': impression.matching_key,
                        'treatment': impression.treatment,
                        'time': impression.time,
                        'changeNumber': impression.change_number,
                        'label': impression.label,
                        'bucketingKey': impression.bucketing_key
                    }
                    for impression in group[1]
                ]
            }
            for group in groupby(
                sorted(impressions, key=lambda i: i.feature_name),
                lambda i: i.feature_name
            )
        ]

    def flush_impressions(self, impressions):
        """
        Send impressions to the backend.

        :param impressions: Impressions bulk
        :type impressions: list
        """
        bulk = self._build_bulk(impressions)
        try:
            response = self._client.post(
                'events',
                '/testImpressions/bulk',
                self._apikey,
                body=bulk,
                extra_headers=self._metadata
            )
            if not 200 <= response.status_code < 300:
                raise APIException(response.body, response.status_code)
        except HttpClientException as exc:
            self._logger.debug('Error flushing events: ', exc_info=True)
            raise APIException(exc.custom_message, original_exception=exc.original_exception)
