import json

from splitio.engine.sender_adapters import ImpressionsSenderAdapter

class InMemorySenderAdapter(ImpressionsSenderAdapter):
    """In Memory Impressions Sender Adapter class."""

    def __init__(self, telemtry_http_client):
        """
        Initialize In memory sender adapter instance

        :param telemtry_http_client: instance of telemetry http api
        :type telemtry_http_client: splitio.api.telemetry.TelemetryAPI
        """
        self._telemtry_http_client = telemtry_http_client

    def record_unique_keys(self, uniques):
        """
        post the unique keys to split back end.

        :param uniques: unique keys disctionary
        :type uniques: Dictionary {'feature1': set(), 'feature2': set(), .. }
        """
        self._telemtry_http_client.record_unique_keys(self._uniques_formatter(uniques))

    def _uniques_formatter(self, uniques):
        """
        Format the unique keys dictionary to a JSON body

        :param uniques: unique keys disctionary
        :type uniques: Dictionary {'feature1': set(), 'feature2': set(), .. }

        :return: unique keys JSON
        :rtype: json
        """
        if len(uniques) == 0:
            return json.loads('{"keys": []}')

        return {
            'keys':  [{'f': feature, 'ks': list(keys)} for feature, keys in uniques.items()]
        }