"""Split API module."""

class APIException(Exception):
    """Exception to raise when an API call fails."""

    def __init__(self, custom_message, status_code=None):
        """Constructor."""
        Exception.__init__(self, custom_message)
        self._status_code = status_code if status_code else -1

    @property
    def status_code(self):
        """Return HTTP status code."""
        return self._status_code


def headers_from_metadata(sdk_metadata):
    """
    Generate a dict with headers required by data-recording API endpoints.

    :param sdk_metadata: SDK Metadata object, generated at sdk initialization time.
    :type sdk_metadata: splitio.client.util.SdkMetadata

    :return: A dictionary with headers.
    :rtype: dict
    """
    return {
        'SplitSDKVersion': sdk_metadata.sdk_version,
        'SplitSDKMachineIP': sdk_metadata.instance_ip,
        'SplitSDKMachineName': sdk_metadata.instance_name
    }
