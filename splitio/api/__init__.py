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

class APIUriException(APIException):
    """Exception to raise when an API call fails due to 414  http error."""

    def __init__(self, custom_message, status_code=None):
        """Constructor."""
        APIException.__init__(self, custom_message, status_code)

def headers_from_metadata(sdk_metadata, client_key=None):
    """
    Generate a dict with headers required by data-recording API endpoints.
    :param sdk_metadata: SDK Metadata object, generated at sdk initialization time.
    :type sdk_metadata: splitio.client.util.SdkMetadata
    :param client_key: client key.
    :type client_key: str
    :return: A dictionary with headers.
    :rtype: dict
    """

    metadata = {
        'SplitSDKVersion': sdk_metadata.sdk_version,
        'SplitSDKMachineIP': sdk_metadata.instance_ip,
        'SplitSDKMachineName': sdk_metadata.instance_name
    } if sdk_metadata.instance_ip != 'NA' and sdk_metadata.instance_ip != 'unknown' else {
        'SplitSDKVersion': sdk_metadata.sdk_version,
    }

    if client_key is not None:
        metadata['SplitSDKClientKey'] = client_key

    return metadata