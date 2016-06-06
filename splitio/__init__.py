from __future__ import absolute_import, division, print_function, unicode_literals

__all__ = ('api', 'cache', 'clients', 'matchers', 'segments', 'settings', 'splits', 'splitters',
           'transformers', 'treatments')

from .clients import SelfRefreshingClient, LocalhostEnvironmentClient


def get_client(api_key, **kwargs):
    """
    Builds a Split Client that refreshes itself at regular intervals. The config parameter is a
    dictionary that allows you to control the behaviour of the client. The following configuration
    values are supported:

    * connectionTimeout: The TCP connection timeout (Default: 1500ms)
    * readTimeout: The HTTP read timeout (Default: 1500ms)
    * featuresRefreshRate: The refresh rate for features (Default: 30s)
    * segmentsRefreshRate: The refresh rate for segments (Default: 60s)
    * metricsRefreshRate: The refresh rate for metrics (Default: 60s)
    * impressionsRefreshRate: The refresh rate for impressions (Default: 60s)
    * randomizeIntervals: Whether to randomize the refres intervals (Default: False)
    * ready: How long to wait (in seconds) for the client to be initialized. 0 to return
      immediately without waiting. (Default: 0s)

    If the api_key argument is 'localhost' a localhost environment client is built based on the
    contents of a .split file in the user's home directory. The definition file has the following
    syntax:

        file: (comment | split_line)+
        comment : '#' string*\n
        split_line : feature_name ' ' treatment\n
        feature_name : string
        treatment : string

    It is possible to change the location of the split file by using the split_definition_file_name
    argument.

    :param api_key: The API key provided by Split.io
    :type api_key: str
    :param config: The configuration dictionary
    :type config: dict
    :param sdk_api_base_url: An override for the default API base URL.
    :type sdk_api_base_url: str
    :param events_api_base_url: An override for the default events base URL.
    :type events_api_base_url: str
    :param split_definition_file_name: Name of the definition file (Optional)
    :type split_definition_file_name: str
    """
    if api_key == 'localhost':
        return LocalhostEnvironmentClient(**kwargs)

    return SelfRefreshingClient(api_key, **kwargs)
