"""Default settings for the Split.IO SDK Python client."""
import os.path
import logging

from splitio.engine.impressions import ImpressionsMode
from splitio.client.input_validator import validate_flag_sets

_LOGGER = logging.getLogger(__name__)
DEFAULT_DATA_SAMPLING = 1

DEFAULT_CONFIG = {
    'operationMode': 'standalone',
    'connectionTimeout': 1500,
    'streamingEnabled': True,
    'featuresRefreshRate': 30,
    'segmentsRefreshRate': 30,
    'metricsRefreshRate': 3600,
    'impressionsRefreshRate': 5 * 60,
    'impressionsBulkSize': 5000,
    'impressionsQueueSize': 10000,
    'eventsPushRate': 10,
    'eventsBulkSize': 5000,
    'eventsQueueSize': 10000,
    'labelsEnabled': True,
    'IPAddressesEnabled': True,
    'impressionsMode': 'OPTIMIZED',
    'impressionListener': None,
    'redisLocalCacheEnabled': True,
    'redisLocalCacheTTL': 5,
    'redisHost': 'localhost',
    'redisPort': 6379,
    'redisDb': 0,
    'redisUsername': None,
    'redisPassword': None,
    'redisSocketTimeout': None,
    'redisSocketConnectTimeout': None,
    'redisSocketKeepalive': None,
    'redisSocketKeepaliveOptions': None,
    'redisConnectionPool': None,
    'redisUnixSocketPath': None,
    'redisEncoding': 'utf-8',
    'redisEncodingErrors': 'strict',
    'redisErrors': None,
    'redisDecodeResponses': True,
    'redisRetryOnTimeout': False,
    'redisSsl': False,
    'redisSslKeyfile': None,
    'redisSslCertfile': None,
    'redisSslCertReqs': None,
    'redisSslCaCerts': None,
    'redisMaxConnections': None,
    'machineName': None,
    'machineIp': None,
    'splitFile': os.path.join(os.path.expanduser('~'), '.split'),
    'segmentDirectory': os.path.expanduser('~'),
    'localhostRefreshEnabled': False,
    'preforkedInitialization': False,
    'dataSampling': DEFAULT_DATA_SAMPLING,
    'storageWrapper': None,
    'storagePrefix': None,
    'storageType': None,
    'flagSetsFilter': None
}

def _parse_operation_mode(sdk_key, config):
    """
    Process incoming config to determine operation mode and storage type

    :param config: user supplied config
    :type config: dict

    :returns: operation mode and storage type
    :rtype: Tuple (str, str)
    """
    if sdk_key == 'localhost':
        _LOGGER.debug('Using Localhost operation mode')
        return 'localhost', 'localhost'

    if 'redisHost' in config or 'redisSentinels' in config:
        _LOGGER.debug('Using Redis storage operation mode')
        return 'consumer', 'redis'

    if  config.get('storageType') is not None:
        if config.get('storageType').lower() == 'pluggable':
            _LOGGER.debug('Using Pluggable storage operation mode')
            return 'consumer', 'pluggable'

        _LOGGER.warning('You passed an invalid storageType, acceptable value is '
                            '`pluggable`. Defaulting storage to In-Memory mode.')

    _LOGGER.debug('Using In-Memory operation mode')
    return 'standalone', 'memory'


def _sanitize_impressions_mode(storage_type, mode, refresh_rate=None):
    """
    Check supplied impressions mode and adjust refresh rate.

    :param config: default + supplied config
    :type config: dict

    :returns: config with sanitized impressions mode & refresh rate
    :rtype: config
    """
    if not isinstance(mode, ImpressionsMode):
        try:
            mode = ImpressionsMode(mode.upper())
        except (ValueError, AttributeError):
            mode = ImpressionsMode.OPTIMIZED
            _LOGGER.warning('You passed an invalid impressionsMode, impressionsMode should be ' \
                            'one of the following values: `debug`, `none` or `optimized`. '
                            ' Defaulting to `optimized` mode.')

    if mode == ImpressionsMode.DEBUG:
        refresh_rate = max(1, refresh_rate) if refresh_rate is not None else 60
    else:
        refresh_rate = max(60, refresh_rate) if refresh_rate is not None else 5 * 60

    return mode, refresh_rate

def sanitize(sdk_key, config):
    """
    Look for inconsistencies or ill-formed configs and tune it accordingly.

    :param sdk_key: sdk key
    :type sdk_key: str

    :param config: DEFAULT + user supplied config
    :type config: dict

    :returns: sanitized config
    :rtype: dict
    """
    config['operationMode'], config['storageType'] = _parse_operation_mode(sdk_key, config)
    processed = DEFAULT_CONFIG.copy()
    processed.update(config)
    imp_mode, imp_rate = _sanitize_impressions_mode(config['storageType'], config.get('impressionsMode'),
                                                    config.get('impressionsRefreshRate'))
    processed['impressionsMode'] = imp_mode
    processed['impressionsRefreshRate'] = imp_rate
    if processed['metricsRefreshRate'] < 60:
        _LOGGER.warning('metricRefreshRate parameter minimum value is 60 seconds, defaulting to 3600 seconds.')
        processed['metricsRefreshRate'] = 3600

    if config['operationMode'] == 'consumer' and config.get('flagSetsFilter') is not None:
        processed['flagSetsFilter'] = None
        _LOGGER.warning('config: FlagSets filter is not applicable for Consumer modes where the SDK does keep rollout data in sync. FlagSet filter was discarded.')
    else:
        processed['flagSetsFilter'] = sorted(validate_flag_sets(processed['flagSetsFilter'], 'SDK Config')) if processed['flagSetsFilter'] is not None else None

    return processed
