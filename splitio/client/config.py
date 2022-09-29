"""Default settings for the Split.IO SDK Python client."""
import os.path
import logging

from splitio.engine.impressions.impressions import ImpressionsMode


_LOGGER = logging.getLogger(__name__)
DEFAULT_DATA_SAMPLING = 1


DEFAULT_CONFIG = {
    'operationMode': 'in-memory',
    'connectionTimeout': 1500,
    'streamingEnabled': True,
    'featuresRefreshRate': 30,
    'segmentsRefreshRate': 30,
    'metricsRefreshRate': 60,
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
    'preforkedInitialization': False,
    'dataSampling': DEFAULT_DATA_SAMPLING,
}


def _parse_operation_mode(apikey, config):
    """
    Process incoming config to determine operation mode.

    :param config: user supplied config
    :type config: dict

    :returns: operation mode
    :rtype: str
    """
    if apikey == 'localhost':
        return 'localhost-standalone'

    if 'redisHost' in config or 'redisSentinels' in config:
        return 'redis-consumer'

    return 'inmemory-standalone'


def _sanitize_impressions_mode(mode, refresh_rate=None):
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
            _LOGGER.warning('You passed an invalid impressionsMode, impressionsMode should be '
                            'one of the following values: `debug`, `none` or `optimized`. '
                            'Defaulting to `optimized` mode.')
            mode = ImpressionsMode.OPTIMIZED

    if mode == ImpressionsMode.DEBUG:
        refresh_rate = max(1, refresh_rate) if refresh_rate is not None else 60
    else:
        refresh_rate = max(60, refresh_rate) if refresh_rate is not None else 5 * 60

    return mode, refresh_rate


def sanitize(apikey, config):
    """
    Look for inconsistencies or ill-formed configs and tune it accordingly.

    :param apikey: customer's apikey
    :type apikey: str

    :param config: DEFAULT + user supplied config
    :type config: dict

    :returns: sanitized config
    :rtype: dict
    """
    config['operationMode'] = _parse_operation_mode(apikey, config)
    processed = DEFAULT_CONFIG.copy()
    processed.update(config)
    imp_mode, imp_rate = _sanitize_impressions_mode(config.get('impressionsMode'),
                                                    config.get('impressionsRefreshRate'))
    processed['impressionsMode'] = imp_mode
    processed['impressionsRefreshRate'] = imp_rate
    return processed
