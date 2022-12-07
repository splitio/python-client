"""General purpose SDK utilities."""

from collections import namedtuple
from splitio.version import __version__
from splitio.util.host_info import get_hostname, get_ip

from splitio.models.telemetry import MethodExceptionsAndLatencies

_MAP_METHOD_TO_ENUM = {'treatment': MethodExceptionsAndLatencies.TREATMENT,
                       'treatments': MethodExceptionsAndLatencies.TREATMENTS,
                        'treatment_with_config': MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG,
                        'treatments_with_config': MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG,
                        'track': MethodExceptionsAndLatencies.TRACK
                       }

SdkMetadata = namedtuple(
    'SdkMetadata',
    ['sdk_version', 'instance_name', 'instance_ip']
)

def _get_hostname_and_ip(config):
    """
    Get current hostname and IP address if config parameters are not set.

    :param config: User supplied config augmented with defaults.
    :type config: dict

    :return: IP address and Hostname
    :rtype: Tuple (str, str)
    """
    if config.get('IPAddressesEnabled') is False:
        return 'NA', 'NA'
    ip_from_config = config.get('machineIp')
    machine_from_config = config.get('machineName')
    ip_address = ip_from_config if ip_from_config is not None else get_ip()
    hostname = machine_from_config if machine_from_config is not None else get_hostname()
    return ip_address, hostname

def get_metadata(config):
    """
    Gather SDK metadata and return a tuple with such info.

    :param config: User supplied config augmented with defaults.
    :type config: dict

    :return: SDK Metadata information.
    :rtype: SdkMetadata
    """
    version = 'python-%s' % __version__
    ip_address, hostname = _get_hostname_and_ip(config)
    return SdkMetadata(version, hostname, ip_address)

def get_method_constant(method):
    """
    Get method name mapped to the Method Enum object

    :return: method name
    :rtype: str
    """
    return _MAP_METHOD_TO_ENUM[method]
