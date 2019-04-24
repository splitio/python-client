"""General purpose SDK utilities."""

import inspect
import socket
from collections import namedtuple
from splitio.version import __version__

SdkMetadata = namedtuple(
    'SdkMetadata',
    ['sdk_version', 'instance_name', 'instance_ip']
)


def _get_ip():
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        sock.connect(('10.255.255.255', 1))
        ip_address = sock.getsockname()[0]
    except Exception:  #pylint: disable=broad-except
        ip_address = 'unknown'
    finally:
        sock.close()
    return ip_address


def _get_hostname(ip_address):
    return 'unknown' if ip_address == 'unknown' else 'ip-' + ip_address.replace('.', '-')


def get_metadata(config):
    """
    Gather SDK metadata and return a tuple with such info.

    :param config: User supplied config augmented with defaults.
    :type config: dict

    :return: SDK Metadata information.
    :rtype: SdkMetadata
    """
    version = 'python-%s' % __version__
    ip_from_config = config.get('machineIp')
    machine_from_config = config.get('machineName')
    ip_address = ip_from_config if ip_from_config is not None else _get_ip()
    hostname = machine_from_config if machine_from_config is not None else _get_hostname(ip_address)
    return SdkMetadata(version, hostname, ip_address)


def get_calls(classes_filter=None):
    """
    Inspect the stack and retrieve an ordered list of caller functions.

    :param class_filter: If not None, only methods from that classes will be returned.
    :type class: list(str)

    :return: list of callers ordered by most recent first.
    :rtype: list(tuple(str, str))
    """
    try:
        return [
            inspect.getframeinfo(frame[0]).function
            for frame in inspect.stack()
            if classes_filter is None
            or 'self' in frame[0].f_locals and frame[0].f_locals['self'].__class__.__name__ in classes_filter  #pylint: disable=line-too-long
        ]
    except Exception:  #pylint: disable=broad-except
        return []
