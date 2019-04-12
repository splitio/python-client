"""General purpose SDK utilities."""

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

def get_metadata(*args, **kwargs):
    """Gather SDK metadata and return a tuple with such info."""
    version = 'python-%s' % __version__
    ip_address = _get_ip()
    hostname = _get_hostname(ip_address)
    return SdkMetadata(version, hostname, ip_address)
