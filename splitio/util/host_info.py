"""Utilities."""
import socket

def get_ip():
    """
    Fetching current host IP address

    :returns: IP address
    :rtype: str
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        sock.connect(('10.255.255.255', 1))
        ip_address = sock.getsockname()[0]
    except Exception:  # pylint: disable=broad-except
        ip_address = 'unknown'
    finally:
        sock.close()
    return ip_address


def get_hostname():
    """
    Fetching current host name

    :returns: host name
    :rtype: str
    """
    try:
        return socket.gethostname()
    except Exception:
        return 'unknown'
