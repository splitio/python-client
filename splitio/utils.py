from __future__ import absolute_import, division, print_function, unicode_literals

import socket


def bytes_to_string(bytes, encode='utf-8'):
    if type(bytes).__name__ == 'bytes':
        return str(bytes, encode)

    return bytes


def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # doesn't even have to be reachable
        s.connect(('10.255.255.255', 1))
        IP = s.getsockname()[0]
    except Exception:
        IP = 'unknown'
    finally:
        s.close()
    return IP


def get_hostname():
    ip = get_ip()
    return 'unknown' if ip == 'unknown' else 'ip-' + ip.replace('.', '-')
