from __future__ import absolute_import, division, print_function, unicode_literals

def bytes_to_string(bytes, encode='utf-8'):
    if type(bytes).__name__ == 'bytes':
        return str(bytes, encode)

    return bytes