"""Datatypes converters for matchers."""

def ts_truncate_seconds(timestamp):
    """
    Set seconds to zero in a timestamp.

    :param ts: Timestamp in seconds.
    :type ts: int

    :return: Timestamp in seconds, but without counting them (ie: DD-MM-YY HH:MM:00)
    :rtype: int
    """
    return timestamp - (timestamp % 60)

def ts_truncate_time(timestamp):
    """
    Set time to zero in a timestamp.

    :param ts: Timestamp in seconds.
    :type ts: int

    :return: Timestamp in seconds, without counting time (ie: DD-MM-YYYY 00:00:00)
    :rtype: int
    """
    return timestamp - (timestamp % 86400)

def java_ts_to_secs(java_ts):
    """
    Convert java timestamp into unix timestamp.

    :param java_ts: java timestamp in milliseconds.
    :type java_ts: int

    :return: Timestamp in seconds.
    :rtype: int
    """
    return java_ts / 1000

def java_ts_truncate_seconds(java_ts):
    """
    Set seconds to zero in a timestamp.

    :param ts: Timestamp in seconds.
    :type ts: int

    :return: Timestamp in seconds, but without counting them (ie: DD-MM-YY HH:MM:00)
    :rtype: int
    """
    return ts_truncate_seconds(java_ts_to_secs(java_ts))

def java_ts_truncate_time(java_ts):
    """
    Set time to zero in a timestamp.

    :param ts: Timestamp in seconds.
    :type ts: int

    :return: Timestamp in seconds, without counting time (ie: DD-MM-YYYY 00:00:00)
    :rtype: int
    """
    return ts_truncate_time(java_ts_to_secs(java_ts))
