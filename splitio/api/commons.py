"""Commons module."""
from collections import namedtuple
from splitio.util.time import get_current_epoch_time_ms

_CACHE_CONTROL = 'Cache-Control'
_CACHE_CONTROL_NO_CACHE = 'no-cache'

def record_telemetry(status_code, elapsed, metric_name, telemetry_runtime_producer):
    """
    Record Telemetry info

    :param status_code: http request status code
    :type status_code: int

    :param elapsed: response time elapsed.
    :type status_code: int

    :param metric_name: metric name for telemetry
    :type metric_name: str

    :param telemetry_runtime_producer: telemetry recording instance
    :type telemetry_runtime_producer: splitio.engine.telemetry.TelemetryRuntimeProducer
    """
    telemetry_runtime_producer.record_sync_latency(metric_name, elapsed)
    if 200 <= status_code < 300:
        telemetry_runtime_producer.record_successful_sync(metric_name, get_current_epoch_time_ms())
        return
    telemetry_runtime_producer.record_sync_error(metric_name, status_code)

class FetchOptions(object):
    """Fetch Options object."""

    def __init__(self, cache_control_headers=False, change_number=None):
        """
        Class constructor.

        :param cache_control_headers: Flag for Cache-Control header
        :type cache_control_headers: bool

        :param change_number: ChangeNumber to use for bypassing CDN in request.
        :type change_number: int
        """
        self._cache_control_headers = cache_control_headers
        self._change_number = change_number

    @property
    def cache_control_headers(self):
        """Return cache control headers."""
        return self._cache_control_headers

    @property
    def change_number(self):
        """Return change number."""
        return self._change_number

    def __eq__(self, other):
        """Match between other options."""
        if self._cache_control_headers != other._cache_control_headers:
            return False
        if self._change_number != other._change_number:
            return False
        return True


def build_fetch(change_number, fetch_options, metadata):
    """
    Build fetch with new flags if that is the case.

    :param change_number: Last known timestamp of definition.
    :type change_number: int

    :param fetch_options: Fetch options for getting definitions.
    :type fetch_options: splitio.api.commons.FetchOptions

    :param metadata: Metadata Headers.
    :type metadata: dict

    :return: Objects for fetch
    :rtype: dict, dict
    """
    query = {'since': change_number}
    extra_headers = metadata
    if fetch_options is None:
        return query, extra_headers
    if fetch_options.cache_control_headers:
        extra_headers[_CACHE_CONTROL] = _CACHE_CONTROL_NO_CACHE
    if fetch_options.change_number is not None:
        query['till'] = fetch_options.change_number
    return query, extra_headers