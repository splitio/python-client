"""SDK Telemetry helpers."""
from bisect import bisect_left


BUCKETS = (
    1000, 1500, 2250, 3375, 5063,
    7594, 11391, 17086, 25629, 38443,
    57665, 86498, 129746, 194620, 291929,
    437894, 656841, 985261, 1477892, 2216838,
    3325257, 4987885, 7481828
)
MAX_LATENCY = 7481828


def get_latency_bucket_index(micros):
    """
    Find the bucket index for a measured latency.

    :param micros: Measured latency in microseconds
    :type micros: int
    :return: Bucket index for the given latency
    :rtype: int
    """
    if micros > MAX_LATENCY:
        return len(BUCKETS) - 1

    return bisect_left(BUCKETS, micros)
