import threading
from splitio import util
from splitio.models.impressions import Impression
from splitio.engine.hashfns import murmur_128
from splitio.engine.cache.lru import SimpleLruCache
from collections import defaultdict, namedtuple

_TIME_INTERVAL_MS = 3600 * 1000  # one hour

def truncate_time(timestamp_ms):
    """
    Truncate a timestamp in milliseconds to have hour granularity.

    :param timestamp_ms: timestamp generated in the impression.
    :type timestamp_ms: int

    :returns: a timestamp with hour, min, seconds, and ms set to 0.
    :rtype: int
    """
    return timestamp_ms - (timestamp_ms % _TIME_INTERVAL_MS)

def truncate_impressions_time(imps):
    """
    Process impressions.

    Impressions are truncated based on time

    :param impressions: List of impression objects with attributes
    :type impressions: list[tuple[splitio.models.impression.Impression, dict]]

    :returns: truncated list of impressions
    :rtype: list[splitio.models.impression.Impression]
    """
    this_hour = truncate_time(util.utctime_ms())
    return [i for i, _ in imps if i.previous_time is None or i.previous_time < this_hour]


class Hasher(object):  # pylint:disable=too-few-public-methods
    """Impression hasher."""

    _PATTERN = "%s:%s:%s:%s:%d"

    def __init__(self, hash_fn=murmur_128, seed=0):
        """
        Class constructor.

        :param hash_fn: Hash function to apply (str, int) -> int
        :type hash_fn: callable

        :param seed: seed to be provided when hashing
        :type seed: int
        """
        self._hash_fn = hash_fn
        self._seed = seed

    def _stringify(self, impression):
        """
        Stringify an impression.

        :param impression: Impression to stringify using _PATTERN
        :type impression: splitio.models.impressions.Impression

        :returns: a string representation of the impression
        :rtype: str
        """
        return self._PATTERN % (impression.matching_key if impression.matching_key else 'UNKNOWN',
                                impression.feature_name if impression.feature_name else 'UNKNOWN',
                                impression.treatment if impression.treatment else 'UNKNOWN',
                                impression.label if impression.label else 'UNKNOWN',
                                impression.change_number if impression.change_number else 0)

    def process(self, impression):
        """
        Hash an impression.

        :param impression: Impression to hash.
        :type impression: splitio.models.impressions.Impression

        :returns: a hash of the supplied impression's relevant fields.
        :rtype: int
        """
        return self._hash_fn(self._stringify(impression), self._seed)


class Observer(object):  # pylint:disable=too-few-public-methods
    """Observe impression and add a previous time if applicable."""

    def __init__(self, size):
        """Class constructor."""
        self._hasher = Hasher()
        self._cache = SimpleLruCache(size)

    def test_and_set(self, impression):
        """
        Examine an impression to determine and set it's previous time accordingly.

        :param impression: Impression to track
        :type impression: splitio.models.impressions.Impression

        :returns: Impression with populated previous time
        :rtype: splitio.models.impressions.Impression
        """
        previous_time = self._cache.test_and_set(self._hasher.process(impression), impression.time)
        return Impression(impression.matching_key,
                          impression.feature_name,
                          impression.treatment,
                          impression.label,
                          impression.change_number,
                          impression.bucketing_key,
                          impression.time,
                          previous_time)
