"""A module for Split.io SDK and uwsgi compatibility

Strong dependency of uWSGI Cache Framework
https://uwsgi-docs.readthedocs.io/en/latest/Caching.html

Sample Command: uwsgi --http :9090 --wsgi-file mysite/wsgi.py --enable-threads --master
                        --cache2 name=splitio,items=5000,store=/tmp/uwsgi_cache.6


Cache item size
; create a cache for images with dynamic size (images can be big, so do not waste memory)
cache2 = name=images,items=20,bitmap=1,blocks=100

; a cache for css (20k per-item is more than enough)
cache2 = name=stylesheets,items=30,blocksize=20000


SPOOLER:
uwsgi --http :9090 --wsgi-file mysite/wsgi.py --processes 4 --threads 2 --enable-threads --master
    --cache2 name=splitio,items=5000,store=/tmp/splitio.cache --spooler myspool --import myspool.spooltasks

"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    #uwsgi is loaded at runtime by uwsgi app.
    import uwsgi
except ImportError:
    def missing_uwsgi_dependencies(*args, **kwargs):
        raise NotImplementedError('Missing uWSGI support dependencies.')
    uwsgi = missing_uwsgi_dependencies

try:
    from jsonpickle import decode, encode
except ImportError:
    def missing_jsonpickle_dependencies(*args, **kwargs):
        raise NotImplementedError('Missing jsonpickle support dependencies.')
    decode = encode = missing_jsonpickle_dependencies


import re
import logging
import time

from itertools import groupby
from six import iteritems
from collections import defaultdict

from splitio.cache import SegmentCache, SplitCache, ImpressionsCache, MetricsCache
from splitio.api import api_factory
from splitio.tasks import update_splits, update_segments, report_metrics, report_impressions
from splitio.splits import Split, ApiSplitChangeFetcher, SplitParser, HashAlgorithm
from splitio.segments import Segment, ApiSegmentChangeFetcher
from splitio.matchers import UserDefinedSegmentMatcher
from splitio.utils import bytes_to_string
from splitio.impressions import Impression
from splitio.metrics import BUCKETS
from splitio.config import DEFAULT_CONFIG

_logger = logging.getLogger(__name__)


def _get_config(user_config):
    sdk_config = DEFAULT_CONFIG
    sdk_config.update(user_config)
    return sdk_config


def uwsgi_update_splits(user_config):
    try:
        config = _get_config(user_config)
        seconds = config['featuresRefreshRate']
        while True:
            split_cache = UWSGISplitCache(get_uwsgi())

            sdk_api = api_factory(config)
            split_change_fetcher = ApiSplitChangeFetcher(sdk_api)

            segment_cache = UWSGISegmentCache(get_uwsgi())
            split_parser = UWSGISplitParser(segment_cache)

            update_splits(split_cache, split_change_fetcher, split_parser)

            time.sleep(seconds)
    except:
        _logger.exception('Exception caught updating splits')


def uwsgi_update_segments(user_config):
    try:
        config = _get_config(user_config)
        seconds = config['segmentsRefreshRate']
        while True:
            segment_cache = UWSGISegmentCache(get_uwsgi())
            sdk_api = api_factory(config)
            segment_change_fetcher = ApiSegmentChangeFetcher(sdk_api)
            update_segments(segment_cache, segment_change_fetcher)

            time.sleep(seconds)
    except:
        _logger.exception('Exception caught updating segments')


def uwsgi_report_impressions(user_config):
    try:
        config = _get_config(user_config)
        seconds = config['impressionsRefreshRate']
        while True:
            impressions_cache = UWSGIImpressionsCache(get_uwsgi())
            sdk_api = api_factory(config)
            report_impressions(impressions_cache, sdk_api)

            time.sleep(seconds)
    except:
        _logger.exception('Exception caught posting impressions')


def uwsgi_report_metrics(user_config):
    try:
        config = _get_config(user_config)
        seconds = config['metricsRefreshRate']
        while True:
            metrics_cache = UWSGIMetricsCache(get_uwsgi())
            sdk_api = api_factory(config)
            report_metrics(metrics_cache, sdk_api)

            time.sleep(seconds)
    except:
        _logger.exception('Exception caught posting metrics')




_SPLITIO_COMMON_CACHE_NAMESPACE = 'splitio'
_SPLITIO_STATS_CACHE_NAMESPACE = 'splitioStats'

class UWSGISplitCache(SplitCache):
    _KEY_TEMPLATE = 'split.{suffix}'
    _KEY_TILL_TEMPLATE = 'splits.till'
    _KEY_CURRENT_SPLITS = 'splits.current'

    def __init__(self, adapter):
        """A SplitCache implementation that uses uwsgi cache as its back-end."""
        self._adapter = adapter

    def is_enabled(self):
        """Returns if uwsgi is enabled or not"""
        return True

    def disable(self):
        """Disable cache. To keep interface"""
        return True

    def add_split(self, split_name, split):
        """
        Stores a Split under a name.
        :param split_name: Name of the split (feature)
        :type split_name: str
        :param split: The split to store
        :type split: Split
        """
        self._adapter.cache_update(self._KEY_TEMPLATE.format(suffix=split_name), encode(split), 0,
                                   _SPLITIO_COMMON_CACHE_NAMESPACE)

        if self._adapter.cache_exists(self._KEY_CURRENT_SPLITS, _SPLITIO_COMMON_CACHE_NAMESPACE):
            current_splits = decode(self._adapter.cache_get(self._KEY_CURRENT_SPLITS, _SPLITIO_COMMON_CACHE_NAMESPACE))
            current_splits[split_name] = True
        else:
            current_splits = {split_name:True}

        self._adapter.cache_update(self._KEY_CURRENT_SPLITS, encode(current_splits), 0, _SPLITIO_COMMON_CACHE_NAMESPACE)

    def remove_split(self, split_name):
        """
        Evicts a Split from the cache.
        :param split_name: Name of the split (feature)
        :type split_name: str
        """

        if self._adapter.cache_exists(self._KEY_CURRENT_SPLITS, _SPLITIO_COMMON_CACHE_NAMESPACE):
            current_splits = decode(self._adapter.cache_get(self._KEY_CURRENT_SPLITS, _SPLITIO_COMMON_CACHE_NAMESPACE))
            current_splits.pop(split_name, None)
            self._adapter.cache_update(self._KEY_CURRENT_SPLITS, encode(current_splits), 0,
                                       _SPLITIO_COMMON_CACHE_NAMESPACE)

        return self._adapter.cache_del(self._KEY_TEMPLATE.format(suffix=split_name), _SPLITIO_COMMON_CACHE_NAMESPACE)

    def get_split(self, split_name):
        """
        Retrieves a Split from the cache.
        :param split_name: Name of the split (feature)
        :type split_name: str
        :return: The split under the name if it exists, None otherwise
        :rtype: Split
        """
        to_decode = self._adapter.cache_get(self._KEY_TEMPLATE.format(suffix=split_name), _SPLITIO_COMMON_CACHE_NAMESPACE)

        if to_decode is None:
            return None

        to_decode = bytes_to_string(to_decode)

        split_dump = decode(to_decode)

        if split_dump is not None:
            segment_cache = UWSGISegmentCache(self._adapter)
            split_parser = UWSGISplitParser(segment_cache)
            split = split_parser.parse(split_dump)
            return split

        return None

    def get_splits_keys(self):
        if self._adapter.cache_exists(self._KEY_CURRENT_SPLITS, _SPLITIO_COMMON_CACHE_NAMESPACE):
            return decode(self._adapter.cache_get(self._KEY_CURRENT_SPLITS, _SPLITIO_COMMON_CACHE_NAMESPACE))
        return dict()

    def get_splits(self):
        current_splits = self.get_splits_keys()

        to_return = []

        for split_name in current_splits:
            to_return.append(self.get_split(split_name))

        return to_return


    def set_change_number(self, change_number):
        """
        Sets the value for the change number
        :param change_number: The change number
        :type change_number: int
        """
        return self._adapter.cache_update(self._KEY_TILL_TEMPLATE, encode(change_number), 0, _SPLITIO_COMMON_CACHE_NAMESPACE)

    def get_change_number(self):
        """
        Retrieves the value of the change number
        :return: The current change number value, -1 otherwise
        :rtype: int
        """
        if self._adapter.cache_exists(self._KEY_TILL_TEMPLATE, _SPLITIO_COMMON_CACHE_NAMESPACE):
            return decode(self._adapter.cache_get(self._KEY_TILL_TEMPLATE, _SPLITIO_COMMON_CACHE_NAMESPACE))
        else:
            return -1

class UWSGISegmentCache(SegmentCache):
    _KEY_TEMPLATE = 'segments.{suffix}'
    _SEGMENT_DATA_KEY_TEMPLATE = 'segmentData.{segment_name}'
    _SEGMENT_CHANGE_NUMBER_KEY_TEMPLATE = 'segment.{segment_name}.till'
    _SEGMENT_REGISTERED = _KEY_TEMPLATE.format(suffix='registered')


    def __init__(self,adapter, disabled_period=300):
        """A Segment Cache implementation that uses uWSGI as its back-end
        :param adapter: The uwsgi module
        :rtype uwsgi: uwsgi
        :param disabled_period: The expiration period for the disabled key.
        :param disabled_period: int
        """
        self._adapter = adapter
        self._disabled_period = disabled_period

    @property
    def disabled_period(self):
        return self._disabled_period

    @disabled_period.setter
    def disabled_period(self, disabled_period):
        self._disabled_period = disabled_period

    def disable(self):
        """Disables the automatic update process. This method will be called if the update fails
        for some reason. Use enable to re-enable the update process."""
        pass

    def enable(self):
        """Enables the automatic update process."""
        pass

    def is_enabled(self):
        """
        :return: Whether the update process is enabled or not.
        :rtype: bool
        """
        return True

    def register_segment(self, segment_name):
        """Register a segment for inclusion in the automatic update process.
        :param segment_name: Name of the segment.
        :type segment_name: str
        """

        if self._adapter.cache_exists(self._SEGMENT_REGISTERED, _SPLITIO_COMMON_CACHE_NAMESPACE):
            segments = decode(self._adapter.cache_get(self._SEGMENT_REGISTERED, _SPLITIO_COMMON_CACHE_NAMESPACE))
        else:
            segments = set()

        segments.add(segment_name)
        self._adapter.cache_update(self._SEGMENT_REGISTERED, encode(segments), 0, _SPLITIO_COMMON_CACHE_NAMESPACE)

    def unregister_segment(self, segment_name):
        """Unregister a segment from the automatic update process.
        :param segment_name: Name of the segment.
        :type segment_name: str
        """

        if self._adapter.cache_exists(self._SEGMENT_REGISTERED, _SPLITIO_COMMON_CACHE_NAMESPACE):
            segments = decode(self._adapter.cache_get(self._SEGMENT_REGISTERED, _SPLITIO_COMMON_CACHE_NAMESPACE))
            #If segment is in set, remove it and update cache
            if segment_name in segments:
                segments.discard(segment_name)
                self._adapter.cache_update(self._SEGMENT_REGISTERED, encode(segments), 0, _SPLITIO_COMMON_CACHE_NAMESPACE)


    def get_registered_segments(self):
        """
        :return: All segments included in the automatic update process.
        :rtype: set
        """
        if self._adapter.cache_exists(self._SEGMENT_REGISTERED, _SPLITIO_COMMON_CACHE_NAMESPACE):
            return decode(self._adapter.cache_get(self._SEGMENT_REGISTERED, _SPLITIO_COMMON_CACHE_NAMESPACE))

        return set()

    def add_keys_to_segment(self, segment_name, segment_keys):
        _key = self._SEGMENT_DATA_KEY_TEMPLATE.format(segment_name=segment_name)
        if self._adapter.cache_exists(_key, _SPLITIO_COMMON_CACHE_NAMESPACE):
            segment_data = decode(self._adapter.cache_get(_key, _SPLITIO_COMMON_CACHE_NAMESPACE))
        else:
            segment_data = set()

        segment_data.update(segment_keys)
        self._adapter.cache_update(_key, encode(segment_data), 0, _SPLITIO_COMMON_CACHE_NAMESPACE)


    def remove_keys_from_segment(self, segment_name, segment_keys):
        _key = self._SEGMENT_DATA_KEY_TEMPLATE.format(segment_name=segment_name)
        if self._adapter.cache_exists(_key, _SPLITIO_COMMON_CACHE_NAMESPACE):
            segment_data = decode(self._adapter.cache_get(_key, _SPLITIO_COMMON_CACHE_NAMESPACE))
            for segment_key in segment_keys:
                segment_data.discard(segment_key)
            self._adapter.cache_update(_key, encode(segment_data), 0, _SPLITIO_COMMON_CACHE_NAMESPACE)

    def is_in_segment(self, segment_name, key):
        _key = self._SEGMENT_DATA_KEY_TEMPLATE.format(segment_name=segment_name)
        if self._adapter.cache_exists(_key, _SPLITIO_COMMON_CACHE_NAMESPACE):
            segment_data = decode(self._adapter.cache_get(_key, _SPLITIO_COMMON_CACHE_NAMESPACE))
            if key in segment_data:
                return True

        return False

    def set_change_number(self, segment_name, change_number):
        self._adapter.cache_update(self._SEGMENT_CHANGE_NUMBER_KEY_TEMPLATE.format(segment_name=segment_name), encode(change_number), 0, _SPLITIO_COMMON_CACHE_NAMESPACE)

    def get_change_number(self, segment_name):
        if self._adapter.cache_exists(self._SEGMENT_CHANGE_NUMBER_KEY_TEMPLATE.format(segment_name=segment_name), _SPLITIO_COMMON_CACHE_NAMESPACE):
            change_number = decode(self._adapter.cache_get(self._SEGMENT_CHANGE_NUMBER_KEY_TEMPLATE.format(segment_name=segment_name), _SPLITIO_COMMON_CACHE_NAMESPACE))
            return int(change_number) if change_number is not None else -1

        return -1


class UWSGISplitParser(SplitParser):
    def __init__(self, segment_cache):
        """
        A SplitParser implementation that registers the segments with the uwsgi segment cache
        implementation upon parsing an IN_SEGMENT matcher.
        """
        super(UWSGISplitParser, self).__init__(None)
        self._segment_cache = segment_cache

    def _parse_split(self, split, block_until_ready=False):
        return UWSGISplit(
            split['name'], split['seed'], split['killed'],
            split['defaultTreatment'], split['trafficTypeName'],
            split['status'], split['changeNumber'],
            segment_cache=self._segment_cache, algo=split.get('algo'),
            traffic_allocation=split.get('trafficAllocation'),
            traffic_allocation_seed=split.get('trafficAllocationSeed')
        )

    def _parse_matcher_in_segment(self, partial_split, matcher, block_until_ready=False, *args,
                                  **kwargs):
        matcher_data = self._get_matcher_attribute('userDefinedSegmentMatcherData', matcher)
        segment = UWSGISplitBasedSegment(matcher_data['segmentName'], partial_split)
        delegate = UserDefinedSegmentMatcher(segment)
        self._segment_cache.register_segment(delegate.segment.name)
        return delegate

class UWSGISplit(Split):
    def __init__(self, name, seed, killed, default_treatment, traffic_type_name, status, change_number, conditions=None, segment_cache=None, algo=None,
                 traffic_allocation=None,
                 traffic_allocation_seed=None):
        """A split implementation that mantains a reference to the segment cache so segments can
        be easily pickled and unpickled.
        :param name: Name of the feature
        :type name: unicode
        :param seed: Seed
        :type seed: int
        :param killed: Whether the split is killed or not
        :type killed: bool
        :param default_treatment: Default treatment for the split
        :type default_treatment: str
        :param conditions: Set of conditions to test
        :type conditions: list
        :param segment_cache: A segment cache
        :type segment_cache: SegmentCache
        """
        super(UWSGISplit, self).__init__(
            name, seed, killed, default_treatment, traffic_type_name, status,
            change_number, conditions, algo, traffic_allocation,
            traffic_allocation_seed)
        self._segment_cache = segment_cache

    @property
    def segment_cache(self):
        return self._segment_cache

    @segment_cache.setter
    def segment_cache(self, segment_cache):
        self._segment_cache = segment_cache

    def __getstate__(self):
        old_dict = self.__dict__.copy()
        del old_dict['_segment_cache']
        return old_dict

    def __setstate__(self, dict):
        self.__dict__.update(dict)
        self._segment_cache = None


class UWSGISplitBasedSegment(Segment):
    def __init__(self, name, split):
        """A Segment that uses a reference to a UWSGISplit uwsgi' instance to check if a key
        is in a segment
        :param name: The name of the segment
        :type name: str
        :param split: A UWSGISplit instance
        :type split: UWSGISplit
        """
        super(UWSGISplitBasedSegment, self).__init__(name)
        self._split = split

    def contains(self, key):
        return self._split.segment_cache.is_in_segment(self.name, key)


class UWSGIImpressionsCache(ImpressionsCache):
    _IMPRESSIONS_KEY = 'impressions'
    _LOCK_IMPRESSION_KEY = 'impressions_lock'
    _OVERWRITE_LOCK_SECONDS = 5

    def __init__(self, adapter, disabled_period=300):
        """An ImpressionsCache implementation that uses uWSGI as its back-end
        :param disabled_period: The expiration period for the disabled key.
        :param disabled_period: int
        """
        self._adapter = adapter
        self._disabled_period = disabled_period

    @property
    def disabled_period(self):
        return self._disabled_period

    @disabled_period.setter
    def disabled_period(self, disabled_period):
        self._disabled_period = disabled_period

    def enable(self):
        """Enables the automatic impressions report process and the registration of impressions."""
        pass

    def disable(self):
        """Disables the automatic impressions report process and the registration of any
        impressions for the specificed disabled period. This method will be called if there's an
        exception while trying to send the impressions back to Split."""
        pass

    def is_enabled(self):
        """
        :return: Whether the automatic report process and impressions registration are enabled.
        :rtype: bool
        """
        return True

    def _build_impressions_dict(self, impressions):
        """Buils a dictionary of impressions that groups them based on their feature name.
        :param impressions: List of impression tuples
        :type impressions: list
        :return: Dictionary of impressions grouped by feature name
        :rtype: dict
        """
        sorted_impressions = sorted(impressions, key=lambda impression: impression.feature_name)
        grouped_impressions = groupby(sorted_impressions,
                                      key=lambda impression: impression.feature_name)
        return dict((feature_name, list(group)) for feature_name, group in grouped_impressions)

    def fetch_all(self):
        """Fetches all impressions from the cache. It returns a dictionary with the impressions
        grouped by feature name.
        :return: All cached impressions so far grouped by feature name
        :rtype: dict
        """
        return self.fetch_all_and_clear()

    def clear(self):
        """Clears all cached impressions"""
        pass

    def __lock_impressions(self):
        initial_time = time.time()
        while True:
            if not self._adapter.cache_exists(self._LOCK_IMPRESSION_KEY, _SPLITIO_STATS_CACHE_NAMESPACE):
                self._adapter.cache_set(self._LOCK_IMPRESSION_KEY, str('locked'), 0, _SPLITIO_STATS_CACHE_NAMESPACE)
                return
            else:
                if time.time() - initial_time > self._OVERWRITE_LOCK_SECONDS:
                    return
            time.sleep(0.3)

    def __unlock_impressions(self):
        self._adapter.cache_del(self._LOCK_IMPRESSION_KEY, _SPLITIO_STATS_CACHE_NAMESPACE)

    def add_impression(self, impression):
        """Adds an impression to the log if it is enabled, otherwise the impression is dropped.
        :param impression: The impression tuple
        :type impression: Impression
        """
        cache_impression = {'keyName':impression.matching_key,
                            'treatment':impression.treatment,
                            'time':impression.time,
                            'changeNumber':impression.change_number,
                            'label':impression.label,
                            'bucketingKey':impression.bucketing_key
                            }

        self.__lock_impressions()

        if self._adapter.cache_exists(self._IMPRESSIONS_KEY, _SPLITIO_STATS_CACHE_NAMESPACE):
            impressions = decode(self._adapter.cache_get(self._IMPRESSIONS_KEY, _SPLITIO_STATS_CACHE_NAMESPACE))
        else:
            impressions = dict()

        if impression.feature_name not in impressions:
            impressions[impression.feature_name] = [cache_impression]
        else:
            impressions_set = impressions[impression.feature_name]
            impressions_set.append(cache_impression)
            impressions_set.reverse()
            impressions[impression.feature_name] = impressions_set

        _logger.debug('Adding impressions to cache: %s' % impressions)
        self._adapter.cache_update(self._IMPRESSIONS_KEY, encode(impressions), 0, _SPLITIO_STATS_CACHE_NAMESPACE)

        self.__unlock_impressions()

    def fetch_all_and_clear(self):
        """Fetches all impressions from the cache and clears it. It returns a dictionary with the
        impressions grouped by feature name.
        :return: All cached impressions so far grouped by feature name
        :rtype: dict
        """

        if self._adapter.cache_exists(self._IMPRESSIONS_KEY, _SPLITIO_STATS_CACHE_NAMESPACE):
            impressions_list = list()

            self.__lock_impressions()

            cached_impressions = decode(self._adapter.cache_get(self._IMPRESSIONS_KEY, _SPLITIO_STATS_CACHE_NAMESPACE))
            self._adapter.cache_del(self._IMPRESSIONS_KEY, _SPLITIO_STATS_CACHE_NAMESPACE)

            self.__unlock_impressions()

            _logger.debug('**** Cached Impressions: %s' % cached_impressions)

            for feature_name in cached_impressions:
                impressions = cached_impressions[feature_name]

                for impression in impressions:
                    impression_tuple = Impression(matching_key=impression['keyName'],
                                                  feature_name=feature_name,
                                                  treatment=impression['treatment'],
                                                  label=impression['label'],
                                                  change_number=impression['changeNumber'],
                                                  bucketing_key=impression['bucketingKey'],
                                                  time=impression['time']
                                                  )
                    impressions_list.append(impression_tuple)

            if not impressions_list:
                return dict()

            return self._build_impressions_dict(impressions_list)

        return dict()


class UWSGIMetricsCache(MetricsCache):
    _KEY_TEMPLATE = 'metrics.{suffix}'
    _METRIC_KEY = _KEY_TEMPLATE.format(suffix='metric')
    _LATENCY_KEY = _KEY_TEMPLATE.format(suffix='latency')
    _KEY_LATENCY_BUCKET = 'latency.{metric_name}.bucket.{bucket_number}'
    _COUNT_FIELD_TEMPLATE = 'count.{counter}'
    _TIME_FIELD_TEMPLATE = 'time.{operation}.{bucket_index}'
    _GAUGE_FIELD_TEMPLATE = 'gauge.{gauge}'

    _LATENCY_FIELD_RE = re.compile('^latency\.(?P<operation>.+)\.bucket\.(?P<bucket_index>.+)$')
    _COUNT_FIELD_RE = re.compile('^count\.(?P<counter>.+)$')
    _TIME_FIELD_RE = re.compile('^time\.(?P<operation>.+)\.(?P<bucket_index>.+)$')
    _GAUGE_FIELD_RE = re.compile('^gauge\.(?P<gauge>.+)$')

    def __init__(self, adapter, disabled_period=300):
        """A MetricsCache implementation that uses uWSGI as its back-end
        :param disabled_period: The expiration period for the disabled key.
        :param disabled_period: int
        """
        super(UWSGIMetricsCache, self).__init__()
        self._adapter = adapter
        self._disabled_period = disabled_period

    @property
    def disabled_period(self):
        return self._disabled_period

    @disabled_period.setter
    def disabled_period(self, disabled_period):
        self._disabled_period = disabled_period

    def enable(self):
        """Enables the automatic metrics report process and the registration of new metrics."""
        pass

    def disable(self):
        """Disables the automatic metrics report process and the registration of any
        metrics for the specified disabled period. This method will be called if there's an
        exception while trying to send the metrics back to Split."""
        pass

    def is_enabled(self):
        """
        :return: Whether the automatic report process and metrics registration are enabled.
        :rtype: bool
        """
        return True

    def _get_count_field(self, counter):
        """Builds the field name for a counter on the metrics.
        :param counter: Name of the counter
        :type counter: str
        :return: Name of the field on the metrics hash for the given counter
        :rtype: str
        """
        return self._COUNT_FIELD_TEMPLATE.format(counter=counter)

    def _get_time_field(self, operation, bucket_index):
        """Builds the field name for a latency counting bucket ont the metrics.
        :param operation: Name of the operation
        :type operation: str
        :param bucket_index: Latency bucket index as returned by get_latency_bucket_index
        :type bucket_index: int
        :return: Name of the field on the metrics hash for the latency bucket counter
        :rtype: str
        """
        return self._TIME_FIELD_TEMPLATE.format(operation=operation,
                                                             bucket_index=bucket_index)

    def _get_all_buckets_time_fields(self, operation):
        """ Builds a list of all the fields in the metrics hash for the latency buckets for a given
        operation.
        :param operation: Name of the operation
        :type operation: str
        :return: List of field names
        :rtype: list
        """
        return [self._get_time_field(operation, bucket) for bucket in range(0, len(BUCKETS))]

    def _get_gauge_field(self, gauge):
        """Builds the field name for a gauge on the metrics hash.
        :param gauge: Name of the gauge
        :type gauge: str
        :return: Name of the field on the metrics hash for the given gauge
        :rtype: str
        """
        return self._GAUGE_FIELD_TEMPLATE.format(gauge=gauge)

    def _build_metrics_counter_data(self, count_metrics):
        """Build metrics counter data in the format expected by the API from the contents of the
        cache.
        :param count_metrics: A dictionary of name/value counter metrics
        :param count_metrics: dict
        :return: A list of of counter metrics
        :rtype: list
        """
        return [{'name': name, 'delta': delta} for name, delta in iteritems(count_metrics)]

    def _build_metrics_times_data(self, time_metrics):
        """Build metrics times data in the format expected by the API from the contents of the
        cache.
        :param time_metrics: A dictionary of name/latencies time metrics
        :param time_metrics: dict
        :return: A list of of time metrics
        :rtype: list
        """
        to_return = [{'name': name, 'latencies': latencies}
                for name, latencies in iteritems(time_metrics)]
        return to_return

    def _build_metrics_gauge_data(self, gauge_metrics):
        """Build metrics gauge data in the format expected by the API from the contents of the
        cache.
        :param gauge_metrics: A dictionary of name/value gauge metrics
        :param gauge_metrics: dict
        :return: A list of of gauge metrics
        :rtype: list
        """
        return [{'name': name, 'value': value} for name, value in iteritems(gauge_metrics)]

    def _build_metrics_from_cache_response(self, response):
        """Builds a dictionary with time, count and gauge metrics based on the result of calling
        fetch_all_and_clear (list of name/value pairs). Each entry in the dictionary is in the
        format accepted by the events API.
        :param response: Response given by the fetch_all_and_clear method
        :type response: lsit
        :return: Dictionary with time, count and gauge metrics
        :rtype: dict
        """
        if response is None:
            return {'count': [], 'gauge': []}

        count = dict()
        gauge = dict()

        for field, value in response.items():
            count_match = self._COUNT_FIELD_RE.match(field)
            if count_match is not None:
                count[count_match.group('counter')] = value
                continue

            gauge_match = self._GAUGE_FIELD_RE.match(field)
            if gauge_match is not None:
                gauge[gauge_match.group('gauge')] = value
                continue

        return {
            'count': self._build_metrics_counter_data(count),
            'gauge': self._build_metrics_gauge_data(gauge)
        }


    def _get_metric(self, field_name):
        if self._adapter.cache_exists(self._METRIC_KEY, _SPLITIO_STATS_CACHE_NAMESPACE):
            metrics = decode(self._adapter.cache_get(self._METRIC_KEY, _SPLITIO_STATS_CACHE_NAMESPACE))
            if field_name in metrics:
                return metrics[field_name]
        return None

    def _set_metric(self, field_name, value):
        if self._adapter.cache_exists(self._METRIC_KEY, _SPLITIO_STATS_CACHE_NAMESPACE):
            metrics = decode(self._adapter.cache_get(self._METRIC_KEY, _SPLITIO_STATS_CACHE_NAMESPACE))
        else:
            metrics = dict()

        metrics[field_name] = value
        self._adapter.cache_update(self._METRIC_KEY, encode(metrics), 0, _SPLITIO_STATS_CACHE_NAMESPACE)
        _logger.error(metrics)

    def get_latency(self, operation):
        _latencies = []
        if self._adapter.cache_exists(self._LATENCY_KEY, _SPLITIO_STATS_CACHE_NAMESPACE):
            latencies = decode(self._adapter.cache_get(self._LATENCY_KEY, _SPLITIO_STATS_CACHE_NAMESPACE))
            for bucket in range(0, len(BUCKETS)):
                _key = self._KEY_LATENCY_BUCKET.format(metric_name=operation, bucket_number=bucket)
                if _key in latencies:
                    _latencies.append(latencies[_key])
                else:
                    _latencies.append(0)

        return [0 for bucket in range(0, len(BUCKETS))]

    def get_latency_bucket_counter(self, operation, bucket_index):
        if self._adapter.cache_exists(self._LATENCY_KEY, _SPLITIO_STATS_CACHE_NAMESPACE):
            latencies = decode(self._adapter.cache_get(self._LATENCY_KEY, _SPLITIO_STATS_CACHE_NAMESPACE))
            _key = self._KEY_LATENCY_BUCKET.format(metric_name=operation, bucket_number=bucket_index)
            if _key in latencies:
                return latencies[_key]
        return 0

    def set_latency_bucket_counter(self, operation, bucket_index, value):
        latencies = dict()
        if self._adapter.cache_exists(self._LATENCY_KEY, _SPLITIO_STATS_CACHE_NAMESPACE):
            latencies = decode(self._adapter.cache_get(self._LATENCY_KEY, _SPLITIO_STATS_CACHE_NAMESPACE))

        latencies[self._KEY_LATENCY_BUCKET.format(metric_name=operation, bucket_number=bucket_index)] = value
        self._adapter.cache_update(self._LATENCY_KEY, encode(latencies), 0, _SPLITIO_STATS_CACHE_NAMESPACE)

    def increment_latency_bucket_counter(self, operation, bucket_index, delta=1):
        latency = self.get_latency_bucket_counter(operation, bucket_index)
        self.set_latency_bucket_counter(operation, bucket_index, latency + delta)

    def set_count(self, counter, value):
        metric_field = self._get_count_field(counter)
        self._set_metric(metric_field, value)

    def get_count(self, counter):
        value = self._get_metric(self._get_count_field(counter))
        if value is not None:
            return value
        return 0

    def increment_count(self, counter, delta=1):
        counter_value = self.get_count(counter) + delta
        self.set_count(counter, counter_value)

    def set_gauge(self, gauge, value):
        gauge_field = self._get_gauge_field(gauge)
        self._set_metric(gauge_field, value)

    def get_gauge(self, gauge):
        value = self._get_metric(self._get_gauge_field(gauge))
        if value is not None:
            return value
        return 0

    def fetch_all_and_clear(self):
        if self._adapter.cache_exists(self._METRIC_KEY, _SPLITIO_STATS_CACHE_NAMESPACE):
            metrics = decode(self._adapter.cache_get(self._METRIC_KEY, _SPLITIO_STATS_CACHE_NAMESPACE))
            return self._build_metrics_from_cache_response(metrics)
        return self._build_metrics_from_cache_response(None)

    def fetch_all_times_and_clear(self):
        if self._adapter.cache_exists(self._LATENCY_KEY, _SPLITIO_STATS_CACHE_NAMESPACE):
            latencies = decode(self._adapter.cache_get(self._LATENCY_KEY, _SPLITIO_STATS_CACHE_NAMESPACE))
            time = defaultdict(lambda: [0] * len(BUCKETS))
            for key in latencies:
                time_match = self._LATENCY_FIELD_RE.match(key)
                if time_match is not None:
                    time[time_match.group('operation')][int(time_match.group('bucket_index'))] = int(latencies[key])
                    latencies[key] = 0
            self._adapter.cache_update(self._LATENCY_KEY, encode(latencies), 0, _SPLITIO_STATS_CACHE_NAMESPACE)
            return self._build_metrics_times_data(time)

        return self._build_metrics_times_data(dict())

class UWSGICacheEmulator(object):
    def __init__(self):
        """UWSGI Cache Emulator for unit tests. Implements uwsgi cache framework interface
        http://uwsgi-docs.readthedocs.io/en/latest/Caching.html#accessing-the-cache-from-your-applications-using-the-cache-api
        """
        self._cache = dict()

    def _check_string_data_type(self, value):
        if type(value).__name__ == 'str':
            return True
        raise TypeError('The value to add into uWSGI cache must be string and %s given' % type(value).__name__)

    def cache_get(self, key, cache_namespace='default'):
        if self.cache_exists(key, cache_namespace):
            return self._cache[cache_namespace][key]
        return None

    def cache_set(self, key, value, expires=0, cache_namespace='default'):
        self._check_string_data_type(value)

        if cache_namespace in self._cache:
            self._cache[cache_namespace][key] = value
        else:
            self._cache[cache_namespace] = {key:value}

    def cache_update(self, key,value, expires=0, cache_namespace='default'):
        self.cache_set(key, value, expires, cache_namespace)

    def cache_exists(self, key, cache_namespace='default'):
        if cache_namespace in self._cache:
            if key in self._cache[cache_namespace]:
                return True
        return False

    def cache_del(self, key, cache_namespace='default'):
        if cache_namespace in self._cache:
            self._cache[cache_namespace].pop(key, None)

    def cache_clear(self, cache_namespace='default'):
        self._cache.pop(cache_namespace, None)


def get_uwsgi(emulator=False):
    """Returns a uwsgi imported module or an emulator to use in unit test """
    if emulator:
        return UWSGICacheEmulator()

    return uwsgi
