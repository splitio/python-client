"""This module contains everything related to segments"""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging

from concurrent.futures import ThreadPoolExecutor
from json import load
from threading import Timer, RLock


class Segment(object):
    def __init__(self, name):
        """
        Basic interface of a Segment.
        :param name: The name of the segment
        :type name: unicode
        """
        self._name = name

    @property
    def name(self):
        """
        :return: The name of the segment
        :rtype: unicode
        """
        return self._name

    def contains(self, key):
        """
        Tests whether a key is in a segment
        :param key: The key to test
        :type key: unicode
        :return: True if the key is contained by the segment, False otherwise
        :rtype: boolean
        """
        return False


class InMemorySegment(Segment):
    def __init__(self, name, change_number=-1, key_set=None):
        """
        An implementation of a Segment that holds keys in set in memory.
        :param name: The name of the segment
        :type name: str
        :param change_number: The change number for the segment
        :type name: int
        :param key_set: Set of keys contained by the segment
        :type key_set: list
        """
        super(InMemorySegment, self).__init__(name)
        self._change_number = change_number
        self._key_set = frozenset(key_set) if key_set is not None else frozenset()

    @property
    def key_set(self):
        return self._key_set

    @key_set.setter
    def key_set(self, key_set):
        self._key_set = key_set

    @property
    def change_number(self):
        return self._change_number

    @change_number.setter
    def change_number(self, change_number):
        self._change_number = change_number

    def contains(self, key):
        """
        Tests whether a key is in a segment
        :param key: The key to test
        :type key: unicode
        :return: True if the key is contained by the segment, False otherwise
        :rtype: boolean
        """
        return key in self._key_set


class SegmentFetcher(object):
    """Basic segment fetcher interface."""
    def fetch(self, name, block_until_ready=False):
        """
        Fetches an empty segment
        :param name: The segment name
        :type name: unicode
        :param block_until_ready: Whether to wait until all the data is available
        :type block_until_ready: bool
        :return: An empty segment
        :rtype: Segment
        """
        return Segment(name)


class SelfRefreshingSegmentFetcher(object):
    def __init__(self, segment_change_fetcher, interval=60, max_workers=5):
        """
        A segment fetcher that generates self refreshing segments.
        :param segment_change_fetcher: A segment change fetcher implementation
        :type segment_change_fetcher: SegmentChangeFetcher
        :param interval: An integer or callable that'll define the refreshing interval
        :type interval: int
        :param max_workers: The max number of workers used to fetch segment changes
        :type max_workers: int
        """
        self._segment_change_fetcher = segment_change_fetcher
        self._executor = ThreadPoolExecutor(max_workers=max_workers)
        self._interval = interval
        self._segments = dict()

    def fetch(self, name, block_until_ready=False):
        """
        Fetch self refreshing segment
        :param name: The name of the segment
        :type name: unicode
        :param block_until_ready: Whether to wait until all the data is available
        :type block_until_ready: bool
        :return: A segment for the given name
        :rtype: Segment
        """
        if name in self._segments:
            return self._segments[name]

        segment = SelfRefreshingSegment(name, self._segment_change_fetcher, self._executor,
                                        self._interval)
        self._segments[name] = segment

        if block_until_ready:
            segment.refresh_segment()
            segment.start(delayed_update=True)
        else:
            segment.start()

        return segment


class SelfRefreshingSegment(InMemorySegment):
    def __init__(self, name, segment_change_fetcher, executor, interval, change_number=-1,
                 greedy=True, key_set=None):
        """
        A segment implementation that refreshes itself periodically using a ThreadPoolExecutor.
        :param name: The name of the segment
        :type name: str
        :param segment_change_fetcher: The segment change fetcher implementation
        :type segment_change_fetcher: SegmentChangeFetcher
        :param executor: A ThreadPoolExecutor that'll run the refreshing process
        :type executor: ThreadPoolExecutor
        :param interval: An integer or callable that'll define the refreshing interval
        :type interval: int
        :param change_number: An integer with the initial value for the "since" API argument
        :type change_number: int
        :param greedy: Request all changes until they are exhausted
        :type greedy: bool
        :param key_set: An optional initial set of keys
        :type key_set: list
        """
        super(SelfRefreshingSegment, self).__init__(name, change_number=change_number,
                                                    key_set=key_set)
        self._segment_change_fetcher = segment_change_fetcher
        self._executor = executor
        self._interval = interval
        self._greedy = greedy
        self._stopped = True
        self._rlock = RLock()
        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def stopped(self):
        """
        :return: Whether the refresh process has been stopped
        :rtype: bool
        """
        return self._stopped

    @stopped.setter
    def stopped(self, stopped):
        """
        :param stopped: Whether to stop the refreshing process
        :type stopped: bool
        """
        self._stopped = stopped

    def start(self, delayed_update=False):
        """Starts the self-refreshing processes of the segment.
        :param delayed_update: Whether to delay the update until the interval has passed
        :type delayed_update: bool
        """
        if not self._stopped:
            return

        self._stopped = False

        if delayed_update:
            self._timer_start()
        else:
            self._timer_refresh()

    def refresh_segment(self):
        """The actual segment refresh process."""
        try:
            with self._rlock:
                while True:
                    response = self._segment_change_fetcher.fetch(self._name,
                                                                  self._change_number)
                    if self._change_number >= response['till']:
                        return

                    if len(response['added']) > 0 or len(response['removed']) > 0:
                        self._logger.info('%s added %s', self._name,
                                          self._summarize_changes(response['added']))
                        self._logger.info('%s removed %s', self._name,
                                          self._summarize_changes(response['removed']))

                        new_key_set = (self._key_set | frozenset(response['added'])) -\
                            frozenset(response['removed'])
                        self._key_set = new_key_set

                    self._change_number = response['till']

                    if not self._greedy:
                        return
        except:
            self._logger.exception('Exception caught refreshing segment')
            self._stopped = True

    def _summarize_changes(self, changes):
        """Summarize the changes received from the segment change fetcher."""
        return '[{summary}{others}]'.format(
            summary=','.join(changes[:min(3, len(changes))]),
            others=',... {} others'.format(3 - len(changes)) if len(changes) > 3 else ''
        )

    def _timer_start(self):
        try:
            if hasattr(self._interval, '__call__'):
                interval = self._interval()
            else:
                interval = self._interval

            timer = Timer(interval, self._timer_refresh)
            timer.daemon = True
            timer.start()
        except:
            self._logger.exception('Exception caught starting timer')
            self._stopped = True

    def _timer_refresh(self):
        """Responsible for setting the periodic calls to _refresh_segment using a Timer thread."""
        if self._stopped:
            return

        try:
            self._executor.submit(self.refresh_segment)
            self._timer_start()
        except:
            self._logger.exception('Exception caught refreshing timer')
            self._stopped = True


class JSONFileSegmentFetcher(SegmentFetcher):
    def __init__(self, file_name):
        """
        A segment fetcher that retrieves the information from a file with the JSON response of a
        segmentChanges resource.
        :param file_name: The name of the file
        :type file_name: str
        """
        with open(file_name) as f:
            self._json = load(f)

        self._added = frozenset(self._json['added'])
        self._removed = frozenset(self._json['removed'])

    def fetch(self, name, block_until_ready=False):
        """
        Fetch in memory segment
        :param name: The name of the segment
        :type name: str
        :param block_until_ready: Whether to wait until all the data is available
        :type block_until_ready: bool
        :return: A segment for the given name
        :rtype: Segment
        """
        segment = InMemorySegment(name, key_set=self._added - self._removed)
        return segment


class SegmentChangeFetcher(object):
    def __init__(self):
        """Fetches changes in the segment since a reference point."""
        self._logger = logging.getLogger(self.__class__.__name__)

    def fetch_from_backend(self, name, since):
        """
        Fetches changes for a given segment.
        :param name: The name of the segment
        :type name: unicode
        :param since: An integer that indicates that we want the changes that occurred AFTER this
                      last change number. A value less than zero implies that the client is
                      requesting information on this segment for the first time.
        :type since: int
        :return: A dictionary with the changes for the segment
        :rtype: dict
        """
        raise NotImplementedError()

    def build_empty_segment_change(self, name, since):
        """
        Builds an "empty" segment change response. Used in case of exceptions or other unforseen
        problems.
        :param name: The name of the segment
        :type name: unicode
        :param since: "till" value of the last segment change.
        :type since: int
        :return: A dictionary with an empty (.e.g. no change) response
        :rtype: dict
        """
        return {
            'name': name,
            'since': since,
            'till': since,
            'added': [],
            'removed': []
        }

    def fetch(self, name, since):
        """
        Fetch changes for a segment. If the segment does not exist, or if there were problems with
        the request, the method returns an empty segment change with the latest change number set
        to a value less than 0.

        If no changes have happened since the change number requested, then return an empty segment
        change with the latest change number equal to the requested change number.

        This is a sample response:

        {
            "name": "demo_segment",
            "added": [
                "some_id_6"
            ],
            "removed": [
                "some_id_1", "some_id_2"
            ],
            "since": 1460890700905,
            "till": 1460890700906
        }

        :param name: The name of the segment
        :type name: unicode
        :param since: An integer that indicates that we want the changes that occurred AFTER this
                      last change number. A value less than zero implies that the client is
                      requesting information on this segment for the first time.
        :type since: int
        :return: A dictionary with the changes
        :rtype: dict
        """
        if type(name).__name__ == 'bytes':
            name = str(name,'utf-8')

        try:
            segment_change = self.fetch_from_backend(name, since)
        except:
            self._logger.exception('Exception caught fetching segment changes')
            segment_change = self.build_empty_segment_change(name, since)

        return segment_change


class ApiSegmentChangeFetcher(SegmentChangeFetcher):
    def __init__(self, api):
        """
        A SegmentChangeFetcher implementation that retrieves the changes from Split.io's RESTful
        SDK API.
        :param api: The API client to use
        :type api: SdkApi
        """
        super(ApiSegmentChangeFetcher, self).__init__()
        self._api = api

    def fetch_from_backend(self, name, since):
        return self._api.segment_changes(name, since)


class CacheBasedSegmentFetcher(SegmentFetcher):
    def __init__(self, segment_cache):
        """
        A segment fetcher based on a segments cache
        :param segment_cache: The segment cache to use
        :type segment_cache: SegmentCache
        """
        self._segment_cache = segment_cache

    def fetch(self, name, block_until_ready=False):
        """
        Fetch cache based segment
        :param name: The name of the segment
        :type name: str
        :param block_until_ready: Whether to wait until all the data is available
        :type block_until_ready: bool
        :return: A segment for the given name
        :rtype: Segment
        """
        segment = CacheBasedSegment(name, self._segment_cache)
        return segment


class CacheBasedSegment(Segment):
    def __init__(self, name, segment_cache):
        """
        A SegmentCached based implementation of a Segment
        :param name: The name of the segment
        :type name: str
        :param segment_cache: The segment cache backend
        :type segment_cache: SegmentCache
        """
        super(CacheBasedSegment, self).__init__(name)
        self._segment_cache = segment_cache

    def contains(self, key):
        return self._segment_cache.is_in_segment(self._name, key)
