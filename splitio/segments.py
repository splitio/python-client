"""This module contains everything related to segments"""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging

from concurrent.futures import ThreadPoolExecutor
from threading import Timer, RLock


class Segment(object):
    def __init__(self, name, key_set=None):
        """
        Basic implementation of an immutable Split.io Segment.
        :param name: The name of the segment
        :type name: unicode
        :param key_set: Set of keys contained by the segment
        :type key_set: list
        """
        self._name = name
        self._key_set = frozenset(key_set) if key_set is not None else frozenset()
        self._logger = logging.getLogger(self.__class__.__name__)

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
        return key in self._key_set


class DummySegmentFetcher(object):
    """A segment fetcher that returns empty segments. Useful for testing"""
    def get_segment(self, name):
        """
        Fetches an empty segment
        :param name: The segment name
        :type name: unicode
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

    def get_segment(self, name):
        """
        Fetch self refreshing segment
        :param name: The name of the segment
        :type name: unicode
        :return: A segment for the given name
        :rtype: Segment
        """
        if name in self._segments:
            return self._segments[name]

        segment = SelfRefreshingSegment(name, self._segment_change_fetcher, self._executor,
                                        self._interval)
        self._segments[name] = segment

        segment.start()

        return segment


class SelfRefreshingSegment(Segment):
    def __init__(self, name, segment_change_fetcher, executor, interval, greedy=True,
                 change_number=-1, key_set=None):
        """
        A segment implementation that refreshes itself periodically using a ThreadPoolExecutor.
        :param name: The name of the segment
        :type name: unicode
        :param segment_change_fetcher: The segment change fetcher implementation
        :type segment_change_fetcher: SegmentChangeFetcher
        :param executor: A ThreadPoolExecutor that'll run the refreshing process
        :type executor: ThreadPoolExecutor
        :param interval: An integer or callable that'll define the refreshing interval
        :type interval: int
        :param greedy: Request all changes until they are exhausted
        :type greedy: bool
        :param change_number: An integer with the initial value for the "since" API argument
        :type change_number: int
        :param key_set: An optional initial set of keys
        :type key_set: list
        """
        super(SelfRefreshingSegment, self).__init__(name, key_set=key_set)
        self._change_number = change_number
        self._segment_change_fetcher = segment_change_fetcher
        self._executor = executor
        self._interval = interval
        self._greedy = greedy
        self._stopped = True
        self._rlock = RLock()

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

    def force_refresh(self):
        """Forces a refresh of the segment"""
        SelfRefreshingSegment._refresh_segment(self)

    def start(self):
        """Starts the self-refreshing processes of the segment"""
        if not self._stopped:
            return

        self._stopped = False
        SelfRefreshingSegment._timer_refresh(self)

    @staticmethod
    def _refresh_segment(segment):
        """
        The actual segment refresh process. This method had to be static due to some limitations
        on Timer and ThreadPoolExecutor.
        :param segment: A self refreshing segment
        :type segment: SelfRefreshingSegment
        """
        try:
            with segment._rlock:
                while True:
                    response = segment._segment_change_fetcher.fetch(segment._name,
                                                                     segment._change_number)

                    if segment._change_number >= response['till']:
                        return

                    if len(response['added']) > 0 or len(response['removed']) > 0:
                        segment._logger.info('%s added %s', segment._name,
                                             SelfRefreshingSegment._summarize_changes(
                                                 response['added']))
                        segment._logger.info('%s removed %s', segment._name,
                                             SelfRefreshingSegment._summarize_changes(
                                                 response['removed']))

                        new_key_set = (segment._key_set | frozenset(response['added'])) -\
                            frozenset(response['removed'])
                        segment._key_set = new_key_set

                    segment._change_number = response['till']

                    if not segment._greedy:
                        return
        except:
            segment._logger('Exception caught refreshing segment')
            segment._stopped = True

    @staticmethod
    def _summarize_changes(changes):
        """Summarize the changes received from the segment change fetcher."""
        return '[{summary}{others}]'.format(
            summary=','.join(changes[:min(3, len(changes))]),
            others=',... {} others'.format(3 - len(changes)) if len(changes) > 3 else ''
        )

    @staticmethod
    def _timer_refresh(segment):
        """
        Responsible for setting the periodic calls to _refresh_segment using a Timer thread.
        :param segment: A self refreshing segment
        :type segment: SelfRefreshingSegment
        """
        if segment._stopped:
            return

        try:
            segment._executor.submit(SelfRefreshingSegment._refresh_segment, segment)

            timer = Timer(segment._interval, SelfRefreshingSegment._timer_refresh, (segment,))
            timer.daemon = True
            timer.start()
        except:
            segment._logger('Exception caught refreshing timer')
            segment._stopped = True


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
