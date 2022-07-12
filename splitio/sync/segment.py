import logging
import time

from splitio.api import APIException
from splitio.api.commons import FetchOptions
from splitio.tasks.util import workerpool
from splitio.models import segments
from splitio.util.backoff import Backoff


_LOGGER = logging.getLogger(__name__)


_ON_DEMAND_FETCH_BACKOFF_BASE = 10  # backoff base starting at 10 seconds
_ON_DEMAND_FETCH_BACKOFF_MAX_WAIT = 60  # don't sleep for more than 1 minute
_ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES = 10


class SegmentSynchronizer(object):
    def __init__(self, segment_api, split_storage, segment_storage):
        """
        Class constructor.

        :param segment_api: API to retrieve segments from backend.
        :type segment_api: splitio.api.SegmentApi

        :param split_storage: Split Storage.
        :type split_storage: splitio.storage.InMemorySplitStorage

        :param segment_storage: Segment storage reference.
        :type segment_storage: splitio.storage.SegmentStorage

        """
        self._api = segment_api
        self._split_storage = split_storage
        self._segment_storage = segment_storage
        self._worker_pool = workerpool.WorkerPool(10, self.synchronize_segment)
        self._worker_pool.start()
        self._backoff = Backoff(
                                _ON_DEMAND_FETCH_BACKOFF_BASE,
                                _ON_DEMAND_FETCH_BACKOFF_MAX_WAIT)

    def recreate(self):
        """
        Create worker_pool on forked processes.

        """
        self._worker_pool = workerpool.WorkerPool(10, self.synchronize_segment)
        self._worker_pool.start()

    def shutdown(self):
        """
        Shutdown worker_pool

        """
        self._worker_pool.stop()

    def _fetch_until(self, segment_name, fetch_options, till=None):
        """
        Hit endpoint, update storage and return when since==till.

        :param segment_name: Name of the segment to update.
        :type segment_name: str

        :param fetch_options Fetch options for getting segment definitions.
        :type fetch_options splitio.api.FetchOptions

        :param till: Passed till from Streaming.
        :type till: int

        :return: last change number
        :rtype: int
        """
        while True:  # Fetch until since==till
            change_number = self._segment_storage.get_change_number(segment_name)
            if change_number is None:
                change_number = -1
            if till is not None and till < change_number:
                # the passed till is less than change_number, no need to perform updates
                return change_number

            try:
                segment_changes = self._api.fetch_segment(segment_name, change_number,
                                                          fetch_options)
            except APIException as exc:
                _LOGGER.error('Exception raised while fetching segment %s', segment_name)
                _LOGGER.debug('Exception information: ', exc_info=True)
                raise exc

            if change_number == -1:  # first time fetching the segment
                new_segment = segments.from_raw(segment_changes)
                self._segment_storage.put(new_segment)
            else:
                self._segment_storage.update(
                    segment_name,
                    segment_changes['added'],
                    segment_changes['removed'],
                    segment_changes['till']
                )

            if segment_changes['till'] == segment_changes['since']:
                return segment_changes['till']

    def _attempt_segment_sync(self, segment_name, fetch_options, till=None):
        """
        Hit endpoint, update storage and return True if sync is complete.

        :param segment_name: Name of the segment to update.
        :type segment_name: str

        :param fetch_options Fetch options for getting split definitions.
        :type fetch_options splitio.api.FetchOptions

        :param till: Passed till from Streaming.
        :type till: int

        :return: Flags to check if it should perform bypass or operation ended
        :rtype: bool, int, int
        """
        self._backoff.reset()
        remaining_attempts = _ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES
        while True:
            remaining_attempts -= 1
            change_number = self._fetch_until(segment_name, fetch_options, till)
            if till is None or till <= change_number:
                return True, remaining_attempts, change_number
            elif remaining_attempts <= 0:
                return False, remaining_attempts, change_number
            how_long = self._backoff.get()
            time.sleep(how_long)

    def synchronize_segment(self, segment_name, till=None):
        """
        Update a segment from queue

        :param segment_name: Name of the segment to update.
        :type segment_name: str

        :param till: ChangeNumber received.
        :type till: int

        :return: True if no error occurs. False otherwise.
        :rtype: bool
        """
        fetch_options = FetchOptions(True)  # Set Cache-Control to no-cache
        successful_sync, remaining_attempts, change_number = self._attempt_segment_sync(segment_name, fetch_options, till)
        attempts = _ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES - remaining_attempts
        if successful_sync:  # succedeed sync
            _LOGGER.debug('Refresh completed in %d attempts.', attempts)
            return True
        with_cdn_bypass = FetchOptions(True, change_number)  # Set flag for bypassing CDN
        without_cdn_successful_sync, remaining_attempts, change_number = self._attempt_segment_sync(segment_name, with_cdn_bypass, till)
        without_cdn_attempts = _ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES - remaining_attempts
        if without_cdn_successful_sync:
            _LOGGER.debug('Refresh completed bypassing the CDN in %d attempts.',
                          without_cdn_attempts)
            return True
        _LOGGER.debug('No changes fetched after %d attempts with CDN bypassed.',
                        without_cdn_attempts)
        return False

    def synchronize_segments(self, segment_names = None, dont_wait = False):
        """
        Submit all current segments and wait for them to finish depend on dont_wait flag, then set the ready flag.

        :param segment_names: Optional, array of segment names to update.
        :type segment_name: {str}

        :param dont_wait: Optional, instruct the function to not wait for task completion
        :type segment_name: boolean

        :return: True if no error occurs or dont_wait flag is True. False otherwise.
        :rtype: bool
        """
        if segment_names is None:
            segment_names = self._split_storage.get_segment_names()
            
        for segment_name in segment_names:
            self._worker_pool.submit_work(segment_name)
        if (dont_wait):
            return True
        return not self._worker_pool.wait_for_completion()
    
    def segment_exist_in_storage(self, segment_name):
        """
        Check if a segment exists in the storage

        :param segment_name: Name of the segment
        :type segment_name: str

        :return: True if segment exist. False otherwise.
        :rtype: bool
        """
        return self._segment_storage.get(segment_name) != None
