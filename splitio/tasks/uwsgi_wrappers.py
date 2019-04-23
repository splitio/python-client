"""Wrappers for tasks when using UWSGI Cache as a synchronization platform."""

import logging
import time

from splitio.client.config import DEFAULT_CONFIG
from splitio.client.util import get_metadata
from splitio.storage.adapters.uwsgi_cache import get_uwsgi
from splitio.storage.uwsgi import UWSGIEventStorage, UWSGIImpressionStorage, \
    UWSGISegmentStorage, UWSGISplitStorage, UWSGITelemetryStorage
from splitio.api.client import HttpClient
from splitio.api.splits import SplitsAPI
from splitio.api.segments import SegmentsAPI
from splitio.api.impressions import ImpressionsAPI
from splitio.api.telemetry import TelemetryAPI
from splitio.api.events import EventsAPI
from splitio.tasks.split_sync import SplitSynchronizationTask
from splitio.tasks.segment_sync import SegmentSynchronizationTask
from splitio.tasks.impressions_sync import ImpressionsSyncTask
from splitio.tasks.events_sync import EventsSyncTask
from splitio.tasks.telemetry_sync import TelemetrySynchronizationTask
from splitio.tasks.util import workerpool

_LOGGER = logging.getLogger(__name__)


def _get_config(user_config):
    """
    Get sdk configuration using defaults + user overrides.

    :param user_config: User configuration.
    :type user_config: dict

    :return: Calculated configuration.
    :rtype: dict
    """
    sdk_config = DEFAULT_CONFIG.copy()
    sdk_config.update(user_config)
    return sdk_config


def uwsgi_update_splits(user_config):
    """
    Update splits task.

    :param user_config: User-provided configuration.
    :type user_config: dict
    """
    config = _get_config(user_config)
    seconds = config['featuresRefreshRate']
    split_sync_task = SplitSynchronizationTask(
        SplitsAPI(
            HttpClient(1500, config.get('sdk_url'), config.get('events_url')), config['apikey']
        ),
        UWSGISplitStorage(get_uwsgi()),
        None, # Time not needed since the task will be triggered manually.
        None  # Ready flag not needed since it will never be set and consumed.
    )

    while True:
        try:
            split_sync_task._update_splits()  #pylint: disable=protected-access
            time.sleep(seconds)
        except Exception:  #pylint: disable=broad-except
            _LOGGER.error('Error updating splits')
            _LOGGER.debug('Error: ', exc_info=True)


def uwsgi_update_segments(user_config):
    """
    Update segments task.

    :param user_config: User-provided configuration.
    :type user_config: dict
    """
    config = _get_config(user_config)
    seconds = config['segmentsRefreshRate']
    segment_sync_task = SegmentSynchronizationTask(
        SegmentsAPI(
            HttpClient(1500, config.get('sdk_url'), config.get('events_url')), config['apikey']
        ),
        UWSGISegmentStorage(get_uwsgi()),
        None, # Split sotrage not needed, segments provided manually,
        None, # Period not needed, task executed manually
        None  # Flag not needed, never consumed or set.
    )

    pool = workerpool.WorkerPool(20, segment_sync_task._update_segment)  #pylint: disable=protected-access
    pool.start()
    split_storage = UWSGISplitStorage(get_uwsgi())
    while True:
        try:
            for split in split_storage.get_all_splits():
                for segment_name in split.get_segment_names():
                    pool.submit_work(segment_name)
            time.sleep(seconds)
        except Exception:  #pylint: disable=broad-except
            _LOGGER.error('Error updating segments')
            _LOGGER.debug('Error: ', exc_info=True)


def uwsgi_report_impressions(user_config):
    """
    Flush impressions task.

    :param user_config: User-provided configuration.
    :type user_config: dict
    """
    config = _get_config(user_config)
    metadata = get_metadata(config)
    seconds = config['impressionsRefreshRate']
    storage = UWSGIImpressionStorage(get_uwsgi())
    impressions_sync_task = ImpressionsSyncTask(
        ImpressionsAPI(
            HttpClient(1500, config.get('sdk_url'), config.get('events_url')),
            config['apikey'],
            metadata
        ),
        storage,
        None, # Period not needed. Task is being triggered manually.
        config['impressionsBulkSize']
    )

    while True:
        try:
            impressions_sync_task._send_impressions()  #pylint: disable=protected-access
            for _ in xrange(0, seconds):
                if storage.should_flush():
                    storage.acknowledge_flush()
                    break
                time.sleep(1)
        except Exception:  #pylint: disable=broad-except
            _LOGGER.error('Error posting impressions')
            _LOGGER.debug('Error: ', exc_info=True)

def uwsgi_report_events(user_config):
    """
    Flush events task.

    :param user_config: User-provided configuration.
    :type user_config: dict
    """
    config = _get_config(user_config)
    metadata = get_metadata(config)
    seconds = config.get('eventsRefreshRate', 30)
    storage = UWSGIEventStorage(get_uwsgi())
    task = EventsSyncTask(
        EventsAPI(
            HttpClient(1500, config.get('sdk_url'), config.get('events_url')),
            config['apikey'],
            metadata
        ),
        storage,
        None, # Period not needed. Task is being triggered manually.
        config['eventsBulkSize']
    )
    while True:
        try:
            task._send_events()  #pylint: disable=protected-access
            for _ in xrange(0, seconds):
                if storage.should_flush():
                    storage.acknowledge_flush()
                    break
                time.sleep(1)
        except Exception:  #pylint: disable=broad-except
            _LOGGER.error('Error posting metrics')
            _LOGGER.debug('Error: ', exc_info=True)

def uwsgi_report_telemetry(user_config):
    """
    Flush events task.

    :param user_config: User-provided configuration.
    :type user_config: dict
    """
    config = _get_config(user_config)
    metadata = get_metadata(config)
    seconds = config.get('metricsRefreshRate', 30)
    storage = UWSGITelemetryStorage(get_uwsgi())
    task = TelemetrySynchronizationTask(
        TelemetryAPI(
            HttpClient(1500, config.get('sdk_url'), config.get('events_url')),
            config['apikey'],
            metadata
        ),
        storage,
        None, # Period not needed. Task is being triggered manually.
    )
    while True:
        try:
            task._flush_telemetry()  #pylint: disable=protected-access
            time.sleep(seconds)
        except Exception:  #pylint: disable=broad-except
            _LOGGER.error('Error posting metrics')
            _LOGGER.debug('Error: ', exc_info=True)
