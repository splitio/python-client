"""Wrappers for tasks when using UWSGI Cache as a synchronization platform."""

import logging
import time

from splitio.client.config import sanitize as sanitize_config
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
from splitio.tasks.util import workerpool
from splitio.sync.split import SplitSynchronizer
from splitio.sync.segment import SegmentSynchronizer
from splitio.sync.impression import ImpressionSynchronizer
from splitio.sync.event import EventSynchronizer
from splitio.sync.telemetry import TelemetrySynchronizer

_LOGGER = logging.getLogger(__name__)


def _get_config(user_config):
    """
    Get sdk configuration using defaults + user overrides.

    :param user_config: User configuration.
    :type user_config: dict

    :return: Calculated configuration.
    :rtype: dict
    """
    return sanitize_config(user_config['apikey'], user_config)


def uwsgi_update_splits(user_config):
    """
    Update splits task.

    :param user_config: User-provided configuration.
    :type user_config: dict
    """
    config = _get_config(user_config)
    metadata = get_metadata(config)
    seconds = config['featuresRefreshRate']
    split_sync = SplitSynchronizer(
        SplitsAPI(
            HttpClient(1500, config.get('sdk_url'), config.get('events_url')), config['apikey'],
            metadata
        ),
        UWSGISplitStorage(get_uwsgi()),
    )

    while True:
        try:
            split_sync.synchronize_splits()  # pylint: disable=protected-access
            time.sleep(seconds)
        except Exception:  # pylint: disable=broad-except
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
    metadata = get_metadata(config)
    segment_sync = SegmentSynchronizer(
        SegmentsAPI(
            HttpClient(1500, config.get('sdk_url'), config.get('events_url')), config['apikey'],
            metadata
        ),
        UWSGISplitStorage(get_uwsgi()),
        UWSGISegmentStorage(get_uwsgi()),
    )

    pool = workerpool.WorkerPool(20, segment_sync.synchronize_segment)  # pylint: disable=protected-access
    pool.start()
    split_storage = UWSGISplitStorage(get_uwsgi())
    while True:
        try:
            for segment_name in split_storage.get_segment_names():
                pool.submit_work(segment_name)
            time.sleep(seconds)
        except Exception:  # pylint: disable=broad-except
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
    impressions_sync = ImpressionSynchronizer(
        ImpressionsAPI(
            HttpClient(1500, config.get('sdk_url'), config.get('events_url')),
            config['apikey'],
            metadata,
            config['impressionsMode']
        ),
        storage,
        config['impressionsBulkSize']
    )

    while True:
        try:
            impressions_sync.synchronize_impressions()  # pylint: disable=protected-access
            for _ in range(0, seconds):
                if storage.should_flush():
                    storage.acknowledge_flush()
                    break
                time.sleep(1)
        except Exception:  # pylint: disable=broad-except
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
    events_sync = EventSynchronizer(
        EventsAPI(
            HttpClient(1500, config.get('sdk_url'), config.get('events_url')),
            config['apikey'],
            metadata
        ),
        storage,
        config['eventsBulkSize']
    )
    while True:
        try:
            events_sync.synchronize_events()  # pylint: disable=protected-access
            for _ in range(0, seconds):
                if storage.should_flush():
                    storage.acknowledge_flush()
                    break
                time.sleep(1)
        except Exception:  # pylint: disable=broad-except
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
    telemetry_sync = TelemetrySynchronizer(
        TelemetryAPI(
            HttpClient(1500, config.get('sdk_url'), config.get('events_url')),
            config['apikey'],
            metadata
        ),
        storage,
    )
    while True:
        try:
            telemetry_sync.synchronize_telemetry()  # pylint: disable=protected-access
            time.sleep(seconds)
        except Exception:  # pylint: disable=broad-except
            _LOGGER.error('Error posting metrics')
            _LOGGER.debug('Error: ', exc_info=True)
