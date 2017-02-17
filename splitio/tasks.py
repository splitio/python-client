"""This module contains everything related to update tasks"""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging

from .splits import Status
from .impressions import build_impressions_data

_logger = logging.getLogger(__name__)


def update_segments(segment_cache, segment_change_fetcher):
    """If updates are enabled, this function updates all the segments listed by the segment cache
    get_registered_segments() method making requests to the Split.io SDK API. If an exception is
    raised, the process is stopped and it won't try to update segments again until enabled_updates
    is called on the segment cache."""
    try:
        if not segment_cache.is_enabled():
            return

        registered_segments = segment_cache.get_registered_segments()
        for name in registered_segments:
            update_segment(segment_cache, name, segment_change_fetcher)
    except:
        _logger.exception('Exception caught updating segment definitions')
        segment_cache.disable()


def update_segment(segment_cache, segment_name, segment_change_fetcher):
    """Updates a segment. It will eagerly request all changes until the change number is the same
    "till" value in the response.
    :param segment_name: The name of the segment
    :type segment_name: str
    """
    till = segment_cache.get_change_number(segment_name)
    _logger.info("Updating segment %s"%segment_name)
    while True:
        response = segment_change_fetcher.fetch(segment_name, till)
        _logger.info("SEGMENT RESPONSE %s"%response)
        if 'till' not in response:
            return

        if till >= response['till']:
            return

        if len(response['removed']) > 0:
            segment_cache.remove_keys_from_segment(segment_name, response['removed'])

        if len(response['added']) > 0:
            segment_cache.add_keys_to_segment(segment_name, response['added'])

        segment_cache.set_change_number(segment_name, response['till'])

        till = response['till']


def update_splits(split_cache, split_change_fetcher, split_parser):
    """If updates are enabled, this function updates (or initializes) the current cached split
    configuration. It can be called by periodic update tasks or directly to force an unscheduled
    update. If an exception is raised, the process is stopped and it won't try to update splits
    again until enabled_updates is called on the splits cache.
    """
    try:
        if not split_cache.is_enabled():
            return

        till = split_cache.get_change_number()

        while True:
            response = split_change_fetcher.fetch(till)

            if 'till' not in response:
                return

            if till >= response['till']:
                _logger.debug("change_number is greater or equal than 'till'")
                return

            if 'splits' in response and len(response['splits']) > 0:
                _logger.debug("Splits field in response. response = %s", response)
                added_features = []
                removed_features = []

                for split_change in response['splits']:
                    if Status(split_change['status']) != Status.ACTIVE:
                        split_cache.remove_split(split_change['name'])
                        continue

                    parsed_split = split_parser.parse(split_change)
                    if parsed_split is None:
                        _logger.warning(
                            'We could not parse the split definition for %s. '
                            'Removing split to be safe.', split_change['name'])
                        split_cache.remove_split(split_change['name'])
                        removed_features.append(split_change['name'])
                        continue

                    added_features.append(split_change['name'])
                    split_cache.add_split(split_change['name'], split_change)

                if len(added_features) > 0:
                    _logger.info('Updated features: %s', added_features)

                if len(removed_features) > 0:
                    _logger.info('Deleted features: %s', removed_features)

            till = response['till']
            split_cache.set_change_number(response['till'])
    except:
        _logger.exception('Exception caught updating split definitions')
        split_cache.disable()


def report_impressions(impressions_cache, sdk_api):
    """If the reporting process is enabled (through the impressions cache), this function collects
    the impressions from the cache and sends them to Split through the events API. If the process
    fails, no exceptions are raised (but they are logged) and the process is disabled.
    """
    try:
        if not impressions_cache.is_enabled():
            return

        impressions = impressions_cache.fetch_all_and_clear()
        test_impressions_data = build_impressions_data(impressions)

        _logger.debug('Impressions to send: %s' % test_impressions_data)

        if len(test_impressions_data) > 0:
            _logger.info('Posting impressions for features: %s.', ', '.join(impressions.keys()))
            sdk_api.test_impressions(test_impressions_data)
    except:
        _logger.exception('Exception caught report impressions. Disabling impressions log.')
        impressions_cache.disable()


def report_metrics(metrics_cache, sdk_api):
    """If the reporting process is enabled (through the metrics cache), this function collects
    the time, count and gauge from the cache and sends them to Split through the events API. If the
    process fails, no exceptions are raised (but they are logged) and the process is disabled."""
    try:
        if not metrics_cache.is_enabled():
            return

        time = metrics_cache.fetch_all_times_and_clear()
        if len(time) > 0:
            _logger.info('Sending times metrics...')
            sdk_api.metrics_times(time)

        metrics = metrics_cache.fetch_all_and_clear()
        if 'count' in metrics and len(metrics['count']) > 0:
            _logger.info('Sending counters metrics...')
            sdk_api.metrics_counters(metrics['count'])

        if 'gauge' in metrics and len(metrics['gauge']) > 0:
            _logger.info('Sending gauge metrics...')
            sdk_api.metrics_gauge(metrics['gauge'])
    except:
        _logger.exception('Exception caught reporting metrics')
        metrics_cache.disable()
