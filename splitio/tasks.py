"""This module contains everything related to update tasks"""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging

from .splits import Status

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

    while True:
        response = segment_change_fetcher.fetch(segment_name, till)

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

            if till >= response['till']:
                _logger.debug("change_number is greater or equal than 'till'")
                return

            if 'splits' in response and len(response['splits']) > 0:
                _logger.debug("Missing or empty 'splits' field in response. response = %s",
                              response)
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
                    split_cache.add_split(split_change['name'], parsed_split)

                if len(added_features) > 0:
                    _logger.info('Updated features: %s', added_features)

                if len(removed_features) > 0:
                    _logger.info('Deleted features: %s', removed_features)

            till = response['till']
            split_cache.set_change_number(response['till'])
    except:
        _logger.exception('Exception caught updating split definitions')
        split_cache.disable()
