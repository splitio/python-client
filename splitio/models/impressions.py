"""Impressions model module."""
from collections import namedtuple


Impression = namedtuple(
    'Impression',
    [
        'matching_key',
        'feature_name',
        'treatment',
        'label',
        'change_number',
        'bucketing_key',
        'time',
        'previous_time'
    ]
)

# pre-python3.7 hack to make previous_time optional
Impression.__new__.__defaults__ = (None,)


class Label(object):  # pylint: disable=too-few-public-methods
    """Impressions labels."""

    # Condition: Split Was Killed
    # Treatment: Default treatment
    # Label: killed
    KILLED = 'killed'

    # Condition: No condition matched
    # Treatment: Default Treatment
    # Label: no condition matched
    NO_CONDITION_MATCHED = 'default rule'

    # Condition: Split definition was not found
    # Treatment: control
    # Label: split not found
    SPLIT_NOT_FOUND = 'definition not found'

    # Condition: Traffic allocation failed
    # Treatment: Default Treatment
    # Label: not in split
    NOT_IN_SPLIT = 'not in split'

    # Condition: There was an exception
    # Treatment: control
    # Label: exception
    EXCEPTION = 'exception'

    # Condition: Evaluation requested while client not ready
    # Treatment: control
    # Label: not ready
    NOT_READY = 'not ready'
