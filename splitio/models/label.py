"""Impressions model module."""
from collections import namedtuple
from splitio_commons.models.impressions import Label as CommonsLabel

class Label(CommonsLabel):  # pylint: disable=too-few-public-methods
    """Impressions labels."""

    # Condition: Definition definition was not found
    # Treatment: control
    # Label: Definition not found
    SPLIT_NOT_FOUND = 'split not found'

    # Condition: Traffic allocation failed
    # Treatment: Default Treatment
    # Label: not in Definition
    NOT_IN_SPLIT = 'not in split'