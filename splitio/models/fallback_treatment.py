"""Segment module."""
import json

class FallbackTreatment(object):
    """Segment object class."""

    def __init__(self, treatment, config=None, label=None):
        """
        Class constructor.

        :param treatment: treatment.
        :type treatment: str

        :param config: config.
        :type config: json
        """
        self._treatment = treatment
        self._config = config
        self._label = label

    @property
    def treatment(self):
        """Return treatment."""
        return self._treatment

    @property
    def config(self):
        """Return config."""
        return self._config
    
    @property
    def label(self):
        """Return label prefix."""
        return self._label