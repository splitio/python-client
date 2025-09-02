"""Segment module."""
import json

class FallbackTreatment(object):
    """FallbackTreatment object class."""

    def __init__(self, treatment, config=None):
        """
        Class constructor.

        :param treatment: treatment.
        :type treatment: str

        :param config: config.
        :type config: json
        """
        self._treatment = treatment
        self._config = None
        if config != None:
            self._config = json.dumps(config)
        self._label_prefix = "fallback - "

    @property
    def treatment(self):
        """Return treatment."""
        return self._treatment

    @property
    def config(self):
        """Return config."""
        return self._config
    
    @property
    def label_prefix(self):
        """Return label prefix."""
        return self._label_prefix