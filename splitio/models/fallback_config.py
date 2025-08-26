"""Segment module."""

class FallbackTreatmentsConfiguration(object):
    """FallbackConfiguration object class."""

    def __init__(self, fallback_config):
        """
        Class constructor.

        :param fallback_config: fallback config object.
        :type fallback_config: FallbackConfig

        :param by_flag_fallback_treatment: Dict of flags and their fallback treatment
        :type by_flag_fallback_treatment: {str: FallbackTreatment}
        """
        self._fallback_config = fallback_config
        
    @property
    def fallback_config(self):
        """Return fallback config."""
        return self._fallback_config
    
    @fallback_config.setter
    def fallback_config(self, new_value):
        """Set fallback config."""
        self._fallback_config = new_value

class FallbackConfig(object):
    """FallbackConfig object class."""

    def __init__(self, global_fallback_treatment=None, by_flag_fallback_treatment=None):
        """
        Class constructor.

        :param global_fallback_treatment: global FallbackTreatment.
        :type global_fallback_treatment: FallbackTreatment

        :param by_flag_fallback_treatment: Dict of flags and their fallback treatment
        :type by_flag_fallback_treatment: {str: FallbackTreatment}
        """
        self._global_fallback_treatment = global_fallback_treatment
        self._by_flag_fallback_treatment = by_flag_fallback_treatment

    @property
    def global_fallback_treatment(self):
        """Return global fallback treatment."""
        return self._global_fallback_treatment

    @global_fallback_treatment.setter
    def global_fallback_treatment(self, new_value):
        """Set global fallback treatment."""
        self._global_fallback_treatment = new_value
    
    @property
    def by_flag_fallback_treatment(self):
        """Return by flag fallback treatment."""
        return self._by_flag_fallback_treatment
    
    @by_flag_fallback_treatment.setter
    def by_flag_fallback_treatment(self, new_value):
        """Set global fallback treatment."""
        self.by_flag_fallback_treatment = new_value
