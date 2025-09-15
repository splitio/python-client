"""Segment module."""
from splitio.models.fallback_treatment import FallbackTreatment
from splitio.client.client import CONTROL

class FallbackTreatmentsConfiguration(object):
    """FallbackTreatmentsConfiguration object class."""

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

class FallbackTreatmentCalculator(object):
    """FallbackTreatmentCalculator object class."""

    def __init__(self, fallback_treatment_configuration):
        """
        Class constructor.

        :param fallback_treatment_configuration: fallback treatment configuration
        :type fallback_treatment_configuration: FallbackTreatmentsConfiguration
        """
        self._label_prefix = "fallback - "
        self._fallback_treatments_configuration = fallback_treatment_configuration

    @property
    def fallback_treatments_configuration(self):
        """Return fallback treatment configuration."""
        return self._fallback_treatments_configuration

    def resolve(self, flag_name, label):
        if self._fallback_treatments_configuration != None:
            if self._fallback_treatments_configuration.by_flag_fallback_treatment != None \
                and self._fallback_treatments_configuration.by_flag_fallback_treatment.get(flag_name) != None:
                return self._copy_with_label(self._fallback_treatments_configuration.by_flag_fallback_treatment.get(flag_name), \
                        self._resolve_label(label))
            
            if self._fallback_treatments_configuration.global_fallback_treatment != None:
                return self._copy_with_label(self._fallback_treatments_configuration.global_fallback_treatment, \
                        self._resolve_label(label))
            
        return FallbackTreatment(CONTROL, None, label)
    
    def _resolve_label(self, label):
        if label == None:
            return None
        
        return self._label_prefix + label

    def _copy_with_label(self, fallback_treatment, label):
        return FallbackTreatment(fallback_treatment.treatment, fallback_treatment.config, label)
    
        