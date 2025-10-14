from splitio.models.fallback_treatment import FallbackTreatment
from splitio.models.fallback_config import FallbackTreatmentsConfiguration, FallbackTreatmentCalculator

class FallbackTreatmentModelTests(object):
    """Fallback treatment model tests."""

    def test_working(self):
        fallback_treatment = FallbackTreatment("on", '{"prop": "val"}')
        assert fallback_treatment.config == '{"prop": "val"}'
        assert fallback_treatment.treatment == 'on'
        
        fallback_treatment = FallbackTreatment("off")
        assert fallback_treatment.config == None
        assert fallback_treatment.treatment == 'off'
        
class FallbackTreatmentsConfigModelTests(object):
    """Fallback treatment configuration model tests."""

    def test_working(self):
        global_fb = FallbackTreatment("on")
        flag_fb = FallbackTreatment("off")
        fallback_config = FallbackTreatmentsConfiguration(global_fb, {"flag1": flag_fb})
        assert fallback_config.global_fallback_treatment == global_fb
        assert fallback_config.by_flag_fallback_treatment == {"flag1": flag_fb}

        fallback_config.global_fallback_treatment = None
        assert fallback_config.global_fallback_treatment == None
        
        fallback_config.by_flag_fallback_treatment["flag2"] = flag_fb
        assert fallback_config.by_flag_fallback_treatment == {"flag1": flag_fb, "flag2": flag_fb}

        fallback_config = FallbackTreatmentsConfiguration("on", {"flag1": "off"})
        assert isinstance(fallback_config.global_fallback_treatment, FallbackTreatment)
        assert fallback_config.global_fallback_treatment.treatment == "on"
        
        assert isinstance(fallback_config.by_flag_fallback_treatment["flag1"], FallbackTreatment)
        assert fallback_config.by_flag_fallback_treatment["flag1"].treatment == "off"
        

class FallbackTreatmentCalculatorTests(object):
    """Fallback treatment calculator model tests."""

    def test_working(self):
        fallback_config = FallbackTreatmentsConfiguration(FallbackTreatment("on" ,"{}"), None)
        fallback_calculator = FallbackTreatmentCalculator(fallback_config)
        assert fallback_calculator.fallback_treatments_configuration == fallback_config
        assert fallback_calculator._label_prefix == "fallback - "
        
        fallback_treatment = fallback_calculator.resolve("feature", "not ready")
        assert fallback_treatment.treatment == "on"
        assert fallback_treatment.label == "fallback - not ready"
        assert fallback_treatment.config == "{}"
        
        fallback_calculator._fallback_treatments_configuration = FallbackTreatmentsConfiguration(FallbackTreatment("on" ,"{}"), {'feature': FallbackTreatment("off" , '{"prop": "val"}')})
        fallback_treatment = fallback_calculator.resolve("feature", "not ready")
        assert fallback_treatment.treatment == "off"
        assert fallback_treatment.label == "fallback - not ready"
        assert fallback_treatment.config == '{"prop": "val"}'
        
        fallback_treatment = fallback_calculator.resolve("feature2", "not ready")
        assert fallback_treatment.treatment == "on"
        assert fallback_treatment.label == "fallback - not ready"
        assert fallback_treatment.config == "{}"
