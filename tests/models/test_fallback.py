from splitio.models.fallback_treatment import FallbackTreatment
from splitio.models.fallback_config import FallbackConfig

class FallbackTreatmentModelTests(object):
    """Fallback treatment model tests."""

    def test_working(self):
        fallback_treatment = FallbackTreatment("on", {"prop": "val"})
        assert fallback_treatment.config == '{"prop": "val"}'
        assert fallback_treatment.treatment == 'on'
        
        fallback_treatment = FallbackTreatment("off")
        assert fallback_treatment.config == None
        assert fallback_treatment.treatment == 'off'
        
class FallbackConfigModelTests(object):
    """Fallback treatment model tests."""

    def test_working(self):
        global_fb = FallbackTreatment("on")
        flag_fb = FallbackTreatment("off")
        fallback_config = FallbackConfig(global_fb, {"flag1": flag_fb})
        assert fallback_config.global_fallback_treatment == global_fb
        assert fallback_config.by_flag_fallback_treatment == {"flag1": flag_fb}

        fallback_config.global_fallback_treatment = None
        assert fallback_config.global_fallback_treatment == None
        
        fallback_config.by_flag_fallback_treatment["flag2"] = flag_fb
        assert fallback_config.by_flag_fallback_treatment == {"flag1": flag_fb, "flag2": flag_fb}
        
                        