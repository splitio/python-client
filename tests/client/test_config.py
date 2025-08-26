"""Configuration unit tests."""
# pylint: disable=protected-access,no-self-use,line-too-long
import pytest
from splitio.client import config
from splitio.engine.impressions.impressions import ImpressionsMode
from splitio.models.fallback_treatment import FallbackTreatment
from splitio.models.fallback_config import FallbackConfig, FallbackTreatmentsConfiguration

class ConfigSanitizationTests(object):
    """Inmemory storage-based integration tests."""

    def test_parse_operation_mode(self):
        """Make sure operation mode is correctly captured."""
        assert (config._parse_operation_mode('some', {})) == ('standalone', 'memory')
        assert (config._parse_operation_mode('localhost', {})) == ('localhost', 'localhost')
        assert (config._parse_operation_mode('some', {'redisHost': 'x'})) == ('consumer', 'redis')
        assert (config._parse_operation_mode('some', {'storageType': 'pluggable'})) == ('consumer', 'pluggable')
        assert (config._parse_operation_mode('some', {'storageType': 'custom2'})) == ('standalone', 'memory')

    def test_sanitize_imp_mode(self):
        """Test sanitization of impressions mode."""
        mode, rate = config._sanitize_impressions_mode('memory', 'OPTIMIZED', 1)
        assert mode == ImpressionsMode.OPTIMIZED
        assert rate == 60

        mode, rate = config._sanitize_impressions_mode('memory', 'DEBUG', 1)
        assert mode == ImpressionsMode.DEBUG
        assert rate == 1

        mode, rate = config._sanitize_impressions_mode('redis', 'OPTIMIZED', 1)
        assert mode == ImpressionsMode.OPTIMIZED
        assert rate == 60

        mode, rate = config._sanitize_impressions_mode('redis', 'debug', 1)
        assert mode == ImpressionsMode.DEBUG
        assert rate == 1

        mode, rate = config._sanitize_impressions_mode('memory', 'ANYTHING', 200)
        assert mode == ImpressionsMode.OPTIMIZED
        assert rate == 200

        mode, rate = config._sanitize_impressions_mode('pluggable', 'ANYTHING', 200)
        assert mode == ImpressionsMode.OPTIMIZED
        assert rate == 200

        mode, rate = config._sanitize_impressions_mode('pluggable', 'NONE', 200)
        assert mode == ImpressionsMode.NONE
        assert rate == 200

        mode, rate = config._sanitize_impressions_mode('pluggable', 'OPTIMIZED', 200)
        assert mode == ImpressionsMode.OPTIMIZED
        assert rate == 200

        mode, rate = config._sanitize_impressions_mode('memory', 43, -1)
        assert mode == ImpressionsMode.OPTIMIZED
        assert rate == 60

        mode, rate = config._sanitize_impressions_mode('memory', 'OPTIMIZED')
        assert mode == ImpressionsMode.OPTIMIZED
        assert rate == 300

        mode, rate = config._sanitize_impressions_mode('memory', 'DEBUG')
        assert mode == ImpressionsMode.DEBUG
        assert rate == 60

    def test_sanitize(self, mocker):
        """Test sanitization."""
        _logger = mocker.Mock()
        mocker.patch('splitio.client.config._LOGGER', new=_logger)
        configs = {}
        processed = config.sanitize('some', configs)
        assert processed['redisLocalCacheEnabled']  # check default is True
        assert processed['flagSetsFilter'] is None
        assert processed['httpAuthenticateScheme'] is config.AuthenticateScheme.NONE

        processed = config.sanitize('some', {'redisHost': 'x', 'flagSetsFilter': ['set']})
        assert processed['flagSetsFilter'] is None

        processed = config.sanitize('some', {'storageType': 'pluggable', 'flagSetsFilter': ['set']})
        assert processed['flagSetsFilter'] is None

        processed = config.sanitize('some', {'httpAuthenticateScheme': 'KERBEROS_spnego'})
        assert processed['httpAuthenticateScheme'] is config.AuthenticateScheme.KERBEROS_SPNEGO

        processed = config.sanitize('some', {'httpAuthenticateScheme': 'kerberos_proxy'})
        assert processed['httpAuthenticateScheme'] is config.AuthenticateScheme.KERBEROS_PROXY

        processed = config.sanitize('some', {'httpAuthenticateScheme': 'anything'})
        assert processed['httpAuthenticateScheme'] is config.AuthenticateScheme.NONE

        processed = config.sanitize('some', {'httpAuthenticateScheme': 'NONE'})
        assert processed['httpAuthenticateScheme'] is config.AuthenticateScheme.NONE
        
        _logger.reset_mock()
        processed = config.sanitize('some', {'fallbackTreatmentsConfiguration': 'NONE'})
        assert processed['fallbackTreatmentsConfiguration'].fallback_config == None
        assert _logger.warning.mock_calls[1] == mocker.call("Config: fallbackTreatmentsConfiguration parameter should be of `FallbackTreatmentsConfiguration` class.")

        _logger.reset_mock()
        processed = config.sanitize('some', {'fallbackTreatmentsConfiguration': FallbackTreatmentsConfiguration(123)})
        assert processed['fallbackTreatmentsConfiguration'].fallback_config.global_fallback_treatment == None
        assert _logger.warning.mock_calls[1] == mocker.call("Config: fallback_config parameter should be of `FallbackConfig` class.")

        _logger.reset_mock()
        processed = config.sanitize('some', {'fallbackTreatmentsConfiguration': FallbackTreatmentsConfiguration(FallbackConfig(FallbackTreatment("123")))})
        assert processed['fallbackTreatmentsConfiguration'].fallback_config.global_fallback_treatment == None
        assert _logger.warning.mock_calls[1] == mocker.call("Config: global fallbacktreatment parameter is discarded.")
        
        fb = FallbackTreatmentsConfiguration(FallbackConfig(FallbackTreatment('on')))
        processed = config.sanitize('some', {'fallbackTreatmentsConfiguration': fb})
        assert processed['fallbackTreatmentsConfiguration'].fallback_config.global_fallback_treatment.treatment == fb.fallback_config.global_fallback_treatment.treatment

        fb = FallbackTreatmentsConfiguration(FallbackConfig(FallbackTreatment('on'), {"flag": FallbackTreatment("off")}))
        processed = config.sanitize('some', {'fallbackTreatmentsConfiguration': fb})
        assert processed['fallbackTreatmentsConfiguration'].fallback_config.global_fallback_treatment.treatment == fb.fallback_config.global_fallback_treatment.treatment
        assert processed['fallbackTreatmentsConfiguration'].fallback_config.by_flag_fallback_treatment["flag"] == fb.fallback_config.by_flag_fallback_treatment["flag"]

        _logger.reset_mock()
        fb = FallbackTreatmentsConfiguration(FallbackConfig(None, {"flag#%": FallbackTreatment("off"), "flag2": FallbackTreatment("on")}))
        processed = config.sanitize('some', {'fallbackTreatmentsConfiguration': fb})
        assert len(processed['fallbackTreatmentsConfiguration'].fallback_config.by_flag_fallback_treatment) == 1
        assert processed['fallbackTreatmentsConfiguration'].fallback_config.by_flag_fallback_treatment.get("flag2") == fb.fallback_config.by_flag_fallback_treatment["flag2"]
        assert _logger.warning.mock_calls[1] == mocker.call('Config: fallback treatment parameter for feature flag %s is discarded.', 'flag#%')