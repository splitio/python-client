"""Configuration unit tests."""
# pylint: disable=protected-access,no-self-use,line-too-long
import pytest
from splitio.client import config
from splitio.engine.impressions.impressions import ImpressionsMode
from splitio.models.fallback_treatment import FallbackTreatment
from splitio.models.fallback_config import FallbackTreatmentsConfiguration

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
        processed = config.sanitize('some', {'fallbackTreatments': 'NONE'})
        assert processed['fallbackTreatments'] == None
        assert _logger.warning.mock_calls[1] == mocker.call("Config: fallbackTreatments parameter should be of `FallbackTreatmentsConfiguration` class.")

        _logger.reset_mock()
        processed = config.sanitize('some', {'fallbackTreatments': FallbackTreatmentsConfiguration(123)})
        assert processed['fallbackTreatments'].global_fallback_treatment == None
        assert _logger.warning.mock_calls[1] == mocker.call("Config: global fallbacktreatment parameter is discarded.")

        _logger.reset_mock()
        processed = config.sanitize('some', {'fallbackTreatments': FallbackTreatmentsConfiguration(FallbackTreatment(123))})
        assert processed['fallbackTreatments'].global_fallback_treatment == None
        assert _logger.warning.mock_calls[1] == mocker.call("Config: global fallbacktreatment parameter is discarded.")
        
        fb = FallbackTreatmentsConfiguration(FallbackTreatment('on'))
        processed = config.sanitize('some', {'fallbackTreatments': fb})
        assert processed['fallbackTreatments'].global_fallback_treatment.treatment == fb.global_fallback_treatment.treatment
        assert processed['fallbackTreatments'].global_fallback_treatment.label == None

        fb = FallbackTreatmentsConfiguration(FallbackTreatment('on'), {"flag": FallbackTreatment("off")})
        processed = config.sanitize('some', {'fallbackTreatments': fb})
        assert processed['fallbackTreatments'].global_fallback_treatment.treatment == fb.global_fallback_treatment.treatment
        assert processed['fallbackTreatments'].by_flag_fallback_treatment["flag"] == fb.by_flag_fallback_treatment["flag"]
        assert processed['fallbackTreatments'].by_flag_fallback_treatment["flag"].label == None

        _logger.reset_mock()
        fb = FallbackTreatmentsConfiguration(None, {"flag#%": FallbackTreatment("off"), "flag2": FallbackTreatment("on")})
        processed = config.sanitize('some', {'fallbackTreatments': fb})
        assert len(processed['fallbackTreatments'].by_flag_fallback_treatment) == 1
        assert processed['fallbackTreatments'].by_flag_fallback_treatment.get("flag2") == fb.by_flag_fallback_treatment["flag2"]
        assert _logger.warning.mock_calls[1] == mocker.call('Config: fallback treatment parameter for feature flag %s is discarded.', 'flag#%')

    def test_sanitize_defaults_proxy_to_none(self):
        """Proxy fields default to None when not supplied."""
        processed = config.sanitize('some', {})
        assert processed['proxyHost'] is None
        assert processed['proxyPort'] is None
        assert processed['proxyProtocol'] is None

    def test_sanitize_proxy_valid(self):
        """A fully valid proxy config is preserved as-is."""
        processed = config.sanitize('some', {
            'proxyHost': 'proxy.example.com',
            'proxyPort': 8080,
            'proxyProtocol': 'http',
        })
        assert processed['proxyHost'] == 'proxy.example.com'
        assert processed['proxyPort'] == 8080
        assert processed['proxyProtocol'] == 'http'

        processed = config.sanitize('some', {
            'proxyHost': 'proxy.example.com',
            'proxyPort': 443,
            'proxyProtocol': 'https',
        })
        assert processed['proxyHost'] == 'proxy.example.com'
        assert processed['proxyPort'] == 443
        assert processed['proxyProtocol'] == 'https'

    def test_sanitize_proxy_host_only_disables_proxy(self, mocker):
        """If proxyHost is set but port/protocol are missing, proxyHost is cleared."""
        _logger = mocker.Mock()
        mocker.patch('splitio.client.config._LOGGER', new=_logger)

        processed = config.sanitize('some', {'proxyHost': 'proxy.example.com'})
        assert processed['proxyHost'] is None
        assert processed['proxyPort'] is None
        assert processed['proxyProtocol'] is None
        _logger.warning.assert_any_call(
            'To use proxy, parameters `proxyHost`, `proxyPort` and `proxyProtocol` must be set as str, int and str instances respectively.'
        )

    def test_sanitize_proxy_missing_protocol_disables_proxy(self, mocker):
        """Missing proxyProtocol invalidates the proxy config."""
        _logger = mocker.Mock()
        mocker.patch('splitio.client.config._LOGGER', new=_logger)

        processed = config.sanitize('some', {
            'proxyHost': 'proxy.example.com',
            'proxyPort': 8080,
        })
        assert processed['proxyHost'] is None
        _logger.warning.assert_any_call(
            'To use proxy, parameters `proxyHost`, `proxyPort` and `proxyProtocol` must be set as str, int and str instances respectively.'
        )

    def test_sanitize_proxy_missing_port_disables_proxy(self, mocker):
        """Missing proxyPort invalidates the proxy config."""
        _logger = mocker.Mock()
        mocker.patch('splitio.client.config._LOGGER', new=_logger)

        processed = config.sanitize('some', {
            'proxyHost': 'proxy.example.com',
            'proxyProtocol': 'https',
        })
        assert processed['proxyHost'] is None
        _logger.warning.assert_any_call(
            'To use proxy, parameters `proxyHost`, `proxyPort` and `proxyProtocol` must be set as str, int and str instances respectively.'
        )

    def test_sanitize_proxy_wrong_types_disables_proxy(self, mocker):
        """Wrong types for proxy fields invalidate the proxy config."""
        _logger = mocker.Mock()
        mocker.patch('splitio.client.config._LOGGER', new=_logger)

        processed = config.sanitize('some', {
            'proxyHost': 'proxy.example.com',
            'proxyPort': '8080',  # str, not int
            'proxyProtocol': 'https',
        })
        assert processed['proxyHost'] is None

        _logger.reset_mock()
        processed = config.sanitize('some', {
            'proxyHost': 'proxy.example.com',
            'proxyPort': 8080,
            'proxyProtocol': 123,  # int, not str
        })
        assert processed['proxyHost'] is None

        _logger.reset_mock()
        processed = config.sanitize('some', {
            'proxyHost': 12345,  # int, not str
            'proxyPort': 8080,
            'proxyProtocol': 'https',
        })
        assert processed['proxyHost'] is None

    def test_sanitize_proxy_invalid_protocol_defaults_to_https(self, mocker):
        """An unrecognized proxyProtocol falls back to 'https'."""
        _logger = mocker.Mock()
        mocker.patch('splitio.client.config._LOGGER', new=_logger)

        processed = config.sanitize('some', {
            'proxyHost': 'proxy.example.com',
            'proxyPort': 8080,
            'proxyProtocol': 'ftp',
        })
        assert processed['proxyHost'] == 'proxy.example.com'
        assert processed['proxyPort'] == 8080
        assert processed['proxyProtocol'] == 'https'
        _logger.warning.assert_any_call(
            'Parameter `proxyProtocol` should be either `http` or `https`, defaulting to `https`'
        )