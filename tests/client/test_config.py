"""Configuration unit tests."""
# pylint: disable=protected-access,no-self-use,line-too-long

from splitio.client import config
from splitio.engine.impressions.impressions import ImpressionsMode


class ConfigSanitizationTests(object):
    """Inmemory storage-based integration tests."""

    def test_parse_operation_mode(self):
        """Make sure operation mode is correctly captured."""
        assert config._parse_operation_mode('some', {}) == 'inmemory-standalone'
        assert config._parse_operation_mode('localhost', {}) == 'localhost-standalone'
        assert config._parse_operation_mode('some', {'redisHost': 'x'}) == 'redis-consumer'
        assert config._parse_operation_mode('some', {'storageType': 'custom'}) == 'custom'
        assert config._parse_operation_mode('some', {'storageType': 'custom2'}) == 'inmemory-standalone'

    def test_sanitize_imp_mode(self):
        """Test sanitization of impressions mode."""
        mode, rate = config._sanitize_impressions_mode('inmemory-standalone', 'OPTIMIZED', 1)
        assert mode == ImpressionsMode.OPTIMIZED
        assert rate == 60

        mode, rate = config._sanitize_impressions_mode('inmemory-standalone', 'DEBUG', 1)
        assert mode == ImpressionsMode.DEBUG
        assert rate == 1

        mode, rate = config._sanitize_impressions_mode('inmemory-standalone', 'debug', 1)
        assert mode == ImpressionsMode.DEBUG
        assert rate == 1

        mode, rate = config._sanitize_impressions_mode('inmemory-standalone', 'ANYTHING', 200)
        assert mode == ImpressionsMode.OPTIMIZED
        assert rate == 200

        mode, rate = config._sanitize_impressions_mode('custom', 'ANYTHING', 200)
        assert mode == ImpressionsMode.DEBUG
        assert rate == 200

        mode, rate = config._sanitize_impressions_mode('custom', 'NONE', 200)
        assert mode == ImpressionsMode.DEBUG
        assert rate == 200

        mode, rate = config._sanitize_impressions_mode('custom', 'OPTIMIZED', 200)
        assert mode == ImpressionsMode.DEBUG
        assert rate == 200

        mode, rate = config._sanitize_impressions_mode('inmemory-standalone', 43, -1)
        assert mode == ImpressionsMode.OPTIMIZED
        assert rate == 60

        mode, rate = config._sanitize_impressions_mode('inmemory-standalone', 'OPTIMIZED')
        assert mode == ImpressionsMode.OPTIMIZED
        assert rate == 300

        mode, rate = config._sanitize_impressions_mode('inmemory-standalone', 'DEBUG')
        assert mode == ImpressionsMode.DEBUG
        assert rate == 60

    def test_sanitize(self):
        """Test sanitization."""
        configs = {}
        processed = config.sanitize('some', configs)

        assert processed['redisLocalCacheEnabled']  # check default is True
