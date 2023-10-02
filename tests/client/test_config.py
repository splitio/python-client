"""Configuration unit tests."""
# pylint: disable=protected-access,no-self-use,line-too-long
import pytest
from splitio.client import config
from splitio.engine.impressions.impressions import ImpressionsMode


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

    def test_sanitize(self):
        """Test sanitization."""
        configs = {}
        processed = config.sanitize('some', configs)
        assert processed['redisLocalCacheEnabled']  # check default is True