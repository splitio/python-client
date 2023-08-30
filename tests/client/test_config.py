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

    def test_sanitize_flag_sets(self):
        """Test sanitization for flag sets."""
        flag_sets = config.sanitize_flag_sets([' set1', 'set2 ', 'set3'])
        assert flag_sets == ['set1', 'set2', 'set3']

        flag_sets = config.sanitize_flag_sets(['1set', '_set2'])
        assert flag_sets == ['1set']

        flag_sets = config.sanitize_flag_sets(['Set1', 'SET2'])
        assert flag_sets == ['set1', 'set2']

        flag_sets = config.sanitize_flag_sets(['se\t1', 's/et2', 's*et3', 's!et4', 'se@t5', 'se#t5', 'se$t5', 'se^t5', 'se%t5', 'se&t5'])
        assert flag_sets == []

        flag_sets = config.sanitize_flag_sets(['set4', 'set1', 'set3', 'set1'])
        assert flag_sets == ['set1', 'set3', 'set4']

        flag_sets = config.sanitize_flag_sets(['w' * 50, 's' * 51])
        assert flag_sets == ['w' * 50]

        flag_sets = config.sanitize_flag_sets('set1')
        assert flag_sets == []

        flag_sets = config.sanitize_flag_sets([12, 33])
        assert flag_sets == []
