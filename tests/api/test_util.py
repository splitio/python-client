"""Split API tests module."""

import pytest
import unittest.mock as mock

from splitio.api import headers_from_metadata
from splitio.client.util import SdkMetadata
from splitio.engine.telemetry import TelemetryStorageProducer
from splitio.storage.inmemmory import InMemoryTelemetryStorage
from splitio.models.telemetry import HTTPExceptionsAndLatencies


class UtilTests(object):
    """Util test cases."""

    def test_headers_from_metadata(self, mocker):
        """Test headers from metadata call."""
        metadata = headers_from_metadata(SdkMetadata('1.0', 'some', '1.2.3.4'))
        assert metadata['SplitSDKVersion'] == '1.0'
        assert metadata['SplitSDKMachineIP'] == '1.2.3.4'
        assert metadata['SplitSDKMachineName'] == 'some'
        assert 'SplitSDKClientKey' not in metadata

        metadata = headers_from_metadata(SdkMetadata('1.0', 'some', '1.2.3.4'), 'abcd')
        assert metadata['SplitSDKVersion'] == '1.0'
        assert metadata['SplitSDKMachineIP'] == '1.2.3.4'
        assert metadata['SplitSDKMachineName'] == 'some'
        assert metadata['SplitSDKClientKey'] == 'abcd'

        metadata = headers_from_metadata(SdkMetadata('1.0', 'some', 'NA'))
        assert metadata['SplitSDKVersion'] == '1.0'
        assert 'SplitSDKMachineIP' not in metadata
        assert 'SplitSDKMachineName' not in metadata
        assert 'SplitSDKClientKey' not in metadata

        metadata = headers_from_metadata(SdkMetadata('1.0', 'some', 'unknown'))
        assert metadata['SplitSDKVersion'] == '1.0'
        assert 'SplitSDKMachineIP' not in metadata
        assert 'SplitSDKMachineName' not in metadata
        assert 'SplitSDKClientKey' not in metadata
