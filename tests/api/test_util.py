"""Split API tests module."""

import pytest
import unittest.mock as mock

from splitio.api.commons import headers_from_metadata, record_telemetry
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

    def test_record_telemetry(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        record_telemetry(200, 100, HTTPExceptionsAndLatencies.SPLIT, telemetry_runtime_producer)
        assert(telemetry_storage._last_synchronization._split != 0)
        assert(telemetry_storage._http_latencies._split[0] == 1)

        record_telemetry(200, 150, HTTPExceptionsAndLatencies.SEGMENT, telemetry_runtime_producer)
        assert(telemetry_storage._last_synchronization._segment != 0)
        assert(telemetry_storage._http_latencies._segment[0] == 1)

        record_telemetry(401, 100, HTTPExceptionsAndLatencies.SPLIT, telemetry_runtime_producer)
        assert(telemetry_storage._http_sync_errors._split['401'] == 1)
        assert(telemetry_storage._http_latencies._split[0] == 2)

        record_telemetry(503, 300, HTTPExceptionsAndLatencies.SEGMENT, telemetry_runtime_producer)
        assert(telemetry_storage._http_sync_errors._segment['503'] == 1)
        assert(telemetry_storage._http_latencies._segment[0] == 2)
