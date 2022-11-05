import json

from splitio.api.telemetry import TelemetryAPI
from splitio.engine.telemetry import TelemetryStorageConsumer

class TelemetrySynchronizer(object):
    """Telemetry synchronizer class."""

    def __init__(self, telemetry_consumer, split_storage, segment_storage, telemetry_api):
        """Initialize Telemetry sync class."""
        self._telemetry_submitter = TelemetrySubmitter(telemetry_consumer, split_storage, segment_storage, telemetry_api)

    def synchronize_config(self):
        """synchronize initial config data class."""
        self._telemetry_submitter.synchronize_config()

    def synchronize_stats(self):
        """synchronize runtime stats class."""
        self._telemetry_submitter.synchronize_stats()

class TelemetrySubmitter(object):
    """Telemetry sumbitter class."""

    def __init__(self, telemetry_consumer, split_storage, segment_storage, telemetry_api):
        """Initialize all producer classes."""
        self._telemetry_init_consumer = telemetry_consumer.get_telemetry_init_consumer()
        self._telemetry_evaluation_consumer = telemetry_consumer.get_telemetry_evaluation_consumer()
        self._telemetry_runtime_consumer = telemetry_consumer.get_telemetry_runtime_consumer()
        self._telemetry_api = telemetry_api
        self._split_storage = split_storage
        self._segment_storage = segment_storage

    def synchronize_config(self):
        """synchronize initial config data classe."""
        self._telemetry_api.record_init(self._telemetry_init_consumer.get_config_stats_to_json())

    def synchronize_stats(self):
        """synchronize runtime stats class."""
        self._telemetry_api.record_stats(self._build_stats())

    def _build_stats(self):
        """Format stats to JSON."""
        merged_dict = {
            'spC': self._split_storage.get_splits_count(),
            'seC': self._segment_storage.get_segments_count(),
            'skC': self._segment_storage.get_segments_keys_count()
        }
        merged_dict.update(self._telemetry_runtime_consumer.pop_formatted_stats())
        merged_dict.update(self._telemetry_evaluation_consumer.pop_formatted_stats())
        return merged_dict
