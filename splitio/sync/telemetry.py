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
        self._telemetry_api.record_init(json.dumps(self._telemetry_init_consumer.get_config_stats()))

    def synchronize_stats(self):
        """synchronize runtime stats class."""
        self._telemetry_api.record_stats(json.dumps({
            **{'iQ': self._telemetry_runtime_consumer.get_impressions_stats('iQ')},
            **{'iDe': self._telemetry_runtime_consumer.get_impressions_stats('iDe')},
            **{'iDr': self._telemetry_runtime_consumer.get_impressions_stats('iDr')},
            **{'eQ': self._telemetry_runtime_consumer.get_events_stats('eQ')},
            **{'eD': self._telemetry_runtime_consumer.get_events_stats('eD')},
            **{'IS': self._telemetry_runtime_consumer.get_last_synchronization()},
            **{'t': self._telemetry_runtime_consumer.pop_tags()},
            **{'hE': self._telemetry_runtime_consumer.pop_http_errors()},
            **{'hL': self._telemetry_runtime_consumer.pop_http_latencies()},
            **{'aR': self._telemetry_runtime_consumer.pop_auth_rejections()},
            **{'tR': self._telemetry_runtime_consumer.pop_token_refreshes()},
            **{'sE': self._telemetry_runtime_consumer.pop_streaming_events()},
            **{'sL': self._telemetry_runtime_consumer.get_session_length()},
            **{'mE': self._telemetry_evaluation_consumer.pop_exceptions()},
            **{'mL': self._telemetry_evaluation_consumer.pop_latencies()},
            **{'spC': self._split_storage.get_splits_count()},
            **{'seC': self._segment_storage.get_segments_count()},
            **{'skC': self._segment_storage.get_segments_keys_count()},
        }))
