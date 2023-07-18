"""Telemetry Sync Class."""
import abc

from splitio.api.telemetry import TelemetryAPI
from splitio.engine.telemetry import TelemetryStorageConsumer
from splitio.models.telemetry import UpdateFromSSE

class TelemetrySynchronizer(object):
    """Telemetry synchronizer class."""

    def __init__(self, telemetry_submitter):
        """Initialize Telemetry sync class."""
        self._telemetry_submitter = telemetry_submitter

    def synchronize_config(self):
        """synchronize initial config data class."""
        self._telemetry_submitter.synchronize_config()

    def synchronize_stats(self):
        """synchronize runtime stats class."""
        self._telemetry_submitter.synchronize_stats()

class TelemetrySubmitter(object, metaclass=abc.ABCMeta):
    """Telemetry sumbitter interface."""

    @abc.abstractmethod
    def synchronize_config(self):
        """synchronize initial config data classe."""

    @abc.abstractmethod
    def synchronize_stats(self):
        """synchronize runtime stats class."""

class InMemoryTelemetrySubmitter(object):
    """Telemetry sumbitter class."""

    def __init__(self, telemetry_consumer, feature_flag_storage, segment_storage, telemetry_api):
        """Initialize all producer classes."""
        self._telemetry_init_consumer = telemetry_consumer.get_telemetry_init_consumer()
        self._telemetry_evaluation_consumer = telemetry_consumer.get_telemetry_evaluation_consumer()
        self._telemetry_runtime_consumer = telemetry_consumer.get_telemetry_runtime_consumer()
        self._telemetry_api = telemetry_api
        self._feature_flag_storage = feature_flag_storage
        self._segment_storage = segment_storage

    def synchronize_config(self):
        """synchronize initial config data classe."""
        self._telemetry_api.record_init(self._telemetry_init_consumer.get_config_stats())

    def synchronize_stats(self):
        """synchronize runtime stats class."""
        self._telemetry_api.record_stats(self._build_stats())

    def _build_stats(self):
        """
        Format stats to Dict.

        :returns: formatted stats
        :rtype: Dict
        """
        merged_dict = {
            'spC': self._feature_flag_storage.get_splits_count(),
            'seC': self._segment_storage.get_segments_count(),
            'skC': self._segment_storage.get_segments_keys_count()
        }
        merged_dict.update(self._telemetry_runtime_consumer.pop_formatted_stats())
        merged_dict.update(self._telemetry_evaluation_consumer.pop_formatted_stats())
        return merged_dict

class RedisTelemetrySubmitter(object):
    """Telemetry sumbitter class."""

    def __init__(self, telemetry_storage):
        """Initialize all producer classes."""
        self._telemetry_storage = telemetry_storage

    def synchronize_config(self):
        """synchronize initial config data classe."""
        self._telemetry_storage.push_config_stats()

    def synchronize_stats(self):
        """No implementation."""
        pass


class LocalhostTelemetrySubmitter(object):
    """Telemetry sumbitter class."""

    def synchronize_config(self):
        """No implementation."""
        pass

    def synchronize_stats(self):
        """No implementation."""
        pass
