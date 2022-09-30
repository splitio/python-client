"""Telemetry engine classes."""
import json

from  splitio.storage.inmemmory import InMemoryTelemetryStorage

class TelemetryStorageProducer(object):
    """Telemetry storage producer class."""

    def __init__(self, telemetry_storage):
        """Initialize all producer classes."""
        self._telemetry_init_producer = TelemetryInitProducer(telemetry_storage)
        self._telemetry_evaluation_producer = TelemetryEvaluationProducer(telemetry_storage)
        self._telemetry_runtime_producer = TelemetryRuntimeProducer(telemetry_storage)

    def get_telemetry_init_producer(self):
        """get init producer instance."""
        return self._telemetry_init_producer

    def get_telemetry_evaluation_producer(self):
        """get evaluation producer instance."""
        return self._telemetry_evaluation_producer

    def get_telemetry_runtime_producer(self):
        """get runtime producer instance."""
        return self._telemetry_runtime_producer

class TelemetryInitProducer(object):
    """Telemetry init producer class."""

    def __init__(self, telemetry_storage):
        """Constructor."""
        self._telemetry_storage = telemetry_storage

    def record_config(self, config):
        """Record configurations."""
        self._telemetry_storage.record_config(config)

    def record_ready_time(self, ready_time):
        """Record ready time."""
        self._telemetry_storage.record_ready_time(ready_time)

    def record_bur_time_out(self):
        """Record block until ready timeout."""
        self._telemetry_storage.record_bur_time_out()

    def record_not_ready_usage(self):
        """record non-ready usage."""
        self._telemetry_storage.record_not_ready_usage()

class TelemetryEvaluationProducer(object):
    """Telemetry evaluation producer class."""

    def __init__(self, telemetry_storage):
        """Constructor."""
        self._telemetry_storage = telemetry_storage

    def record_latency(self, method, latency):
        """Record method latency time."""
        self._telemetry_storage.record_latency(method, latency)

    def record_exception(self, method):
        """Record method exception time."""
        self._telemetry_storage.record_exception(method)

class TelemetryRuntimeProducer(object):
    """Telemetry runtime producer class."""

    def __init__(self, telemetry_storage):
        """Constructor."""
        self._telemetry_storage = telemetry_storage

    def add_tag(self, tag):
        """Record tag string."""
        self._telemetry_storage.add_tag(tag)

    def record_impression_stats(self, data_type, count):
        """Record impressions stats."""
        self._telemetry_storage.record_impression_stats(data_type, count)

    def record_event_stats(self, data_type, count):
        """Record events stats."""
        self._telemetry_storage.record_event_stats(data_type, count)

    def record_suceessful_sync(self, resource, time):
        """Record successful sync."""
        self._telemetry_storage.record_suceessful_sync(resource, time)

    def record_sync_error(self, resource, status):
        """Record sync error."""
        self._telemetry_storage.record_sync_error(resource, status)

    def record_sync_latency(self, resource, latency):
        """Record latency time."""
        self._telemetry_storage.record_sync_latency(resource, latency)

    def record_auth_rejections(self):
        """Record auth rejection."""
        self._telemetry_storage.record_auth_rejections()

    def record_token_refreshes(self):
        """Record sse token refresh."""
        self._telemetry_storage.record_token_refreshes()

    def record_streaming_event(self, streaming_event):
        """Record incoming streaming event."""
        self._telemetry_storage.record_streaming_event(streaming_event)

    def record_session_length(self, session):
        """Record session length."""
        self._telemetry_storage.record_session_length(session)

class TelemetryStorageConsumer(object):
    """Telemetry storage consumer class."""

    def __init__(self, telemetry_storage):
        """Initialize all consumer classes."""
        self._telemetry_init_consumer = TelemetryInitConsumer(telemetry_storage)
        self._telemetry_evaluation_consumer = TelemetryEvaluationConsumer(telemetry_storage)
        self._telemetry_runtime_consumer = TelemetryRuntimeConsumer(telemetry_storage)

    def get_telemetry_init_consumer(self):
        """Get telemetry init instance"""
        return self._telemetry_init_consumer

    def get_telemetry_evaluation_consumer(self):
        """Get telemetry evaluation instance"""
        return self._telemetry_evaluation_consumer

    def get_telemetry_runtime_consumer(self):
        """Get telemetry runtime instance"""
        return self._telemetry_runtime_consumer

class TelemetryInitConsumer(object):
    """Telemetry init consumer class."""

    def __init__(self, telemetry_storage):
        """Constructor."""
        self._telemetry_storage = telemetry_storage

    def get_bur_time_outs(self):
        """Get block until ready timeout."""
        return self._telemetry_storage.get_bur_time_outs()

    def get_not_ready_usage(self):
        """Get none-ready usage."""
        return self._telemetry_storage.get_not_ready_usage()

    def get_config_stats(self):
        """Get none-ready usage."""
        return self._telemetry_storage.get_config_stats()

    def get_config_stats_to_json(self):
        config_stats = self._telemetry_storage.get_config_stats()
        return json.dumps({
            'oM': config_stats['operationMode'],
            'sT': config_stats['storageType'],
            'sE': config_stats['streamingEnabled'],
            'rR': config_stats['refreshRate'],
            'uO': config_stats['urlOverride'],
            'iQ': config_stats['impressionsQueueSize'],
            'eQ': config_stats['eventsQueueSize'],
            'iM': config_stats['impressionsMode'],
            'iL': config_stats['impressionListener'],
            'hP': config_stats['httpProxy'],
            'aF': config_stats['activeFactoryCount'],
            'rF': config_stats['redundantFactoryCount'],
            'bT': config_stats['blockUntilReadyTimeout'],
            'nR': config_stats['notReady'],
            'uC': config_stats['userConsent'],
            'tR': config_stats['timeUntilReady']}
        )

class TelemetryEvaluationConsumer(object):
    """Telemetry evaluation consumer class."""

    def __init__(self, telemetry_storage):
        """Constructor."""
        self._telemetry_storage = telemetry_storage

    def pop_exceptions(self):
        """Get and reset method exceptions."""
        return self._telemetry_storage.pop_exceptions()

    def pop_latencies(self):
        """Get and reset eval latencies."""
        return self._telemetry_storage.pop_latencies()

    def pop_formatted_stats(self):
        """Get formatted and reset stats."""
        exceptions = self.pop_exceptions()
        latencies = self.pop_latencies()
        return {
            **{'mE': {'t': exceptions['treatment'],
                      'ts': exceptions['treatments'],
                      'tc': exceptions['treatmentWithConfig'],
                      'tcs': exceptions['treatmentsWithConfig'],
                      'tr': exceptions['track']}
               },
            **{'mL':  {'t': latencies['treatment'],
                      'ts': latencies['treatments'],
                      'tc': latencies['treatmentWithConfig'],
                      'tcs': latencies['treatmentsWithConfig'],
                      'tr': latencies['track']}
               },
        }

class TelemetryRuntimeConsumer(object):
    """Telemetry runtime consumer class."""

    def __init__(self, telemetry_storage):
        """Constructor."""
        self._telemetry_storage = telemetry_storage

    def get_impressions_stats(self, type):
        """Get impressions stats"""
        return self._telemetry_storage.get_impressions_stats(type)

    def get_events_stats(self, type):
        """Get events stats"""
        return self._telemetry_storage.get_events_stats(type)

    def get_last_synchronization(self):
        """Get last sync"""
        return self._telemetry_storage.get_last_synchronization()

    def pop_tags(self):
        """Get and reset http errors."""
        return self._telemetry_storage.pop_tags()

    def pop_http_errors(self):
        """Get and reset http errors."""
        return self._telemetry_storage.pop_http_errors()

    def pop_http_latencies(self):
        """Get and reset http latencies."""
        return self._telemetry_storage.pop_http_latencies()

    def pop_auth_rejections(self):
        """Get and reset auth rejections."""
        return self._telemetry_storage.pop_auth_rejections()

    def pop_token_refreshes(self):
        """Get and reset token refreshes."""
        return self._telemetry_storage.pop_token_refreshes()

    def pop_streaming_events(self):
        """Get and reset streaming events."""
        return self._telemetry_storage.pop_streaming_events()

    def get_session_length(self):
        """Get session length"""
        return self._telemetry_storage.get_session_length()

    def pop_formatted_stats(self):
        """Get formatted and reset stats."""
        last_synchronization = self.get_last_synchronization()
        http_errors = self.pop_http_errors()
        http_latencies = self.pop_http_latencies()
        return {
            **{'iQ': self.get_impressions_stats('impressionsQueued')},
            **{'iDe': self.get_impressions_stats('impressionsDeduped')},
            **{'iDr': self.get_impressions_stats('impressionsDropped')},
            **{'eQ': self.get_events_stats('eventsQueued')},
            **{'eD': self.get_events_stats('eventsDropped')},
            **{'lS': {'sp': last_synchronization['split'],
                      'se': last_synchronization['segment'],
                      'ms': last_synchronization['mySegment'],
                      'im': last_synchronization['impression'],
                      'ic': last_synchronization['impressionCount'],
                      'ev': last_synchronization['event'],
                      'te': last_synchronization['telemetry'],
                      'to': last_synchronization['token']}
               },
            **{'t': self.pop_tags()},
            **{'hE': {'sp': http_errors['split'],
                      'se': http_errors['segment'],
                      'ms': http_errors['mySegment'],
                      'im': http_errors['impression'],
                      'ic': http_errors['impressionCount'],
                      'ev': http_errors['event'],
                      'te': http_errors['telemetry'],
                      'to': http_errors['token']}
                },
            **{'hL': {'sp': http_latencies['split'],
                      'se': http_latencies['segment'],
                      'ms': http_latencies['mySegment'],
                      'im': http_latencies['impression'],
                      'ic': http_latencies['impressionCount'],
                      'ev': http_latencies['event'],
                      'te': http_latencies['telemetry'],
                      'to': http_latencies['token']}
                },
            **{'aR': self.pop_auth_rejections()},
            **{'tR': self.pop_token_refreshes()},
            **{'sE': [{'e': event['type'],
                       'd': event['data'],
                       't': event['time']
                      } for event in self.pop_streaming_events()]
                },
            **{'sL': self.get_session_length()}
        }