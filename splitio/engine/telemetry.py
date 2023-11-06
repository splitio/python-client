"""Telemetry engine classes."""
import json
import os

import logging
_LOGGER = logging.getLogger(__name__)

from  splitio.storage.inmemmory import InMemoryTelemetryStorage
from splitio.models.telemetry import CounterConstants, UpdateFromSSE

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

    def record_config(self, config, extra_config, total_flag_sets=0, invalid_flag_sets=0):
        """Record configurations."""
        self._telemetry_storage.record_config(config, extra_config, total_flag_sets, invalid_flag_sets)
        current_app, app_worker_id = self._get_app_worker_id()
        if  current_app is not None:
            self.add_config_tag("initilization:" + current_app)
            self.add_config_tag("worker:#" + app_worker_id)

    def record_ready_time(self, ready_time):
        """Record ready time."""
        self._telemetry_storage.record_ready_time(ready_time)

    def record_flag_sets(self, flag_sets):
        """Record flag sets."""
        self._telemetry_storage.record_flag_sets(flag_sets)

    def record_invalid_flag_sets(self, flag_sets):
        """Record invalid flag sets."""
        self._telemetry_storage.record_invalid_flag_sets(flag_sets)

    def record_bur_time_out(self):
        """Record block until ready timeout."""
        self._telemetry_storage.record_bur_time_out()

    def record_not_ready_usage(self):
        """record non-ready usage."""
        self._telemetry_storage.record_not_ready_usage()

    def record_active_and_redundant_factories(self, active_factory_count, redundant_factory_count):
        self._telemetry_storage.record_active_and_redundant_factories(active_factory_count, redundant_factory_count)

    def add_config_tag(self, tag):
        """Record tag string."""
        self._telemetry_storage.add_config_tag(tag)

    def _get_app_worker_id(self):
        try:
            import uwsgi
            return "uwsgi", str(uwsgi.worker_id())
        except ModuleNotFoundError:
            _LOGGER.debug("NO uwsgi")
            pass

        if 'gunicorn' in os.environ.get("SERVER_SOFTWARE", ""):
            return "gunicorn", str(os.getpid())
        else:
            return None, None


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

    def record_successful_sync(self, resource, time):
        """Record successful sync."""
        self._telemetry_storage.record_successful_sync(resource, time)

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

    def record_update_from_sse(self, event):
        """Record update from sse."""
        self._telemetry_storage.record_update_from_sse(event)

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
        """Get config stats."""
        config_stats = self._telemetry_storage.get_config_stats()
        config_stats.update({'t': self.pop_config_tags()})
        return config_stats

    def get_config_stats_to_json(self):
        """Get config stats in json."""
        return json.dumps(self._telemetry_storage.get_config_stats())

    def pop_config_tags(self):
        """Get and reset tags."""
        return self._telemetry_storage.pop_config_tags()

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
        """
        Get formatted and reset stats.

        :returns: formatted stats
        :rtype: Dict
        """
        exceptions = self.pop_exceptions()['methodExceptions']
        latencies = self.pop_latencies()['methodLatencies']
        return {
            'mE': {
                't': exceptions['treatment'],
                'ts': exceptions['treatments'],
                'tc': exceptions['treatment_with_config'],
                'tcs': exceptions['treatments_with_config'],
                'tf': exceptions['treatments_by_flag_set'],
                'tfs': exceptions['treatments_by_flag_sets'],
                'tcf': exceptions['treatments_with_config_by_flag_set'],
                'tcfs': exceptions['treatments_with_config_by_flag_sets'],
                'tr': exceptions['track']
               },
            'mL':  {
                't': latencies['treatment'],
                'ts': latencies['treatments'],
                'tc': latencies['treatment_with_config'],
                'tcs': latencies['treatments_with_config'],
                'tf': latencies['treatments_by_flag_set'],
                'tfs': latencies['treatments_by_flag_sets'],
                'tcf': latencies['treatments_with_config_by_flag_set'],
                'tcfs': latencies['treatments_with_config_by_flag_sets'],
                'tr': latencies['track']
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
        return self._telemetry_storage.get_last_synchronization()['lastSynchronizations']

    def pop_tags(self):
        """Get and reset tags."""
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

    def pop_update_from_sse(self, event):
        """Get and reset update from sse."""
        return self._telemetry_storage.pop_update_from_sse(event)

    def get_session_length(self):
        """Get session length"""
        return self._telemetry_storage.get_session_length()

    def pop_formatted_stats(self):
        """
        Get formatted and reset stats.

        :returns: formatted stats
        :rtype: Dict
        """
        last_synchronization = self.get_last_synchronization()
        http_errors = self.pop_http_errors()['httpErrors']
        http_latencies = self.pop_http_latencies()['httpLatencies']

        return {
            'iQ': self.get_impressions_stats(CounterConstants.IMPRESSIONS_QUEUED),
            'iDe': self.get_impressions_stats(CounterConstants.IMPRESSIONS_DEDUPED),
            'iDr': self.get_impressions_stats(CounterConstants.IMPRESSIONS_DROPPED),
            'eQ': self.get_events_stats(CounterConstants.EVENTS_QUEUED),
            'eD': self.get_events_stats(CounterConstants.EVENTS_DROPPED),
            'ufs': {event.value: self.pop_update_from_sse(event) for event in UpdateFromSSE},
            'lS': {'sp': last_synchronization['split'],
                      'se': last_synchronization['segment'],
                      'im': last_synchronization['impression'],
                      'ic': last_synchronization['impressionCount'],
                      'ev': last_synchronization['event'],
                      'te': last_synchronization['telemetry'],
                      'to': last_synchronization['token']
               },
            't': self.pop_tags(),
            'hE': {'sp': http_errors['split'],
                      'se': http_errors['segment'],
                      'im': http_errors['impression'],
                      'ic': http_errors['impressionCount'],
                      'ev': http_errors['event'],
                      'te': http_errors['telemetry'],
                      'to': http_errors['token']
                },
            'hL': {'sp': http_latencies['split'],
                      'se': http_latencies['segment'],
                      'im': http_latencies['impression'],
                      'ic': http_latencies['impressionCount'],
                      'ev': http_latencies['event'],
                      'te': http_latencies['telemetry'],
                      'to': http_latencies['token']
                },
            'aR': self.pop_auth_rejections(),
            'tR': self.pop_token_refreshes(),
            'sE': [{'e': event['e'],
                       'd': event['d'],
                       't': event['t']
                      } for event in self.pop_streaming_events()['streamingEvents']],
            'sL': self.get_session_length()
        }
