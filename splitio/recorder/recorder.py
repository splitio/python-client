"""Stats Recorder."""
import abc
import logging


from six import add_metaclass


_LOGGER = logging.getLogger(__name__)


@add_metaclass(abc.ABCMeta)
class StatsRecorder(object):
    """StatsRecorder interface."""

    @abc.abstractmethod
    def record_treatment_stats(self, impressions, latency, operation):
        """
        Record stats for treatment evaluation.

        :param impressions: impressions generated for each evaluation performed
        :type impressions: array
        :param latency: time took for doing evaluation
        :type latency: int
        :param operation: operation type
        :type operation: str
        """
        pass

    @abc.abstractmethod
    def record_track_stats(self, events):
        """
        Record stats for tracking events.

        :param events: events tracked
        :type events: array
        """
        pass


class StandardRecorder(StatsRecorder):
    """StandardRecorder class."""

    def __init__(self, impressions_manager, telemetry_storage, event_storage, impression_storage):
        """
        Class constructor.

        :param impressions_manager: impression manager instance
        :type impressions_manager: splitio.engine.impressions.Manager
        :param telemetry_storage: telemetry storage instance
        :type telemetry_storage: splitio.storage.TelemetryStorage
        :param event_storage: event storage instance
        :type event_storage: splitio.storage.EventStorage
        :param impression_storage: impression storage instance
        :type impression_storage: splitio.storage.ImpressionStorage
        """
        self._impressions_manager = impressions_manager
        self._telemetry_storage = telemetry_storage
        self._event_sotrage = event_storage
        self._impression_storage = impression_storage

    def record_treatment_stats(self, impressions, latency, operation):
        """
        Record stats for treatment evaluation.

        :param impressions: impressions generated for each evaluation performed
        :type impressions: array
        :param latency: time took for doing evaluation
        :type latency: int
        :param operation: operation type
        :type operation: str
        """
        try:
            impressions = self._impressions_manager.process_impressions(impressions)
            if self._impression_storage.put(impressions):
                self._telemetry_storage.inc_latency(operation, latency)
        except Exception:  # pylint: disable=broad-except
            _LOGGER.error('Error recording impressions and metrics')
            _LOGGER.debug('Error: ', exc_info=True)

    def record_track_stats(self, event):
        """
        Record stats for tracking events.

        :param event: events tracked
        :type event: splitio.models.events.EventWrapper
        """
        return self._event_sotrage.put(event)


class PipelinedRecorder(StatsRecorder):
    """PipelinedRecorder class."""

    def __init__(self, pipe, impressions_manager, telemetry_storage, event_storage, impression_storage):
        """
        Class constructor.

        :param pipe: redis pipeline function
        :type pipe: callable
        :param impressions_manager: impression manager instance
        :type impressions_manager: splitio.engine.impressions.Manager
        :param telemetry_storage: telemetry storage instance
        :type telemetry_storage: splitio.storage.redis.RedisTelemetryStorage
        :param event_storage: event storage instance
        :type event_storage: splitio.storage.EventStorage
        :param impression_storage: impression storage instance
        :type impression_storage: splitio.storage.redis.RedisImpressionsStorage
        """
        self._make_pipe = pipe
        self._impressions_manager = impressions_manager
        self._telemetry_storage = telemetry_storage
        self._event_sotrage = event_storage
        self._impression_storage = impression_storage

    def record_treatment_stats(self, impressions, latency, operation):
        """
        Record stats for treatment evaluation.

        :param impressions: impressions generated for each evaluation performed
        :type impressions: array
        :param latency: time took for doing evaluation
        :type latency: int
        :param operation: operation type
        :type operation: str
        """
        try:
            impressions = self._impressions_manager.process_impressions(impressions)
            pipe = self._make_pipe()
            self._impression_storage.add_impressions_to_pipe(impressions, pipe)
            self._telemetry_storage.add_latency_to_pipe(operation, latency, pipe)
            result = pipe.execute()
            if len(result) == 2:
                self._impression_storage.expire_key(result[0], len(impressions))
        except Exception:  # pylint: disable=broad-except
            _LOGGER.error('Error recording impressions and metrics')
            _LOGGER.debug('Error: ', exc_info=True)

    def record_track_stats(self, event):
        """
        Record stats for tracking events.

        :param event: events tracked
        :type event: splitio.models.events.EventWrapper
        """
        return self._event_sotrage.put(event)
