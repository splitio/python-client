"""Stats Recorder."""
import abc
import logging
import random

from splitio.client.config import DEFAULT_DATA_SAMPLING
from splitio.models.telemetry import MethodExceptionsAndLatencies

_LOGGER = logging.getLogger(__name__)


class StatsRecorder(object, metaclass=abc.ABCMeta):
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

    def __init__(self, impressions_manager, event_storage, impression_storage, telemetry_evaluation_producer):
        """
        Class constructor.

        :param impressions_manager: impression manager instance
        :type impressions_manager: splitio.engine.impressions.Manager
        :param event_storage: event storage instance
        :type event_storage: splitio.storage.EventStorage
        :param impression_storage: impression storage instance
        :type impression_storage: splitio.storage.ImpressionStorage
        """
        self._impressions_manager = impressions_manager
        self._event_sotrage = event_storage
        self._impression_storage = impression_storage
        self._telemetry_evaluation_producer = telemetry_evaluation_producer

    def record_treatment_stats(self, impressions, latency, operation, method_name):
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
            if method_name is not None:
                self._telemetry_evaluation_producer.record_latency(operation, latency)
            impressions = self._impressions_manager.process_impressions(impressions)
            self._impression_storage.put(impressions)
        except Exception:  # pylint: disable=broad-except
            _LOGGER.error('Error recording impressions')
            _LOGGER.debug('Error: ', exc_info=True)

    def record_track_stats(self, event, latency):
        """
        Record stats for tracking events.

        :param event: events tracked
        :type event: splitio.models.events.EventWrapper
        """
        self._telemetry_evaluation_producer.record_latency(MethodExceptionsAndLatencies.TRACK, latency)
        return self._event_sotrage.put(event)


class PipelinedRecorder(StatsRecorder):
    """PipelinedRecorder class."""

    def __init__(self, pipe, impressions_manager, event_storage,
                 impression_storage, telemetry_redis_storage, data_sampling=DEFAULT_DATA_SAMPLING):
        """
        Class constructor.

        :param pipe: redis pipeline function
        :type pipe: callable
        :param impressions_manager: impression manager instance
        :type impressions_manager: splitio.engine.impressions.Manager
        :param event_storage: event storage instance
        :type event_storage: splitio.storage.EventStorage
        :param impression_storage: impression storage instance
        :type impression_storage: splitio.storage.redis.RedisImpressionsStorage
        :param data_sampling: data sampling factor
        :type data_sampling: number
        """
        self._make_pipe = pipe
        self._impressions_manager = impressions_manager
        self._event_sotrage = event_storage
        self._impression_storage = impression_storage
        self._data_sampling = data_sampling
        self._telemetry_redis_storage = telemetry_redis_storage

    def record_treatment_stats(self, impressions, latency, operation, method_name):
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
            if self._data_sampling < DEFAULT_DATA_SAMPLING:
                rnumber = random.uniform(0, 1)
                if self._data_sampling < rnumber:
                    return
            impressions = self._impressions_manager.process_impressions(impressions)
            if not impressions:
                return

            pipe = self._make_pipe()
            self._impression_storage.add_impressions_to_pipe(impressions, pipe)
            if method_name is not None:
                self._telemetry_redis_storage.add_latency_to_pipe(operation, latency, pipe)
            result = pipe.execute()
            if len(result) == 2:
                self._impression_storage.expire_key(result[0], len(impressions))
                self._telemetry_redis_storage.expire_latency_keys(result[1], latency)
        except Exception:  # pylint: disable=broad-except
            _LOGGER.error('Error recording impressions')
            _LOGGER.debug('Error: ', exc_info=True)

    def record_track_stats(self, event, latency):
        """
        Record stats for tracking events.

        :param event: events tracked
        :type event: splitio.models.events.EventWrapper
        """
        try:
            pipe = self._make_pipe()
            self._event_sotrage.add_events_to_pipe(event, pipe)
            self._telemetry_redis_storage.add_latency_to_pipe(MethodExceptionsAndLatencies.TRACK, latency, pipe)
            result = pipe.execute()
            if len(result) == 2:
                self._event_sotrage.expire_keys(result[0], len(event))
                self._telemetry_redis_storage.expire_latency_keys(result[1], latency)
                if result[0] > 0:
                    return True
            return False
        except Exception:  # pylint: disable=broad-except
            _LOGGER.error('Error recording events')
            _LOGGER.debug('Error: ', exc_info=True)
            return False
