"""Stats Recorder."""
import abc
import logging
import random


from splitio.client.config import DEFAULT_DATA_SAMPLING


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

    def __init__(self, impressions_manager, event_storage, impression_storage):
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
            self._impression_storage.put(impressions)
        except Exception:  # pylint: disable=broad-except
            _LOGGER.error('Error recording impressions')
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

    def __init__(self, pipe, impressions_manager, event_storage,
                 impression_storage, data_sampling=DEFAULT_DATA_SAMPLING):
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
            # TODO @matias.melograno
            # Changing logic until TelemetryV2 released to avoid using pipelined operations
            # Deprecated Old Telemetry
            if self._data_sampling < DEFAULT_DATA_SAMPLING:
                rnumber = random.uniform(0, 1)
                if self._data_sampling < rnumber:
                    return
            impressions = self._impressions_manager.process_impressions(impressions)
            if not impressions:
                return
            # pipe = self._make_pipe()
            # self._impression_storage.add_impressions_to_pipe(impressions, pipe)
            # self._telemetry_storage.add_latency_to_pipe(operation, latency, pipe)
            # result = pipe.execute()
            # if len(result) == 2:
            #   self._impression_storage.expire_key(result[0], len(impressions))
            self._impression_storage.put(impressions)
        except Exception:  # pylint: disable=broad-except
            _LOGGER.error('Error recording impressions')
            _LOGGER.debug('Error: ', exc_info=True)

    def record_track_stats(self, event):
        """
        Record stats for tracking events.

        :param event: events tracked
        :type event: splitio.models.events.EventWrapper
        """
        return self._event_sotrage.put(event)
