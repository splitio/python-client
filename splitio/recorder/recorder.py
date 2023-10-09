"""Stats Recorder."""
import abc
import logging
import random

from splitio.client.config import DEFAULT_DATA_SAMPLING
from splitio.client.listener import ImpressionListenerException
from splitio.models.telemetry import MethodExceptionsAndLatencies
from splitio.models import telemetry

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

    async def _send_impressions_to_listener_async(self, impressions):
        """
        Send impression result to custom listener.

        :param impressions: List of impression objects with attributes
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]
        """
        if self._listener is not None:
            try:
                for impression, attributes in impressions:
                    await self._listener.log_impression(impression, attributes)
            except ImpressionListenerException:
                pass
#                self._logger.error('An exception was raised while calling user-custom impression listener')
#                self._logger.debug('Error', exc_info=True)

    def _send_impressions_to_listener(self, impressions):
        """
        Send impression result to custom listener.

        :param impressions: List of impression objects with attributes
        :type impressions: list[tuple[splitio.models.impression.Impression, dict]]
        """
        if self._listener is not None:
            try:
                for impression, attributes in impressions:
                    self._listener.log_impression(impression, attributes)
            except ImpressionListenerException:
                pass
#                self._logger.error('An exception was raised while calling user-custom impression listener')
#                self._logger.debug('Error', exc_info=True)

class StandardRecorder(StatsRecorder):
    """StandardRecorder class."""

    def __init__(self, impressions_manager, event_storage, impression_storage, telemetry_evaluation_producer, telemetry_runtime_producer, listener=None):
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
        self._telemetry_runtime_producer = telemetry_runtime_producer
        self._listener = listener

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
            impressions, deduped, for_listener = self._impressions_manager.process_impressions(impressions)
            if deduped > 0:
                self._telemetry_runtime_producer.record_impression_stats(telemetry.CounterConstants.IMPRESSIONS_DEDUPED, deduped)
            self._impression_storage.put(impressions)
            self._send_impressions_to_listener(for_listener)
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


class StandardRecorderAsync(StatsRecorder):
    """StandardRecorder async class."""

    def __init__(self, impressions_manager, event_storage, impression_storage, telemetry_evaluation_producer, telemetry_runtime_producer, listener=None):
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
        self._telemetry_runtime_producer = telemetry_runtime_producer
        self._listener = listener

    async def record_treatment_stats(self, impressions, latency, operation, method_name):
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
                await self._telemetry_evaluation_producer.record_latency(operation, latency)
            impressions, deduped, for_listener = self._impressions_manager.process_impressions(impressions)
            if deduped > 0:
                await self._telemetry_runtime_producer.record_impression_stats(telemetry.CounterConstants.IMPRESSIONS_DEDUPED, deduped)

            await self._impression_storage.put(impressions)
            await self._send_impressions_to_listener_async(for_listener)
        except Exception:  # pylint: disable=broad-except
            _LOGGER.error('Error recording impressions')
            _LOGGER.debug('Error: ', exc_info=True)

    async def record_track_stats(self, event, latency):
        """
        Record stats for tracking events.

        :param event: events tracked
        :type event: splitio.models.events.EventWrapper
        """
        await self._telemetry_evaluation_producer.record_latency(MethodExceptionsAndLatencies.TRACK, latency)
        return await self._event_sotrage.put(event)


class PipelinedRecorder(StatsRecorder):
    """PipelinedRecorder class."""

    def __init__(self, pipe, impressions_manager, event_storage,
                 impression_storage, telemetry_redis_storage, data_sampling=DEFAULT_DATA_SAMPLING, listener=None):
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
        self._listener = listener

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
            impressions, deduped, for_listener = self._impressions_manager.process_impressions(impressions)
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
            self._send_impressions_to_listener(for_listener)
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

class PipelinedRecorderAsync(StatsRecorder):
    """PipelinedRecorder async class."""

    def __init__(self, pipe, impressions_manager, event_storage,
                 impression_storage, telemetry_redis_storage, data_sampling=DEFAULT_DATA_SAMPLING, listener=None):
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
        self._listener = listener

    async def record_treatment_stats(self, impressions, latency, operation, method_name):
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
            impressions, deduped, for_listener = self._impressions_manager.process_impressions(impressions)
            if not impressions:
                return

            pipe = self._make_pipe()
            self._impression_storage.add_impressions_to_pipe(impressions, pipe)
            if method_name is not None:
                self._telemetry_redis_storage.add_latency_to_pipe(operation, latency, pipe)
            result = await pipe.execute()
            if len(result) == 2:
                await self._impression_storage.expire_key(result[0], len(impressions))
                await self._telemetry_redis_storage.expire_latency_keys(result[1], latency)
            await self._send_impressions_to_listener_async(for_listener)
        except Exception:  # pylint: disable=broad-except
            _LOGGER.error('Error recording impressions')
            _LOGGER.debug('Error: ', exc_info=True)

    async def record_track_stats(self, event, latency):
        """
        Record stats for tracking events.

        :param event: events tracked
        :type event: splitio.models.events.EventWrapper
        """
        try:
            pipe = self._make_pipe()
            self._event_sotrage.add_events_to_pipe(event, pipe)
            self._telemetry_redis_storage.add_latency_to_pipe(MethodExceptionsAndLatencies.TRACK, latency, pipe)
            result = await pipe.execute()
            if len(result) == 2:
                await self._event_sotrage.expire_keys(result[0], len(event))
                await self._telemetry_redis_storage.expire_latency_keys(result[1], latency)
                if result[0] > 0:
                    return True
            return False
        except Exception:  # pylint: disable=broad-except
            _LOGGER.error('Error recording events')
            _LOGGER.debug('Error: ', exc_info=True)
            return False
