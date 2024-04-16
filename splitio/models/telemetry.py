"""SDK Telemetry helpers."""
from bisect import bisect_left
import threading
import os
from enum import Enum
import abc

from splitio.engine.impressions import ImpressionsMode
from splitio.optional.loaders import asyncio

BUCKETS = (
    1000, 1500, 2250, 3375, 5063,
    7594, 11391, 17086, 25629, 38443,
    57665, 86498, 129746, 194620, 291929,
    437894, 656841, 985261, 1477892, 2216838,
    3325257, 4987885, 7481828
)

MAX_LATENCY = 7481828
MAX_LATENCY_BUCKET_COUNT = 23
MAX_STREAMING_EVENTS = 20
MAX_TAGS = 10

class CounterConstants(Enum):
    """Impressions and events counters constants"""
    IMPRESSIONS_QUEUED = 'impressionsQueued'
    IMPRESSIONS_DEDUPED = 'impressionsDeduped'
    IMPRESSIONS_DROPPED = 'impressionsDropped'
    EVENTS_QUEUED = 'eventsQueued'
    EVENTS_DROPPED = 'eventsDropped'

class _ConfigParams(Enum):
    """Config parameters constants"""
    SPLITS_REFRESH_RATE = 'featuresRefreshRate'
    SEGMENTS_REFRESH_RATE = 'segmentsRefreshRate'
    IMPRESSIONS_REFRESH_RATE = 'impressionsRefreshRate'
    EVENTS_REFRESH_RATE = 'eventsPushRate'
    TELEMETRY_REFRESH_RATE = 'metricsRefreshRate'
    OPERATION_MODE = 'operationMode'
    STORAGE_TYPE = 'storageType'
    STREAMING_ENABLED = 'streamingEnabled'
    IMPRESSIONS_QUEUE_SIZE = 'impressionsQueueSize'
    EVENTS_QUEUE_SIZE = 'eventsQueueSize'
    IMPRESSIONS_MODE = 'impressionsMode'
    IMPRESSIONS_LISTENER = 'impressionListener'

class _ExtraConfig(Enum):
    """Extra config constants"""
    ACTIVE_FACTORY_COUNT = 'activeFactoryCount'
    REDUNDANT_FACTORY_COUNT = 'redundantFactoryCount'
    BLOCK_UNTIL_READY_TIMEOUT = 'blockUntilReadyTimeout'
    NOT_READY = 'notReady'
    TIME_UNTIL_READY = 'timeUntilReady'
    REFRESH_RATE = 'refreshRate'
    HTTP_PROXY = 'httpProxy'
    HTTPS_PROXY_ENV = 'HTTPS_PROXY'

class _ApiURLs(Enum):
    """Api URL constants"""
    SDK_URL = 'sdk_url'
    EVENTS_URL = 'events_url'
    AUTH_URL = 'auth_url'
    STREAMING_URL = 'streaming_url'
    TELEMETRY_URL = 'telemetry_url'
    URL_OVERRIDE = 'urlOverride'

class HTTPExceptionsAndLatencies(Enum):
    """Sync exceptions and latencies constants"""
    HTTP_ERRORS = 'httpErrors'
    HTTP_LATENCIES = 'httpLatencies'
    SPLIT = 'split'
    SEGMENT = 'segment'
    IMPRESSION = 'impression'
    IMPRESSION_COUNT = 'impressionCount'
    EVENT = 'event'
    TELEMETRY = 'telemetry'
    TOKEN = 'token'

class MethodExceptionsAndLatencies(Enum):
    """Method exceptions and latencies constants"""
    METHOD_LATENCIES = 'methodLatencies'
    METHOD_EXCEPTIONS = 'methodExceptions'
    TREATMENT = 'treatment'
    TREATMENTS = 'treatments'
    TREATMENT_WITH_CONFIG = 'treatment_with_config'
    TREATMENTS_WITH_CONFIG = 'treatments_with_config'
    TREATMENTS_BY_FLAG_SET = 'treatments_by_flag_set'
    TREATMENTS_BY_FLAG_SETS = 'treatments_by_flag_sets'
    TREATMENTS_WITH_CONFIG_BY_FLAG_SET = 'treatments_with_config_by_flag_set'
    TREATMENTS_WITH_CONFIG_BY_FLAG_SETS = 'treatments_with_config_by_flag_sets'
    TRACK = 'track'

class _LastSynchronizationConstants(Enum):
    """Last sync constants"""
    LAST_SYNCHRONIZATIONS = 'lastSynchronizations'

class SSEStreamingStatus(Enum):
    """SSE streaming status enums"""
    ENABLED = 0
    DISABLED = 1
    PAUSED = 2

class SSEConnectionError(Enum):
    """SSE Connection Error enums"""
    REQUESTED = 0
    NON_REQUESTED = 1

class SSESyncMode(Enum):
    """SSE sync mode enums"""
    STREAMING = 0
    POLLING = 1

class _StreamingEventsConstant(Enum):
    """Storage types constant"""
    STREAMING_EVENTS = 'streamingEvents'

class StreamingEventTypes(Enum):
    """Streaming event types constants"""
    CONNECTION_ESTABLISHED = 0
    OCCUPANCY_PRI = 10
    OCCUPANCY_SEC = 20
    STREAMING_STATUS = 30
    SSE_CONNECTION_ERROR = 40
    TOKEN_REFRESH =  50
    ABLY_ERROR = 60
    SYNC_MODE_UPDATE = 70

class StorageType(Enum):
    """Storage types constants"""
    MEMORY = 'memory'
    REDIS = 'redis'
    PLUGGABLE = 'pluggable'

class OperationMode(Enum):
    """Storage modes constants"""
    STANDALONE = 'standalone'
    CONSUMER = 'consumer'
    PARTIAL_CONSUMER = 'partial_consumer'

class UpdateFromSSE(Enum):
    """Update from sse constants"""
    SPLIT_UPDATE = 'sp'

def get_latency_bucket_index(micros):
    """
    Find the bucket index for a measured latency.

    :param micros: Measured latency in microseconds
    :type micros: int
    :return: Bucket index for the given latency
    :rtype: int
    """
    if micros > MAX_LATENCY:
        return len(BUCKETS) - 1

    return bisect_left(BUCKETS, micros)

class MethodLatenciesBase(object, metaclass=abc.ABCMeta):
    """
    Method Latency base class

    """
    def _reset_all(self):
        """Reset variables"""
        self._treatment = [0] * MAX_LATENCY_BUCKET_COUNT
        self._treatments = [0] * MAX_LATENCY_BUCKET_COUNT
        self._treatment_with_config = [0] * MAX_LATENCY_BUCKET_COUNT
        self._treatments_with_config = [0] * MAX_LATENCY_BUCKET_COUNT
        self._treatments_by_flag_set = [0] * MAX_LATENCY_BUCKET_COUNT
        self._treatments_by_flag_sets = [0] * MAX_LATENCY_BUCKET_COUNT
        self._treatments_with_config_by_flag_set = [0] * MAX_LATENCY_BUCKET_COUNT
        self._treatments_with_config_by_flag_sets = [0] * MAX_LATENCY_BUCKET_COUNT
        self._track = [0] * MAX_LATENCY_BUCKET_COUNT

    @abc.abstractmethod
    def add_latency(self, method, latency):
        """
        Add Latency method
        """

    @abc.abstractmethod
    def pop_all(self):
        """
        Pop all latencies
        """

class MethodLatencies(MethodLatenciesBase):
    """
    Method Latency class

    """
    def __init__(self):
        """Constructor"""
        self._lock = threading.RLock()
        with self._lock:
            self._reset_all()

    def add_latency(self, method, latency):
        """
        Add Latency method

        :param method: passed method name
        :type method: str
        :param latency: amount of latency in microseconds
        :type latency: int
        """
        latency_bucket = get_latency_bucket_index(latency)
        with self._lock:
            if method == MethodExceptionsAndLatencies.TREATMENT:
                self._treatment[latency_bucket] += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS:
                self._treatments[latency_bucket] += 1
            elif method == MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG:
                self._treatment_with_config[latency_bucket] += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG:
                self._treatments_with_config[latency_bucket] += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET:
                self._treatments_by_flag_set[latency_bucket] += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS:
                self._treatments_by_flag_sets[latency_bucket] += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET:
                self._treatments_with_config_by_flag_set[latency_bucket] += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS:
                self._treatments_with_config_by_flag_sets[latency_bucket] += 1
            elif method == MethodExceptionsAndLatencies.TRACK:
                self._track[latency_bucket] += 1
            else:
                return

    def pop_all(self):
        """
        Pop all latencies

        :return: Dictonary of latencies
        :rtype: dict
        """
        with self._lock:
            latencies = {MethodExceptionsAndLatencies.METHOD_LATENCIES.value: {
                MethodExceptionsAndLatencies.TREATMENT.value: self._treatment,
                MethodExceptionsAndLatencies.TREATMENTS.value: self._treatments,
                MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG.value: self._treatment_with_config,
                MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG.value: self._treatments_with_config,
                MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET.value: self._treatments_by_flag_set,
                MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS.value: self._treatments_by_flag_sets,
                MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET.value: self._treatments_with_config_by_flag_set,
                MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS.value: self._treatments_with_config_by_flag_sets,
                MethodExceptionsAndLatencies.TRACK.value: self._track}
            }
            self._reset_all()
            return latencies


class MethodLatenciesAsync(MethodLatenciesBase):
    """
    Method async Latency class

    """
    @classmethod
    async def create(cls):
        """Constructor"""
        self = cls()
        self._lock = asyncio.Lock()
        async with self._lock:
            self._reset_all()
        return self

    async def add_latency(self, method, latency):
        """
        Add Latency method

        :param method: passed method name
        :type method: str
        :param latency: amount of latency in microseconds
        :type latency: int
        """
        latency_bucket = get_latency_bucket_index(latency)
        async with self._lock:
            if method == MethodExceptionsAndLatencies.TREATMENT:
                self._treatment[latency_bucket] += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS:
                self._treatments[latency_bucket] += 1
            elif method == MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG:
                self._treatment_with_config[latency_bucket] += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG:
                self._treatments_with_config[latency_bucket] += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET:
                self._treatments_by_flag_set[latency_bucket] += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS:
                self._treatments_by_flag_sets[latency_bucket] += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET:
                self._treatments_with_config_by_flag_set[latency_bucket] += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS:
                self._treatments_with_config_by_flag_sets[latency_bucket] += 1
            elif method == MethodExceptionsAndLatencies.TRACK:
                self._track[latency_bucket] += 1
            else:
                return

    async def pop_all(self):
        """
        Pop all latencies

        :return: Dictonary of latencies
        :rtype: dict
        """
        async with self._lock:
            latencies = {MethodExceptionsAndLatencies.METHOD_LATENCIES.value: {
                MethodExceptionsAndLatencies.TREATMENT.value: self._treatment,
                MethodExceptionsAndLatencies.TREATMENTS.value: self._treatments,
                MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG.value: self._treatment_with_config,
                MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG.value: self._treatments_with_config,
                MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET.value: self._treatments_by_flag_set,
                MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS.value: self._treatments_by_flag_sets,
                MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET.value: self._treatments_with_config_by_flag_set,
                MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS.value: self._treatments_with_config_by_flag_sets,
                MethodExceptionsAndLatencies.TRACK.value: self._track}
            }
            self._reset_all()
            return latencies


class HTTPLatenciesBase(object, metaclass=abc.ABCMeta):
    """
    HTTP Latency class

    """
    def _reset_all(self):
        """Reset variables"""
        self._split = [0] * MAX_LATENCY_BUCKET_COUNT
        self._segment = [0] * MAX_LATENCY_BUCKET_COUNT
        self._impression = [0] * MAX_LATENCY_BUCKET_COUNT
        self._impression_count = [0] * MAX_LATENCY_BUCKET_COUNT
        self._event = [0] * MAX_LATENCY_BUCKET_COUNT
        self._telemetry = [0] * MAX_LATENCY_BUCKET_COUNT
        self._token = [0] * MAX_LATENCY_BUCKET_COUNT

    @abc.abstractmethod
    def add_latency(self, resource, latency):
        """
        Add Latency method
        """

    @abc.abstractmethod
    def pop_all(self):
        """
        Pop all latencies
        """


class HTTPLatencies(HTTPLatenciesBase):
    """
    HTTP Latency class

    """
    def __init__(self):
        """Constructor"""
        self._lock = threading.RLock()
        with self._lock:
            self._reset_all()

    def add_latency(self, resource, latency):
        """
        Add Latency method

        :param resource: passed resource name
        :type resource: str
        :param latency: amount of latency in microseconds
        :type latency: int
        """
        latency_bucket = get_latency_bucket_index(latency)
        with self._lock:
            if resource == HTTPExceptionsAndLatencies.SPLIT:
                self._split[latency_bucket] += 1
            elif resource == HTTPExceptionsAndLatencies.SEGMENT:
                self._segment[latency_bucket] += 1
            elif resource == HTTPExceptionsAndLatencies.IMPRESSION:
                self._impression[latency_bucket] += 1
            elif resource == HTTPExceptionsAndLatencies.IMPRESSION_COUNT:
                self._impression_count[latency_bucket] += 1
            elif resource == HTTPExceptionsAndLatencies.EVENT:
                self._event[latency_bucket] += 1
            elif resource == HTTPExceptionsAndLatencies.TELEMETRY:
                self._telemetry[latency_bucket] += 1
            elif resource == HTTPExceptionsAndLatencies.TOKEN:
                self._token[latency_bucket] += 1
            else:
                return

    def pop_all(self):
        """
        Pop all latencies

        :return: Dictonary of latencies
        :rtype: dict
        """
        with self._lock:
            latencies = {HTTPExceptionsAndLatencies.HTTP_LATENCIES.value: {HTTPExceptionsAndLatencies.SPLIT.value: self._split, HTTPExceptionsAndLatencies.SEGMENT.value: self._segment, HTTPExceptionsAndLatencies.IMPRESSION.value: self._impression,
                                        HTTPExceptionsAndLatencies.IMPRESSION_COUNT.value: self._impression_count, HTTPExceptionsAndLatencies.EVENT.value: self._event,
                                        HTTPExceptionsAndLatencies.TELEMETRY.value: self._telemetry, HTTPExceptionsAndLatencies.TOKEN.value: self._token}
                    }
            self._reset_all()
            return latencies


class HTTPLatenciesAsync(HTTPLatenciesBase):
    """
    HTTP Latency async class

    """
    @classmethod
    async def create(cls):
        """Constructor"""
        self = cls()
        self._lock = asyncio.Lock()
        async with self._lock:
            self._reset_all()
        return self

    async def add_latency(self, resource, latency):
        """
        Add Latency method

        :param resource: passed resource name
        :type resource: str
        :param latency: amount of latency in microseconds
        :type latency: int
        """
        latency_bucket = get_latency_bucket_index(latency)
        async with self._lock:
            if resource == HTTPExceptionsAndLatencies.SPLIT:
                self._split[latency_bucket] += 1
            elif resource == HTTPExceptionsAndLatencies.SEGMENT:
                self._segment[latency_bucket] += 1
            elif resource == HTTPExceptionsAndLatencies.IMPRESSION:
                self._impression[latency_bucket] += 1
            elif resource == HTTPExceptionsAndLatencies.IMPRESSION_COUNT:
                self._impression_count[latency_bucket] += 1
            elif resource == HTTPExceptionsAndLatencies.EVENT:
                self._event[latency_bucket] += 1
            elif resource == HTTPExceptionsAndLatencies.TELEMETRY:
                self._telemetry[latency_bucket] += 1
            elif resource == HTTPExceptionsAndLatencies.TOKEN:
                self._token[latency_bucket] += 1
            else:
                return

    async def pop_all(self):
        """
        Pop all latencies

        :return: Dictonary of latencies
        :rtype: dict
        """
        async with self._lock:
            latencies = {HTTPExceptionsAndLatencies.HTTP_LATENCIES.value: {HTTPExceptionsAndLatencies.SPLIT.value: self._split, HTTPExceptionsAndLatencies.SEGMENT.value: self._segment, HTTPExceptionsAndLatencies.IMPRESSION.value: self._impression,
                                        HTTPExceptionsAndLatencies.IMPRESSION_COUNT.value: self._impression_count, HTTPExceptionsAndLatencies.EVENT.value: self._event,
                                        HTTPExceptionsAndLatencies.TELEMETRY.value: self._telemetry, HTTPExceptionsAndLatencies.TOKEN.value: self._token}
                    }
            self._reset_all()
            return latencies


class MethodExceptionsBase(object, metaclass=abc.ABCMeta):
    """
    Method exceptions base class

    """
    def _reset_all(self):
        """Reset variables"""
        self._treatment = 0
        self._treatments = 0
        self._treatment_with_config = 0
        self._treatments_with_config = 0
        self._treatments_by_flag_set = 0
        self._treatments_by_flag_sets = 0
        self._treatments_with_config_by_flag_set = 0
        self._treatments_with_config_by_flag_sets = 0
        self._track = 0

    @abc.abstractmethod
    def add_exception(self, method):
        """
        Add exceptions method
        """

    @abc.abstractmethod
    def pop_all(self):
        """
        Pop all exceptions
        """


class MethodExceptions(MethodExceptionsBase):
    """
    Method exceptions class

    """
    def __init__(self):
        """Constructor"""
        self._lock = threading.RLock()
        with self._lock:
            self._reset_all()

    def add_exception(self, method):
        """
        Add exceptions method

        :param method: passed method name
        :type method: str
        """
        with self._lock:
            if method == MethodExceptionsAndLatencies.TREATMENT:
                self._treatment += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS:
                self._treatments += 1
            elif method == MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG:
                self._treatment_with_config += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG:
                self._treatments_with_config += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET:
                self._treatments_by_flag_set += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS:
                self._treatments_by_flag_sets += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET:
                self._treatments_with_config_by_flag_set += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS:
                self._treatments_with_config_by_flag_sets += 1
            elif method == MethodExceptionsAndLatencies.TRACK:
                self._track += 1
            else:
                return

    def pop_all(self):
        """
        Pop all exceptions

        :return: Dictonary of exceptions
        :rtype: dict
        """
        with self._lock:
            exceptions = {
                MethodExceptionsAndLatencies.METHOD_EXCEPTIONS.value: {
                MethodExceptionsAndLatencies.TREATMENT.value: self._treatment,
                MethodExceptionsAndLatencies.TREATMENTS.value: self._treatments,
                MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG.value: self._treatment_with_config,
                MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG.value: self._treatments_with_config,
                MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET.value: self._treatments_by_flag_set,
                MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS.value: self._treatments_by_flag_sets,
                MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET.value: self._treatments_with_config_by_flag_set,
                MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS.value: self._treatments_with_config_by_flag_sets,
                MethodExceptionsAndLatencies.TRACK.value: self._track}
            }
            self._reset_all()
            return exceptions


class MethodExceptionsAsync(MethodExceptionsBase):
    """
    Method async exceptions class

    """
    @classmethod
    async def create(cls):
        """Constructor"""
        self = cls()
        self._lock = asyncio.Lock()
        async with self._lock:
            self._reset_all()
        return self

    async def add_exception(self, method):
        """
        Add exceptions method

        :param method: passed method name
        :type method: str
        """
        async with self._lock:
            if method == MethodExceptionsAndLatencies.TREATMENT:
                self._treatment += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS:
                self._treatments += 1
            elif method == MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG:
                self._treatment_with_config += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG:
                self._treatments_with_config += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET:
                self._treatments_by_flag_set += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS:
                self._treatments_by_flag_sets += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET:
                self._treatments_with_config_by_flag_set += 1
            elif method == MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS:
                self._treatments_with_config_by_flag_sets += 1
            elif method == MethodExceptionsAndLatencies.TRACK:
                self._track += 1
            else:
                return

    async def pop_all(self):
        """
        Pop all exceptions

        :return: Dictonary of exceptions
        :rtype: dict
        """
        async with self._lock:
            exceptions = {
                MethodExceptionsAndLatencies.METHOD_EXCEPTIONS.value: {
                MethodExceptionsAndLatencies.TREATMENT.value: self._treatment,
                MethodExceptionsAndLatencies.TREATMENTS.value: self._treatments,
                MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG.value: self._treatment_with_config,
                MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG.value: self._treatments_with_config,
                MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET.value: self._treatments_by_flag_set,
                MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS.value: self._treatments_by_flag_sets,
                MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET.value: self._treatments_with_config_by_flag_set,
                MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS.value: self._treatments_with_config_by_flag_sets,
                MethodExceptionsAndLatencies.TRACK.value: self._track}
            }
            self._reset_all()
            return exceptions


class LastSynchronizationBase(object, metaclass=abc.ABCMeta):
    """
    Last Synchronization info base class

    """
    def _reset_all(self):
        """Reset variables"""
        self._split = 0
        self._segment = 0
        self._impression = 0
        self._impression_count = 0
        self._event = 0
        self._telemetry = 0
        self._token = 0

    @abc.abstractmethod
    def add_latency(self, resource, sync_time):
        """
        Add Latency method
        """

    @abc.abstractmethod
    def get_all(self):
        """
        get all exceptions
        """

class LastSynchronization(LastSynchronizationBase):
    """
    Last Synchronization info class

    """
    def __init__(self):
        """Constructor"""
        self._lock = threading.RLock()
        with self._lock:
            self._reset_all()

    def add_latency(self, resource, sync_time):
        """
        Add Latency method

        :param resource: passed resource name
        :type resource: str
        :param sync_time: amount of last sync time
        :type sync_time: int
        """
        with self._lock:
            if resource == HTTPExceptionsAndLatencies.SPLIT:
                self._split = sync_time
            elif resource == HTTPExceptionsAndLatencies.SEGMENT:
                self._segment = sync_time
            elif resource == HTTPExceptionsAndLatencies.IMPRESSION:
                self._impression = sync_time
            elif resource == HTTPExceptionsAndLatencies.IMPRESSION_COUNT:
                self._impression_count = sync_time
            elif resource == HTTPExceptionsAndLatencies.EVENT:
                self._event = sync_time
            elif resource == HTTPExceptionsAndLatencies.TELEMETRY:
                self._telemetry = sync_time
            elif resource == HTTPExceptionsAndLatencies.TOKEN:
                self._token = sync_time
            else:
                return

    def get_all(self):
        """
        get all exceptions

        :return: Dictonary of latencies
        :rtype: dict
        """
        with self._lock:
            return {
                _LastSynchronizationConstants.LAST_SYNCHRONIZATIONS.value: {
                HTTPExceptionsAndLatencies.SPLIT.value: self._split,
                HTTPExceptionsAndLatencies.SEGMENT.value: self._segment,
                HTTPExceptionsAndLatencies.IMPRESSION.value: self._impression,
                HTTPExceptionsAndLatencies.IMPRESSION_COUNT.value: self._impression_count,
                HTTPExceptionsAndLatencies.EVENT.value: self._event,
                HTTPExceptionsAndLatencies.TELEMETRY.value: self._telemetry,
                HTTPExceptionsAndLatencies.TOKEN.value: self._token}
            }

class LastSynchronizationAsync(LastSynchronizationBase):
    """
    Last Synchronization async info class

    """
    @classmethod
    async def create(cls):
        """Constructor"""
        self = cls()
        self._lock = asyncio.Lock()
        async with self._lock:
            self._reset_all()
        return self

    async def add_latency(self, resource, sync_time):
        """
        Add Latency method

        :param resource: passed resource name
        :type resource: str
        :param sync_time: amount of last sync time
        :type sync_time: int
        """
        async with self._lock:
            if resource == HTTPExceptionsAndLatencies.SPLIT:
                self._split = sync_time
            elif resource == HTTPExceptionsAndLatencies.SEGMENT:
                self._segment = sync_time
            elif resource == HTTPExceptionsAndLatencies.IMPRESSION:
                self._impression = sync_time
            elif resource == HTTPExceptionsAndLatencies.IMPRESSION_COUNT:
                self._impression_count = sync_time
            elif resource == HTTPExceptionsAndLatencies.EVENT:
                self._event = sync_time
            elif resource == HTTPExceptionsAndLatencies.TELEMETRY:
                self._telemetry = sync_time
            elif resource == HTTPExceptionsAndLatencies.TOKEN:
                self._token = sync_time
            else:
                return

    async def get_all(self):
        """
        get all exceptions

        :return: Dictonary of latencies
        :rtype: dict
        """
        async with self._lock:
            return {
                _LastSynchronizationConstants.LAST_SYNCHRONIZATIONS.value: {
                HTTPExceptionsAndLatencies.SPLIT.value: self._split,
                HTTPExceptionsAndLatencies.SEGMENT.value: self._segment,
                HTTPExceptionsAndLatencies.IMPRESSION.value: self._impression,
                HTTPExceptionsAndLatencies.IMPRESSION_COUNT.value: self._impression_count,
                HTTPExceptionsAndLatencies.EVENT.value: self._event,
                HTTPExceptionsAndLatencies.TELEMETRY.value: self._telemetry,
                HTTPExceptionsAndLatencies.TOKEN.value: self._token}
            }


class HTTPErrorsBase(object, metaclass=abc.ABCMeta):
    """
    Http errors base class

    """
    def _reset_all(self):
        """Reset variables"""
        self._split = {}
        self._segment = {}
        self._impression = {}
        self._impression_count = {}
        self._event = {}
        self._telemetry = {}
        self._token = {}

    @abc.abstractmethod
    def add_error(self, resource, status):
        """
        Add Latency method
        """

    @abc.abstractmethod
    def pop_all(self):
        """
        Pop all errors
        """


class HTTPErrors(HTTPErrorsBase):
    """
    Http errors class

    """
    def __init__(self):
        """Constructor"""
        self._lock = threading.RLock()
        with self._lock:
            self._reset_all()

    def add_error(self, resource, status):
        """
        Add Latency method

        :param resource: passed resource name
        :type resource: str
        :param status: http error code
        :type status: str
        """
        status = str(status)
        with self._lock:
            if resource == HTTPExceptionsAndLatencies.SPLIT:
                if status not in self._split:
                    self._split[status] = 0
                self._split[status] += 1
            elif resource == HTTPExceptionsAndLatencies.SEGMENT:
                if status not in self._segment:
                    self._segment[status] = 0
                self._segment[status] += 1
            elif resource == HTTPExceptionsAndLatencies.IMPRESSION:
                if status not in self._impression:
                    self._impression[status] = 0
                self._impression[status] += 1
            elif resource == HTTPExceptionsAndLatencies.IMPRESSION_COUNT:
                if status not in self._impression_count:
                    self._impression_count[status] = 0
                self._impression_count[status] += 1
            elif resource == HTTPExceptionsAndLatencies.EVENT:
                if status not in self._event:
                    self._event[status] = 0
                self._event[status] += 1
            elif resource == HTTPExceptionsAndLatencies.TELEMETRY:
                if status not in self._telemetry:
                    self._telemetry[status] = 0
                self._telemetry[status] += 1
            elif resource == HTTPExceptionsAndLatencies.TOKEN:
                if status not in self._token:
                    self._token[status] = 0
                self._token[status] += 1
            else:
                return

    def pop_all(self):
        """
        Pop all errors

        :return: Dictonary of exceptions
        :rtype: dict
        """
        with self._lock:
            http_errors = {
                HTTPExceptionsAndLatencies.HTTP_ERRORS.value: {
                    HTTPExceptionsAndLatencies.SPLIT.value: self._split,
                    HTTPExceptionsAndLatencies.SEGMENT.value: self._segment,
                    HTTPExceptionsAndLatencies.IMPRESSION.value: self._impression,
                    HTTPExceptionsAndLatencies.IMPRESSION_COUNT.value: self._impression_count, HTTPExceptionsAndLatencies.EVENT.value: self._event,
                    HTTPExceptionsAndLatencies.TELEMETRY.value: self._telemetry, HTTPExceptionsAndLatencies.TOKEN.value: self._token
                }
            }
            self._reset_all()
            return http_errors


class HTTPErrorsAsync(HTTPErrorsBase):
    """
    Http error async class

    """
    @classmethod
    async def create(cls):
        """Constructor"""
        self = cls()
        self._lock = asyncio.Lock()
        async with self._lock:
            self._reset_all()
        return self

    async def add_error(self, resource, status):
        """
        Add Latency method

        :param resource: passed resource name
        :type resource: str
        :param status: http error code
        :type status: str
        """
        status = str(status)
        async with self._lock:
            if resource == HTTPExceptionsAndLatencies.SPLIT:
                if status not in self._split:
                    self._split[status] = 0
                self._split[status] += 1
            elif resource == HTTPExceptionsAndLatencies.SEGMENT:
                if status not in self._segment:
                    self._segment[status] = 0
                self._segment[status] += 1
            elif resource == HTTPExceptionsAndLatencies.IMPRESSION:
                if status not in self._impression:
                    self._impression[status] = 0
                self._impression[status] += 1
            elif resource == HTTPExceptionsAndLatencies.IMPRESSION_COUNT:
                if status not in self._impression_count:
                    self._impression_count[status] = 0
                self._impression_count[status] += 1
            elif resource == HTTPExceptionsAndLatencies.EVENT:
                if status not in self._event:
                    self._event[status] = 0
                self._event[status] += 1
            elif resource == HTTPExceptionsAndLatencies.TELEMETRY:
                if status not in self._telemetry:
                    self._telemetry[status] = 0
                self._telemetry[status] += 1
            elif resource == HTTPExceptionsAndLatencies.TOKEN:
                if status not in self._token:
                    self._token[status] = 0
                self._token[status] += 1
            else:
                return

    async def pop_all(self):
        """
        Pop all errors

        :return: Dictonary of exceptions
        :rtype: dict
        """
        async with self._lock:
            http_errors = {
                HTTPExceptionsAndLatencies.HTTP_ERRORS.value: {
                    HTTPExceptionsAndLatencies.SPLIT.value: self._split,
                    HTTPExceptionsAndLatencies.SEGMENT.value: self._segment,
                    HTTPExceptionsAndLatencies.IMPRESSION.value: self._impression,
                    HTTPExceptionsAndLatencies.IMPRESSION_COUNT.value: self._impression_count, HTTPExceptionsAndLatencies.EVENT.value: self._event,
                    HTTPExceptionsAndLatencies.TELEMETRY.value: self._telemetry, HTTPExceptionsAndLatencies.TOKEN.value: self._token
                }
            }
            self._reset_all()
            return http_errors


class TelemetryCountersBase(object, metaclass=abc.ABCMeta):
    """
    Counters base class

    """
    def _reset_all(self):
        """Reset variables"""
        self._impressions_queued = 0
        self._impressions_deduped = 0
        self._impressions_dropped = 0
        self._events_queued = 0
        self._events_dropped = 0
        self._auth_rejections = 0
        self._token_refreshes = 0
        self._session_length = 0
        self._update_from_sse = {}

    @abc.abstractmethod
    def record_impressions_value(self, resource, value):
        """
        Append to the resource value
        """

    @abc.abstractmethod
    def record_events_value(self, resource, value):
        """
        Append to the resource value
        """

    @abc.abstractmethod
    def record_auth_rejections(self):
        """
        Increament the auth rejection resource by one.
        """

    @abc.abstractmethod
    def record_token_refreshes(self):
        """
        Increament the token refreshes resource by one.
        """

    @abc.abstractmethod
    def record_session_length(self, session):
        """
        Set the session length value
        """

    @abc.abstractmethod
    def get_counter_stats(self, resource):
        """
        Get resource counter value
        """

    @abc.abstractmethod
    def get_session_length(self):
        """
        Get session length
        """

    @abc.abstractmethod
    def pop_auth_rejections(self):
        """
        Pop auth rejections
        """

    @abc.abstractmethod
    def pop_token_refreshes(self):
        """
        Pop token refreshes
        """


class TelemetryCounters(TelemetryCountersBase):
    """
    Counters class

    """
    def __init__(self):
        """Constructor"""
        self._lock = threading.RLock()
        with self._lock:
            self._reset_all()

    def record_impressions_value(self, resource, value):
        """
        Append to the resource value

        :param resource: passed resource name
        :type resource: str
        :param value: value to be appended
        :type value: int
        """
        with self._lock:
            if resource == CounterConstants.IMPRESSIONS_QUEUED:
                self._impressions_queued += value
            elif resource == CounterConstants.IMPRESSIONS_DEDUPED:
                self._impressions_deduped += value
            elif resource == CounterConstants.IMPRESSIONS_DROPPED:
                self._impressions_dropped += value
            else:
                return

    def record_events_value(self, resource, value):
        """
        Append to the resource value

        :param resource: passed resource name
        :type resource: str
        :param value: value to be appended
        :type value: int
        """
        with self._lock:
            if resource == CounterConstants.EVENTS_QUEUED:
                self._events_queued += value
            elif resource == CounterConstants.EVENTS_DROPPED:
                self._events_dropped += value
            else:
                return

    def record_update_from_sse(self, event):
        """
        Increment the update from sse resource by one.
        """
        with self._lock:
            if event.value not in self._update_from_sse:
                self._update_from_sse[event.value] = 0
            self._update_from_sse[event.value] += 1

    def record_auth_rejections(self):
        """
        Increment the auth rejection resource by one.

        """
        with self._lock:
            self._auth_rejections += 1

    def record_token_refreshes(self):
        """
        Increment the token refreshes resource by one.

        """
        with self._lock:
            self._token_refreshes += 1

    def pop_update_from_sse(self, event):
        """
        Pop update from sse
        :return: update from sse value
        :rtype: int
        """
        with self._lock:
            if self._update_from_sse.get(event.value) is None:
                return 0

            update_from_sse = self._update_from_sse[event.value]
            self._update_from_sse[event.value] = 0
            return update_from_sse

    def record_session_length(self, session):
        """
        Set the session length value

        :param session: value to be set
        :type session: int
        """
        with self._lock:
            self._session_length = session

    def get_counter_stats(self, resource):
        """
        Get resource counter value

        :param resource: passed resource name
        :type resource: str

        :return: resource value
        :rtype: int
        """

        with self._lock:
            if resource == CounterConstants.IMPRESSIONS_QUEUED:
                return self._impressions_queued

            elif resource == CounterConstants.IMPRESSIONS_DEDUPED:
                return self._impressions_deduped

            elif resource == CounterConstants.IMPRESSIONS_DROPPED:
                return self._impressions_dropped

            elif resource == CounterConstants.EVENTS_QUEUED:
                return self._events_queued

            elif resource == CounterConstants.EVENTS_DROPPED:
                return self._events_dropped

            else:
                return 0

    def get_session_length(self):
        """
        Get session length

        :return: session length value
        :rtype: int
        """
        with self._lock:
            return self._session_length

    def pop_auth_rejections(self):
        """
        Pop auth rejections

        :return: auth rejections value
        :rtype: int
        """
        with self._lock:
            auth_rejections = self._auth_rejections
            self._auth_rejections = 0
            return auth_rejections

    def pop_token_refreshes(self):
        """
        Pop token refreshes

        :return: token refreshes value
        :rtype: int
        """
        with self._lock:
            token_refreshes = self._token_refreshes
            self._token_refreshes = 0
            return token_refreshes

class TelemetryCountersAsync(TelemetryCountersBase):
    """
    Counters async class

    """
    @classmethod
    async def create(cls):
        """Constructor"""
        self = cls()
        self._lock = asyncio.Lock()
        async with self._lock:
            self._reset_all()
        return self

    async def record_impressions_value(self, resource, value):
        """
        Append to the resource value

        :param resource: passed resource name
        :type resource: str
        :param value: value to be appended
        :type value: int
        """
        async with self._lock:
            if resource == CounterConstants.IMPRESSIONS_QUEUED:
                self._impressions_queued += value
            elif resource == CounterConstants.IMPRESSIONS_DEDUPED:
                self._impressions_deduped += value
            elif resource == CounterConstants.IMPRESSIONS_DROPPED:
                self._impressions_dropped += value
            else:
                return

    async def record_events_value(self, resource, value):
        """
        Append to the resource value

        :param resource: passed resource name
        :type resource: str
        :param value: value to be appended
        :type value: int
        """
        async with self._lock:
            if resource == CounterConstants.EVENTS_QUEUED:
                self._events_queued += value
            elif resource == CounterConstants.EVENTS_DROPPED:
                self._events_dropped += value
            else:
                return

    async def record_update_from_sse(self, event):
        """
        Increment the update from sse resource by one.
        """
        async with self._lock:
            if event.value not in self._update_from_sse:
                self._update_from_sse[event.value] = 0
            self._update_from_sse[event.value] += 1

    async def record_auth_rejections(self):
        """
        Increment the auth rejection resource by one.

        """
        async with self._lock:
            self._auth_rejections += 1

    async def record_token_refreshes(self):
        """
        Increment the token refreshes resource by one.

        """
        async with self._lock:
            self._token_refreshes += 1

    async def pop_update_from_sse(self, event):
        """
        Pop update from sse
        :return: update from sse value
        :rtype: int
        """
        async with self._lock:
            if self._update_from_sse.get(event.value) is None:
                return 0

            update_from_sse = self._update_from_sse[event.value]
            self._update_from_sse[event.value] = 0
            return update_from_sse

    async def record_session_length(self, session):
        """
        Set the session length value

        :param session: value to be set
        :type session: int
        """
        async with self._lock:
            self._session_length = session

    async def get_counter_stats(self, resource):
        """
        Get resource counter value

        :param resource: passed resource name
        :type resource: str

        :return: resource value
        :rtype: int
        """
        async with self._lock:
            if resource == CounterConstants.IMPRESSIONS_QUEUED:
                return self._impressions_queued

            elif resource == CounterConstants.IMPRESSIONS_DEDUPED:
                return self._impressions_deduped

            elif resource == CounterConstants.IMPRESSIONS_DROPPED:
                return self._impressions_dropped

            elif resource == CounterConstants.EVENTS_QUEUED:
                return self._events_queued

            elif resource == CounterConstants.EVENTS_DROPPED:
                return self._events_dropped

            else:
                return 0

    async def get_session_length(self):
        """
        Get session length

        :return: session length value
        :rtype: int
        """
        async with self._lock:
            return self._session_length

    async def pop_auth_rejections(self):
        """
        Pop auth rejections

        :return: auth rejections value
        :rtype: int
        """
        async with self._lock:
            auth_rejections = self._auth_rejections
            self._auth_rejections = 0
            return auth_rejections

    async def pop_token_refreshes(self):
        """
        Pop token refreshes

        :return: token refreshes value
        :rtype: int
        """
        async with self._lock:
            token_refreshes = self._token_refreshes
            self._token_refreshes = 0
            return token_refreshes


class StreamingEvent(object):
    """
    Streaming event class

    """
    def __init__(self, streaming_event):
        """
        Constructor

        :param streaming_event: Streaming event tuple: ('type', 'data', 'time')
        :type streaming_event: dict
        """
        self._type = streaming_event[0].value
        self._data = streaming_event[1]
        self._time = streaming_event[2]

    @property
    def type(self):
        """
        Get streaming event type

        :return: streaming event type
        :rtype: str
        """
        return self._type

    @property
    def data(self):
        """
        Get streaming event data

        :return: streaming event data
        :rtype: str
        """
        return self._data

    @property
    def time(self):
        """
        Get streaming event time

        :return: streaming event time
        :rtype: int
        """
        return self._time

class StreamingEventsAsync(object):
    """
    Streaming events async class

    """
    @classmethod
    async def create(cls):
        """Constructor"""
        self = cls()
        self._lock = asyncio.Lock()
        async with self._lock:
            self._streaming_events = []
        return self

    async def record_streaming_event(self, streaming_event):
        """
        Record new streaming event

        :param streaming_event: Streaming event dict:
                {'type': string, 'data': string, 'time': string}
        :type streaming_event: dict
        """
        if not StreamingEvent(streaming_event):
            return
        async with self._lock:
            if len(self._streaming_events) < MAX_STREAMING_EVENTS:
                self._streaming_events.append(StreamingEvent(streaming_event))

    async def pop_streaming_events(self):
        """
        Get and reset streaming events

        :return: streaming events dict
        :rtype: dict
        """
        async with self._lock:
            streaming_events = self._streaming_events
            self._streaming_events = []
            return {_StreamingEventsConstant.STREAMING_EVENTS.value: [
                {'e': streaming_event.type, 'd': streaming_event.data,
                't': streaming_event.time} for streaming_event in streaming_events]}

class StreamingEvents(object):
    """
    Streaming events class

    """
    def __init__(self):
        """Constructor"""
        self._lock = threading.RLock()
        with self._lock:
            self._streaming_events = []

    def record_streaming_event(self, streaming_event):
        """
        Record new streaming event

        :param streaming_event: Streaming event dict:
                {'type': string, 'data': string, 'time': string}
        :type streaming_event: dict
        """
        if not StreamingEvent(streaming_event):
            return
        with self._lock:
            if len(self._streaming_events) < MAX_STREAMING_EVENTS:
                self._streaming_events.append(StreamingEvent(streaming_event))

    def pop_streaming_events(self):
        """
        Get and reset streaming events

        :return: streaming events dict
        :rtype: dict
        """

        with self._lock:
            streaming_events = self._streaming_events
            self._streaming_events = []
            return {_StreamingEventsConstant.STREAMING_EVENTS.value: [
                {'e': streaming_event.type, 'd': streaming_event.data,
                't': streaming_event.time} for streaming_event in streaming_events]}


class TelemetryConfigBase(object, metaclass=abc.ABCMeta):
    """
    Telemetry init config base class

    """
    def _reset_all(self):
        """Reset variables"""
        self._block_until_ready_timeout = 0
        self._not_ready = 0
        self._time_until_ready = 0
        self._operation_mode = None
        self._storage_type = None
        self._streaming_enabled = None
        self._refresh_rate = {
            _ConfigParams.SPLITS_REFRESH_RATE.value: 0,
            _ConfigParams.SEGMENTS_REFRESH_RATE.value: 0,
            _ConfigParams.IMPRESSIONS_REFRESH_RATE.value: 0,
            _ConfigParams.EVENTS_REFRESH_RATE.value: 0,
            _ConfigParams.TELEMETRY_REFRESH_RATE.value: 0}
        self._url_override = {
            _ApiURLs.SDK_URL.value: False,
            _ApiURLs.EVENTS_URL.value: False,
            _ApiURLs.AUTH_URL.value: False,
            _ApiURLs.STREAMING_URL.value: False,
            _ApiURLs.TELEMETRY_URL.value: False}
        self._impressions_queue_size = 0
        self._events_queue_size = 0
        self._impressions_mode = None
        self._impression_listener = False
        self._http_proxy = None
        self._active_factory_count = 0
        self._redundant_factory_count = 0
        self._flag_sets = 0
        self._flag_sets_invalid = 0

    @abc.abstractmethod
    def record_config(self, config, extra_config, total_flag_sets, invalid_flag_sets):
        """
        Record configurations.
        """

    @abc.abstractmethod
    def record_active_and_redundant_factories(self, active_factory_count, redundant_factory_count):
        """
        Record active and redundant factories counts
        """

    @abc.abstractmethod
    def record_ready_time(self, ready_time):
        """
        Record ready time.
        """

    @abc.abstractmethod
    def record_bur_time_out(self):
        """
        Record block until ready timeout count
        """

    @abc.abstractmethod
    def record_not_ready_usage(self):
        """
        record non-ready usage count
        """

    @abc.abstractmethod
    def get_bur_time_outs(self):
        """
        Get block until ready timeout.
        """

    @abc.abstractmethod
    def get_non_ready_usage(self):
        """
        Get non-ready usage.
        """

    @abc.abstractmethod
    def get_stats(self):
        """
        Get config stats.
        """

    def _get_operation_mode(self, op_mode):
        """
        Get formatted operation mode

        :param op_mode: config operation mode
        :type config: str

        :return: operation mode
        :rtype: int
        """
        if op_mode == OperationMode.STANDALONE.value:
            return 0

        elif op_mode == OperationMode.CONSUMER.value:
            return 1

        else:
            return 2

    def _get_storage_type(self, op_mode, st_type):
        """
        Get storage type from operation mode

        :param op_mode: config operation mode
        :type config: str

        :return: storage type
        :rtype: str
        """
        if op_mode == OperationMode.STANDALONE.value:
            return StorageType.MEMORY.value

        elif st_type == StorageType.REDIS.value:
            return StorageType.REDIS.value

        else:
            return StorageType.PLUGGABLE.value

    def _get_refresh_rates(self, config):
        """
        Get refresh rates within config dict

        :param config: config dict
        :type config: dict

        :return: refresh rates
        :rtype: RefreshRates object
        """
        return {
            _ConfigParams.SPLITS_REFRESH_RATE.value: config[_ConfigParams.SPLITS_REFRESH_RATE.value],
            _ConfigParams.SEGMENTS_REFRESH_RATE.value: config[_ConfigParams.SEGMENTS_REFRESH_RATE.value],
            _ConfigParams.IMPRESSIONS_REFRESH_RATE.value: config[_ConfigParams.IMPRESSIONS_REFRESH_RATE.value],
            _ConfigParams.EVENTS_REFRESH_RATE.value: config[_ConfigParams.EVENTS_REFRESH_RATE.value],
            _ConfigParams.TELEMETRY_REFRESH_RATE.value: config[_ConfigParams.TELEMETRY_REFRESH_RATE.value]
        }

    def _get_url_overrides(self, config):
        """
        Get URL override within the config dict.

        :param config: config dict
        :type config: dict

        :return: URL overrides dict
        :rtype: URLOverrides object
        """
        return  {
            _ApiURLs.SDK_URL.value: True if _ApiURLs.SDK_URL.value in config else False,
            _ApiURLs.EVENTS_URL.value: True if _ApiURLs.EVENTS_URL.value in config else False,
            _ApiURLs.AUTH_URL.value: True if _ApiURLs.AUTH_URL.value in config else False,
            _ApiURLs.STREAMING_URL.value: True if _ApiURLs.STREAMING_URL.value in config else False,
            _ApiURLs.TELEMETRY_URL.value: True if _ApiURLs.TELEMETRY_URL.value in config else False
        }

    def _get_impressions_mode(self, imp_mode):
        """
        Get impressions mode from operation mode

        :param op_mode: config operation mode
        :type config: str

        :return: impressions mode
        :rtype: int
        """
        if imp_mode == ImpressionsMode.DEBUG.value:
            return 1

        elif imp_mode == ImpressionsMode.OPTIMIZED.value:
            return 0

        else:
            return 2

    def _check_if_proxy_detected(self):
        """
        Return boolean flag if network https proxy is detected

        :return: https network proxy flag
        :rtype: boolean
        """
        for x in os.environ:
            if x.upper() == _ExtraConfig.HTTPS_PROXY_ENV.value:
                return True

        return False


class TelemetryConfig(TelemetryConfigBase):
    """
    Telemetry init config class

    """
    def __init__(self):
        """Constructor"""
        self._lock = threading.RLock()
        with self._lock:
            self._reset_all()

    def record_config(self, config, extra_config, total_flag_sets, invalid_flag_sets):
        """
        Record configurations.

        :param config: config dict: {
            'operationMode': int, 'storageType': string, 'streamingEnabled': boolean,
            'refreshRate' : {
                'featuresRefreshRate': int,
                'segmentsRefreshRate': int,
                'impressionsRefreshRate': int,
                'eventsPushRate': int,
                'metricsRefreshRate': int
            }
            'urlOverride' : {
                'sdk_url': boolean, 'events_url': boolean, 'auth_url': boolean,
                'streaming_url': boolean, 'telemetry_url': boolean, }
            },
            'impressionsQueueSize': int, 'eventsQueueSize': int, 'impressionsMode': string,
            'impressionsListener': boolean, 'activeFactoryCount': int, 'redundantFactoryCount': int
        }
        :type config: dict
        """
        with self._lock:
            self._operation_mode = self._get_operation_mode(config[_ConfigParams.OPERATION_MODE.value])
            self._storage_type = self._get_storage_type(config[_ConfigParams.OPERATION_MODE.value], config[_ConfigParams.STORAGE_TYPE.value])
            self._streaming_enabled = config[_ConfigParams.STREAMING_ENABLED.value]
            self._refresh_rate = self._get_refresh_rates(config)
            self._url_override = self._get_url_overrides(extra_config)
            self._impressions_queue_size = config[_ConfigParams.IMPRESSIONS_QUEUE_SIZE.value]
            self._events_queue_size = config[_ConfigParams.EVENTS_QUEUE_SIZE.value]
            self._impressions_mode = self._get_impressions_mode(config[_ConfigParams.IMPRESSIONS_MODE.value])
            self._impression_listener = True if config[_ConfigParams.IMPRESSIONS_LISTENER.value] is not None else False
            self._http_proxy = self._check_if_proxy_detected()
            self._flag_sets = total_flag_sets
            self._flag_sets_invalid = invalid_flag_sets

    def record_active_and_redundant_factories(self, active_factory_count, redundant_factory_count):
        """
        Record active and redundant factories counts

        :param active_factory_count: active factories count
        :type active_factory_count: int

        :param redundant_factory_count: redundant factories count
        :type redundant_factory_count: int
        """
        with self._lock:
            self._active_factory_count = active_factory_count
            self._redundant_factory_count = redundant_factory_count

    def record_ready_time(self, ready_time):
        """
        Record ready time.

        :param ready_time: SDK ready time
        :type ready_time: int
        """
        with self._lock:
            self._time_until_ready = ready_time

    def record_bur_time_out(self):
        """
        Record block until ready timeout count

        """
        with self._lock:
            self._block_until_ready_timeout += 1

    def record_not_ready_usage(self):
        """
        record non-ready usage count

        """
        with self._lock:
            self._not_ready += 1

    def get_bur_time_outs(self):
        """
        Get block until ready timeout.

        :return: block until ready timeouts count
        :rtype: int
        """
        with self._lock:
            return self._block_until_ready_timeout

    def get_non_ready_usage(self):
        """
        Get non-ready usage.

        :return: non-ready usage count
        :rtype: int
        """
        with self._lock:
            return self._not_ready

    def get_stats(self):
        """
        Get config stats.

        :return: dict of all config stats.
        :rtype: dict
        """
        with self._lock:
            return {
                'bT':  self._block_until_ready_timeout,
                'nR': self._not_ready,
                'tR': self._time_until_ready,
                'oM': self._operation_mode,
                'sT': self._storage_type,
                'sE': self._streaming_enabled,
                'rR': {
                    'sp': self._refresh_rate[_ConfigParams.SPLITS_REFRESH_RATE.value],
                    'se': self._refresh_rate[_ConfigParams.SEGMENTS_REFRESH_RATE.value],
                    'im': self._refresh_rate[_ConfigParams.IMPRESSIONS_REFRESH_RATE.value],
                    'ev': self._refresh_rate[_ConfigParams.EVENTS_REFRESH_RATE.value],
                    'te': self._refresh_rate[_ConfigParams.TELEMETRY_REFRESH_RATE.value]},
                'uO': {
                    's': self._url_override[_ApiURLs.SDK_URL.value],
                    'e': self._url_override[_ApiURLs.EVENTS_URL.value],
                    'a': self._url_override[_ApiURLs.AUTH_URL.value],
                    'st': self._url_override[_ApiURLs.STREAMING_URL.value],
                    't': self._url_override[_ApiURLs.TELEMETRY_URL.value]},
                'iQ': self._impressions_queue_size,
                'eQ': self._events_queue_size,
                'iM': self._impressions_mode,
                'iL': self._impression_listener,
                'hp': self._http_proxy,
                'aF': self._active_factory_count,
                'rF': self._redundant_factory_count,
                'fsT': self._flag_sets,
                'fsI': self._flag_sets_invalid
            }


class TelemetryConfigAsync(TelemetryConfigBase):
    """
    Telemetry init config async class

    """
    @classmethod
    async def create(cls):
        """Constructor"""
        self = cls()
        self._lock = asyncio.Lock()
        async with self._lock:
            self._reset_all()
        return self

    async def record_config(self, config, extra_config, total_flag_sets, invalid_flag_sets):
        """
        Record configurations.

        :param config: config dict: {
            'operationMode': int, 'storageType': string, 'streamingEnabled': boolean,
            'refreshRate' : {
                'featuresRefreshRate': int,
                'segmentsRefreshRate': int,
                'impressionsRefreshRate': int,
                'eventsPushRate': int,
                'metricsRefreshRate': int
            }
            'urlOverride' : {
                'sdk_url': boolean, 'events_url': boolean, 'auth_url': boolean,
                'streaming_url': boolean, 'telemetry_url': boolean, }
            },
            'impressionsQueueSize': int, 'eventsQueueSize': int, 'impressionsMode': string,
            'impressionsListener': boolean, 'activeFactoryCount': int, 'redundantFactoryCount': int
        }
        :type config: dict
        """
        async with self._lock:
            self._operation_mode = self._get_operation_mode(config[_ConfigParams.OPERATION_MODE.value])
            self._storage_type = self._get_storage_type(config[_ConfigParams.OPERATION_MODE.value], config[_ConfigParams.STORAGE_TYPE.value])
            self._streaming_enabled = config[_ConfigParams.STREAMING_ENABLED.value]
            self._refresh_rate = self._get_refresh_rates(config)
            self._url_override = self._get_url_overrides(extra_config)
            self._impressions_queue_size = config[_ConfigParams.IMPRESSIONS_QUEUE_SIZE.value]
            self._events_queue_size = config[_ConfigParams.EVENTS_QUEUE_SIZE.value]
            self._impressions_mode = self._get_impressions_mode(config[_ConfigParams.IMPRESSIONS_MODE.value])
            self._impression_listener = True if config[_ConfigParams.IMPRESSIONS_LISTENER.value] is not None else False
            self._http_proxy = self._check_if_proxy_detected()
            self._flag_sets = total_flag_sets
            self._flag_sets_invalid = invalid_flag_sets

    async def record_active_and_redundant_factories(self, active_factory_count, redundant_factory_count):
        """
        Record active and redundant factories counts

        :param active_factory_count: active factories count
        :type active_factory_count: int

        :param redundant_factory_count: redundant factories count
        :type redundant_factory_count: int
        """
        async with self._lock:
            self._active_factory_count = active_factory_count
            self._redundant_factory_count = redundant_factory_count

    async def record_ready_time(self, ready_time):
        """
        Record ready time.

        :param ready_time: SDK ready time
        :type ready_time: int
        """
        async with self._lock:
            self._time_until_ready = ready_time

    async def record_bur_time_out(self):
        """
        Record block until ready timeout count

        """
        async with self._lock:
            self._block_until_ready_timeout += 1

    async def record_not_ready_usage(self):
        """
        record non-ready usage count

        """
        async with self._lock:
            self._not_ready += 1

    async def get_bur_time_outs(self):
        """
        Get block until ready timeout.

        :return: block until ready timeouts count
        :rtype: int
        """
        async with self._lock:
            return self._block_until_ready_timeout

    async def get_non_ready_usage(self):
        """
        Get non-ready usage.

        :return: non-ready usage count
        :rtype: int
        """
        async with self._lock:
            return self._not_ready

    async def get_stats(self):
        """
        Get config stats.

        :return: dict of all config stats.
        :rtype: dict
        """
        async with self._lock:
            return {
                'bT':  self._block_until_ready_timeout,
                'nR': self._not_ready,
                'tR': self._time_until_ready,
                'oM': self._operation_mode,
                'sT': self._storage_type,
                'sE': self._streaming_enabled,
                'rR': {
                    'sp': self._refresh_rate[_ConfigParams.SPLITS_REFRESH_RATE.value],
                    'se': self._refresh_rate[_ConfigParams.SEGMENTS_REFRESH_RATE.value],
                    'im': self._refresh_rate[_ConfigParams.IMPRESSIONS_REFRESH_RATE.value],
                    'ev': self._refresh_rate[_ConfigParams.EVENTS_REFRESH_RATE.value],
                    'te': self._refresh_rate[_ConfigParams.TELEMETRY_REFRESH_RATE.value]},
                'uO': {
                    's': self._url_override[_ApiURLs.SDK_URL.value],
                    'e': self._url_override[_ApiURLs.EVENTS_URL.value],
                    'a': self._url_override[_ApiURLs.AUTH_URL.value],
                    'st': self._url_override[_ApiURLs.STREAMING_URL.value],
                    't': self._url_override[_ApiURLs.TELEMETRY_URL.value]},
                'iQ': self._impressions_queue_size,
                'eQ': self._events_queue_size,
                'iM': self._impressions_mode,
                'iL': self._impression_listener,
                'hp': self._http_proxy,
                'aF': self._active_factory_count,
                'rF': self._redundant_factory_count,
                'fsT': self._flag_sets,
                'fsI': self._flag_sets_invalid
            }