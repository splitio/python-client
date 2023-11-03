"""Feature Flag changes processing worker."""
import logging
import threading
import gzip
import zlib
import base64
import json
from enum import Enum

from splitio.models.splits import from_raw, Status
from splitio.models.telemetry import UpdateFromSSE
from splitio.push.parser import UpdateType
from splitio.util.storage_helper import update_feature_flag_storage

_LOGGER = logging.getLogger(__name__)

class CompressionMode(Enum):
    """Compression modes """

    NO_COMPRESSION = 0
    GZIP_COMPRESSION = 1
    ZLIB_COMPRESSION = 2

class SplitWorker(object):
    """Feature Flag Worker for processing updates."""

    _centinel = object()

    def __init__(self, synchronize_feature_flag, synchronize_segment, feature_flag_queue, feature_flag_storage, segment_storage, telemetry_runtime_producer):
        """
        Class constructor.

        :param synchronize_feature_flag: handler to perform feature flag synchronization on incoming event
        :type synchronize_feature_flag: callable

        :param synchronize_segment: handler to perform segment synchronization on incoming event
        :type synchronize_segment: function

        :param feature_flag_queue: queue with feature flag updates notifications
        :type feature_flag_queue: queue

        :param feature_flag_storage: feature flag storage instance
        :type feature_flag_storage: splitio.storage.inmemory.InMemorySplitStorage

        :param segment_storage: segment storage instance
        :type segment_storage: splitio.storage.inmemory.InMemorySegmentStorage

        :param telemetry_runtime_producer: Telemetry runtime producer instance
        :type telemetry_runtime_producer: splitio.engine.telemetry.TelemetryRuntimeProducer
        """
        self._feature_flag_queue = feature_flag_queue
        self._handler = synchronize_feature_flag
        self._segment_handler = synchronize_segment
        self._running = False
        self._worker = None
        self._feature_flag_storage = feature_flag_storage
        self._segment_storage = segment_storage
        self._compression_handlers = {
            CompressionMode.NO_COMPRESSION: lambda event: base64.b64decode(event.feature_flag_definition),
            CompressionMode.GZIP_COMPRESSION: lambda event: gzip.decompress(base64.b64decode(event.feature_flag_definition)).decode('utf-8'),
            CompressionMode.ZLIB_COMPRESSION: lambda event: zlib.decompress(base64.b64decode(event.feature_flag_definition)).decode('utf-8'),
        }
        self._telemetry_runtime_producer = telemetry_runtime_producer

    def is_running(self):
        """Return whether the working is running."""
        return self._running

    def _get_feature_flag_definition(self, event):
        """return feature flag definition in event."""
        cm = CompressionMode(event.compression) # will throw if the number is not defined in compression mode
        return self._compression_handlers[cm](event)

    def _check_instant_ff_update(self, event):
        if event.update_type == UpdateType.SPLIT_UPDATE and event.compression is not None and event.previous_change_number == self._feature_flag_storage.get_change_number():
            return True
        return False

    def _run(self):
        """Run worker handler."""
        while self.is_running():
            event = self._feature_flag_queue.get()
            if not self.is_running():
                break
            if event == self._centinel:
                continue
            _LOGGER.debug('Processing feature flag update %d', event.change_number)
            try:
                if self._check_instant_ff_update(event):
                    try:
                        new_feature_flag = from_raw(json.loads(self._get_feature_flag_definition(event)))
                        segment_list = update_feature_flag_storage(self._feature_flag_storage, [new_feature_flag], event.change_number)
                        for segment_name in segment_list:
                            if self._segment_storage.get(segment_name) is None:
                                _LOGGER.debug('Fetching new segment %s', segment_name)
                                self._segment_handler(segment_name, event.change_number)
                        self._telemetry_runtime_producer.record_update_from_sse(UpdateFromSSE.SPLIT_UPDATE)
                        continue
                    except Exception as e:
                        _LOGGER.error('Exception raised in updating feature flag')
                        _LOGGER.debug(str(e))
                        _LOGGER.debug('Exception information: ', exc_info=True)
                        pass
                self._handler(event.change_number)
            except Exception as e:  # pylint: disable=broad-except
                _LOGGER.error('Exception raised in feature flag synchronization')
                _LOGGER.debug(str(e))
                _LOGGER.debug('Exception information: ', exc_info=True)

    def start(self):
        """Start worker."""
        if self.is_running():
            _LOGGER.debug('Worker is already running')
            return
        self._running = True

        _LOGGER.debug('Starting Feature Flag Worker')
        self._worker = threading.Thread(target=self._run, name='PushFeatureFlagWorker', daemon=True)
        self._worker.start()

    def stop(self):
        """Stop worker."""
        _LOGGER.debug('Stopping Feature Flag Worker')
        if not self.is_running():
            _LOGGER.debug('Worker is not running')
            return
        self._running = False
        self._feature_flag_queue.put(self._centinel)
