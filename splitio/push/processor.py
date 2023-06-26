"""Message processor & Notification manager keeper implementations."""

from queue import Queue

from splitio.push.parser import UpdateType
from splitio.push.splitworker import SplitWorker
from splitio.push.segmentworker import SegmentWorker

class MessageProcessor(object):
    """Message processor class."""

    def __init__(self, synchronizer, telemetry_runtime_producer):
        """
        Class constructor.

        :param synchronizer: synchronizer component
        :type synchronizer: splitio.sync.synchronizer.Synchronizer
        """
        self._feature_flag_queue = Queue()
        self._segments_queue = Queue()
        self._synchronizer = synchronizer
        self._feature_flag_worker = SplitWorker(synchronizer.synchronize_splits, synchronizer.synchronize_segment, self._feature_flag_queue, synchronizer.split_sync.feature_flag_storage, synchronizer.segment_storage, telemetry_runtime_producer)
        self._segments_worker = SegmentWorker(synchronizer.synchronize_segment, self._segments_queue)
        self._handlers = {
            UpdateType.SPLIT_UPDATE: self._handle_feature_flag_update,
            UpdateType.SPLIT_KILL: self._handle_feature_flag_kill,
            UpdateType.SEGMENT_UPDATE: self._handle_segment_change
        }

    def _handle_feature_flag_update(self, event):
        """
        Handle incoming feature flag update notification.

        :param event: Incoming feature flag change event
        :type event: splitio.push.parser.SplitChangeUpdate
        """
        self._feature_flag_queue.put(event)

    def _handle_feature_flag_kill(self, event):
        """
        Handle incoming feature flag kill notification.

        :param event: Incoming feature flag kill event
        :type event: splitio.push.parser.SplitKillUpdate
        """
        self._synchronizer.kill_split(event.feature_flag_name, event.default_treatment,
                                      event.change_number)
        self._feature_flag_queue.put(event)

    def _handle_segment_change(self, event):
        """
        Handle incoming segment update notification.

        :param event: Incoming segment change event
        :type event: splitio.push.parser.Update
        """
        self._segments_queue.put(event)

    def update_workers_status(self, enabled):
        """
        Enable/Disable push update workers.

        :param enabled: if True, enable workers. If False, disable them.
        :type enabled: bool
        """
        if enabled:
            self._feature_flag_worker.start()
            self._segments_worker.start()
        else:
            self._feature_flag_worker.stop()
            self._segments_worker.stop()

    def handle(self, event):
        """
        Handle incoming update event.

        :param event: incoming data update event.
        :type event: splitio.push.BaseUpdate
        """
        try:
            handle = self._handlers[event.update_type]
        except KeyError as exc:
            raise Exception('no handler for notification type: %s' % event.update_type) from exc

        handle(event)

    def shutdown(self):
        """Stop feature flags & segments workers."""
        self._feature_flag_worker.stop()
        self._segments_worker.stop()
