"""Message processor & Notification manager keeper implementations."""

from queue import Queue

from six import raise_from

from splitio.push.splitworker import SplitWorker
from splitio.push.segmentworker import SegmentWorker


NOTIFICATION_SPLIT_CHANGE = "SPLIT_CHANGE"
NOTIFICATION_SPLIT_KILL = "SPLIT_KILL"
NOTIFICATION_SEGMENT_CHANGE = "SEGMENT_CHANGE"


class MessageProcessor(object):
    """Message processor class."""

    def __init__(self, synchronizer):
        """
        Class constructor.

        :param synchronizer: synchronizer component
        :type synchronizer: splitio.engine.synchronizer.Synchronizer
        """
        self._split_queue = Queue()
        self._segments_queue = Queue()
        self._split_worker = SplitWorker(lambda x: 0, self._split_queue)
        self._segments_worker = SegmentWorker(lambda x, y: 0, self._split_queue)
        self._handlers = {
            NOTIFICATION_SPLIT_CHANGE: self._handle_split_update,
            NOTIFICATION_SPLIT_KILL: self._handle_split_kill,
            NOTIFICATION_SEGMENT_CHANGE: self._handle_segment_change
        }

    def _handle_split_update(self, event):
        """
        Handle incoming split update notification.

        :param event: Incoming split change event
        :type event: splitio.push.parser.Update
        """
        #TODO
        print('received a split change event ', event)

    def _handle_split_kill(self, event):
        """
        Handle incoming split kill notification.

        :param event: Incoming split kill event
        :type event: splitio.push.parser.Update
        """
        #TODO
        print('received a split kill event ', event)

    def _handle_segment_change(self, event):
        """
        Handle incoming segment update notification.

        :param event: Incoming segment change event
        :type event: splitio.push.parser.Update
        """
        #TODO
        print('received a segment change event ', event)

    def handle(self, event):
        """
        Handle incoming update event.

        :param event: incoming data update event.
        :type event: splitio.push.Update
        """
        try:
            notification_type = event.data['type']
        except KeyError as exc:
            raise_from('update notification without type.', exc)

        try:
            handler = self._handlers[notification_type]
        except KeyError as exc:
            raise_from('no handler for notification type: %s' % notification_type, exc)

        handler(event)
