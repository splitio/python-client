"""Message processor & Notification manager keeper implementations."""

from queue import Queue
import abc

from splitio.push.parser import UpdateType
from splitio.push.workers import SplitWorker, SplitWorkerAsync, SegmentWorker, SegmentWorkerAsync
from splitio.optional.loaders import asyncio

class MessageProcessorBase(object, metaclass=abc.ABCMeta):
    """Message processor template."""

    @abc.abstractmethod
    def update_workers_status(self, enabled):
        """Enable/Disable push update workers."""

    @abc.abstractmethod
    def handle(self, event):
        """Handle incoming update event."""

    @abc.abstractmethod
    def shutdown(self):
        """Stop splits & segments workers."""

class MessageProcessor(MessageProcessorBase):
    """Message processor class."""

    def __init__(self, synchronizer):
        """
        Class constructor.

        :param synchronizer: synchronizer component
        :type synchronizer: splitio.sync.synchronizer.Synchronizer
        """
        self._split_queue = Queue()
        self._segments_queue = Queue()
        self._synchronizer = synchronizer
        self._split_worker = SplitWorker(synchronizer.synchronize_splits, self._split_queue)
        self._segments_worker = SegmentWorker(synchronizer.synchronize_segment, self._segments_queue)
        self._handlers = {
            UpdateType.SPLIT_UPDATE: self._handle_split_update,
            UpdateType.SPLIT_KILL: self._handle_split_kill,
            UpdateType.SEGMENT_UPDATE: self._handle_segment_change
        }

    def _handle_split_update(self, event):
        """
        Handle incoming split update notification.

        :param event: Incoming split change event
        :type event: splitio.push.parser.SplitChangeUpdate
        """
        self._split_queue.put(event)

    def _handle_split_kill(self, event):
        """
        Handle incoming split kill notification.

        :param event: Incoming split kill event
        :type event: splitio.push.parser.SplitKillUpdate
        """
        self._synchronizer.kill_split(event.split_name, event.default_treatment,
                                      event.change_number)
        self._split_queue.put(event)

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
            self._split_worker.start()
            self._segments_worker.start()
        else:
            self._split_worker.stop()
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
        """Stop splits & segments workers."""
        self._split_worker.stop()
        self._segments_worker.stop()


class MessageProcessorAsync(MessageProcessorBase):
    """Message processor class."""

    def __init__(self, synchronizer):
        """
        Class constructor.

        :param synchronizer: synchronizer component
        :type synchronizer: splitio.sync.synchronizer.Synchronizer
        """
        self._split_queue = asyncio.Queue()
        self._segments_queue = asyncio.Queue()
        self._synchronizer = synchronizer
        self._split_worker = SplitWorkerAsync(synchronizer.synchronize_splits, self._split_queue)
        self._segments_worker = SegmentWorkerAsync(synchronizer.synchronize_segment, self._segments_queue)
        self._handlers = {
            UpdateType.SPLIT_UPDATE: self._handle_split_update,
            UpdateType.SPLIT_KILL: self._handle_split_kill,
            UpdateType.SEGMENT_UPDATE: self._handle_segment_change
        }

    async def _handle_split_update(self, event):
        """
        Handle incoming split update notification.

        :param event: Incoming split change event
        :type event: splitio.push.parser.SplitChangeUpdate
        """
        await self._split_queue.put(event)

    async def _handle_split_kill(self, event):
        """
        Handle incoming split kill notification.

        :param event: Incoming split kill event
        :type event: splitio.push.parser.SplitKillUpdate
        """
        await self._synchronizer.kill_split(event.split_name, event.default_treatment,
                                      event.change_number)
        await self._split_queue.put(event)

    async def _handle_segment_change(self, event):
        """
        Handle incoming segment update notification.

        :param event: Incoming segment change event
        :type event: splitio.push.parser.Update
        """
        await self._segments_queue.put(event)

    async def update_workers_status(self, enabled):
        """
        Enable/Disable push update workers.

        :param enabled: if True, enable workers. If False, disable them.
        :type enabled: bool
        """
        if enabled:
            self._split_worker.start()
            self._segments_worker.start()
        else:
            await self._split_worker.stop()
            await self._segments_worker.stop()

    async def handle(self, event):
        """
        Handle incoming update event.

        :param event: incoming data update event.
        :type event: splitio.push.BaseUpdate
        """
        try:
            handle = self._handlers[event.update_type]
        except KeyError as exc:
            raise Exception('no handler for notification type: %s' % event.update_type) from exc

        await handle(event)

    async def shutdown(self):
        """Stop splits & segments workers."""
        await self._split_worker.stop()
        await self._segments_worker.stop()
