"""Push managers that own the auth token lifecycle and reconnect SSE whenever the token is refreshed."""
import logging

from splitio_commons.push.manager import PushManager, PushManagerAsync
from splitio_commons.push.status_tracker import Status

_LOGGER = logging.getLogger(__name__)


class PushManagerWithAuthRefresh(PushManager):
    """
    Push manager whose auth synchronizer is managed here (not by the base class), so that the
    auth synchronizer's refresh callback (`reconnect`) can reopen SSE with the refreshed token.
    """

    def __init__(self, *args, **kwargs):
        PushManager.__init__(self, *args, manage_auth_sync=False, **kwargs)
        self._auth_started = False
        self._stopped = False  # set by stop(), to not connect if stopped while authenticating

    def start(self):
        """Fetch the first token (once), and connect."""
        self._stopped = False
        if not self._auth_started:
            self._auth_synchronizer.start()
            self._auth_started = True
        if self._stopped:
            return
        PushManager.start(self)

    def restart(self):
        """Fetch a new token and reconnect (see `reconnect`)."""
        self._stopped = False
        PushManager.restart(self)

    def stop(self, blocking=False):
        """Stop the connection and the token refresh."""
        self._stopped = True
        self._auth_synchronizer.stop()
        PushManager.stop(self, blocking)

    def reconnect(self):
        """Auth synchronizer's refresh callback: reopen SSE with the new token, or fall back to polling if push is now disabled."""
        token = self._auth_synchronizer.token
        if token is None or self._stopped:
            return
        self.update_workers_status(False)
        PushManager.stop(self, True)
        if not token.push_enabled:
            self._feedback_loop.put(Status.PUSH_NONRETRYABLE_ERROR)
            return
        PushManager.start(self)


class PushManagerAsyncWithAuthRefresh(PushManagerAsync):
    """Async version of `PushManagerWithAuthRefresh`."""

    def __init__(self, *args, **kwargs):
        PushManagerAsync.__init__(self, *args, manage_auth_sync=False, **kwargs)
        self._auth_started = False

    async def start(self):
        """Fetch the first token (once), and connect."""
        if not self._auth_started:
            await self._auth_synchronizer.start()
            self._auth_started = True
        await PushManagerAsync.start(self)

    async def stop(self, blocking=False):
        """Stop the connection and the token refresh."""
        self._auth_synchronizer.stop()
        await PushManagerAsync.stop(self, blocking)

    async def reconnect(self):
        """Auth synchronizer's refresh callback: reopen SSE with the new token, or fall back to polling if push is now disabled."""
        token = self._auth_synchronizer.token
        if token is None:
            return
        await self.update_workers_status(False)
        await PushManagerAsync.stop(self, True)
        if not token.push_enabled:
            await self._feedback_loop.put(Status.PUSH_NONRETRYABLE_ERROR)
            return
        await PushManagerAsync.start(self)
