"""Shared pytest fixtures."""
import faulthandler
import socket
import sys
import threading
import weakref

import pytest

from splitio.client.factory import SplitFactoryBase, SplitFactory, SplitFactoryAsync

_FACTORY_CLASSES = (SplitFactoryBase, SplitFactory, SplitFactoryAsync)
_MISSING = object()

_BLOCKED_SUFFIX = '.split.io'
_net_violations = []
_current_test = {'id': None}
_live_factories = weakref.WeakSet()
_real_getaddrinfo = socket.getaddrinfo
_real_thread_init = threading.Thread.__init__
_real_factory_init = SplitFactory.__init__


def _guarded_getaddrinfo(host, *args, **kwargs):
    """Refuse to resolve the Split backend hosts, and remember who tried."""
    name = host.decode() if isinstance(host, bytes) else host
    if isinstance(name, str) and name.endswith(_BLOCKED_SUFFIX):
        thread = threading.current_thread()
        _net_violations.append((getattr(thread, '_created_by_test', None) or _current_test['id'], name, thread.name))
        raise socket.gaierror(socket.EAI_NONAME, 'network access to %s blocked by tests/conftest.py' % name)
    return _real_getaddrinfo(host, *args, **kwargs)


def _tracking_thread_init(self, *args, **kwargs):
    """Remember which test created each thread, to name the culprit of a leaked background client."""
    parent = threading.current_thread()
    self._created_by_test = getattr(parent, '_created_by_test', None) or _current_test['id']
    _real_thread_init(self, *args, **kwargs)


def _tracking_factory_init(self, *args, **kwargs):
    _real_factory_init(self, *args, **kwargs)
    _live_factories.add(self)


socket.getaddrinfo = _guarded_getaddrinfo
threading.Thread.__init__ = _tracking_thread_init
SplitFactory.__init__ = _tracking_factory_init


@pytest.fixture(autouse=True)
def _no_split_network(request):
    """Tests must not contact the Split backend (sdk/events/telemetry/auth *.split.io)."""
    _current_test['id'] = request.node.nodeid
    start = len(_net_violations)
    yield
    _destroy_leaked_factories()
    _current_test['id'] = None
    found = _net_violations[start:]
    if found:
        pytest.fail('test contacted the Split backend (mock the HTTP layer): %s' % sorted(
            {'%s from thread %s created in %s' % (host, thr, origin) for origin, host, thr in found}), pytrace=False)


@pytest.fixture(autouse=True)
def _restore_factory_ready_property():
    """
    Undo class-level overrides of `ready` made by tests.

    Many tests do `type(factory).ready = PropertyMock(...)`, which replaces the property on the
    factory class itself and leaks into every later test in the session.
    """
    saved = {cls: vars(cls).get('ready', _MISSING) for cls in _FACTORY_CLASSES}
    yield
    for cls, original in saved.items():
        if original is _MISSING:
            if 'ready' in vars(cls):
                delattr(cls, 'ready')
        else:
            setattr(cls, 'ready', original)


def pytest_sessionfinish(session, exitstatus):
    """
    Safety net against a process that never exits after the tests end.

    A leaked non-daemon thread keeps the interpreter alive forever (and the CI step until its
    timeout). Name those threads, and if the process is still alive after 120s dump every stack to
    stderr and exit with code 1. The timer is cancelled when the interpreter exits normally.
    """
    # Timers (e.g. the push manager's TokenRefresh) are non-daemon: a leaked one blocks exit until it fires.
    for thread in threading.enumerate():
        if isinstance(thread, threading.Timer):
            thread.cancel()
    blocking = [t.name for t in threading.enumerate() if not t.daemon and t is not threading.main_thread()]
    if blocking:
        sys.stderr.write('\nNon-daemon threads still alive, they may block the process exit: %s\n' % blocking)
        sys.stderr.flush()
    faulthandler.dump_traceback_later(120, exit=True)


def _destroy_leaked_factories():
    """Stop the sync/polling threads of factories a test created but didn't destroy."""
    for factory in list(_live_factories):
        _live_factories.discard(factory)
        try:
            if not factory.destroyed:
                done = threading.Event()
                factory.destroy(done)
                done.wait(5)
        except Exception:  # pylint: disable=broad-except
            pass  # factories built from mocks may not be destroyable; threads are daemons anyway
