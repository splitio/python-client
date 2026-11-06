"""Shared pytest fixtures."""
import faulthandler
import socket
import threading

import pytest

from splitio.client.factory import SplitFactoryBase, SplitFactory, SplitFactoryAsync

_FACTORY_CLASSES = (SplitFactoryBase, SplitFactory, SplitFactoryAsync)
_MISSING = object()


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
    Fail fast if the process doesn't exit after the tests end.

    A leaked non-daemon thread would keep the interpreter alive forever (and the CI step until its
    timeout). If still alive after 120s, dump every thread's stack to stderr and exit with code 1.
    The timer is cancelled when the interpreter exits normally.
    """
    faulthandler.dump_traceback_later(120, exit=True)


_BLOCKED_SUFFIX = '.split.io'
_net_violations = []
_current_test = {'id': None}
_real_getaddrinfo = socket.getaddrinfo
_real_thread_init = threading.Thread.__init__


def _tracking_thread_init(self, *args, **kwargs):
    # remember which test created the thread, to name the real culprit of a leaked background client
    parent = threading.current_thread()
    self._created_by_test = getattr(parent, '_created_by_test', None) or _current_test['id']
    _real_thread_init(self, *args, **kwargs)


threading.Thread.__init__ = _tracking_thread_init


def _guarded_getaddrinfo(host, *args, **kwargs):
    name = host.decode() if isinstance(host, bytes) else host
    if isinstance(name, str) and name.endswith(_BLOCKED_SUFFIX):
        _net_violations.append((getattr(threading.current_thread(), '_created_by_test', None) or _current_test['id'],
                                name, threading.current_thread().name))
        raise socket.gaierror(socket.EAI_NONAME, 'network access to %s blocked by tests/conftest.py' % name)
    return _real_getaddrinfo(host, *args, **kwargs)


socket.getaddrinfo = _guarded_getaddrinfo


@pytest.fixture(autouse=True)
def _no_split_network(request):
    """Tests must not contact the Split backend (sdk/events/telemetry/auth *.split.io)."""
    _current_test['id'] = request.node.nodeid
    start = len(_net_violations)
    yield
    _current_test['id'] = None
    found = _net_violations[start:]
    if found:
        pytest.fail('test contacted the Split backend (mock the HTTP layer): %s' % sorted(
            {'%s from thread %s created in %s' % (host, thr, origin) for origin, host, thr in found}), pytrace=False)
