"""Shared pytest fixtures."""
import faulthandler
import socket
import sys
import threading
import time
import traceback
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
    Fail fast if the process doesn't exit after the tests end.

    A leaked non-daemon thread would keep the interpreter alive forever (and the CI step until its
    timeout). If still alive after 120s, dump every thread's stack to stderr and exit with code 1.
    The timer is cancelled when the interpreter exits normally.
    """
    _report_leaked_threads()
    # Timers (e.g. the push manager's TokenRefresh) are non-daemon: a leaked one blocks exit until it fires.
    for thread in threading.enumerate():
        if isinstance(thread, threading.Timer):
            thread.cancel()
    threading.Thread(target=_report_blocking_threads, args=(60,), daemon=True, name='exit-watchdog').start()
    faulthandler.dump_traceback_later(120, exit=True)


def _report_blocking_threads(delay):
    """
    If the interpreter is still alive `delay` seconds after the session, say what blocks it.

    faulthandler only prints the first 100 threads (all idle daemon workers here), so print the
    main thread and every non-worker thread explicitly, with their stacks.
    """
    time.sleep(delay)
    frames = sys._current_frames()
    me = threading.current_thread()
    main = threading.main_thread()
    others = [t for t in threading.enumerate()
              if t is not me and t is not main and not t.name.startswith('pool_worker_')]
    lines = ['\nPROCESS DID NOT EXIT %ds after the session.' % delay]
    lines.append('main thread alive: %s' % main.is_alive())
    for thread in [main] + others[:40]:
        lines.append('  %s (%s, daemon=%s)' % (thread.name, type(thread).__name__, thread.daemon))
        if thread.ident in frames:
            lines.extend('    ' + l for l in ''.join(traceback.format_stack(frames[thread.ident])).splitlines())
    lines.append('non-daemon threads: %s' % [t.name for t in threading.enumerate() if not t.daemon])
    sys.stderr.write('\n'.join(lines) + '\n')
    sys.stderr.flush()


def _report_leaked_threads():
    """Print the SDK threads still running after the last test, grouped by the test that started them."""
    leaked = {}
    for thread in threading.enumerate():
        if thread is threading.main_thread() or thread.name.startswith('pool_worker_'):
            continue
        if thread.name == 'exit-watchdog':
            continue
        leaked.setdefault(getattr(thread, '_created_by_test', None), []).append(thread.name)
    if leaked:
        sys.stderr.write('\nTHREADS STILL ALIVE AFTER THE SESSION (factory/client not destroyed?):\n')
        for test, names in sorted(leaked.items(), key=lambda kv: str(kv[0])):
            sys.stderr.write('  %s\n    %s\n' % (test, ', '.join(sorted(set(names)))))
        sys.stderr.flush()


def _destroy_leaked_factories():
    """Stop the sync/polling threads of factories a test created but didn't destroy."""
    for factory in list(_live_factories):
        _live_factories.discard(factory)
        try:
            if not factory.destroyed:
                done = threading.Event()
                factory.destroy(done)
                done.wait(5)
        except Exception:
            pass  # factories built from mocks may not be destroyable; threads are daemons anyway
