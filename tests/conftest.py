"""Shared pytest fixtures."""
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
