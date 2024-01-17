"""Log helper."""

_LOGGER_NAMESPACE = 'class'

def get_logger_namespace():
    return _LOGGER_NAMESPACE

def set_logger_namespace(logger_namespace):
    global _LOGGER_NAMESPACE
    _LOGGER_NAMESPACE = logger_namespace