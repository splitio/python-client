from __future__ import absolute_import, division, print_function, \
    unicode_literals

from .factories import get_factory # noqa
from .key import Key # noqa
from .version import __version__  # noqa

__all__ = ('api', 'brokers', 'cache', 'clients', 'matchers', 'segments',
           'settings', 'splits', 'splitters', 'transformers', 'treatments',
           'version', 'factories', 'manager')


# Functions defined to maintain compatibility with previous sdk versions.
# ======================================================================
#
# This functions are not supposed to be used directly, factory method should be
# called instead, but since they were previously exposed, they're re-added here
# as helper function so that if someone was using we don't break their code.

def get_client(apikey, **kwargs):
    from .clients import Client
    from .brokers import get_self_refreshing_broker
    broker = get_self_refreshing_broker(apikey, **kwargs)
    return Client(broker)


def get_redis_client(apikey, **kwargs):
    from .clients import Client
    from .brokers import get_redis_broker
    broker = get_redis_broker(apikey, **kwargs)
    return Client(broker)
