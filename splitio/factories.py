"""A module for Split.io Factories"""
from __future__ import absolute_import, division, print_function, unicode_literals

from splitio.clients import Client
from splitio.brokers import get_self_refreshing_broker, get_redis_broker, get_uwsgi_broker
from splitio.managers import RedisSplitManager, SelfRefreshingSplitManager, \
    LocalhostSplitManager, UWSGISplitManager
from splitio.impressions import ImpressionListenerWrapper

import logging


class SplitFactory(object):
    def __init__(self):
        """Basic interface of a SplitFactory. Specific implementations need to override the
        client and manager method.
        """
        self._logger = logging.getLogger(self.__class__.__name__)

    def client(self):  # pragma: no cover
        """Get the split client implementation. Subclasses need to override this method.
        :return: The split client implementation.
        :rtype: SplitClient
        """
        raise NotImplementedError()

    def manager(self):  # pragma: no cover
        """Get the split manager implementation. Subclasses need to override this method.
        :return: The split manager implementation.
        :rtype: SplitManager
        """
        raise NotImplementedError()


class MainSplitFactory(SplitFactory):
    def __init__(self, api_key, **kwargs):
        super(MainSplitFactory, self).__init__()

        config = dict()
        if 'config' in kwargs:
            config = kwargs['config']

        labels_enabled = config.get('labelsEnabled', True)

        impression_listener = ImpressionListenerWrapper(config.get('impressionListener')) if 'impressionListener' in config else None # noqa: E501,E261

        if 'redisHost' in config or 'redisSentinels' in config:
            broker = get_redis_broker(api_key, **kwargs)
            self._client = Client(broker, labels_enabled, impression_listener)
            self._manager = RedisSplitManager(broker)
        else:
            if 'uwsgiClient' in config and config['uwsgiClient']:
                broker = get_uwsgi_broker(api_key, **kwargs)
                self._client = Client(broker, labels_enabled, impression_listener)
                self._manager = UWSGISplitManager(broker)
            else:
                broker = get_self_refreshing_broker(api_key, **kwargs)
                self._client = Client(broker, labels_enabled, impression_listener)
                self._manager = SelfRefreshingSplitManager(broker)

    def client(self):  # pragma: no cover
        """Get the split client implementation. Subclasses need to override this method.
        :return: The split client implementation.
        :rtype: SplitClient
        """
        return self._client

    def manager(self):  # pragma: no cover
        """Get the split manager implementation. Subclasses need to override this method.
        :return: The split manager implementation.
        :rtype: SplitManager
        """
        return self._manager


class LocalhostSplitFactory(SplitFactory):
    def __init__(self, **kwargs):
        super(LocalhostSplitFactory, self).__init__()

        if 'split_definition_file_name' in kwargs:
            broker = get_self_refreshing_broker(
                'localhost',
                split_definition_file_name=kwargs['split_definition_file_name']
            )
        else:
            broker = get_self_refreshing_broker('localhost')

        self._client = Client(broker)
        self._manager = LocalhostSplitManager(broker.get_split_fetcher())

    def client(self):  # pragma: no cover
        """Get the split client implementation.
        :return: The split client implementation.
        :rtype: SplitClient
        """
        return self._client

    def manager(self):  # pragma: no cover
        """Get the split manager implementation.
        :return: The split manager implementation.
        :rtype: SplitManager
        """
        return self._manager


def get_factory(api_key, **kwargs):
    """
    :param api_key:
    :param kwargs:
    :return:
    """
    if api_key == 'localhost':
        return LocalhostSplitFactory(**kwargs)
    else:
        return MainSplitFactory(api_key, **kwargs)
