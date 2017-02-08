"""A module for Split.io Factories"""
from __future__ import absolute_import, division, print_function, unicode_literals

from splitio.clients import get_client, get_redis_client, get_uwsgi_client
from splitio.managers import (RedisSplitManager, SelfRefreshingSplitManager, LocalhostSplitManager, UWSGISplitManager)
from splitio.redis_support import get_redis
from splitio.uwsgi import get_uwsgi

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

        if 'redisHost' in config:
            self._client = get_redis_client(api_key, **kwargs)
            redis = get_redis(config)
            self._manager = RedisSplitManager(redis)
        else:
            if 'uwsgiClient' in config and config['uwsgiClient'] :
                self._client = get_uwsgi_client(api_key, **kwargs)
                self._manager = UWSGISplitManager(get_uwsgi())
            else:
                self._client = get_client(api_key, **kwargs)
                self._manager = SelfRefreshingSplitManager(self._client.get_split_fetcher())



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
            self._client = get_client('localhost', split_definition_file_name=kwargs['split_definition_file_name'])
        else:
            self._client = get_client('localhost')

        self._manager = LocalhostSplitManager(self._client.get_split_fetcher())

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

