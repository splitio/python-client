Introduction
============

This project provides Python programs access to the `Split.io <http://split.io/>`_ SDK API. For more information on the Split.io SDK, please consult the documentation.

Installation and Requirements
-----------------------------

``splitio_client`` supports both Python 2 (2.7 or later) and Python 3 (3.3 or later). Stable versions can be installed from `PyPI <https://pypi.python.org>`_ using pip: ::

  pip install splitio_client

and development versions are installed directly from the `Github <https://github.com/splitio/python-client>`_ repository: ::

  pip install -e git+git@github.com:splitio/python-client.git@development#egg=splitio_client

Quickstart
----------

Before you begin, make sure that you have an **API key** for the Split.io services. Consult the Split.io documentation on how to get an API key for any of your environments.

The main entry point for this project is the ``get_factory`` function. This function creates Split clients that keep the cached information up-to-date with periodic requests to the SDK API. Impressions (which treatments were given to each user) and metrics are also sent periodically to the Split events backend.

The following snippet shows you how to create a basic client using the default configuration, and request a treatment for user: ::

  >>> from splitio import get_factory
  >>> factory = get_factory('some_api_key')
  >>> client = factory.client()
  >>> client.get_treatment('some_user', 'some_feature')
  'SOME_TREATMENT'

Bucketing key
-------------
In advanced mode the key can be set as two different parts, one of them just to match the condition and the other one to calculate the treatment bucket ::

  >>> from splitio import get_factory, Key
  >>> user = 'some_user_or_anonymous'
  >>> bucketing_key = 'some_random_string'
  >>> split_key = Key(user, bucketing_key)
  >>> factory = get_factory('API_KEY')
  >>> client = factory.client()
  >>> client.get_treatment(split_key, 'some_feature')

Manager API
-----------
Manager API is very useful to get a representation (view) of cached splits: ::

  >>> from splitio import get_factory
  >>> factory = get_factory('API_KEY')
  >>> manager = factory.manager()

Available methods:

**splits():** Returns a list of SplitView instance ::
  >>> manager.splits()

**split(name):** Returns a SplitView instance ::
  >>> manager.split('some_test_name')

**split_names():** Returns a list of Split names (String) ::
  >>> manager.split_names()

Client configuration
--------------------

It's possible to control certain aspects of the client behaviour by supplying a ``config`` dictionary. For instance, the following snippets shows you how to set the segment update interval to 10 seconds: ::

  >>> from splitio import get_factory
  >>> config = {'segmentsRefreshRate': 10}
  >>> factory = get_factory('some_api_key', config=config)
  >>> client = factory.client()

All the possible configuration options are:

+------------------------+------+--------------------------------------------------------+---------+
| Key                    | Type | Description                                            | Default |
+========================+======+========================================================+=========+
| connectionTimeout      | int  | The timeout for HTTP connections in milliseconds.      | 1500    |
+------------------------+------+--------------------------------------------------------+---------+
| readTimeout            | int  | The read timeout for HTTP connections in milliseconds. | 1500    |
+------------------------+------+--------------------------------------------------------+---------+
| featuresRefreshRate    | int  | The features (splits) update refresh period in         | 30      |
|                        |      | seconds.                                               |         |
+------------------------+------+--------------------------------------------------------+---------+
| segmentsRefreshRate    | int  | The segments update refresh period in seconds.         | 60      |
+------------------------+------+--------------------------------------------------------+---------+
| metricsRefreshRate     | int  | The metrics report period in seconds                   | 60      |
+------------------------+------+--------------------------------------------------------+---------+
| impressionsRefreshRate | int  | The impressions report period in seconds               | 60      |
+------------------------+------+--------------------------------------------------------+---------+
| ready                  | int  | How long to wait (in milliseconds) for the features    |         |
|                        |      | and segments information to be available. If the       |         |
|                        |      | timeout is exceeded, a ``TimeoutException`` will be    |         |
|                        |      | raised. If value is 0, the constructor will return     |         |
|                        |      | immediately but not all the information might be       |         |
|                        |      | available right away.                                  |         |
+------------------------+------+--------------------------------------------------------+---------+

.. _localhost_environment:
The localhost environment
-------------------------

During development it is possible to create a 'localhost client' to avoid hitting the
Split.io API SDK. The configuration is taken from a ``.split`` file in the user's *HOME*
directory. The ``.split`` file has the following format: ::

  file: (comment | split_line)+
  comment : '#' string*\n
  split_line : feature_name ' ' treatment\n
  feature_name : string
  treatment : string

This is an example of a ``.split`` file: ::

  # This is a comment
  feature_0 treatment_0
  feature_1 treatment_1

Whenever a treatment is requested for the feature ``feature_0``, ``treatment_0`` is going to be returned. The same goes for ``feature_1`` and ``treatment_1``. The following example illustrates the behaviour: ::

  >>> from splitio import get_factory
  >>> factory = get_factory('localhost')
  >>> client = factory.client()
  >>> client.get_treatment('some_user', 'feature_0')
  'treatment_0'
  >>> client.get_treatment('some_other_user', 'feature_0')
  'treatment_0'
  >>> client.get_treatment('yet_another_user', 'feature_1')
  'treatment_1'
  >>> client.get_treatment('some_user', 'non_existent_feature')
  'CONTROL'

Notice that an API key is not necessary for the localhost environment, and the ``CONTROL`` is returned for non existent features.

It is possible to specify a different splits file using the ``split_definition_file_name`` argument: ::

  >>> from splitio import get_factory
  >>> factory = get_factory('localhost', split_definition_file_name='/path/to/splits/file')
  >>> client = factory.client()

Specifying Split.io environments
--------------------------------

By default, all requests are sent to the Split production environments. It is possible to change this by supplying values for the ``sdk_api_base_url`` and ``events_api_base_url`` arguments: ::

  >>> from splitio import get_factory
  >>> factory = get_factory('some_api_key',
          sdk_api_base_url='https://sdk-staging.split.io/api',
          events_api_base_url='https://sdk-events.split.io/api')
  >>> client = factory.client()

Notice that you're going to need a **different API key** than the one used for the production environments.

.. _redis_support:
Redis support
-------------

For environments that restrict the usage of threads or background tasks, it is possible to use the Split.io client with a `Redis <http://redis.io>`_ backend. Right now we only support Redis version 2.6 or later and we use the Python `redis <https://pypi.python.org/pypi/redis/2.10.5>`_ library to establish connections to the instances.

Before you can use it, you need to install the ``splitio_client`` with support for redis: ::

  pip install splitio_client[redis]

The client depends on the information for features and segments being updated externally. In order to do that, we provide the ``update_splits`` and ``update_segments`` scripts or even the ``splitio.bin.synchronizer`` service.

The scripts are configured through a JSON settings file, like the following: ::

    {
      "apiKey": "some-api-key",
      "sdkApiBaseUrl": "https://sdk.split.io/api",
      "eventsApiBaseUrl": "https://events.split.io/api",
      "redisFactory": 'some.redis.factory',
      "redisHost": "localhost",
      "redisPort": 6379,
      "redisDb": 0,
    }

These are the possible configuration parameters:

+------------------------+------+--------------------------------------------------------+-------------------------------+
| Key                    | Type | Description                                            | Default                       |
+========================+======+========================================================+===============================+
| apiKey                 | str  | A valid Split.io API key.                              | None                          |
+------------------------+------+--------------------------------------------------------+-------------------------------+
| sdkApiBaseUrl          | str  | The SDK API url base                                   | "https://sdk.split.io/api"    |
+------------------------+------+--------------------------------------------------------+-------------------------------+
| eventsApiBaseUrl       | str  | The Events API url base                                | "https://events.split.io/api" |
+------------------------+------+--------------------------------------------------------+-------------------------------+
| redisFactory           | str  | A reference to a method that creates a redis client to | None                          |
|                        |      | be used by the Split.io components. If this value is   |                               |
|                        |      | not provided, the redisHost, redisPort and redisDb     |                               |
|                        |      | values are used to create a StrictRedis instance.      |                               |
+------------------------+------+--------------------------------------------------------+-------------------------------+
| redisHost              | str  | Hostname of the Redis instance                         | "localhost"                   |
+------------------------+------+--------------------------------------------------------+-------------------------------+
| redisPort              | int  | The port of the Redis instance                         | 6379                          |
+------------------------+------+--------------------------------------------------------+-------------------------------+
| redisDb                | int  | The db number on the Redis instance                    | 0                             |
+------------------------+------+--------------------------------------------------------+-------------------------------+

Let's assume that the configuration file is called ``splitio-config.json`` and that the client is installed in a virtualenv in ``/home/user/venv``. The feature update script can be run with: ::

  $ /home/user/venv/bin/python -m splitio.update_scripts.update_splits splitio-config.json

and similarily the segment update script is run with: ::

  $ /home/user/venv/bin/python -m splitio.update_scripts.update_segments splitio-config.json

There are two additional scripts called ``post_impressions`` and ``post_metrics`` responsible of sending impressions and metrics back to Split.io.

All these scripts need to run periodically, and one way to do that is through ``contrab``: ::

    * * * * * /home/user/venv/bin/python -m splitio.update_scripts.update_splits /path/to/splitio-config.json >/dev/null 2>&1
    * * * * * /home/user/venv/bin/python -m splitio.update_scripts.update_segments /path/to/splitio-config.json >/dev/null 2>&1
    * * * * * /home/user/venv/bin/python -m splitio.update_scripts.post_impressions /path/to/splitio-config.json >/dev/null 2>&1
    * * * * * /home/user/venv/bin/python -m splitio.update_scripts.post_metrics /path/to/splitio-config.json >/dev/null 2>&1

There are other scheduling solutions like ``anacron`` or ``fcron`` that can serve this purpose as well.

On the other hand, there is available a python script named ``splitio.bin.synchronizer`` in order to run as a service instead of a ``cron-job``. For production environment we recomend run it via ``supervisord`` ::

    $ /home/user/venv/bin/python -m splitio.bin.synchronizer --help

    Usage:
      synchronizer [options] <config_file>
      synchronizer -h | --help
      synchronizer --version

    Options:
      --splits-refresh-rate=SECONDS         The SECONDS rate to fetch Splits definitions [default: 30]
      --segments-refresh-rate=SECONDS       The SECONDS rate to fetch the Segments keys [default: 30]
      --impression-refresh-rate=SECONDS     The SECONDS rate to send key impressions [default: 60]
      --metrics-refresh-rate=SECONDS        The SECONDS rate to send SDK metrics [default: 60]
      -h --help                             Show this screen.
      --version                             Show version.

    Configuration file:
        The configuration file is a JSON file with the following fields:

        {
          "apiKey": "YOUR_API_KEY",
          "redisHost": "REDIS_DNS_OR_IP",
          "redisPort": 6379,
          "redisDb": 0
        }


    Examples:
        python -m splitio.bin.synchronizer splitio-config.json
        python -m splitio.bin.synchronizer --splits-refresh-rate=10 splitio-config.json


Once the scripts are running, you can access a client using the ``get_factory().client()`` function with the ``config_file`` parameter: ::

  >>> from splitio import get_factory
  >>> factory = get_factory(None,
          config_file='splitio-config.json')

The first argument is the API key which is not necessary in this context, but if you pass "localhost" as its value, a localhost environment client will be generated as shown in a previous section.

Sentinel support
^^^^^^^^^^^^^^^^

In order to support Redis' Sentinel host discovery, you need to provide a custom redis factory (through the ``redisFactory`` config key). The first step is to write the factory, which just a Python function that takes no arguments: ::

  # redis_config.py
  from redis.sentinel import Sentinel

  def my_redis_factory():
    sentinel = Sentinel([('localhost', 26379)], socket_timeout=0.1)
    master = sentinel.master_for('some_master', socket_timeout=0.1)
    return master

Afterwards you tell the client to use this factory using the config file: ::

  {
    "apiKey": "some-api-key",
    "sdkApiBaseUrl": "https://sdk.split.io/api",
    "eventsApiBaseUrl": "https://events.split.io/api",
    "redisFactory": 'redis_config.my_redis_factory'
  }
