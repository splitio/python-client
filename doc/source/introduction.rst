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

The main entry point for this project is the ``get_client`` function. This function creates Split clients that keep the cached information up-to-date with periodic requests to the SDK API. Impressions (which treatments were given to each user) and metrics are also sent periodically to the Split events backend.

The following snippet shows you how to create a basic client using the default configuration, and request a treatment for user: ::

  >>> from splitio import get_client
  >>> client = get_client('some_api_key')
  >>> client.get_treatment('some_user', 'some_feature')
  'SOME_TREATMENT'

Client configuration
--------------------

It's possible to control certain aspects of the client behaviour by supplying a ``config`` dictionary. For instance, the following snippets shows you how to set the segment update interval to 10 seconds: ::

  >>> from splitio import get_client
  >>> config = {'segmentsRefreshRate': 10}
  >>> client = get_client('some_api_key', config=config)

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

  >>> from splitio import get_client
  >>> client = get_client('localhost')
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

  >>> from splitio import get_client
  >>> client = get_client(
          split_definition_file_name='/path/to/splits/file')

Specifying Split.io environments
--------------------------------

By default, all requests are sent to the Split production environments. It is possible to change this by supplying values for the ``sdk_api_base_url`` and ``events_api_base_url`` arguments: ::

  >>> from splitio import get_client
  >>> client = get_client('some_api_key',
          sdk_api_base_url='https://sdk-staging.split.io/api',
          events_api_base_url='https://sdk-events.split.io/api')

Notice that you're going to need a **different API key** than the one used for the production environments.
