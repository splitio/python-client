# Split.io Python Client

This project provides Python programs access to the [Split.io](http://split.io/)` SDK API. For more information on the Split.io SDK, please consult the documentation.

## Installation and Requirements

`split-client` supports both Python 2 (2.7 or later) and Python 3 (3.3 or later). Stable versions can be installed from [PyPI](https://pypi.python.org) using pip:

```
  pip install splitio_client
```

and development versions are installed directly from the [Github](https://github.com/splitio/python-client) repository:

```
  pip install -e git+git@github.com:splitio/python-client.git@development#egg=splitio_client
```

## Quickstart

Before you begin, make sure that you have an **API key** for the Split.io services. Consult the Split.io documentation on how to get an API key for any of your environments.

The main entry point for this project is the `get_client` function. This function creates Split clients that keep the cached information up-to-date with periodic requests to the SDK API. Impressions (which treatments were given to each user) and metrics are also sent periodically to the Split events backend.

The following snippet shows you how to create a basic client using the default configuration, and request a treatment for user:

```
  >>> from splitio import get_client
  >>> client = get_client('some_api_key')
  >>> client.get_treatment('some_user', 'some_feature')
  'SOME_TREATMENT'
```

## Additional information

You can get more information on how to use this package in the included documentation.