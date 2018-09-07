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
  >>> from splitio import get_factory
  >>> factory = get_factory('some_api_key')
  >>> client = factory.client()
  >>> client.get_treatment('some_user', 'some_feature')
  'SOME_TREATMENT'
```

## Impression Listener
In order to handling the result of a `get_treatment`(a.k.a. `Impression`) for own purposes, client is able to access it by using a custom `Impression Listener`. Sdk options have a parameter called `impressionListener` where you could add an implementation of `ImpressionListener`. You **must** implement the `log_impression` method. This method in particular receives a paramater that has data in the following schema:

| Name | Type | Description |
| --- | --- | --- |
| impression | Impression | Impression object that has the feature_name, treatment result, label, etc. |
| attributes | Array | A list of attributes passed by the client. |
| instance-id | String | Corresponds to the IP of the machine where the SDK is running. |
| sdk-language-version | String | Indicates the version of the sdk. In this case the language will be python plus the version of it. |

### Implementing custom Impression Listener
Below you could find an example of how implement a custom Impression Listener:
```python
# Import ImpressionListener interface
from splitio.impressions import ImpressionListener

# Implementation Sample for a Custom Impression Listener
class CustomImpressionListener(ImpressionListener)
{
  def log_impression(self, data):
    # Custom behavior
}
```

### Attaching custom Impression Listener
```python
factory = get_factory(
  'YOUR_API_KEY',
  config={
    # ...
    'impressionListener': CustomImpressionListener()
  },
  # ...
)
split = factory.client()
```

## Additional information

You can get more information on how to use this package in the included documentation.