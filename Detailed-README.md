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

## Logging
Split SDK uses logging module from Python.

### Logging sample
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

## Cache
Split SDK depends on the popular [redis-py](https://github.com/andymccurdy/redis-py) library.

### Cache Adapter
The redis-py library is supported as the python interface to the Redis key-value store. This library uses a connection pool to manage connections to a Redis server. For further information about how to configure the ```redis-py``` client, please take a look on [redis-py official docs](https://github.com/andymccurdy/redis-py)

For ```redis``` and their dependencies such as ```jsonpickle``` you can use ```pip``` running the command ```pip install splitio_client[redis,cpphash]==5.5.0```

#### Provided redis-py connection - sample code
```python
#Default imports
from __future__ import print_function

import sys

from splitio import get_factory
from splitio.exceptions import TimeoutException

# redis-py options
'''The options below, will be loaded as:
r = redis.StrictRedis(host='localhost', port=6379, db=0, prefix='')
'''
config = {
    'redisDb' : 0, 
    'redisHost' : 'localhost',
    'redisPosrt': 6379,
    'redisPrefix': ''
}

# Create the Split Client instance.
try:
    factory = get_factory('API_KEY', config=config)
    split = factory.client()
except TimeoutException:
    sys.exit()
```

#### Provided redis-py connection - sample code for Sentinel Support
```python
#Default imports
from __future__ import print_function

import sys

from splitio import get_factory
from splitio.exceptions import TimeoutException

# redis-py options
'''
The options below, will be loaded as:
sentinel = Sentinel(redisSentinels, { db: redisDb, socket_timeout: redisSocketTimeout })
master = sentinel.master_for(redisMasterService)
'''
config = {
    'redisDb': 0,
    'redisPrefix': '',
    'redisSentinels': [('IP', PORT), ('IP', PORT), ('IP', PORT)],
    'redisMasterService': 'SERVICE_MASTER_NAME',
    'redisSocketTimeout': 3
}

# Create the Split Client instance.
try:
    factory = get_factory('API_KEY', config=config)
    split = factory.client()
except TimeoutException:
    sys.exit()
```

## Additional information

You can get more information on how to use this package in the included documentation.