"""Default settings for the Split.IO SDK Python client"""
from __future__ import absolute_import, division, print_function, unicode_literals


SDK_API_BASE_URL = 'https://sdk.split.io/api'

SDK_VERSION = 'python-0.0.1'

DEFAULT_CONFIG = {
    'connectionTimeout': 1500,
    'readTimeout': 1500,
    'featuresRefreshRate': 30,
    'segmentsRefreshRate': 60,
    'metricsRefreshRate': 60,
    'impressionsRefreshRate': 60,
    'randomizeIntervals': False
}

MAX_INTERVAL = 180
