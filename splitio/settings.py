"""Default settings for the Split.IO SDK Python client"""
from __future__ import absolute_import, division, print_function, unicode_literals


SDK_API_BASE_URL = 'https://sdk.split.io/api'
EVENTS_API_BASE_URL = 'https://events.split.io/api'

PACKAGE_VERSION_MAJOR = '0'
PACKAGE_VERSION_MINOR = '0'
PACKAGE_VERSION_MICRO = '1'
PACKAGE_VERSION = '{major}.{minor}.{micro}'.format(major=PACKAGE_VERSION_MAJOR,
                                                   minor=PACKAGE_VERSION_MINOR,
                                                   micro=PACKAGE_VERSION_MICRO)
SDK_VERSION = 'python-{package_version}'.format(PACKAGE_VERSION)

DEFAULT_CONFIG = {
    'connectionTimeout': 1500,
    'readTimeout': 1500,
    'featuresRefreshRate': 30,
    'segmentsRefreshRate': 60,
    'metricsRefreshRate': 60,
    'impressionsRefreshRate': 60,
    'randomizeIntervals': False,
    'maxImpressionsLogSize': -1,
    'maxMetricsCallsBeforeFlush': 1000,
    'ready': 0
}

MAX_INTERVAL = 180
