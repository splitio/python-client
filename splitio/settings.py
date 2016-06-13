"""Default settings for the Split.IO SDK Python client"""
from __future__ import absolute_import, division, print_function, unicode_literals

from .version import __version__

SDK_API_BASE_URL = 'https://sdk.split.io/api'
EVENTS_API_BASE_URL = 'https://events.split.io/api'

SDK_VERSION = 'python-{package_version}'.format(package_version=__version__)

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
