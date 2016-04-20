"""Default settings for the Split.IO SDK Python client"""
from __future__ import absolute_import, division, print_function, unicode_literals


SDK_API_BASE_URL = 'https://sdk-loadtesting.split.io/api'

SEGMENT_CHANGES_URL_TEMPLATE = '{base_url}/segmentChanges/{segment_name}/'
SPLIT_CHANGES_URL_TEMPLATE = '{base_url}/splitChanges/'

SDK_VERSION = 'python-0.0.1'
