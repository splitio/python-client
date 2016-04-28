"""Unit tests for the matchers module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase

from splitio.clients import Client, SelfRefreshingClient, randomize_interval
from splitio.treatments import CONTROL
from splitio.test.utils import MockUtilsMixin
