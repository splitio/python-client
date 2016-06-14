"""
Post Split.io metrics.

Usage:
  post_metrics <config_file>
  post_metrics -h | --help
  post_metrics --version

Options:
  -h --help                    Show this screen.
  --version                    Show version.
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging

from splitio.api import api_factory
from splitio.config import SDK_VERSION, parse_config_file
from splitio.redis_support import get_redis, RedisMetricsCache
from splitio.tasks import report_metrics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('splitio.update_scripts.post_impressions')


def run(arguments):
    try:
        config = parse_config_file(arguments['<config_file>'])
        redis = get_redis(config)
        metrics_cache = RedisMetricsCache(redis)
        sdk_api = api_factory(config)
        report_metrics(metrics_cache, sdk_api)
    except:
        logger.exception('Exception caught posting metrics')


if __name__ == '__main__':
    from docopt import docopt
    arguments = docopt(__doc__, version=SDK_VERSION)
    run(arguments)
