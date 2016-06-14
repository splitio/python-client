"""
Update Split.io segments.

Usage:
  update_segments <config_file>
  update_segments -h | --help
  update_segments --version

Options:
  -h --help                    Show this screen.
  --version                    Show version.
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging

from splitio.api import api_factory
from splitio.config import SDK_VERSION, parse_config_file
from splitio.redis_support import get_redis, RedisSegmentCache
from splitio.segments import ApiSegmentChangeFetcher
from splitio.tasks import update_segments

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('splitio.update_scripts.update_segments')


def run(arguments):
    try:
        config = parse_config_file(arguments['<config_file>'])
        redis = get_redis(config)
        segment_cache = RedisSegmentCache(redis)
        sdk_api = api_factory(config)
        segment_change_fetcher = ApiSegmentChangeFetcher(sdk_api)
        update_segments(segment_cache, segment_change_fetcher)
    except:
        logger.exception('Exception caught updating segments')


if __name__ == '__main__':
    from docopt import docopt
    arguments = docopt(__doc__, version=SDK_VERSION)
    run(arguments)
