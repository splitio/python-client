"""
Update Split.io splits.

Usage:
  update_splits <config_file>
  update_splits -h | --help
  update_splits --version

Options:
  -h --help                    Show this screen.
  --version                    Show version.
"""
from __future__ import absolute_import, division, print_function, unicode_literals

import logging

from splitio.api import api_factory
from splitio.config import SDK_VERSION, parse_config_file
from splitio.redis_support import get_redis, RedisSplitCache, RedisSegmentCache, RedisSplitParser
from splitio.splits import ApiSplitChangeFetcher
from splitio.tasks import update_splits

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('splitio.update_scripts.update_splits')


def run(arguments):
    try:
        config = parse_config_file(arguments['<config_file>'])
        redis = get_redis(config)
        split_cache = RedisSplitCache(redis)
        sdk_api = api_factory(config)
        split_change_fetcher = ApiSplitChangeFetcher(sdk_api)
        segment_cache = RedisSegmentCache(redis)
        split_parser = RedisSplitParser(segment_cache)
        update_splits(split_cache, split_change_fetcher, split_parser)
    except:
        logger.exception('Exception caught updating splits')


if __name__ == '__main__':
    from docopt import docopt
    arguments = docopt(__doc__, version=SDK_VERSION)
    run(arguments)
