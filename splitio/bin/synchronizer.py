"""
       __      ____        _ _ _
      / /__   / ___| _ __ | (_) |_
     / / \ \  \___ \| '_ \| | | __|
     \ \  \ \  ___) | |_) | | | |_
      \_\ / / |____/| .__/|_|_|\__|
         /_/        |_|

Split.io Synchronizer Service.

Usage:
  synchronizer [options] <config_file>
  synchronizer -h | --help
  synchronizer --version

Options:
  --splits-refresh-rate=SECONDS         The SECONDS rate to fetch Splits definitions [default: 30]
  --segments-refresh-rate=SECONDS       The SECONDS rate to fetch the Segments keys [default: 30]
  --impression-refresh-rate=SECONDS     The SECONDS rate to send key impressions [default: 60]
  --metrics-refresh-rate=SECONDS        The SECONDS rate to send SDK metrics [default: 60]
  -h --help                             Show this screen.
  --version                             Show version.

Configuration file:
    The configuration file is a JSON file with the following fields:

    {
      "apiKey": "YOUR_API_KEY",
      "redisHost": "REDIS_DNS_OR_IP",
      "redisPort": 6379,
      "redisDb": 0
    }


Examples:
    python -m splitio.bin.synchronizer splitio-config.json
    python -m splitio.bin.synchronizer --splits-refresh-rate=10 splitio-config.json


"""

from splitio.api import api_factory
from splitio.config import SDK_VERSION, parse_config_file
from splitio.redis_support import get_redis, RedisSplitCache, RedisSegmentCache, RedisSplitParser, RedisImpressionsCache, RedisMetricsCache
from splitio.splits import ApiSplitChangeFetcher
from splitio.tasks import update_splits, update_segments, report_metrics, report_impressions
from splitio.segments import ApiSegmentChangeFetcher

import time
import threading
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('splitio.bin.synchronizer')


def _update_splits(seconds, config):
    try:
        while True:
            redis = get_redis(config)
            split_cache = RedisSplitCache(redis)
            sdk_api = api_factory(config)
            split_change_fetcher = ApiSplitChangeFetcher(sdk_api)
            segment_cache = RedisSegmentCache(redis)
            split_parser = RedisSplitParser(segment_cache)
            update_splits(split_cache, split_change_fetcher, split_parser)

            time.sleep(seconds)
    except:
        logger.exception('Exception caught updating splits')

def _update_segments(seconds, config):
    try:
        while True:
            redis = get_redis(config)
            segment_cache = RedisSegmentCache(redis)
            sdk_api = api_factory(config)
            segment_change_fetcher = ApiSegmentChangeFetcher(sdk_api)
            update_segments(segment_cache, segment_change_fetcher)

            time.sleep(seconds)
    except:
        logger.exception('Exception caught updating segments')

def _report_impressions(seconds, config):
    try:
        while True:
            redis = get_redis(config)
            impressions_cache = RedisImpressionsCache(redis)
            sdk_api = api_factory(config)
            report_impressions(impressions_cache, sdk_api)

            time.sleep(seconds)
    except:
        logger.exception('Exception caught posting impressions')

def _report_metrics(seconds, config):
    try:
        while True:
            redis = get_redis(config)
            metrics_cache = RedisMetricsCache(redis)
            sdk_api = api_factory(config)
            report_metrics(metrics_cache, sdk_api)

            time.sleep(seconds)
    except:
        logger.exception('Exception caught posting metrics')

def run(arguments):
    config = parse_config_file(arguments['<config_file>'])

    update_splits_thread    = threading.Thread(target=_update_splits,
                                               args=(int(arguments['--splits-refresh-rate']), config))

    update_segments_thread  = threading.Thread(target=_update_segments,
                                               args=(int(arguments['--segments-refresh-rate']), config))

    report_impressions_thread = threading.Thread(target=_report_impressions,
                                                 args=(int(arguments['--impression-refresh-rate']), config))

    report_metrics_thread = threading.Thread(target=_report_metrics,
                                                 args=(int(arguments['--metrics-refresh-rate']), config))

    update_splits_thread.start()
    update_segments_thread.start()
    report_impressions_thread.start()
    report_metrics_thread.start()


if __name__ == '__main__':
    from docopt import docopt
    arguments = docopt(__doc__, version=SDK_VERSION)
    run(arguments)