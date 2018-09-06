try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from splitio.config import SDK_VERSION, DEFAULT_CONFIG

from os.path import dirname, join
from json import load
from unittest import TestCase

from splitio.clients import Client
from splitio.redis_support import (RedisSplitCache, get_redis)
from splitio.brokers import RedisBroker
from splitio.impressions import (Impression, ImpressionListener, ImpressionListenerWrapper)
from splitio import get_factory


class ImpressionListenerClient(ImpressionListener):
    def log_impression(self, data):
        self._data_logged = data

    def get_impression(self):
        return self._data_logged


class ImpressionListenerClientWithException(ImpressionListener):
    def log_impression(self, data):
        raise Exception('Simulate exception.')


class CustomImpressionListenerTestOnRedis(TestCase):
    def setUp(self):
        self._some_config = mock.MagicMock()
        self._split_changes_file_name = join(dirname(__file__),
                                             'splitCustomImpressionListener.json')

        with open(self._split_changes_file_name) as f:
            self._json = load(f)
            split_definition = self._json['splits'][0]
            split_name = split_definition['name']

        self._redis = get_redis({'redisPrefix': 'customImpressionListenerTest'})

        self._redis_split_cache = RedisSplitCache(self._redis)
        self._redis_split_cache.add_split(split_name, split_definition)
        self._client = Client(RedisBroker(self._redis, self._some_config))

        self.some_feature = 'feature_0'
        self.some_impression_0 = Impression(matching_key=mock.MagicMock(),
                                            feature_name=self.some_feature,
                                            treatment=mock.MagicMock(),
                                            label=mock.MagicMock(),
                                            change_number=mock.MagicMock(),
                                            bucketing_key=mock.MagicMock(),
                                            time=mock.MagicMock())

    def test_send_data_to_client(self):
        impression_client = ImpressionListenerClient()
        impression_wrapper = ImpressionListenerWrapper(impression_client)

        impression_wrapper.log_impression(self.some_impression_0)

        self.assertIn('impression', impression_client._data_logged)
        impression_logged = impression_client._data_logged['impression']
        self.assertIsInstance(impression_logged, Impression)
        self.assertDictEqual({
            'impression': {
                'keyName': self.some_impression_0.matching_key,
                'treatment': self.some_impression_0.treatment,
                'time': self.some_impression_0.time,
                'changeNumber': self.some_impression_0.change_number,
                'label': self.some_impression_0.label,
                'bucketingKey': self.some_impression_0.bucketing_key
            }
        }, {
            'impression': {
                'keyName': impression_logged.matching_key,
                'treatment': impression_logged.treatment,
                'time': impression_logged.time,
                'changeNumber': impression_logged.change_number,
                'label': impression_logged.label,
                'bucketingKey': impression_logged.bucketing_key
            }
        })

        self.assertIn('instance-id', impression_client._data_logged)
        self.assertEqual(impression_client._data_logged['instance-id'],
                         DEFAULT_CONFIG['splitSdkMachineIp'])

        self.assertIn('sdk-language-version', impression_client._data_logged)
        self.assertEqual(impression_client._data_logged['sdk-language-version'], SDK_VERSION)

        self.assertIn('attributes', impression_client._data_logged)

    def test_client_throwing_exception_in_listener(self):
        impressionListenerClient = ImpressionListenerClientWithException()

        config = {
            'ready': 180000,
            'impressionListener': impressionListenerClient,
            'redisDb': 0,
            'redisHost': 'localhost',
            'redisPosrt': 6379,
            'redisPrefix': 'customImpressionListenerTest'
        }
        factory = get_factory('asdqwe123456', config=config)
        split = factory.client()

        self.assertEqual(split.get_treatment('valid', 'iltest'), 'on')

    def test_client(self):
        impressionListenerClient = ImpressionListenerClient()

        config = {
            'ready': 180000,
            'impressionListener': impressionListenerClient,
            'redisDb': 0,
            'redisHost': 'localhost',
            'redisPosrt': 6379,
            'redisPrefix': 'customImpressionListenerTest'
        }
        factory = get_factory('asdqwe123456', config=config)
        split = factory.client()

        self.assertEqual(split.get_treatment('valid', 'iltest'), 'on')
        self.assertEqual(split.get_treatment('invalid', 'iltest'), 'off')
        self.assertEqual(split.get_treatment('valid', 'iltest_invalid'), 'control')

    def test_client_without_impression_listener(self):
        config = {
            'ready': 180000,
            'redisDb': 0,
            'redisHost': 'localhost',
            'redisPosrt': 6379,
            'redisPrefix': 'customImpressionListenerTest'
        }
        factory = get_factory('asdqwe123456', config=config)
        split = factory.client()

        self.assertEqual(split.get_treatment('valid', 'iltest'), 'on')
        self.assertEqual(split.get_treatment('invalid', 'iltest'), 'off')
        self.assertEqual(split.get_treatment('valid', 'iltest_invalid'), 'control')

    def test_client_when_impression_listener_is_none(self):
        config = {
            'ready': 180000,
            'redisDb': 0,
            'impressionListener': None,
            'redisHost': 'localhost',
            'redisPosrt': 6379,
            'redisPrefix': 'customImpressionListenerTest'
        }
        factory = get_factory('asdqwe123456', config=config)
        split = factory.client()

        self.assertEqual(split.get_treatment('valid', 'iltest'), 'on')
        self.assertEqual(split.get_treatment('invalid', 'iltest'), 'off')
        self.assertEqual(split.get_treatment('valid', 'iltest_invalid'), 'control')
