try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from os.path import dirname, join
from json import load
from unittest import TestCase

from splitio.clients import Client
from splitio.redis_support import (RedisSplitCache, get_redis)
from splitio.brokers import RedisBroker
from splitio import get_factory


class GetTreatmentsTest(TestCase):
    def setUp(self):
        self._some_config = mock.MagicMock()
        self._split_changes_file_name = join(dirname(__file__),
                                             'splitGetTreatments.json')

        with open(self._split_changes_file_name) as f:
            self._json = load(f)
            split_definition = self._json['splits'][0]
            split_name = split_definition['name']

        self._redis = get_redis({'redisPrefix': 'getTreatmentsTest'})

        self._redis_split_cache = RedisSplitCache(self._redis)
        self._redis_split_cache.add_split(split_name, split_definition)
        self._client = Client(RedisBroker(self._redis, self._some_config))

        self._config = {
            'ready': 180000,
            'redisDb': 0,
            'redisHost': 'localhost',
            'redisPosrt': 6379,
            'redisPrefix': 'getTreatmentsTest'
        }
        self._factory = get_factory('asdqwe123456', config=self._config)
        self._split = self._factory.client()

    def test_clien_with_distinct_features(self):
        results = self._split.get_treatments('some_key', ['some_feature', 'some_feature_2'])
        self.assertIn('some_feature', results)
        self.assertIn('some_feature_2', results)
        self.assertDictEqual(results, {
            'some_feature': 'control',
            'some_feature_2': 'control'
        })

    def test_clien_with_repeated_features(self):
        results = self._split.get_treatments('some_key', ['some_feature', 'some_feature_2',
                                             'some_feature', 'some_feature'])
        self.assertIn('some_feature', results)
        self.assertIn('some_feature_2', results)
        self.assertDictEqual(results, {
            'some_feature': 'control',
            'some_feature_2': 'control'
        })

    def test_clien_with_none_and_repeated_features(self):
        results = self._split.get_treatments('some_key', ['some_feature', None, 'some_feature_2',
                                             'some_feature', 'some_feature', None])
        self.assertIn('some_feature', results)
        self.assertIn('some_feature_2', results)
        self.assertDictEqual(results, {
            'some_feature': 'control',
            'some_feature_2': 'control'
        })

    def test_client_with_valid_none_and_repeated_features_and_invalid_key(self):
        features = ['some_feature', 'get_treatments_test', 'some_feature_2',
                    'some_feature', 'get_treatments_test', None, 'valid']
        results = self._split.get_treatments('some_key', features)
        self.assertIn('some_feature', results)
        self.assertIn('some_feature_2', results)
        self.assertIn('get_treatments_test', results)
        self.assertEqual(results['some_feature'], 'control')
        self.assertEqual(results['some_feature_2'], 'control')
        self.assertEqual(results['get_treatments_test'], 'off')

    def test_client_with_valid_none_and_repeated_features_and_valid_key(self):
        features = ['some_feature', 'get_treatments_test', 'some_feature_2',
                    'some_feature', 'get_treatments_test', None, 'valid']
        results = self._split.get_treatments('valid', features)
        self.assertIn('some_feature', results)
        self.assertIn('some_feature_2', results)
        self.assertIn('get_treatments_test', results)
        self.assertEqual(results['some_feature'], 'control')
        self.assertEqual(results['some_feature_2'], 'control')
        self.assertEqual(results['get_treatments_test'], 'on')

    def test_client_with_valid_none_invalid_and_repeated_features_and_valid_key(self):
        features = ['some_feature', 'get_treatments_test', 'some_feature_2',
                    'some_feature', 'get_treatments_test', None, 'valid',
                    True, [], True]
        results = self._split.get_treatments('valid', features)
        self.assertIn('some_feature', results)
        self.assertIn('some_feature_2', results)
        self.assertIn('get_treatments_test', results)
        self.assertEqual(results['some_feature'], 'control')
        self.assertEqual(results['some_feature_2'], 'control')
        self.assertEqual(results['get_treatments_test'], 'on')
