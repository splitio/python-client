"""Unit tests for the tasks module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

try:
    from jsonpickle import decode, encode
except ImportError:
    def missing_jsonpickle_dependencies(*args, **kwargs):
        raise NotImplementedError('Missing jsonpickle support dependencies.')
    decode = encode = missing_jsonpickle_dependencies


from unittest import TestCase

from splitio.uwsgi import UWSGISplitCache, UWSGISegmentCache, UWSGIImpressionsCache, UWSGIMetricsCache, get_uwsgi
from splitio.impressions import Impression
from splitio.metrics import BUCKETS


class MockUWSGILock(object):
    def __enter__(self, *args, **kwargs):
        pass

    def __exit__(self, *args, **kwargs):
        pass

def mock_lock(*args, **kwargs):
    pass



class UWSGICacheEmulatorTests(TestCase):
    def setUp(self):
        self.uwsgi = get_uwsgi(emulator=True)
        self.cache_namespace = 'splitio'
        self.cache_key = 'some_key'
        self.some_value = 'some_string_value'
        self.some_other_value = 'some_other_string_value'

    def test_set_and_get(self):
        self.uwsgi.cache_set(self.cache_key, str(self.some_value), 0, self.cache_namespace)
        data = self.uwsgi.cache_get(self.cache_key, self.cache_namespace)
        self.assertEqual(self.some_value, data)

    def test_update_and_get(self):
        self.uwsgi.cache_update(self.cache_key, str(self.some_other_value), 0, self.cache_namespace)
        data = self.uwsgi.cache_get(self.cache_key, self.cache_namespace)
        self.assertEqual(self.some_other_value, data)

    def test_set_exists_del(self):
        self.uwsgi.cache_set(self.cache_key, str(self.some_value), 0, self.cache_namespace)
        self.assertTrue(self.uwsgi.cache_exists(self.cache_key, self.cache_namespace))
        self.uwsgi.cache_del(self.cache_key, self.cache_namespace)
        self.assertFalse(self.uwsgi.cache_exists(self.cache_key, self.cache_namespace))


class UWSGISplitCacheTests(TestCase):
    def setUp(self):
        mock.patch('splitio.uwsgi.UWSGILock', autospec=True)
        self.uwsgi_adapter = get_uwsgi(emulator=True)
        self.split_cache = UWSGISplitCache(self.uwsgi_adapter)
        self.split_json = """
        {
         "orgId":null,
         "environment":null,
         "trafficTypeId":null,
         "trafficTypeName":"user",
         "name":"test_multi_condition",
         "seed":-1329591480,
         "status":"ACTIVE",
         "killed":false,
         "defaultTreatment":"off",
         "changeNumber":1325599980,
         "conditions":[
            {
               "matcherGroup":{
                  "combiner":"AND",
                  "matchers":[
                     {
                        "keySelector":null,
                        "matcherType":"WHITELIST",
                        "negate":false,
                        "userDefinedSegmentMatcherData":null,
                        "whitelistMatcherData":{
                           "whitelist":[
                              "fake_id_on"
                           ]
                        },
                        "unaryNumericMatcherData":null,
                        "betweenMatcherData":null
                     }
                  ]
               },
               "partitions":[
                  {
                     "treatment":"on",
                     "size":100
                  }
               ]
            },
            {
               "matcherGroup":{
                  "combiner":"AND",
                  "matchers":[
                     {
                        "keySelector":null,
                        "matcherType":"WHITELIST",
                        "negate":false,
                        "userDefinedSegmentMatcherData":null,
                        "whitelistMatcherData":{
                           "whitelist":[
                              "fake_id_off"
                           ]
                        },
                        "unaryNumericMatcherData":null,
                        "betweenMatcherData":null
                     }
                  ]
               },
               "partitions":[
                  {
                     "treatment":"off",
                     "size":100
                  }
               ]
            },
            {
               "matcherGroup":{
                  "combiner":"AND",
                  "matchers":[
                     {
                        "keySelector":{
                           "trafficType":"user",
                           "attribute":null
                        },
                        "matcherType":"IN_SEGMENT",
                        "negate":false,
                        "userDefinedSegmentMatcherData":{
                           "segmentName":"demo"
                        },
                        "whitelistMatcherData":null,
                        "unaryNumericMatcherData":null,
                        "betweenMatcherData":null
                     }
                  ]
               },
               "partitions":[
                  {
                     "treatment":"on",
                     "size":100
                  },
                  {
                     "treatment":"off",
                     "size":0
                  }
               ]
            },
            {
               "matcherGroup":{
                  "combiner":"AND",
                  "matchers":[
                     {
                        "keySelector":{
                           "trafficType":"user",
                           "attribute":"some_attribute"
                        },
                        "matcherType":"EQUAL_TO",
                        "negate":false,
                        "userDefinedSegmentMatcherData":null,
                        "whitelistMatcherData":null,
                        "unaryNumericMatcherData":{
                           "dataType":"NUMBER",
                           "value":42
                        },
                        "betweenMatcherData":null
                     }
                  ]
               },
               "partitions":[
                  {
                     "treatment":"on",
                     "size":100
                  },
                  {
                     "treatment":"off",
                     "size":0
                  }
               ]
            }
         ]
      }"""

    def test_add_get_split(self):
        self.split_cache.add_split('test_multi_condition', decode(self.split_json))
        split = self.split_cache.get_split('test_multi_condition')
        self.assertEqual(split.name, 'test_multi_condition')

    def test_remove_split(self):
        self.split_cache.add_split('test_multi_condition', decode(self.split_json))
        self.split_cache.remove_split('test_multi_condition')
        split = self.split_cache.get_split('test_multi_condition')
        self.assertIsNone(split)

    # Because accessing the cache and blocking is costly in uwsgi mode,
    # the split list is maintained by the sync task in order to minimize locking.
    # So the add_split call doesn't update it. This is an inconsistency with the rest of
    # the storages but yields for better performance.
#    def test_get_split_keys(self):
#        self.split_cache.add_split('test_multi_condition', decode(self.split_json))
#        current_keys = self.split_cache.get_splits_keys()
#        self.assertIn('test_multi_condition', current_keys)

    @mock.patch('splitio.uwsgi.UWSGILock')
    def test_get_splits(self, lock_mock):
        lock_mock.return_value = MockUWSGILock()
        self.split_cache.add_split('test_multi_condition', decode(self.split_json))
        self.split_cache.update_split_list(['test_multi_condition'], [])
        current_splits = self.split_cache.get_splits()
        self.assertEqual(self.split_cache.get_split('test_multi_condition').name, current_splits[0].name)

    def test_change_number(self):
        change_number = 1325599980
        self.split_cache.set_change_number(change_number)
        self.assertEqual(change_number, self.split_cache.get_change_number())


class UWSGISegmentCacheTests(TestCase):
    def setUp(self):
        self.some_segment_name = mock.MagicMock()
        self.some_segment_name_str = 'some_segment_name'
        self.some_segment_keys = ['key_1', 'key_2']
        self.remove_segment_keys = ['key_1']
        self.some_key = mock.MagicMock()
        self.some_change_number = mock.MagicMock()

        self.some_uwsgi = get_uwsgi(emulator=True)
        self.segment_cache = UWSGISegmentCache(self.some_uwsgi)

    def test_register_segment(self):
        """Test that register a segment"""
        self.segment_cache.register_segment(self.some_segment_name_str)
        registered_segments = self.segment_cache.get_registered_segments()
        self.assertIn(self.some_segment_name_str, registered_segments)

    def test_unregister_segment(self):
        self.segment_cache.unregister_segment(self.some_segment_name_str)
        registered_segments = self.segment_cache.get_registered_segments()
        self.assertNotIn(self.some_segment_name_str, registered_segments)

    def test_add_segment_keys(self):
        self.segment_cache.register_segment(self.some_segment_name_str)
        self.segment_cache.add_keys_to_segment(self.some_segment_name_str, self.some_segment_keys)
        self.assertTrue(self.segment_cache.is_in_segment(self.some_segment_name_str, self.some_segment_keys[0]))
        self.assertTrue(self.segment_cache.is_in_segment(self.some_segment_name_str, self.some_segment_keys[1]))

    def test_remove_segment_keys(self):
        self.segment_cache.add_keys_to_segment(self.some_segment_name_str, self.some_segment_keys)
        self.segment_cache.remove_keys_from_segment(self.some_segment_name_str, self.remove_segment_keys)
        self.assertFalse(self.segment_cache.is_in_segment(self.some_segment_name_str, self.remove_segment_keys[0]))
        self.assertTrue(self.segment_cache.is_in_segment(self.some_segment_name_str, self.some_segment_keys[1]))

    def test_change_number(self):
        change_number = 1325599980
        self.segment_cache.set_change_number(self.some_segment_name_str, change_number)
        self.assertEqual(change_number, self.segment_cache.get_change_number(self.some_segment_name_str))


class UWSGIImpressionCacheTest(TestCase):
    def setUp(self):
        self._impression_1 = Impression(matching_key='matching_key',
                                      feature_name='feature_name_1',
                                      treatment='treatment',
                                      label='label',
                                      change_number='change_number',
                                      bucketing_key='bucketing_key',
                                      time=1325599980)

        self._impression_2 = Impression(matching_key='matching_key',
                                        feature_name='feature_name_2',
                                        treatment='treatment',
                                        label='label',
                                        change_number='change_number',
                                        bucketing_key='bucketing_key',
                                        time=1325599980)

        self.impression_cache = UWSGIImpressionsCache(get_uwsgi(emulator=True))

    @mock.patch('splitio.uwsgi.UWSGILock')
    def test_impression(self, lock_mock):
        lock_mock.return_value = MockUWSGILock()
        self.impression_cache.add_impression(self._impression_1)
        self.impression_cache.add_impression(self._impression_2)
        impressions = self.impression_cache.fetch_all_and_clear()
        expected = {'feature_name_1': [self._impression_1], 'feature_name_2': [self._impression_2]}
        self.assertEqual(expected,impressions)


class UWSGIMetricsCacheTest(TestCase):
    def setUp(self):
        self.metrics_cache = UWSGIMetricsCache(get_uwsgi(emulator=True))
        self.operation = 'sdk.getTreatment'

    def test_increment_latency(self):
        self.metrics_cache.increment_latency_bucket_counter(self.operation,2)
        self.assertEqual(1,self.metrics_cache.get_latency_bucket_counter(self.operation, 2))

    def test_counter(self):
        self.metrics_cache.set_count('some_counter',2)
        self.metrics_cache.increment_count('some_counter')
        self.assertEqual(3, self.metrics_cache.get_count('some_counter'))

    def test_gauge(self):
        self.metrics_cache.set_gauge('some_gauge',123123123123)
        self.assertEqual(123123123123, self.metrics_cache.get_gauge('some_gauge'))

    def test_times(self):
        bucket_index = 5
        self.metrics_cache.increment_latency_bucket_counter(self.operation, bucket_index)
        latencies = [0] * len(BUCKETS)
        latencies[bucket_index]=1
        expected = [{'name':self.operation, 'latencies':latencies}]
        self.assertEqual(expected,self.metrics_cache.fetch_all_times_and_clear())

    def test_count_gauge(self):
        self.metrics_cache.set_gauge('some_gauge', 10)
        self.metrics_cache.set_count('some_count', 20)
        expected = {'count': [{'name':'some_count', 'delta':20}], 'gauge': [{'name':'some_gauge', 'value': 10}]}
        self.assertEqual(expected, self.metrics_cache.fetch_all_and_clear())

