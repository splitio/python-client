"""Pluggable storage test module."""
import json

from splitio.models.splits import Split
from splitio.models import splits, segments
from splitio.models.segments import Segment
from splitio.models.impressions import Impression
from splitio.models.events import Event, EventWrapper
from splitio.storage.pluggable import PluggableSplitStorage, PluggableSegmentStorage, PluggableImpressionsStorage, PluggableEventsStorage
from splitio.client.util import get_metadata, SdkMetadata

from tests.integration import splits_json
import pytest

class StorageMockAdapter(object):
    def __init__(self):
        self._keys = {}

    def get(self, key):
        if key not in self._keys:
            return None
        return self._keys[key]

    def get_items(self, key):
        if key not in self._keys:
            return None
        return list(self._keys[key])

    def get_many(self, keys):
        return [self.get(key) for key in keys]

    def set(self, key, value):
        self._keys[key] = value

    def push_items(self, key, *value):
        items = []
        if key in self._keys:
            items = self._keys[key]
        [items.append(item) for item in value]
        self._keys[key] = items

    def delete(self, key):
        if key in self._keys:
            del self._keys[key]

    def increment(self, key, value):
        if key not in self._keys:
            self._keys[key] = 0
        self._keys[key]+= value
        return self._keys[key]

    def decrement(self, key, value):
        if key not in self._keys:
            return None
        self._keys[key]-= value
        return self._keys[key]

    def get_keys_by_prefix(self, prefix):
        keys = []
        for key in self._keys:
            if prefix in key:
                keys.append(key)
        return keys

    def get_many(self, keys):
        returned_keys = []
        for key in self._keys:
            if key in keys:
                returned_keys.append(self._keys[key])
        return returned_keys

    def add_items(self, key, added_items):
        items = set()
        if key in self._keys:
            items = set(self._keys[key])
        [items.add(item) for item in added_items]
        self._keys[key] = items

    def remove_items(self, key, removed_items):
        new_items = set()
        for item in self._keys[key]:
            if item not in removed_items:
                new_items.add(item)
        self._keys[key] = new_items

    def item_contains(self, key, item):
        if item in self._keys[key]:
            return True
        return False

    def get_items_count(self, key):
        if key in self._keys:
            return len(self._keys[key])
        return None

    def expire(self, key, ttl):
        #Not needed for Memory storage
        pass

class PluggableSplitStorageTests(object):
    """In memory split storage test cases."""

    def setup_method(self):
        """Prepare storages with test data."""
        self.mock_adapter = StorageMockAdapter()
        self.pluggable_split_storage = PluggableSplitStorage(self.mock_adapter, 'myprefix')

    def test_init(self):
        assert(self.pluggable_split_storage._prefix == "myprefix.SPLITIO.split.{split_name}")
        assert(self.pluggable_split_storage._traffic_type_prefix == "myprefix.SPLITIO.trafficType.{traffic_type_name}")
        assert(self.pluggable_split_storage._split_till_prefix == "myprefix.SPLITIO.splits.till")

        pluggable2 = PluggableSplitStorage(self.mock_adapter)
        assert(pluggable2._prefix == "SPLITIO.split.{split_name}")
        assert(pluggable2._traffic_type_prefix == "SPLITIO.trafficType.{traffic_type_name}")
        assert(pluggable2._split_till_prefix == "SPLITIO.splits.till")

    # TODO: To be added when producer mode is aupported
#    def test_put_many(self):
#        split1 = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
#        split2_temp = splits_json['splitChange1_2']['splits'][0].copy()
#        split2_temp['name'] = 'another_split'
#        split2 = splits.from_raw(split2_temp)
#        change_number = splits_json['splitChange1_2']['till']
#        traffic_type = splits_json['splitChange1_2']['splits'][0]['trafficTypeName']
#
#        self.pluggable_split_storage.put_many([split1, split2], change_number)
#        assert (self.mock_adapter._keys['myprefix.SPLITIO.split.' + split1.name] == split1.to_json())
#        assert (self.mock_adapter._keys['myprefix.SPLITIO.split.' + split2.name] == split2.to_json())
#        assert (self.mock_adapter._keys['myprefix.SPLITIO.trafficType.' + traffic_type] == 2)
#        assert (self.mock_adapter._keys["myprefix.SPLITIO.splits.till"] == change_number)

    def test_get(self):
        self.mock_adapter._keys = {}
        split1 = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
        split_name = splits_json['splitChange1_2']['splits'][0]['name']

        self.mock_adapter.set(self.pluggable_split_storage._prefix.format(split_name=split_name), split1.to_json())
        assert(self.pluggable_split_storage.get(split_name).to_json() ==  splits.from_raw(splits_json['splitChange1_2']['splits'][0]).to_json())
        assert(self.pluggable_split_storage.get('not_existing') == None)

    def test_fetch_many(self):
        self.mock_adapter._keys = {}
        split1 = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
        split2_temp = splits_json['splitChange1_2']['splits'][0].copy()
        split2_temp['name'] = 'another_split'
        split2 = splits.from_raw(split2_temp)

        self.mock_adapter.set(self.pluggable_split_storage._prefix.format(split_name=split1.name), split1.to_json())
        self.mock_adapter.set(self.pluggable_split_storage._prefix.format(split_name=split2.name), split2.to_json())
        fetched = self.pluggable_split_storage.fetch_many([split1.name, split2.name])
        assert(fetched[split1.name].to_json() == split1.to_json())
        assert(fetched[split2.name].to_json() == split2.to_json())

    # TODO: To be added when producer mode is aupported
#    def test_remove(self):
#        self.mock_adapter._keys = {}
#        split1 = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
#        change_number = splits_json['splitChange1_2']['till']
#        split_name = splits_json['splitChange1_2']['splits'][0]['name']
#        traffic_type = splits_json['splitChange1_2']['splits'][0]['trafficTypeName']
#
#        self.pluggable_split_storage.put_many([split1], change_number)
#        assert(self.pluggable_split_storage.traffic_type_exists(traffic_type) == True)
#        self.pluggable_split_storage.remove(split1.name)
#        assert(self.pluggable_split_storage.get(split_name) == None)
#        assert(self.pluggable_split_storage.traffic_type_exists(traffic_type) == False)

    def test_get_change_number(self):
        self.mock_adapter._keys = {}
        self.mock_adapter.set("myprefix.SPLITIO.splits.till", 1234)
        assert(self.pluggable_split_storage.get_change_number() == 1234)

    def test_get_split_names(self):
        self.mock_adapter._keys = {}
        split1 = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
        split2_temp = splits_json['splitChange1_2']['splits'][0].copy()
        split2_temp['name'] = 'another_split'
        split2 = splits.from_raw(split2_temp)

        self.mock_adapter.set(self.pluggable_split_storage._prefix.format(split_name=split1.name), split1.to_json())
        self.mock_adapter.set(self.pluggable_split_storage._prefix.format(split_name=split2.name), split2.to_json())
        assert(self.pluggable_split_storage.get_split_names() == [split1.name, split2.name])

    def test_get_all(self):
        self.mock_adapter._keys = {}
        split1 = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
        split2_temp = splits_json['splitChange1_2']['splits'][0].copy()
        split2_temp['name'] = 'another_split'
        split2 = splits.from_raw(split2_temp)

        self.mock_adapter.set(self.pluggable_split_storage._prefix.format(split_name=split1.name), split1.to_json())
        self.mock_adapter.set(self.pluggable_split_storage._prefix.format(split_name=split2.name), split2.to_json())
        all_splits = self.pluggable_split_storage.get_all()
        assert([all_splits[0].to_json(), all_splits[1].to_json()] == [split1.to_json(), split2.to_json()])

    # TODO: To be added when producer mode is aupported
#    def test_kill_locally(self):
#        self.mock_adapter._keys = {}
#        split_temp = splits_json['splitChange1_2']['splits'][0]
#        split_temp['killed'] = False
#        split1 = splits.from_raw(split_temp)
#        split_name = splits_json['splitChange1_2']['splits'][0]['name']
#
#        self.pluggable_split_storage.put_many([split1], 123)
#
        # should not apply if change number is lower
#        self.pluggable_split_storage.kill_locally(split_name, "off", 12)
#        assert(self.pluggable_split_storage.get(split_name).killed == False)
#
#        self.pluggable_split_storage.kill_locally(split_name, "off", 124)
#        assert(self.pluggable_split_storage.get(split_name).killed == True)

    # TODO: To be added when producer mode is aupported
#    def test_traffic_type_count(self):
#        self.mock_adapter._keys = {}
#        self.pluggable_split_storage._increase_traffic_type_count('user')
#        assert(self.pluggable_split_storage.is_valid_traffic_type('user'))
#
#        self.pluggable_split_storage._increase_traffic_type_count('user')
#        assert(self.mock_adapter._keys['myprefix.SPLITIO.trafficType.user'] == 2)
#
#        self.pluggable_split_storage._decrease_traffic_type_count('user')
#        assert(self.mock_adapter._keys['myprefix.SPLITIO.trafficType.user'] == 1)
#
#        self.pluggable_split_storage._decrease_traffic_type_count('user')
#        assert(not self.pluggable_split_storage.is_valid_traffic_type('user'))

    # TODO: To be added when producer mode is aupported
#    def test_put(self):
#        self.mock_adapter._keys = {}
#        split = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
#        self.pluggable_split_storage.put(split)
#        assert(self.mock_adapter._keys['myprefix.SPLITIO.trafficType.user'] == 1)
#        assert(split.to_json() == self.mock_adapter.get('myprefix.SPLITIO.split.' + split.name))
#
        # changing traffic type should delete existing one and add new one
#        split._traffic_type_name = 'account'
#        self.pluggable_split_storage.put(split)
#        assert('myprefix.SPLITIO.trafficType.user' not in self.mock_adapter._keys)
#        assert(self.mock_adapter._keys['myprefix.SPLITIO.trafficType.account'] == 1)
#
        # making update without changing traffic type should not increase the count
#        split._killed = 'False'
#        self.pluggable_split_storage.put(split)
#        assert(self.mock_adapter._keys['myprefix.SPLITIO.trafficType.account'] == 1)
#        assert(split.to_json()['killed'] == self.mock_adapter.get('myprefix.SPLITIO.split.' + split.name)['killed'])

class PluggableSegmentStorageTests(object):
    """In memory split storage test cases."""

    def setup_method(self):
        """Prepare storages with test data."""
        self.mock_adapter = StorageMockAdapter()
        self.pluggable_segment_storage = PluggableSegmentStorage(self.mock_adapter, 'myprefix')

    def test_init(self):
        assert(self.pluggable_segment_storage._prefix == "myprefix.SPLITIO.segment.{segment_name}")
        assert(self.pluggable_segment_storage._segment_till_prefix == "myprefix.SPLITIO.segment.{segment_name}.till")

        pluggable2 = PluggableSegmentStorage(self.mock_adapter)
        assert(pluggable2._prefix == "SPLITIO.segment.{segment_name}")
        assert(pluggable2._segment_till_prefix == "SPLITIO.segment.{segment_name}.till")

    # TODO: to be added when get_keys() is added
#    def test_update(self):
#        self.mock_adapter.set(self.pluggable_segment_storage._prefix.format(segment_name='segment1'), {'key1', 'key2'})
#        self.mock_adapter.set(self.pluggable_segment_storage._segment_till_prefix.format(segment_name='segment1'), 123)
#
#        assert('myprefix.SPLITIO.segment.segment1' in self.mock_adapter._keys)
#        assert(self.mock_adapter._keys['myprefix.SPLITIO.segment.segment1'] == set(['key1', 'key2']))
#        assert(self.mock_adapter._keys['myprefix.SPLITIO.segment.segment1.till'] == 123)

    def test_get_change_number(self):
        self.mock_adapter._keys = {}
        assert(self.pluggable_segment_storage.get_change_number('segment1') is None)

        self.mock_adapter.set(self.pluggable_segment_storage._segment_till_prefix.format(segment_name='segment1'), 123)
        assert(self.pluggable_segment_storage.get_change_number('segment1') == 123)

    # TODO: To be added when producer mode is implemented
#        self.pluggable_segment_storage.set_change_number('segment1', 124)
#        assert(self.mock_adapter._keys['myprefix.SPLITIO.segment.segment1.till'] == 124)

    def test_get_segment_names(self):
        self.mock_adapter._keys = {}
        assert(self.pluggable_segment_storage.get_segment_names() == [])

        self.mock_adapter.set(self.pluggable_segment_storage._prefix.format(segment_name='segment1'), {'key1', 'key2'})
        self.mock_adapter.set(self.pluggable_segment_storage._prefix.format(segment_name='segment2'), {})
        self.mock_adapter.set(self.pluggable_segment_storage._prefix.format(segment_name='segment3'), {'key1', 'key5'})
        assert(self.pluggable_segment_storage.get_segment_names() == ['segment1', 'segment2', 'segment3'])

    # TODO: to be added when get_keys() is added
#    def test_get_keys(self):
#        self.mock_adapter._keys = {}
#        self.pluggable_segment_storage.update('segment1', ['key1', 'key2'], [], 123)
#        assert(self.pluggable_segment_storage.get_keys('segment1').sort() == ['key1', 'key2'].sort())

    def test_segment_contains(self):
        self.mock_adapter._keys = {}
        self.mock_adapter.set(self.pluggable_segment_storage._prefix.format(segment_name='segment1'), {'key1', 'key2'})
        assert(not self.pluggable_segment_storage.segment_contains('segment1', 'key5'))
        assert(self.pluggable_segment_storage.segment_contains('segment1', 'key1'))

    # TODO: To be added when producer mode is implemented
#    def get_segment_keys_count(self):
#        self.mock_adapter._keys = {}
#        self.pluggable_segment_storage.update('segment1', ['key1', 'key2'], [], 123)
#        self.pluggable_segment_storage.update('segment2', [], [], 123)
#        self.pluggable_segment_storage.update('segment3', ['key1', 'key5'], [], 123)
#        assert(self.pluggable_segment_storage.get_segment_keys_count() == 4)

    def test_get(self):
        self.mock_adapter._keys = {}
        self.mock_adapter.set(self.pluggable_segment_storage._prefix.format(segment_name='segment1'), {'key1', 'key2'})
        segment = self.pluggable_segment_storage.get('segment1')
        assert(segment.name == 'segment1')
        assert(segment.keys == {'key1', 'key2'})

    # TODO: To be added when producer mode is implemented
#    def test_put(self):
#        self.mock_adapter._keys = {}
#        self.pluggable_segment_storage.update('segment1', ['key1', 'key2'], [], 123)
#        segment = self.pluggable_segment_storage.get('segment1')
#        segment._name = 'segment2'
#        segment._keys.add('key3')
#
#        self.pluggable_segment_storage.put(segment)
#        assert('myprefix.SPLITIO.segment.segment2' in self.mock_adapter._keys)
#        assert(self.mock_adapter._keys['myprefix.SPLITIO.segment.segment2'] == {'key1', 'key2', 'key3'})
#        assert(self.mock_adapter._keys['myprefix.SPLITIO.segment.segment2.till'] == 123)


class PluggableImpressionsStorageTests(object):
    """In memory impressions storage test cases."""

    def setup_method(self):
        """Prepare storages with test data."""
        self.mock_adapter = StorageMockAdapter()
        self.metadata = SdkMetadata('python-1.1.1', 'hostname', 'ip')
        self.pluggable_imp_storage = PluggableImpressionsStorage(self.mock_adapter, self.metadata, 'myprefix')

    def test_init(self):
        assert(self.pluggable_imp_storage._impressions_queue_key == "myprefix.SPLITIO.impressions")
        assert(self.pluggable_imp_storage._sdk_metadata == {
                                's': self.metadata.sdk_version,
                                'n': self.metadata.instance_name,
                                'i': self.metadata.instance_ip,
                            })

        pluggable2 = PluggableImpressionsStorage(self.mock_adapter, self.metadata)
        assert(pluggable2._impressions_queue_key == "SPLITIO.impressions")

    def test_put(self):
        impressions = [
            Impression('key1', 'feature1', 'on', 'some_label', 123456, 'buck1', 321654),
            Impression('key2', 'feature2', 'on', 'some_label', 123456, 'buck1', 321654),
            Impression('key3', 'feature2', 'on', 'some_label', 123456, 'buck1', 321654),
            Impression('key4', 'feature1', 'on', 'some_label', 123456, 'buck1', 321654)
        ]
        self.pluggable_imp_storage.put(impressions)
        assert(self.pluggable_imp_storage._impressions_queue_key in self.mock_adapter._keys)
        assert(self.mock_adapter._keys["myprefix.SPLITIO.impressions"] == self.pluggable_imp_storage._wrap_impressions(impressions))

        impressions2 = [
            Impression('key5', 'feature1', 'off', 'some_label', 123456, 'buck1', 321654),
            Impression('key6', 'feature2', 'off', 'some_label', 123456, 'buck1', 321654),
        ]
        self.pluggable_imp_storage.put(impressions2)
        assert(self.mock_adapter._keys["myprefix.SPLITIO.impressions"] == self.pluggable_imp_storage._wrap_impressions(impressions + impressions2))

    def test_wrap_impressions(self):
        impressions = [
            Impression('key1', 'feature1', 'on', 'some_label', 123456, 'buck1', 321654),
            Impression('key2', 'feature2', 'off', 'some_label', 123456, 'buck1', 321654),
        ]
        assert(self.pluggable_imp_storage._wrap_impressions(impressions) == [
            json.dumps({
                'm': {
                    's': self.metadata.sdk_version,
                    'n': self.metadata.instance_name,
                    'i': self.metadata.instance_ip,
                },
                'i': {
                    'k': 'key1',
                    'b': 'buck1',
                    'f': 'feature1',
                    't': 'on',
                    'r': 'some_label',
                    'c': 123456,
                    'm': 321654,
                }
            }),
            json.dumps({
                'm': {
                    's': self.metadata.sdk_version,
                    'n': self.metadata.instance_name,
                    'i': self.metadata.instance_ip,
                },
                'i': {
                    'k': 'key2',
                    'b': 'buck1',
                    'f': 'feature2',
                    't': 'off',
                    'r': 'some_label',
                    'c': 123456,
                    'm': 321654,
                }
            })
        ])

    def test_expire_key(self):
        self.expired_called = False
        self.key = ""
        self.ttl = 0
        def mock_expire(impressions_queue_key, ttl):
            self.key = impressions_queue_key
            self.ttl = ttl
            self.expired_called = True

        self.mock_adapter.expire = mock_expire

        # should not call if total_keys are higher
        self.pluggable_imp_storage.expire_key(200, 10)
        assert(not self.expired_called)

        self.pluggable_imp_storage.expire_key(200, 200)
        assert(self.expired_called)
        assert(self.key == "myprefix.SPLITIO.impressions")
        assert(self.ttl == self.pluggable_imp_storage.IMPRESSIONS_KEY_DEFAULT_TTL)


class PluggableEventsStorageTests(object):
    """In memory events storage test cases."""

    def setup_method(self):
        """Prepare storages with test data."""
        self.mock_adapter = StorageMockAdapter()
        self.metadata = SdkMetadata('python-1.1.1', 'hostname', 'ip')
        self.pluggable_events_storage = PluggableEventsStorage(self.mock_adapter, self.metadata, 'myprefix')

    def test_init(self):
        assert(self.pluggable_events_storage._events_queue_key == "myprefix.SPLITIO.events")
        assert(self.pluggable_events_storage._sdk_metadata == {
                                's': self.metadata.sdk_version,
                                'n': self.metadata.instance_name,
                                'i': self.metadata.instance_ip,
                            })

        pluggable2 = PluggableEventsStorage(self.mock_adapter, self.metadata)
        assert(pluggable2._events_queue_key == "SPLITIO.events")

    def test_put(self):
        events = [
            EventWrapper(event=Event('key1', 'user', 'purchase', 10, 123456, None),  size=32768),
            EventWrapper(event=Event('key2', 'user', 'purchase', 10, 123456, None),  size=32768),
            EventWrapper(event=Event('key3', 'user', 'purchase', 10, 123456, None),  size=32768),
            EventWrapper(event=Event('key4', 'user', 'purchase', 10, 123456, None),  size=32768),
        ]
        self.pluggable_events_storage.put(events)
        assert(self.pluggable_events_storage._events_queue_key in self.mock_adapter._keys)
        assert(self.mock_adapter._keys["myprefix.SPLITIO.events"] == self.pluggable_events_storage._wrap_events(events))

        events2 = [
            EventWrapper(event=Event('key5', 'user', 'purchase', 10, 123456, None),  size=32768),
            EventWrapper(event=Event('key6', 'user', 'purchase', 10, 123456, None),  size=32768),
        ]
        self.pluggable_events_storage.put(events2)
        assert(self.mock_adapter._keys["myprefix.SPLITIO.events"] == self.pluggable_events_storage._wrap_events(events + events2))

    def test_wrap_events(self):
        events = [
            EventWrapper(event=Event('key1', 'user', 'purchase', 10, 123456, None),  size=32768),
            EventWrapper(event=Event('key2', 'user', 'purchase', 10, 123456, None),  size=32768),
        ]
        assert(self.pluggable_events_storage._wrap_events(events) == [
            json.dumps({
                'e': {
                    'key': 'key1',
                    'trafficTypeName': 'user',
                    'eventTypeId': 'purchase',
                    'value': 10,
                    'timestamp': 123456,
                    'properties': None,
                },
                'm': {
                    's': self.metadata.sdk_version,
                    'n': self.metadata.instance_name,
                    'i': self.metadata.instance_ip,
                }
            }),
            json.dumps({
                'e': {
                    'key': 'key2',
                    'trafficTypeName': 'user',
                    'eventTypeId': 'purchase',
                    'value': 10,
                    'timestamp': 123456,
                    'properties': None,
                },
                'm': {
                    's': self.metadata.sdk_version,
                    'n': self.metadata.instance_name,
                    'i': self.metadata.instance_ip,
                }
            })
        ])

    def test_expire_key(self):
        self.expired_called = False
        self.key = ""
        self.ttl = 0
        def mock_expire(impressions_event_key, ttl):
            self.key = impressions_event_key
            self.ttl = ttl
            self.expired_called = True

        self.mock_adapter.expire = mock_expire

        # should not call if total_keys are higher
        self.pluggable_events_storage.expire_key(200, 10)
        assert(not self.expired_called)

        self.pluggable_events_storage.expire_key(200, 200)
        assert(self.expired_called)
        assert(self.key == "myprefix.SPLITIO.events")
        assert(self.ttl == self.pluggable_events_storage._EVENTS_KEY_DEFAULT_TTL)
