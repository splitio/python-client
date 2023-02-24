"""Pluggable storage test module."""
from splitio.models.splits import Split
from splitio.models import splits, segments
from splitio.models.segments import Segment
from splitio.storage.pluggable import PluggableSplitStorage, PluggableSegmentStorage

from tests.integration import splits_json
import pytest

class StorageMockAdapter(object):
    def __init__(self):
        self._keys = {}

    def get(self, key):
        if key not in self._keys:
            return None
        return self._keys[key]

    def get_many(self, keys):
        return [self.get(key) for key in keys]

    def set(self, key, value):
        self._keys[key] = value

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
