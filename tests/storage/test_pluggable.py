"""Pluggable storage test module."""
from splitio.models.splits import Split
from splitio.models import splits
from splitio.storage.pluggable import PluggableSplitStorage

from tests.integration import splits_json
import pytest

class MockAdapter(object):
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

    def decrement(self, key, value):
        if key not in self._keys:
            return
        self._keys[key]-= value

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

class PluggableSplitStorageTests(object):
    """In memory split storage test cases."""

    def setup_method(self):
        """Prepare storages with test data."""
        self.mock_adapter = MockAdapter()
        self.pluggable_split_storage = PluggableSplitStorage(self.mock_adapter, 'myprefix')

    def test_init(self):
        assert(self.pluggable_split_storage._prefix == "myprefix.split.")
        assert(self.pluggable_split_storage._traffic_type_prefix == "myprefix.trafficType.")
        assert(self.pluggable_split_storage._split_till_prefix == "myprefix.splits.till")

        pluggable2 = PluggableSplitStorage(self.mock_adapter)
        assert(pluggable2._prefix == "split.")
        assert(pluggable2._traffic_type_prefix == "trafficType.")
        assert(pluggable2._split_till_prefix == "splits.till")

    def test_put_many(self):
        split1 = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
        split2_temp = splits_json['splitChange1_2']['splits'][0].copy()
        split2_temp['name'] = 'another_split'
        split2 = splits.from_raw(split2_temp)
        change_number = splits_json['splitChange1_2']['till']
        traffic_type = splits_json['splitChange1_2']['splits'][0]['trafficTypeName']

        self.pluggable_split_storage.put_many([split1, split2], change_number)
        assert (self.mock_adapter._keys['myprefix.split.' + split1.name] == split1.to_json())
        assert (self.mock_adapter._keys['myprefix.split.' + split2.name] == split2.to_json())
        assert (self.mock_adapter._keys['myprefix.trafficType.' + traffic_type] == 2)
        assert (self.mock_adapter._keys["myprefix.splits.till"] == change_number)

    def test_get(self):
        self.mock_adapter._keys = {}
        split1 = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
        change_number = splits_json['splitChange1_2']['till']
        split_name = splits_json['splitChange1_2']['splits'][0]['name']

        self.pluggable_split_storage.put_many([split1], change_number)
        assert(self.pluggable_split_storage.get(split_name).to_json() ==  splits.from_raw(splits_json['splitChange1_2']['splits'][0]).to_json())
        assert(self.pluggable_split_storage.get('not_existing') == None)

    def test_fetch_many(self):
        self.mock_adapter._keys = {}
        split1 = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
        split2_temp = splits_json['splitChange1_2']['splits'][0].copy()
        split2_temp['name'] = 'another_split'
        split2 = splits.from_raw(split2_temp)
        change_number = splits_json['splitChange1_2']['till']

        self.pluggable_split_storage.put_many([split1, split2], change_number)
        fetched = self.pluggable_split_storage.fetch_many([split1.name, split2.name])
        assert(fetched[split1.name].to_json() == split1.to_json())
        assert(fetched[split2.name].to_json() == split2.to_json())

    def test_remove(self):
        self.mock_adapter._keys = {}
        split1 = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
        change_number = splits_json['splitChange1_2']['till']
        split_name = splits_json['splitChange1_2']['splits'][0]['name']
        traffic_type = splits_json['splitChange1_2']['splits'][0]['trafficTypeName']

        self.pluggable_split_storage.put_many([split1], change_number)
        assert(self.pluggable_split_storage.traffic_type_exists(traffic_type) == True)
        self.pluggable_split_storage.remove(split1.name)
        assert(self.pluggable_split_storage.get(split_name) == None)
        assert(self.pluggable_split_storage.traffic_type_exists(traffic_type) == False)

    def test_change_number(self):
        self.mock_adapter._keys = {}
        self.pluggable_split_storage.set_change_number(1234)
        assert(self.pluggable_split_storage.get_change_number() == 1234)

    def test_get_split_names(self):
        self.mock_adapter._keys = {}
        split1 = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
        split2_temp = splits_json['splitChange1_2']['splits'][0].copy()
        split2_temp['name'] = 'another_split'
        split2 = splits.from_raw(split2_temp)
        change_number = splits_json['splitChange1_2']['till']

        self.pluggable_split_storage.put_many([split1, split2], change_number)
        assert(self.pluggable_split_storage.get_split_names() == [split1.name, split2.name])

    def test_get_all(self):
        self.mock_adapter._keys = {}
        split1 = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
        split2_temp = splits_json['splitChange1_2']['splits'][0].copy()
        split2_temp['name'] = 'another_split'
        split2 = splits.from_raw(split2_temp)
        change_number = splits_json['splitChange1_2']['till']

        self.pluggable_split_storage.put_many([split1, split2], change_number)
        all_splits = self.pluggable_split_storage.get_all()
        assert([all_splits[0].to_json(), all_splits[1].to_json()] == [split1.to_json(), split2.to_json()])

    def test_kill_locally(self):
        self.mock_adapter._keys = {}
        split_temp = splits_json['splitChange1_2']['splits'][0]
        split_temp['killed'] = False
        split1 = splits.from_raw(split_temp)
        split_name = splits_json['splitChange1_2']['splits'][0]['name']

        self.pluggable_split_storage.put_many([split1], 123)

        # should not apply if change number is lower
        self.pluggable_split_storage.kill_locally(split_name, "off", 12)
        assert(self.pluggable_split_storage.get(split_name).killed == False)

        self.pluggable_split_storage.kill_locally(split_name, "off", 124)
        assert(self.pluggable_split_storage.get(split_name).killed == True)

    def test_traffic_type_count(self):
        self.mock_adapter._keys = {}
        self.pluggable_split_storage._increase_traffic_type_count('user')
        assert(self.pluggable_split_storage.is_valid_traffic_type('user'))

        self.pluggable_split_storage._increase_traffic_type_count('user')
        assert(self.mock_adapter._keys['myprefix.trafficType.user'] == 2)

        self.pluggable_split_storage._decrease_traffic_type_count('user')
        assert(self.mock_adapter._keys['myprefix.trafficType.user'] == 1)

        self.pluggable_split_storage._decrease_traffic_type_count('user')
        assert(not self.pluggable_split_storage.is_valid_traffic_type('user'))

    def test_put(self):
        self.mock_adapter._keys = {}
        split = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
        self.pluggable_split_storage.put(split)
        assert(self.mock_adapter._keys['myprefix.trafficType.user'] == 1)
        assert(split.to_json() == self.mock_adapter.get('myprefix.split.' + split.name))
