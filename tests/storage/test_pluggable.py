"""Pluggable storage test module."""
import json
import threading
import pytest

from splitio.optional.loaders import asyncio
from splitio.models.splits import Split
from splitio.models import splits, segments
from splitio.models.segments import Segment
from splitio.models.impressions import Impression
from splitio.models.events import Event, EventWrapper
from splitio.storage.pluggable import PluggableSplitStorage, PluggableSegmentStorage, PluggableImpressionsStorage, PluggableEventsStorage, \
    PluggableTelemetryStorage, PluggableEventsStorageAsync, PluggableSegmentStorageAsync, PluggableImpressionsStorageAsync,\
    PluggableSplitStorageAsync, PluggableTelemetryStorageAsync
from splitio.client.util import get_metadata, SdkMetadata
from splitio.models.telemetry import MAX_TAGS, MethodExceptionsAndLatencies, OperationMode
from tests.integration import splits_json

class StorageMockAdapter(object):
    def __init__(self):
        self._keys = {}
        self._expire = {}
        self._lock = threading.RLock()

    def get(self, key):
        with self._lock:
            if key not in self._keys:
                return None
            return self._keys[key]

    def get_items(self, key):
        with self._lock:
            if key not in self._keys:
                return None
            return list(self._keys[key])

    def set(self, key, value):
        with self._lock:
            self._keys[key] = value

    def push_items(self, key, *value):
        with self._lock:
            items = []
            if key in self._keys:
                items = self._keys[key]
            [items.append(item) for item in value]
            self._keys[key] = items
            return len(self._keys[key])

    def delete(self, key):
        with self._lock:
            if key in self._keys:
                del self._keys[key]

    def pop_items(self, key):
        with self._lock:
            if key not in self._keys:
                return None
            items = list(self._keys[key])
            del self._keys[key]
            return items

    def increment(self, key, value):
        with self._lock:
            if key not in self._keys:
                self._keys[key] = 0
            self._keys[key]+= value
            return self._keys[key]

    def decrement(self, key, value):
        with self._lock:
            if key not in self._keys:
                return None
            self._keys[key]-= value
            return self._keys[key]

    def get_keys_by_prefix(self, prefix):
        with self._lock:
            keys = []
            for key in self._keys:
                if prefix in key:
                    keys.append(key)
            return keys

    def get_many(self, keys):
        with self._lock:
            returned_keys = []
            for key in self._keys:
                if key in keys:
                    returned_keys.append(self._keys[key])
            return returned_keys

    def add_items(self, key, added_items):
        with self._lock:
            items = set()
            if key in self._keys:
                items = set(self._keys[key])
            [items.add(item) for item in added_items]
            self._keys[key] = items

    def remove_items(self, key, removed_items):
        with self._lock:
            new_items = set()
            for item in self._keys[key]:
                if item not in removed_items:
                    new_items.add(item)
            self._keys[key] = new_items

    def item_contains(self, key, item):
        with self._lock:
            if item in self._keys[key]:
                return True
            return False

    def get_items_count(self, key):
        with self._lock:
            if key in self._keys:
                return len(self._keys[key])
            return None

    def expire(self, key, ttl):
        with self._lock:
            if key in self._expire:
                self._expire[key] = -1
            else:
                self._expire[key] = ttl
            # should only be called once per key.

class StorageMockAdapterAsync(object):
    def __init__(self):
        self._keys = {}
        self._expire = {}
        self._lock = asyncio.Lock()

    async def get(self, key):
        async with self._lock:
            if key not in self._keys:
                return None
            return self._keys[key]

    async def get_items(self, key):
        async with self._lock:
            if key not in self._keys:
                return None
            return list(self._keys[key])

    async def set(self, key, value):
        async with self._lock:
            self._keys[key] = value

    async def push_items(self, key, *value):
        async with self._lock:
            items = []
            if key in self._keys:
                items = self._keys[key]
            [items.append(item) for item in value]
            self._keys[key] = items
            return len(self._keys[key])

    async def delete(self, key):
        async with self._lock:
            if key in self._keys:
                del self._keys[key]

    async def pop_items(self, key):
        async with self._lock:
            if key not in self._keys:
                return None
            items = list(self._keys[key])
            del self._keys[key]
            return items

    async def increment(self, key, value):
        async with self._lock:
            if key not in self._keys:
                self._keys[key] = 0
            self._keys[key]+= value
            return self._keys[key]

    async def decrement(self, key, value):
        async with self._lock:
            if key not in self._keys:
                return None
            self._keys[key]-= value
            return self._keys[key]

    async def get_keys_by_prefix(self, prefix):
        async with self._lock:
            keys = []
            for key in self._keys:
                if prefix in key:
                    keys.append(key)
            return keys

    async def get_many(self, keys):
        async with self._lock:
            returned_keys = []
            for key in self._keys:
                if key in keys:
                    returned_keys.append(self._keys[key])
            return returned_keys

    async def add_items(self, key, added_items):
        async with self._lock:
            items = set()
            if key in self._keys:
                items = set(self._keys[key])
            [items.add(item) for item in added_items]
            self._keys[key] = items

    async def remove_items(self, key, removed_items):
        async with self._lock:
            new_items = set()
            for item in self._keys[key]:
                if item not in removed_items:
                    new_items.add(item)
            self._keys[key] = new_items

    async def item_contains(self, key, item):
        async with self._lock:
            if item in self._keys[key]:
                return True
            return False

    async def get_items_count(self, key):
        async with self._lock:
            if key in self._keys:
                return len(self._keys[key])
            return None

    async def expire(self, key, ttl):
        async with self._lock:
            if key in self._expire:
                self._expire[key] = -1
            else:
                self._expire[key] = ttl


class PluggableSplitStorageTests(object):
    """In memory split storage test cases."""

    def setup_method(self):
        """Prepare storages with test data."""
        self.mock_adapter = StorageMockAdapter()

    def test_init(self):
        for sprefix in [None, 'myprefix']:
            pluggable_split_storage = PluggableSplitStorage(self.mock_adapter, prefix=sprefix)
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
        assert(pluggable_split_storage._prefix == prefix + "SPLITIO.split.{feature_flag_name}")
        assert(pluggable_split_storage._traffic_type_prefix == prefix + "SPLITIO.trafficType.{traffic_type_name}")
        assert(pluggable_split_storage._feature_flag_till_prefix == prefix + "SPLITIO.splits.till")

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
        for sprefix in [None, 'myprefix']:
            pluggable_split_storage = PluggableSplitStorage(self.mock_adapter, prefix=sprefix)

            split1 = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
            split_name = splits_json['splitChange1_2']['splits'][0]['name']

            self.mock_adapter.set(pluggable_split_storage._prefix.format(feature_flag_name=split_name), split1.to_json())
            assert(pluggable_split_storage.get(split_name).to_json() ==  splits.from_raw(splits_json['splitChange1_2']['splits'][0]).to_json())
            assert(pluggable_split_storage.get('not_existing') == None)

    def test_fetch_many(self):
        self.mock_adapter._keys = {}
        for sprefix in [None, 'myprefix']:
            pluggable_split_storage = PluggableSplitStorage(self.mock_adapter, prefix=sprefix)
            split1 = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
            split2_temp = splits_json['splitChange1_2']['splits'][0].copy()
            split2_temp['name'] = 'another_split'
            split2 = splits.from_raw(split2_temp)

            self.mock_adapter.set(pluggable_split_storage._prefix.format(feature_flag_name=split1.name), split1.to_json())
            self.mock_adapter.set(pluggable_split_storage._prefix.format(feature_flag_name=split2.name), split2.to_json())
            fetched = pluggable_split_storage.fetch_many([split1.name, split2.name])
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
        for sprefix in [None, 'myprefix']:
            pluggable_split_storage = PluggableSplitStorage(self.mock_adapter, prefix=sprefix)
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
            self.mock_adapter.set(prefix + "SPLITIO.splits.till", 1234)
            assert(pluggable_split_storage.get_change_number() == 1234)

    def test_get_split_names(self):
        self.mock_adapter._keys = {}
        for sprefix in [None, 'myprefix']:
            pluggable_split_storage = PluggableSplitStorage(self.mock_adapter, prefix=sprefix)
            split1 = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
            split2_temp = splits_json['splitChange1_2']['splits'][0].copy()
            split2_temp['name'] = 'another_split'
            split2 = splits.from_raw(split2_temp)
            self.mock_adapter.set(pluggable_split_storage._prefix.format(feature_flag_name=split1.name), split1.to_json())
            self.mock_adapter.set(pluggable_split_storage._prefix.format(feature_flag_name=split2.name), split2.to_json())
            assert(pluggable_split_storage.get_split_names() == [split1.name, split2.name])

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


class PluggableSplitStorageAsyncTests(object):
    """In memory async split storage test cases."""

    def setup_method(self):
        """Prepare storages with test data."""
        self.mock_adapter = StorageMockAdapterAsync()

    def test_init(self):
        for sprefix in [None, 'myprefix']:
            pluggable_split_storage = PluggableSplitStorageAsync(self.mock_adapter, prefix=sprefix)
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
        assert(pluggable_split_storage._prefix == prefix + "SPLITIO.split.{feature_flag_name}")
        assert(pluggable_split_storage._traffic_type_prefix == prefix + "SPLITIO.trafficType.{traffic_type_name}")
        assert(pluggable_split_storage._feature_flag_till_prefix == prefix + "SPLITIO.splits.till")

    @pytest.mark.asyncio
    async def test_get(self):
        self.mock_adapter._keys = {}
        for sprefix in [None, 'myprefix']:
            pluggable_split_storage = PluggableSplitStorageAsync(self.mock_adapter, prefix=sprefix)

            split1 = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
            split_name = splits_json['splitChange1_2']['splits'][0]['name']

            await self.mock_adapter.set(pluggable_split_storage._prefix.format(feature_flag_name=split_name), split1.to_json())
            split = await pluggable_split_storage.get(split_name)
            assert(split.to_json() ==  splits.from_raw(splits_json['splitChange1_2']['splits'][0]).to_json())
            assert(await pluggable_split_storage.get('not_existing') == None)

    @pytest.mark.asyncio
    async def test_fetch_many(self):
        self.mock_adapter._keys = {}
        for sprefix in [None, 'myprefix']:
            pluggable_split_storage = PluggableSplitStorageAsync(self.mock_adapter, prefix=sprefix)
            split1 = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
            split2_temp = splits_json['splitChange1_2']['splits'][0].copy()
            split2_temp['name'] = 'another_split'
            split2 = splits.from_raw(split2_temp)

            await self.mock_adapter.set(pluggable_split_storage._prefix.format(feature_flag_name=split1.name), split1.to_json())
            await self.mock_adapter.set(pluggable_split_storage._prefix.format(feature_flag_name=split2.name), split2.to_json())
            fetched = await pluggable_split_storage.fetch_many([split1.name, split2.name])
            assert(fetched[split1.name].to_json() == split1.to_json())
            assert(fetched[split2.name].to_json() == split2.to_json())

    @pytest.mark.asyncio
    async def test_get_change_number(self):
        self.mock_adapter._keys = {}
        for sprefix in [None, 'myprefix']:
            pluggable_split_storage = PluggableSplitStorageAsync(self.mock_adapter, prefix=sprefix)
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
            await self.mock_adapter.set(prefix + "SPLITIO.splits.till", 1234)
            assert(await pluggable_split_storage.get_change_number() == 1234)

    @pytest.mark.asyncio
    async def test_get_split_names(self):
        self.mock_adapter._keys = {}
        for sprefix in [None, 'myprefix']:
            pluggable_split_storage = PluggableSplitStorageAsync(self.mock_adapter, prefix=sprefix)
            split1 = splits.from_raw(splits_json['splitChange1_2']['splits'][0])
            split2_temp = splits_json['splitChange1_2']['splits'][0].copy()
            split2_temp['name'] = 'another_split'
            split2 = splits.from_raw(split2_temp)
            await self.mock_adapter.set(pluggable_split_storage._prefix.format(feature_flag_name=split1.name), split1.to_json())
            await self.mock_adapter.set(pluggable_split_storage._prefix.format(feature_flag_name=split2.name), split2.to_json())

            assert(await pluggable_split_storage.get_split_names() == [split1.name, split2.name])

class PluggableSegmentStorageTests(object):
    """In memory split storage test cases."""

    def setup_method(self):
        """Prepare storages with test data."""
        self.mock_adapter = StorageMockAdapter()

    def test_init(self):
        for sprefix in [None, 'myprefix']:
            pluggable_segment_storage = PluggableSegmentStorage(self.mock_adapter, prefix=sprefix)
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
            assert(pluggable_segment_storage._prefix == prefix + "SPLITIO.segment.{segment_name}")
            assert(pluggable_segment_storage._segment_till_prefix == prefix + "SPLITIO.segment.{segment_name}.till")

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
        for sprefix in [None, 'myprefix']:
            pluggable_segment_storage = PluggableSegmentStorage(self.mock_adapter, prefix=sprefix)
            assert(pluggable_segment_storage.get_change_number('segment1') is None)

            self.mock_adapter.set(pluggable_segment_storage._segment_till_prefix.format(segment_name='segment1'), 123)
            assert(pluggable_segment_storage.get_change_number('segment1') == 123)

    # TODO: To be added when producer mode is implemented
#        self.pluggable_segment_storage.set_change_number('segment1', 124)
#        assert(self.mock_adapter._keys['myprefix.SPLITIO.segment.segment1.till'] == 124)

    def test_get_segment_names(self):
        self.mock_adapter._keys = {}
        for sprefix in [None, 'myprefix']:
            pluggable_segment_storage = PluggableSegmentStorage(self.mock_adapter, prefix=sprefix)
            assert(pluggable_segment_storage.get_segment_names() == [])

            self.mock_adapter.set(pluggable_segment_storage._prefix.format(segment_name='segment1'), {'key1', 'key2'})
            self.mock_adapter.set(pluggable_segment_storage._prefix.format(segment_name='segment2'), {})
            self.mock_adapter.set(pluggable_segment_storage._prefix.format(segment_name='segment3'), {'key1', 'key5'})
            assert(pluggable_segment_storage.get_segment_names() == ['segment1', 'segment2', 'segment3'])

    # TODO: to be added when get_keys() is added
#    def test_get_keys(self):
#        self.mock_adapter._keys = {}
#        self.pluggable_segment_storage.update('segment1', ['key1', 'key2'], [], 123)
#        assert(self.pluggable_segment_storage.get_keys('segment1').sort() == ['key1', 'key2'].sort())

    def test_segment_contains(self):
        self.mock_adapter._keys = {}
        for sprefix in [None, 'myprefix']:
            pluggable_segment_storage = PluggableSegmentStorage(self.mock_adapter, prefix=sprefix)
            self.mock_adapter.set(pluggable_segment_storage._prefix.format(segment_name='segment1'), {'key1', 'key2'})
            assert(not pluggable_segment_storage.segment_contains('segment1', 'key5'))
            assert(pluggable_segment_storage.segment_contains('segment1', 'key1'))

    # TODO: To be added when producer mode is implemented
#    def get_segment_keys_count(self):
#        self.mock_adapter._keys = {}
#        self.pluggable_segment_storage.update('segment1', ['key1', 'key2'], [], 123)
#        self.pluggable_segment_storage.update('segment2', [], [], 123)
#        self.pluggable_segment_storage.update('segment3', ['key1', 'key5'], [], 123)
#        assert(self.pluggable_segment_storage.get_segment_keys_count() == 4)

    def test_get(self):
        self.mock_adapter._keys = {}
        for sprefix in [None, 'myprefix']:
            pluggable_segment_storage = PluggableSegmentStorage(self.mock_adapter, prefix=sprefix)
            self.mock_adapter.set(pluggable_segment_storage._prefix.format(segment_name='segment1'), {'key1', 'key2'})
            segment = pluggable_segment_storage.get('segment1')
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


class PluggableSegmentStorageAsyncTests(object):
    """In memory async segment storage test cases."""

    def setup_method(self):
        """Prepare storages with test data."""
        self.mock_adapter = StorageMockAdapterAsync()

    def test_init(self):
        for sprefix in [None, 'myprefix']:
            pluggable_segment_storage = PluggableSegmentStorageAsync(self.mock_adapter, prefix=sprefix)
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
            assert(pluggable_segment_storage._prefix == prefix + "SPLITIO.segment.{segment_name}")
            assert(pluggable_segment_storage._segment_till_prefix == prefix + "SPLITIO.segment.{segment_name}.till")

    @pytest.mark.asyncio
    async def test_get_change_number(self):
        self.mock_adapter._keys = {}
        for sprefix in [None, 'myprefix']:
            pluggable_segment_storage = PluggableSegmentStorageAsync(self.mock_adapter, prefix=sprefix)
            assert(await pluggable_segment_storage.get_change_number('segment1') is None)

            await self.mock_adapter.set(pluggable_segment_storage._segment_till_prefix.format(segment_name='segment1'), 123)
            assert(await pluggable_segment_storage.get_change_number('segment1') == 123)

    @pytest.mark.asyncio
    async def test_get_segment_names(self):
        self.mock_adapter._keys = {}
        for sprefix in [None, 'myprefix']:
            pluggable_segment_storage = PluggableSegmentStorageAsync(self.mock_adapter, prefix=sprefix)
            assert(await pluggable_segment_storage.get_segment_names() == [])

            await self.mock_adapter.set(pluggable_segment_storage._prefix.format(segment_name='segment1'), {'key1', 'key2'})
            await self.mock_adapter.set(pluggable_segment_storage._prefix.format(segment_name='segment2'), {})
            await self.mock_adapter.set(pluggable_segment_storage._prefix.format(segment_name='segment3'), {'key1', 'key5'})
            assert(await pluggable_segment_storage.get_segment_names() == ['segment1', 'segment2', 'segment3'])

    @pytest.mark.asyncio
    async def test_segment_contains(self):
        self.mock_adapter._keys = {}
        for sprefix in [None, 'myprefix']:
            pluggable_segment_storage = PluggableSegmentStorageAsync(self.mock_adapter, prefix=sprefix)
            await self.mock_adapter.set(pluggable_segment_storage._prefix.format(segment_name='segment1'), {'key1', 'key2'})
            assert(not await pluggable_segment_storage.segment_contains('segment1', 'key5'))
            assert(await pluggable_segment_storage.segment_contains('segment1', 'key1'))

    @pytest.mark.asyncio
    async def test_get(self):
        self.mock_adapter._keys = {}
        for sprefix in [None, 'myprefix']:
            pluggable_segment_storage = PluggableSegmentStorageAsync(self.mock_adapter, prefix=sprefix)
            await self.mock_adapter.set(pluggable_segment_storage._prefix.format(segment_name='segment1'), {'key1', 'key2'})
            segment = await pluggable_segment_storage.get('segment1')
            assert(segment.name == 'segment1')
            assert(segment.keys == {'key1', 'key2'})


class PluggableImpressionsStorageTests(object):
    """In memory impressions storage test cases."""

    def setup_method(self):
        """Prepare storages with test data."""
        self.mock_adapter = StorageMockAdapter()
        self.metadata = SdkMetadata('python-1.1.1', 'hostname', 'ip')

    def test_init(self):
        for sprefix in [None, 'myprefix']:
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
            pluggable_imp_storage = PluggableImpressionsStorage(self.mock_adapter, self.metadata, prefix=sprefix)
            assert(pluggable_imp_storage._impressions_queue_key == prefix + "SPLITIO.impressions")
            assert(pluggable_imp_storage._sdk_metadata == {
                                    's': self.metadata.sdk_version,
                                    'n': self.metadata.instance_name,
                                    'i': self.metadata.instance_ip,
                                })


    def test_put(self):
        for sprefix in [None, 'myprefix']:
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
            pluggable_imp_storage = PluggableImpressionsStorage(self.mock_adapter, self.metadata, prefix=sprefix)
            impressions = [
                Impression('key1', 'feature1', 'on', 'some_label', 123456, 'buck1', 321654),
                Impression('key2', 'feature2', 'on', 'some_label', 123456, 'buck1', 321654),
                Impression('key3', 'feature2', 'on', 'some_label', 123456, 'buck1', 321654),
                Impression('key4', 'feature1', 'on', 'some_label', 123456, 'buck1', 321654)
            ]
            assert(pluggable_imp_storage.put(impressions))
            assert(pluggable_imp_storage._impressions_queue_key in self.mock_adapter._keys)
            assert(self.mock_adapter._keys[prefix + "SPLITIO.impressions"] == pluggable_imp_storage._wrap_impressions(impressions))
            assert(self.mock_adapter._expire[prefix + "SPLITIO.impressions"] == PluggableImpressionsStorage.IMPRESSIONS_KEY_DEFAULT_TTL)

            impressions2 = [
                Impression('key5', 'feature1', 'off', 'some_label', 123456, 'buck1', 321654),
                Impression('key6', 'feature2', 'off', 'some_label', 123456, 'buck1', 321654),
            ]
            assert(pluggable_imp_storage.put(impressions2))
            assert(self.mock_adapter._keys[prefix + "SPLITIO.impressions"] == pluggable_imp_storage._wrap_impressions(impressions + impressions2))

    def test_wrap_impressions(self):
        for sprefix in [None, 'myprefix']:
            pluggable_imp_storage = PluggableImpressionsStorage(self.mock_adapter, self.metadata, prefix=sprefix)
            impressions = [
                Impression('key1', 'feature1', 'on', 'some_label', 123456, 'buck1', 321654),
                Impression('key2', 'feature2', 'off', 'some_label', 123456, 'buck1', 321654),
            ]
            assert(pluggable_imp_storage._wrap_impressions(impressions) == [
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
        for sprefix in [None, 'myprefix']:
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
            pluggable_imp_storage = PluggableImpressionsStorage(self.mock_adapter, self.metadata, prefix=sprefix)
            self.expired_called = False
            self.key = ""
            self.ttl = 0
            def mock_expire(impressions_queue_key, ttl):
                self.key = impressions_queue_key
                self.ttl = ttl
                self.expired_called = True

            self.mock_adapter.expire = mock_expire

            # should not call if total_keys are higher
            pluggable_imp_storage.expire_key(200, 10)
            assert(not self.expired_called)

            pluggable_imp_storage.expire_key(200, 200)
            assert(self.expired_called)
            assert(self.key == prefix + "SPLITIO.impressions")
            assert(self.ttl == pluggable_imp_storage.IMPRESSIONS_KEY_DEFAULT_TTL)


class PluggableImpressionsStorageAsyncTests(object):
    """In memory impressions storage test cases."""

    def setup_method(self):
        """Prepare storages with test data."""
        self.mock_adapter = StorageMockAdapterAsync()
        self.metadata = SdkMetadata('python-1.1.1', 'hostname', 'ip')

    def test_init(self):
        for sprefix in [None, 'myprefix']:
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
            pluggable_imp_storage = PluggableImpressionsStorageAsync(self.mock_adapter, self.metadata, prefix=sprefix)
            assert(pluggable_imp_storage._impressions_queue_key == prefix + "SPLITIO.impressions")
            assert(pluggable_imp_storage._sdk_metadata == {
                                    's': self.metadata.sdk_version,
                                    'n': self.metadata.instance_name,
                                    'i': self.metadata.instance_ip,
                                })

    @pytest.mark.asyncio
    async def test_put(self):
        for sprefix in [None, 'myprefix']:
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
            pluggable_imp_storage = PluggableImpressionsStorageAsync(self.mock_adapter, self.metadata, prefix=sprefix)
            impressions = [
                Impression('key1', 'feature1', 'on', 'some_label', 123456, 'buck1', 321654),
                Impression('key2', 'feature2', 'on', 'some_label', 123456, 'buck1', 321654),
                Impression('key3', 'feature2', 'on', 'some_label', 123456, 'buck1', 321654),
                Impression('key4', 'feature1', 'on', 'some_label', 123456, 'buck1', 321654)
            ]
            assert(await pluggable_imp_storage.put(impressions))
            assert(pluggable_imp_storage._impressions_queue_key in self.mock_adapter._keys)
            assert(self.mock_adapter._keys[prefix + "SPLITIO.impressions"] == pluggable_imp_storage._wrap_impressions(impressions))
            assert(self.mock_adapter._expire[prefix + "SPLITIO.impressions"] == PluggableImpressionsStorageAsync.IMPRESSIONS_KEY_DEFAULT_TTL)

            impressions2 = [
                Impression('key5', 'feature1', 'off', 'some_label', 123456, 'buck1', 321654),
                Impression('key6', 'feature2', 'off', 'some_label', 123456, 'buck1', 321654),
            ]
            assert(await pluggable_imp_storage.put(impressions2))
            assert(self.mock_adapter._keys[prefix + "SPLITIO.impressions"] == pluggable_imp_storage._wrap_impressions(impressions + impressions2))

    def test_wrap_impressions(self):
        for sprefix in [None, 'myprefix']:
            pluggable_imp_storage = PluggableImpressionsStorageAsync(self.mock_adapter, self.metadata, prefix=sprefix)
            impressions = [
                Impression('key1', 'feature1', 'on', 'some_label', 123456, 'buck1', 321654),
                Impression('key2', 'feature2', 'off', 'some_label', 123456, 'buck1', 321654),
            ]
            assert(pluggable_imp_storage._wrap_impressions(impressions) == [
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

    @pytest.mark.asyncio
    async def test_expire_key(self):
        for sprefix in [None, 'myprefix']:
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
            pluggable_imp_storage = PluggableImpressionsStorageAsync(self.mock_adapter, self.metadata, prefix=sprefix)
            self.expired_called = False
            self.key = ""
            self.ttl = 0
            async def mock_expire(impressions_queue_key, ttl):
                self.key = impressions_queue_key
                self.ttl = ttl
                self.expired_called = True

            self.mock_adapter.expire = mock_expire

            # should not call if total_keys are higher
            await pluggable_imp_storage.expire_key(200, 10)
            assert(not self.expired_called)

            await pluggable_imp_storage.expire_key(200, 200)
            assert(self.expired_called)
            assert(self.key == prefix + "SPLITIO.impressions")
            assert(self.ttl == pluggable_imp_storage.IMPRESSIONS_KEY_DEFAULT_TTL)


class PluggableEventsStorageTests(object):
    """Pluggable events storage test cases."""

    def setup_method(self):
        """Prepare storages with test data."""
        self.mock_adapter = StorageMockAdapter()
        self.metadata = SdkMetadata('python-1.1.1', 'hostname', 'ip')

    def test_init(self):
        for sprefix in [None, 'myprefix']:
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
            pluggable_events_storage = PluggableEventsStorage(self.mock_adapter, self.metadata, prefix=sprefix)
            assert(pluggable_events_storage._events_queue_key == prefix + "SPLITIO.events")
            assert(pluggable_events_storage._sdk_metadata == {
                                    's': self.metadata.sdk_version,
                                    'n': self.metadata.instance_name,
                                    'i': self.metadata.instance_ip,
                                })

    def test_put(self):
        for sprefix in [None, 'myprefix']:
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
            pluggable_events_storage = PluggableEventsStorage(self.mock_adapter, self.metadata, prefix=sprefix)
            events = [
                EventWrapper(event=Event('key1', 'user', 'purchase', 10, 123456, None),  size=32768),
                EventWrapper(event=Event('key2', 'user', 'purchase', 10, 123456, None),  size=32768),
                EventWrapper(event=Event('key3', 'user', 'purchase', 10, 123456, None),  size=32768),
                EventWrapper(event=Event('key4', 'user', 'purchase', 10, 123456, None),  size=32768),
            ]
            assert(pluggable_events_storage.put(events))
            assert(pluggable_events_storage._events_queue_key in self.mock_adapter._keys)
            assert(self.mock_adapter._keys[prefix + "SPLITIO.events"] == pluggable_events_storage._wrap_events(events))
            assert(self.mock_adapter._expire[prefix + "SPLITIO.events"] == PluggableEventsStorage._EVENTS_KEY_DEFAULT_TTL)

            events2 = [
                EventWrapper(event=Event('key5', 'user', 'purchase', 10, 123456, None),  size=32768),
                EventWrapper(event=Event('key6', 'user', 'purchase', 10, 123456, None),  size=32768),
            ]
            assert(pluggable_events_storage.put(events2))
            assert(self.mock_adapter._keys[prefix + "SPLITIO.events"] == pluggable_events_storage._wrap_events(events + events2))

    def test_wrap_events(self):
        for sprefix in [None, 'myprefix']:
            pluggable_events_storage = PluggableEventsStorage(self.mock_adapter, self.metadata, prefix=sprefix)
            events = [
                EventWrapper(event=Event('key1', 'user', 'purchase', 10, 123456, None),  size=32768),
                EventWrapper(event=Event('key2', 'user', 'purchase', 10, 123456, None),  size=32768),
            ]
            assert(pluggable_events_storage._wrap_events(events) == [
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
        for sprefix in [None, 'myprefix']:
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
            pluggable_events_storage = PluggableEventsStorage(self.mock_adapter, self.metadata, prefix=sprefix)
            self.expired_called = False
            self.key = ""
            self.ttl = 0
            def mock_expire(impressions_event_key, ttl):
                self.key = impressions_event_key
                self.ttl = ttl
                self.expired_called = True

            self.mock_adapter.expire = mock_expire

            # should not call if total_keys are higher
            pluggable_events_storage.expire_key(200, 10)
            assert(not self.expired_called)

            pluggable_events_storage.expire_key(200, 200)
            assert(self.expired_called)
            assert(self.key == prefix + "SPLITIO.events")
            assert(self.ttl == pluggable_events_storage._EVENTS_KEY_DEFAULT_TTL)


class PluggableEventsStorageAsyncTests(object):
    """Pluggable events storage test cases."""

    def setup_method(self):
        """Prepare storages with test data."""
        self.mock_adapter = StorageMockAdapterAsync()
        self.metadata = SdkMetadata('python-1.1.1', 'hostname', 'ip')

    def test_init(self):
        for sprefix in [None, 'myprefix']:
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
            pluggable_events_storage = PluggableEventsStorageAsync(self.mock_adapter, self.metadata, prefix=sprefix)
            assert(pluggable_events_storage._events_queue_key == prefix + "SPLITIO.events")
            assert(pluggable_events_storage._sdk_metadata == {
                                    's': self.metadata.sdk_version,
                                    'n': self.metadata.instance_name,
                                    'i': self.metadata.instance_ip,
                                })

    @pytest.mark.asyncio
    async def test_put(self):
        for sprefix in [None, 'myprefix']:
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
            pluggable_events_storage = PluggableEventsStorageAsync(self.mock_adapter, self.metadata, prefix=sprefix)
            events = [
                EventWrapper(event=Event('key1', 'user', 'purchase', 10, 123456, None),  size=32768),
                EventWrapper(event=Event('key2', 'user', 'purchase', 10, 123456, None),  size=32768),
                EventWrapper(event=Event('key3', 'user', 'purchase', 10, 123456, None),  size=32768),
                EventWrapper(event=Event('key4', 'user', 'purchase', 10, 123456, None),  size=32768),
            ]
            assert(await pluggable_events_storage.put(events))
            assert(pluggable_events_storage._events_queue_key in self.mock_adapter._keys)
            assert(self.mock_adapter._keys[prefix + "SPLITIO.events"] == pluggable_events_storage._wrap_events(events))
            assert(self.mock_adapter._expire[prefix + "SPLITIO.events"] == PluggableEventsStorageAsync._EVENTS_KEY_DEFAULT_TTL)

            events2 = [
                EventWrapper(event=Event('key5', 'user', 'purchase', 10, 123456, None),  size=32768),
                EventWrapper(event=Event('key6', 'user', 'purchase', 10, 123456, None),  size=32768),
            ]
            assert(await pluggable_events_storage.put(events2))
            assert(self.mock_adapter._keys[prefix + "SPLITIO.events"] == pluggable_events_storage._wrap_events(events + events2))

    def test_wrap_events(self):
        for sprefix in [None, 'myprefix']:
            pluggable_events_storage = PluggableEventsStorageAsync(self.mock_adapter, self.metadata, prefix=sprefix)
            events = [
                EventWrapper(event=Event('key1', 'user', 'purchase', 10, 123456, None),  size=32768),
                EventWrapper(event=Event('key2', 'user', 'purchase', 10, 123456, None),  size=32768),
            ]
            assert(pluggable_events_storage._wrap_events(events) == [
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

    @pytest.mark.asyncio
    async def test_expire_key(self):
        for sprefix in [None, 'myprefix']:
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
            pluggable_events_storage = PluggableEventsStorageAsync(self.mock_adapter, self.metadata, prefix=sprefix)
            self.expired_called = False
            self.key = ""
            self.ttl = 0
            async def mock_expire(impressions_event_key, ttl):
                self.key = impressions_event_key
                self.ttl = ttl
                self.expired_called = True

            self.mock_adapter.expire = mock_expire

            # should not call if total_keys are higher
            await pluggable_events_storage.expire_key(200, 10)
            assert(not self.expired_called)

            await pluggable_events_storage.expire_key(200, 200)
            assert(self.expired_called)
            assert(self.key == prefix + "SPLITIO.events")
            assert(self.ttl == pluggable_events_storage._EVENTS_KEY_DEFAULT_TTL)


class PluggableTelemetryStorageTests(object):
    """Pluggable telemetry storage test cases."""

    def setup_method(self):
        """Prepare storages with test data."""
        self.mock_adapter = StorageMockAdapter()
        self.sdk_metadata = SdkMetadata('python-1.1.1', 'hostname', 'ip')

    def test_init(self):
        for sprefix in [None, 'myprefix']:
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
            pluggable_telemetry_storage = PluggableTelemetryStorage(self.mock_adapter, self.sdk_metadata, prefix=sprefix)
            assert(pluggable_telemetry_storage._telemetry_config_key == prefix + 'SPLITIO.telemetry.init')
            assert(pluggable_telemetry_storage._telemetry_latencies_key == prefix + 'SPLITIO.telemetry.latencies')
            assert(pluggable_telemetry_storage._telemetry_exceptions_key == prefix + 'SPLITIO.telemetry.exceptions')
            assert(pluggable_telemetry_storage._sdk_metadata == self.sdk_metadata.sdk_version + '/' + self.sdk_metadata.instance_name + '/' + self.sdk_metadata.instance_ip)
            assert(pluggable_telemetry_storage._config_tags == [])

    def test_reset_config_tags(self):
        for sprefix in [None, 'myprefix']:
            pluggable_telemetry_storage = PluggableTelemetryStorage(self.mock_adapter, self.sdk_metadata, prefix=sprefix)
            pluggable_telemetry_storage._config_tags = ['a']
            pluggable_telemetry_storage._reset_config_tags()
            assert(pluggable_telemetry_storage._config_tags == [])

    def test_add_config_tag(self):
        for sprefix in [None, 'myprefix']:
            pluggable_telemetry_storage = PluggableTelemetryStorage(self.mock_adapter, self.sdk_metadata, prefix=sprefix)
            pluggable_telemetry_storage.add_config_tag('q')
            assert(pluggable_telemetry_storage._config_tags == ['q'])

            pluggable_telemetry_storage._config_tags = []
            for i in range(0, 20):
                pluggable_telemetry_storage.add_config_tag('q' + str(i))
            assert(len(pluggable_telemetry_storage._config_tags) == MAX_TAGS)
            assert(pluggable_telemetry_storage._config_tags == ['q' + str(i)  for i in range(0, MAX_TAGS)])

    def test_record_config(self):
        for sprefix in [None, 'myprefix']:
            pluggable_telemetry_storage = PluggableTelemetryStorage(self.mock_adapter, self.sdk_metadata, prefix=sprefix)
            self.config = {}
            self.extra_config = {}
            def record_config_mock(config, extra_config, af, inf):
                self.config = config
                self.extra_config = extra_config

            pluggable_telemetry_storage._tel_config.record_config = record_config_mock
            pluggable_telemetry_storage.record_config({'item': 'value'}, {'item2': 'value2'}, 0, 0)
            assert(self.config == {'item': 'value'})
            assert(self.extra_config == {'item2': 'value2'})

    def test_pop_config_tags(self):
        for sprefix in [None, 'myprefix']:
            pluggable_telemetry_storage = PluggableTelemetryStorage(self.mock_adapter, self.sdk_metadata, prefix=sprefix)
            pluggable_telemetry_storage._config_tags = ['a']
            pluggable_telemetry_storage.pop_config_tags()
            assert(pluggable_telemetry_storage._config_tags == [])

    def test_record_active_and_redundant_factories(self):
        for sprefix in [None, 'myprefix']:
            pluggable_telemetry_storage = PluggableTelemetryStorage(self.mock_adapter, self.sdk_metadata, prefix=sprefix)
            self.active_factory_count = 0
            self.redundant_factory_count = 0
            def record_active_and_redundant_factories_mock(active_factory_count, redundant_factory_count):
                self.active_factory_count = active_factory_count
                self.redundant_factory_count = redundant_factory_count

            pluggable_telemetry_storage._tel_config.record_active_and_redundant_factories = record_active_and_redundant_factories_mock
            pluggable_telemetry_storage.record_active_and_redundant_factories(2, 1)
            assert(self.active_factory_count == 2)
            assert(self.redundant_factory_count == 1)

    def test_record_latency(self):
        for sprefix in [None, 'myprefix']:
            pluggable_telemetry_storage = PluggableTelemetryStorage(self.mock_adapter, self.sdk_metadata, prefix=sprefix)
            def expire_keys_mock(*args, **kwargs):
                assert(args[0] == pluggable_telemetry_storage._telemetry_latencies_key + '::python-1.1.1/hostname/ip/treatment/0')
                assert(args[1] == pluggable_telemetry_storage._TELEMETRY_KEY_DEFAULT_TTL)
                assert(args[2] == 1)
                assert(args[3] == 1)
            pluggable_telemetry_storage.expire_keys = expire_keys_mock
            # should increment bucket 0
            pluggable_telemetry_storage.record_latency(MethodExceptionsAndLatencies.TREATMENT, 0)
            assert(self.mock_adapter._keys[pluggable_telemetry_storage._telemetry_latencies_key + '::python-1.1.1/hostname/ip/treatment/0'] == 1)

            def expire_keys_mock2(*args, **kwargs):
                assert(args[0] == pluggable_telemetry_storage._telemetry_latencies_key + '::python-1.1.1/hostname/ip/treatment/3')
                assert(args[1] == pluggable_telemetry_storage._TELEMETRY_KEY_DEFAULT_TTL)
                assert(args[2] == 1)
                assert(args[3] == 1)
            pluggable_telemetry_storage.expire_keys = expire_keys_mock2
            # should increment bucket 3
            pluggable_telemetry_storage.record_latency(MethodExceptionsAndLatencies.TREATMENT, 3)

            def expire_keys_mock3(*args, **kwargs):
                assert(args[0] == pluggable_telemetry_storage._telemetry_latencies_key + '::python-1.1.1/hostname/ip/treatment/3')
                assert(args[1] == pluggable_telemetry_storage._TELEMETRY_KEY_DEFAULT_TTL)
                assert(args[2] == 1)
                assert(args[3] == 2)
            pluggable_telemetry_storage.expire_keys = expire_keys_mock3
            # should increment bucket 3
            pluggable_telemetry_storage.record_latency(MethodExceptionsAndLatencies.TREATMENT, 3)
            assert(self.mock_adapter._keys[pluggable_telemetry_storage._telemetry_latencies_key + '::python-1.1.1/hostname/ip/treatment/3'] == 2)

    def test_record_exception(self):
        for sprefix in [None, 'myprefix']:
            pluggable_telemetry_storage = PluggableTelemetryStorage(self.mock_adapter, self.sdk_metadata, prefix=sprefix)
            def expire_keys_mock(*args, **kwargs):
                assert(args[0] == pluggable_telemetry_storage._telemetry_exceptions_key + '::python-1.1.1/hostname/ip/treatment')
                assert(args[1] == pluggable_telemetry_storage._TELEMETRY_KEY_DEFAULT_TTL)
                assert(args[2] == 1)
                assert(args[3] == 1)

            pluggable_telemetry_storage.expire_keys = expire_keys_mock
            pluggable_telemetry_storage.record_exception(MethodExceptionsAndLatencies.TREATMENT)
            assert(self.mock_adapter._keys[pluggable_telemetry_storage._telemetry_exceptions_key + '::python-1.1.1/hostname/ip/treatment'] == 1)

    def test_push_config_stats(self):
        for sprefix in [None, 'myprefix']:
            pluggable_telemetry_storage = PluggableTelemetryStorage(self.mock_adapter, self.sdk_metadata, prefix=sprefix)
            pluggable_telemetry_storage.record_config(
                        {'operationMode': 'standalone',
                    'streamingEnabled': True,
                    'impressionsQueueSize': 100,
                    'eventsQueueSize': 200,
                    'impressionsMode': 'DEBUG',''
                    'impressionListener': None,
                    'featuresRefreshRate': 30,
                    'segmentsRefreshRate': 30,
                    'impressionsRefreshRate': 60,
                    'eventsPushRate': 60,
                    'metricsRefreshRate': 10,
                    'storageType': None
                    }, {}, 0, 0
            )
            pluggable_telemetry_storage.record_active_and_redundant_factories(2, 1)
            pluggable_telemetry_storage.push_config_stats()
            assert(self.mock_adapter._keys[pluggable_telemetry_storage._telemetry_config_key + "::" + pluggable_telemetry_storage._sdk_metadata] == '{"aF": 2, "rF": 1, "sT": "memory", "oM": 0, "t": []}')


class PluggableTelemetryStorageAsyncTests(object):
    """Pluggable telemetry storage test cases."""

    def setup_method(self):
        """Prepare storages with test data."""
        self.mock_adapter = StorageMockAdapterAsync()
        self.sdk_metadata = SdkMetadata('python-1.1.1', 'hostname', 'ip')

    @pytest.mark.asyncio
    async def test_init(self):
        for sprefix in [None, 'myprefix']:
            if sprefix == 'myprefix':
                prefix = 'myprefix.'
            else:
                prefix = ''
            pluggable_telemetry_storage = await PluggableTelemetryStorageAsync.create(self.mock_adapter, self.sdk_metadata, prefix=sprefix)
            assert(pluggable_telemetry_storage._telemetry_config_key == prefix + 'SPLITIO.telemetry.init')
            assert(pluggable_telemetry_storage._telemetry_latencies_key == prefix + 'SPLITIO.telemetry.latencies')
            assert(pluggable_telemetry_storage._telemetry_exceptions_key == prefix + 'SPLITIO.telemetry.exceptions')
            assert(pluggable_telemetry_storage._sdk_metadata == self.sdk_metadata.sdk_version + '/' + self.sdk_metadata.instance_name + '/' + self.sdk_metadata.instance_ip)
            assert(pluggable_telemetry_storage._config_tags == [])

    @pytest.mark.asyncio
    async def test_reset_config_tags(self):
        for sprefix in [None, 'myprefix']:
            pluggable_telemetry_storage = await PluggableTelemetryStorageAsync.create(self.mock_adapter, self.sdk_metadata, prefix=sprefix)
            pluggable_telemetry_storage._config_tags = ['a']
            await pluggable_telemetry_storage._reset_config_tags()
            assert(pluggable_telemetry_storage._config_tags == [])

    @pytest.mark.asyncio
    async def test_add_config_tag(self):
        for sprefix in [None, 'myprefix']:
            pluggable_telemetry_storage = await PluggableTelemetryStorageAsync.create(self.mock_adapter, self.sdk_metadata, prefix=sprefix)
            await pluggable_telemetry_storage.add_config_tag('q')
            assert(pluggable_telemetry_storage._config_tags == ['q'])

            pluggable_telemetry_storage._config_tags = []
            for i in range(0, 20):
                await pluggable_telemetry_storage.add_config_tag('q' + str(i))
            assert(len(pluggable_telemetry_storage._config_tags) == MAX_TAGS)
            assert(pluggable_telemetry_storage._config_tags == ['q' + str(i)  for i in range(0, MAX_TAGS)])

    @pytest.mark.asyncio
    async def test_record_config(self):
        for sprefix in [None, 'myprefix']:
            pluggable_telemetry_storage = await PluggableTelemetryStorageAsync.create(self.mock_adapter, self.sdk_metadata, prefix=sprefix)
            self.config = {}
            self.extra_config = {}
            async def record_config_mock(config, extra_config, tf, ifs):
                self.config = config
                self.extra_config = extra_config

            pluggable_telemetry_storage._tel_config.record_config = record_config_mock
            await pluggable_telemetry_storage.record_config({'item': 'value'}, {'item2': 'value2'}, 0, 0)
            assert(self.config == {'item': 'value'})
            assert(self.extra_config == {'item2': 'value2'})

    @pytest.mark.asyncio
    async def test_pop_config_tags(self):
        for sprefix in [None, 'myprefix']:
            pluggable_telemetry_storage = await PluggableTelemetryStorageAsync.create(self.mock_adapter, self.sdk_metadata, prefix=sprefix)
            pluggable_telemetry_storage._config_tags = ['a']
            await pluggable_telemetry_storage.pop_config_tags()
            assert(pluggable_telemetry_storage._config_tags == [])

    @pytest.mark.asyncio
    async def test_record_active_and_redundant_factories(self):
        for sprefix in [None, 'myprefix']:
            pluggable_telemetry_storage = await PluggableTelemetryStorageAsync.create(self.mock_adapter, self.sdk_metadata, prefix=sprefix)
            self.active_factory_count = 0
            self.redundant_factory_count = 0
            async def record_active_and_redundant_factories_mock(active_factory_count, redundant_factory_count):
                self.active_factory_count = active_factory_count
                self.redundant_factory_count = redundant_factory_count

            pluggable_telemetry_storage._tel_config.record_active_and_redundant_factories = record_active_and_redundant_factories_mock
            await pluggable_telemetry_storage.record_active_and_redundant_factories(2, 1)
            assert(self.active_factory_count == 2)
            assert(self.redundant_factory_count == 1)

    @pytest.mark.asyncio
    async def test_record_latency(self):
        for sprefix in [None, 'myprefix']:
            pluggable_telemetry_storage = await PluggableTelemetryStorageAsync.create(self.mock_adapter, self.sdk_metadata, prefix=sprefix)
            async def expire_keys_mock(*args, **kwargs):
                assert(args[0] == pluggable_telemetry_storage._telemetry_latencies_key + '::python-1.1.1/hostname/ip/treatment/0')
                assert(args[1] == pluggable_telemetry_storage._TELEMETRY_KEY_DEFAULT_TTL)
                assert(args[2] == 1)
                assert(args[3] == 1)
            pluggable_telemetry_storage.expire_keys = expire_keys_mock
            # should increment bucket 0
            await pluggable_telemetry_storage.record_latency(MethodExceptionsAndLatencies.TREATMENT, 0)
            assert(self.mock_adapter._keys[pluggable_telemetry_storage._telemetry_latencies_key + '::python-1.1.1/hostname/ip/treatment/0'] == 1)

            async def expire_keys_mock2(*args, **kwargs):
                assert(args[0] == pluggable_telemetry_storage._telemetry_latencies_key + '::python-1.1.1/hostname/ip/treatment/3')
                assert(args[1] == pluggable_telemetry_storage._TELEMETRY_KEY_DEFAULT_TTL)
                assert(args[2] == 1)
                assert(args[3] == 1)
            pluggable_telemetry_storage.expire_keys = expire_keys_mock2
            # should increment bucket 3
            await pluggable_telemetry_storage.record_latency(MethodExceptionsAndLatencies.TREATMENT, 3)

            async def expire_keys_mock3(*args, **kwargs):
                assert(args[0] == pluggable_telemetry_storage._telemetry_latencies_key + '::python-1.1.1/hostname/ip/treatment/3')
                assert(args[1] == pluggable_telemetry_storage._TELEMETRY_KEY_DEFAULT_TTL)
                assert(args[2] == 1)
                assert(args[3] == 2)
            pluggable_telemetry_storage.expire_keys = expire_keys_mock3
            # should increment bucket 3
            await pluggable_telemetry_storage.record_latency(MethodExceptionsAndLatencies.TREATMENT, 3)
            assert(self.mock_adapter._keys[pluggable_telemetry_storage._telemetry_latencies_key + '::python-1.1.1/hostname/ip/treatment/3'] == 2)

    @pytest.mark.asyncio
    async def test_record_exception(self):
        for sprefix in [None, 'myprefix']:
            pluggable_telemetry_storage = await PluggableTelemetryStorageAsync.create(self.mock_adapter, self.sdk_metadata, prefix=sprefix)
            async def expire_keys_mock(*args, **kwargs):
                assert(args[0] == pluggable_telemetry_storage._telemetry_exceptions_key + '::python-1.1.1/hostname/ip/treatment')
                assert(args[1] == pluggable_telemetry_storage._TELEMETRY_KEY_DEFAULT_TTL)
                assert(args[2] == 1)
                assert(args[3] == 1)

            pluggable_telemetry_storage.expire_keys = expire_keys_mock
            await pluggable_telemetry_storage.record_exception(MethodExceptionsAndLatencies.TREATMENT)
            assert(self.mock_adapter._keys[pluggable_telemetry_storage._telemetry_exceptions_key + '::python-1.1.1/hostname/ip/treatment'] == 1)

    @pytest.mark.asyncio
    async def test_push_config_stats(self):
        for sprefix in [None, 'myprefix']:
            pluggable_telemetry_storage = await PluggableTelemetryStorageAsync.create(self.mock_adapter, self.sdk_metadata, prefix=sprefix)
            await pluggable_telemetry_storage.record_config(
                        {'operationMode': 'standalone',
                    'streamingEnabled': True,
                    'impressionsQueueSize': 100,
                    'eventsQueueSize': 200,
                    'impressionsMode': 'DEBUG',''
                    'impressionListener': None,
                    'featuresRefreshRate': 30,
                    'segmentsRefreshRate': 30,
                    'impressionsRefreshRate': 60,
                    'eventsPushRate': 60,
                    'metricsRefreshRate': 10,
                    'storageType': None
                    }, {}, 0, 0
            )
            await pluggable_telemetry_storage.record_active_and_redundant_factories(2, 1)
            await pluggable_telemetry_storage.push_config_stats()
            assert(self.mock_adapter._keys[pluggable_telemetry_storage._telemetry_config_key + "::" + pluggable_telemetry_storage._sdk_metadata] == '{"aF": 2, "rF": 1, "sT": "memory", "oM": 0, "t": []}')
