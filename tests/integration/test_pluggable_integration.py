"""Pluggable storage end to end tests."""
#pylint: disable=no-self-use,protected-access,line-too-long,too-few-public-methods
import pytest
import json
import os

from splitio.client.util import get_metadata
from splitio.models import splits, impressions, events
from splitio.storage.pluggable import PluggableEventsStorage, PluggableImpressionsStorage, PluggableSegmentStorage, \
    PluggableSplitStorage, PluggableEventsStorageAsync, PluggableImpressionsStorageAsync, PluggableSegmentStorageAsync,\
    PluggableSplitStorageAsync
from splitio.client.config import DEFAULT_CONFIG
from tests.storage.test_pluggable import StorageMockAdapter, StorageMockAdapterAsync

class PluggableSplitStorageIntegrationTests(object):
    """Pluggable Split storage e2e tests."""

    def test_put_fetch(self):
        """Test storing and retrieving splits in pluggable."""
        adapter = StorageMockAdapter()
        try:
            storage = PluggableSplitStorage(adapter)
            split_fn = os.path.join(os.path.dirname(__file__), 'files', 'split_changes.json')
            with open(split_fn, 'r') as flo:
                data = json.loads(flo.read())
                for split in data['ff']['d']:
                    adapter.set(storage._prefix.format(feature_flag_name=split['name']), split)
                    adapter.increment(storage._traffic_type_prefix.format(traffic_type_name=split['trafficTypeName']), 1)
                adapter.set(storage._feature_flag_till_prefix, data['ff']['t'])

            split_objects = [splits.from_raw(raw) for raw in data['ff']['d']]
            for split_object in split_objects:
                raw = split_object.to_json()

            original_splits = {split.name: split for split in split_objects}
            fetched_splits = {name: storage.get(name) for name in original_splits.keys()}

            assert set(original_splits.keys()) == set(fetched_splits.keys())

            for original_split in original_splits.values():
                fetched_split = fetched_splits[original_split.name]
                assert original_split.traffic_type_name == fetched_split.traffic_type_name
                assert original_split.seed == fetched_split.seed
                assert original_split.algo == fetched_split.algo
                assert original_split.status == fetched_split.status
                assert original_split.change_number == fetched_split.change_number
                assert original_split.killed == fetched_split.killed
                assert original_split.default_treatment == fetched_split.default_treatment
                for index, original_condition in enumerate(original_split.conditions):
                    fetched_condition = fetched_split.conditions[index]
                    assert original_condition.label == fetched_condition.label
                    assert original_condition.condition_type == fetched_condition.condition_type
                    assert len(original_condition.matchers) == len(fetched_condition.matchers)
                    assert len(original_condition.partitions) == len(fetched_condition.partitions)

            adapter.set(storage._feature_flag_till_prefix, data['ff']['t'])
            assert storage.get_change_number() == data['ff']['t']

            assert storage.is_valid_traffic_type('user') is True
            assert storage.is_valid_traffic_type('account') is True
            assert storage.is_valid_traffic_type('anything-else') is False

        finally:
            to_delete = [
                "SPLITIO.split.sample_feature",
                "SPLITIO.splits.till",
                "SPLITIO.split.all_feature",
                "SPLITIO.split.killed_feature",
                "SPLITIO.split.Risk_Max_Deductible",
                "SPLITIO.split.whitelist_feature",
                "SPLITIO.split.regex_test",
                "SPLITIO.split.boolean_test",
                "SPLITIO.split.dependency_test",
                "SPLITIO.trafficType.user",
                "SPLITIO.trafficType.account"
            ]
            for item in to_delete:
                adapter.delete(item)

            storage = PluggableSplitStorage(adapter)
            assert storage.is_valid_traffic_type('user') is False
            assert storage.is_valid_traffic_type('account') is False

    def test_get_all(self):
        """Test get all names & splits."""
        adapter = StorageMockAdapter()
        try:
            storage = PluggableSplitStorage(adapter)
            split_fn = os.path.join(os.path.dirname(__file__), 'files', 'split_changes.json')
            with open(split_fn, 'r') as flo:
                data = json.loads(flo.read())
                for split in data['ff']['d']:
                    adapter.set(storage._prefix.format(feature_flag_name=split['name']), split)
                    adapter.increment(storage._traffic_type_prefix.format(traffic_type_name=split['trafficTypeName']), 1)
                adapter.set(storage._feature_flag_till_prefix, data['ff']['t'])

            split_objects = [splits.from_raw(raw) for raw in data['ff']['d']]
            original_splits = {split.name: split for split in split_objects}
            fetched_names = storage.get_split_names()
            fetched_splits = {split.name: split for split in storage.get_all_splits()}
            assert set(fetched_names) == set(fetched_splits.keys())

            for original_split in original_splits.values():
                fetched_split = fetched_splits[original_split.name]
                assert original_split.traffic_type_name == fetched_split.traffic_type_name
                assert original_split.seed == fetched_split.seed
                assert original_split.algo == fetched_split.algo
                assert original_split.status == fetched_split.status
                assert original_split.change_number == fetched_split.change_number
                assert original_split.killed == fetched_split.killed
                assert original_split.default_treatment == fetched_split.default_treatment
                for index, original_condition in enumerate(original_split.conditions):
                    fetched_condition = fetched_split.conditions[index]
                    assert original_condition.label == fetched_condition.label
                    assert original_condition.condition_type == fetched_condition.condition_type
                    assert len(original_condition.matchers) == len(fetched_condition.matchers)
                    assert len(original_condition.partitions) == len(fetched_condition.partitions)
        finally:
            [adapter.delete(key) for key in ['SPLITIO.split.sample_feature',
                'SPLITIO.splits.till',
                'SPLITIO.split.all_feature',
                'SPLITIO.split.killed_feature',
                'SPLITIO.split.Risk_Max_Deductible',
                'SPLITIO.split.whitelist_feature',
                'SPLITIO.split.regex_test',
                'SPLITIO.split.boolean_test',
                'SPLITIO.split.dependency_test']]


class PluggableSegmentStorageIntegrationTests(object):
    """Pluggable Segment storage e2e tests."""

    def test_put_fetch_contains(self):
        """Test storing and retrieving splits in pluggable."""
        adapter = StorageMockAdapter()
        try:
            storage = PluggableSegmentStorage(adapter)
            adapter.set(storage._prefix.format(segment_name='some_segment'), {'key1', 'key2', 'key3', 'key4'})
            adapter.set(storage._segment_till_prefix.format(segment_name='some_segment'), 123)
            assert storage.segment_contains('some_segment', 'key0') is False
            assert storage.segment_contains('some_segment', 'key1') is True
            assert storage.segment_contains('some_segment', 'key2') is True
            assert storage.segment_contains('some_segment', 'key3') is True
            assert storage.segment_contains('some_segment', 'key4') is True
            assert storage.segment_contains('some_segment', 'key5') is False

            fetched = storage.get('some_segment')
            assert fetched.keys == set(['key1', 'key2', 'key3', 'key4'])
            assert fetched.change_number == 123
        finally:
            adapter.delete('SPLITIO.segment.some_segment')
            adapter.delete('SPLITIO.segment.some_segment.till')


class PluggableImpressionsStorageIntegrationTests(object):
    """Pluggable Impressions storage e2e tests."""

    def _put_impressions(self, adapter, metadata):
        storage = PluggableImpressionsStorage(adapter, metadata)
        storage.put([
            impressions.Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654, None, None),
            impressions.Impression('key2', 'feature1', 'on', 'l1', 123456, 'b1', 321654, None, None),
            impressions.Impression('key3', 'feature1', 'on', 'l1', 123456, 'b1', 321654, None, None)
        ])


    def test_put_fetch_contains(self):
        """Test storing and retrieving splits in pluggable."""
        adapter = StorageMockAdapter()
        try:
            self._put_impressions(adapter, get_metadata({}))

            imps = adapter.pop_items('SPLITIO.impressions')
            assert len(imps) == 3
            for rawImpression in imps:
                impression = json.loads(rawImpression)
                assert impression['m']['i'] != 'NA'
                assert impression['m']['n'] != 'NA'
        finally:
            adapter.delete('SPLITIO.impressions')

    def test_put_fetch_contains_ip_address_disabled(self):
        """Test storing and retrieving splits in pluggable."""
        adapter = StorageMockAdapter()
        try:
            cfg = DEFAULT_CONFIG.copy()
            cfg.update({'IPAddressesEnabled': False})
            self._put_impressions(adapter, get_metadata(cfg))

            imps = adapter.pop_items('SPLITIO.impressions')
            assert len(imps) == 3
            for rawImpression in imps:
                impression = json.loads(rawImpression)
                assert impression['m']['i'] == 'NA'
                assert impression['m']['n'] == 'NA'
        finally:
            adapter.delete('SPLITIO.impressions')


class PluggableEventsStorageIntegrationTests(object):
    """Pluggable Events storage e2e tests."""
    def _put_events(self, adapter, metadata):
        storage = PluggableEventsStorage(adapter, metadata)
        storage.put([
            events.EventWrapper(
                event=events.Event('key1', 'user', 'purchase', 3.5, 123456, None),
                size=1024,
            ),
            events.EventWrapper(
                event=events.Event('key2', 'user', 'purchase', 3.5, 123456, None),
                size=1024,
            ),
            events.EventWrapper(
                event=events.Event('key3', 'user', 'purchase', 3.5, 123456, None),
                size=1024,
            ),
        ])

    def test_put_fetch_contains(self):
        """Test storing and retrieving splits in pluggable."""
        adapter = StorageMockAdapter()
        try:
            self._put_events(adapter, get_metadata({}))
            evts = adapter.pop_items('SPLITIO.events')
            assert len(evts) == 3
            for rawEvent in evts:
                event = json.loads(rawEvent)
                assert event['m']['i'] != 'NA'
                assert event['m']['n'] != 'NA'
        finally:
            adapter.delete('SPLITIO.events')

    def test_put_fetch_contains_ip_address_disabled(self):
        """Test storing and retrieving splits in pluggable."""
        adapter = StorageMockAdapter()
        try:
            cfg = DEFAULT_CONFIG.copy()
            cfg.update({'IPAddressesEnabled': False})
            self._put_events(adapter, get_metadata(cfg))

            evts = adapter.pop_items('SPLITIO.events')
            assert len(evts) == 3
            for rawEvent in evts:
                event = json.loads(rawEvent)
                assert event['m']['i'] == 'NA'
                assert event['m']['n'] == 'NA'
        finally:
            adapter.delete('SPLITIO.events')


class PluggableSplitStorageIntegrationAsyncTests(object):
    """Pluggable Split storage e2e tests."""

    @pytest.mark.asyncio
    async def test_put_fetch(self):
        """Test storing and retrieving splits in pluggable."""
        adapter = StorageMockAdapterAsync()
        try:
            storage = PluggableSplitStorageAsync(adapter)
            split_fn = os.path.join(os.path.dirname(__file__), 'files', 'split_changes.json')
            with open(split_fn, 'r') as flo:
                data = json.loads(flo.read())
                for split in data['ff']['d']:
                    await adapter.set(storage._prefix.format(feature_flag_name=split['name']), split)
                    await adapter.increment(storage._traffic_type_prefix.format(traffic_type_name=split['trafficTypeName']), 1)
                await adapter.set(storage._feature_flag_till_prefix, data['ff']['t'])

            split_objects = [splits.from_raw(raw) for raw in data['ff']['d']]
            for split_object in split_objects:
                raw = split_object.to_json()

            original_splits = {split.name: split for split in split_objects}
            fetched_splits = {name: await storage.get(name) for name in original_splits.keys()}

            assert set(original_splits.keys()) == set(fetched_splits.keys())

            for original_split in original_splits.values():
                fetched_split = fetched_splits[original_split.name]
                assert original_split.traffic_type_name == fetched_split.traffic_type_name
                assert original_split.seed == fetched_split.seed
                assert original_split.algo == fetched_split.algo
                assert original_split.status == fetched_split.status
                assert original_split.change_number == fetched_split.change_number
                assert original_split.killed == fetched_split.killed
                assert original_split.default_treatment == fetched_split.default_treatment
                for index, original_condition in enumerate(original_split.conditions):
                    fetched_condition = fetched_split.conditions[index]
                    assert original_condition.label == fetched_condition.label
                    assert original_condition.condition_type == fetched_condition.condition_type
                    assert len(original_condition.matchers) == len(fetched_condition.matchers)
                    assert len(original_condition.partitions) == len(fetched_condition.partitions)

            await adapter.set(storage._feature_flag_till_prefix, data['ff']['t'])
            assert await storage.get_change_number() == data['ff']['t']

            assert await storage.is_valid_traffic_type('user') is True
            assert await storage.is_valid_traffic_type('account') is True
            assert await storage.is_valid_traffic_type('anything-else') is False

        finally:
            to_delete = [
                "SPLITIO.split.sample_feature",
                "SPLITIO.splits.till",
                "SPLITIO.split.all_feature",
                "SPLITIO.split.killed_feature",
                "SPLITIO.split.Risk_Max_Deductible",
                "SPLITIO.split.whitelist_feature",
                "SPLITIO.split.regex_test",
                "SPLITIO.split.boolean_test",
                "SPLITIO.split.dependency_test",
                "SPLITIO.trafficType.user",
                "SPLITIO.trafficType.account"
            ]
            for item in to_delete:
                await adapter.delete(item)

            storage = PluggableSplitStorageAsync(adapter)
            assert await storage.is_valid_traffic_type('user') is False
            assert await storage.is_valid_traffic_type('account') is False

    @pytest.mark.asyncio
    async def test_get_all(self):
        """Test get all names & splits."""
        adapter = StorageMockAdapterAsync()
        try:
            storage = PluggableSplitStorageAsync(adapter)
            split_fn = os.path.join(os.path.dirname(__file__), 'files', 'split_changes.json')
            with open(split_fn, 'r') as flo:
                data = json.loads(flo.read())
                for split in data['ff']['d']:
                    await adapter.set(storage._prefix.format(feature_flag_name=split['name']), split)
                    await adapter.increment(storage._traffic_type_prefix.format(traffic_type_name=split['trafficTypeName']), 1)
                await adapter.set(storage._feature_flag_till_prefix, data['ff']['t'])

            split_objects = [splits.from_raw(raw) for raw in data['ff']['d']]
            original_splits = {split.name: split for split in split_objects}
            fetched_names = await storage.get_split_names()
            fetched_splits = {split.name: split for split in await storage.get_all_splits()}
            assert set(fetched_names) == set(fetched_splits.keys())

            for original_split in original_splits.values():
                fetched_split = fetched_splits[original_split.name]
                assert original_split.traffic_type_name == fetched_split.traffic_type_name
                assert original_split.seed == fetched_split.seed
                assert original_split.algo == fetched_split.algo
                assert original_split.status == fetched_split.status
                assert original_split.change_number == fetched_split.change_number
                assert original_split.killed == fetched_split.killed
                assert original_split.default_treatment == fetched_split.default_treatment
                for index, original_condition in enumerate(original_split.conditions):
                    fetched_condition = fetched_split.conditions[index]
                    assert original_condition.label == fetched_condition.label
                    assert original_condition.condition_type == fetched_condition.condition_type
                    assert len(original_condition.matchers) == len(fetched_condition.matchers)
                    assert len(original_condition.partitions) == len(fetched_condition.partitions)
        finally:
            [await adapter.delete(key) for key in ['SPLITIO.split.sample_feature',
                'SPLITIO.splits.till',
                'SPLITIO.split.all_feature',
                'SPLITIO.split.killed_feature',
                'SPLITIO.split.Risk_Max_Deductible',
                'SPLITIO.split.whitelist_feature',
                'SPLITIO.split.regex_test',
                'SPLITIO.split.boolean_test',
                'SPLITIO.split.dependency_test']]


class PluggableSegmentStorageIntegrationAsyncTests(object):
    """Pluggable Segment storage e2e tests."""

    @pytest.mark.asyncio
    async def test_put_fetch_contains(self):
        """Test storing and retrieving splits in pluggable."""
        adapter = StorageMockAdapterAsync()
        try:
            storage = PluggableSegmentStorageAsync(adapter)
            await adapter.set(storage._prefix.format(segment_name='some_segment'), {'key1', 'key2', 'key3', 'key4'})
            await adapter.set(storage._segment_till_prefix.format(segment_name='some_segment'), 123)
            assert await storage.segment_contains('some_segment', 'key0') is False
            assert await storage.segment_contains('some_segment', 'key1') is True
            assert await storage.segment_contains('some_segment', 'key2') is True
            assert await storage.segment_contains('some_segment', 'key3') is True
            assert await storage.segment_contains('some_segment', 'key4') is True
            assert await storage.segment_contains('some_segment', 'key5') is False

            fetched = await storage.get('some_segment')
            assert fetched.keys == set(['key1', 'key2', 'key3', 'key4'])
            assert fetched.change_number == 123
        finally:
            await adapter.delete('SPLITIO.segment.some_segment')
            await adapter.delete('SPLITIO.segment.some_segment.till')

class PluggableEventsStorageIntegrationAsyncTests(object):
    """Pluggable Events storage e2e tests."""
    async def _put_events(self, adapter, metadata):
        storage = PluggableEventsStorageAsync(adapter, metadata)
        await storage.put([
            events.EventWrapper(
                event=events.Event('key1', 'user', 'purchase', 3.5, 123456, None),
                size=1024,
            ),
            events.EventWrapper(
                event=events.Event('key2', 'user', 'purchase', 3.5, 123456, None),
                size=1024,
            ),
            events.EventWrapper(
                event=events.Event('key3', 'user', 'purchase', 3.5, 123456, None),
                size=1024,
            ),
        ])

    @pytest.mark.asyncio
    async def test_put_fetch_contains(self):
        """Test storing and retrieving splits in pluggable."""
        adapter = StorageMockAdapterAsync()
        try:
            await self._put_events(adapter, get_metadata({}))
            evts = await adapter.pop_items('SPLITIO.events')
            assert len(evts) == 3
            for rawEvent in evts:
                event = json.loads(rawEvent)
                assert event['m']['i'] != 'NA'
                assert event['m']['n'] != 'NA'
        finally:
            await adapter.delete('SPLITIO.events')

    @pytest.mark.asyncio
    async def test_put_fetch_contains_ip_address_disabled(self):
        """Test storing and retrieving splits in pluggable."""
        adapter = StorageMockAdapterAsync()
        try:
            cfg = DEFAULT_CONFIG.copy()
            cfg.update({'IPAddressesEnabled': False})
            await self._put_events(adapter, get_metadata(cfg))

            evts = await adapter.pop_items('SPLITIO.events')
            assert len(evts) == 3
            for rawEvent in evts:
                event = json.loads(rawEvent)
                assert event['m']['i'] == 'NA'
                assert event['m']['n'] == 'NA'
        finally:
            await adapter.delete('SPLITIO.events')
