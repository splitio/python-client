"""Redis storage end to end tests."""
#pylint: disable=no-self-use,protected-access,line-too-long,too-few-public-methods

import json
import os

from splitio.client.util import get_metadata
from splitio.models import splits, impressions, events
from splitio.storage.redis import RedisSplitStorage, RedisSegmentStorage, RedisImpressionsStorage, \
    RedisEventsStorage
from splitio.storage.adapters.redis import _build_default_client
from splitio.client.config import DEFAULT_CONFIG


class SplitStorageTests(object):
    """Redis Split storage e2e tests."""

    def test_put_fetch(self):
        """Test storing and retrieving splits in redis."""
        adapter = _build_default_client({})
        try:
            storage = RedisSplitStorage(adapter)
            with open(os.path.join(os.path.dirname(__file__), 'files', 'split_changes.json'), 'r') as flo:
                split_changes = json.load(flo)

            split_objects = [splits.from_raw(raw) for raw in split_changes['splits']]
            for split_object in split_objects:
                raw = split_object.to_json()
                adapter.set(RedisSplitStorage._SPLIT_KEY.format(split_name=split_object.name), json.dumps(raw))
                adapter.incr(RedisSplitStorage._TRAFFIC_TYPE_KEY.format(traffic_type_name=split_object.traffic_type_name))

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

            adapter.set(RedisSplitStorage._SPLIT_TILL_KEY, split_changes['till'])
            assert storage.get_change_number() == split_changes['till']

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

            storage = RedisSplitStorage(adapter)
            assert storage.is_valid_traffic_type('user') is False
            assert storage.is_valid_traffic_type('account') is False

    def test_get_all(self):
        """Test get all names & splits."""
        adapter = _build_default_client({})
        try:
            storage = RedisSplitStorage(adapter)
            with open(os.path.join(os.path.dirname(__file__), 'files', 'split_changes.json'), 'r') as flo:
                split_changes = json.load(flo)

            split_objects = [splits.from_raw(raw) for raw in split_changes['splits']]
            for split_object in split_objects:
                raw = split_object.to_json()
                adapter.set(RedisSplitStorage._SPLIT_KEY.format(split_name=split_object.name), json.dumps(raw))

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
            adapter.delete(
                'SPLITIO.split.sample_feature',
                'SPLITIO.splits.till',
                'SPLITIO.split.all_feature',
                'SPLITIO.split.killed_feature',
                'SPLITIO.split.Risk_Max_Deductible',
                'SPLITIO.split.whitelist_feature',
                'SPLITIO.split.regex_test',
                'SPLITIO.split.boolean_test',
                'SPLITIO.split.dependency_test'
            )

class SegmentStorageTests(object):
    """Redis Segment storage e2e tests."""

    def test_put_fetch_contains(self):
        """Test storing and retrieving splits in redis."""
        adapter = _build_default_client({})
        try:
            storage = RedisSegmentStorage(adapter)
            adapter.sadd(storage._get_key('some_segment'), 'key1', 'key2', 'key3', 'key4')
            adapter.set(storage._get_till_key('some_segment'), 123)
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
            adapter.delete('SPLITIO.segment.some_segment', 'SPLITIO.segment.some_segment.till')


class ImpressionsStorageTests(object):
    """Redis Impressions storage e2e tests."""

    def _put_impressions(self, adapter, metadata):
        storage = RedisImpressionsStorage(adapter, metadata)
        storage.put([
            impressions.Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654),
            impressions.Impression('key2', 'feature1', 'on', 'l1', 123456, 'b1', 321654),
            impressions.Impression('key3', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ])


    def test_put_fetch_contains(self):
        """Test storing and retrieving splits in redis."""
        adapter = _build_default_client({})
        try:
            self._put_impressions(adapter, get_metadata({}))

            imps = adapter.lrange('SPLITIO.impressions', 0, 2)
            assert len(imps) == 3
            for rawImpression in imps:
                impression = json.loads(rawImpression)
                assert impression['m']['i'] != 'NA'
                assert impression['m']['n'] != 'NA'
        finally:
            adapter.delete('SPLITIO.impressions')

    def test_put_fetch_contains_ip_address_disabled(self):
        """Test storing and retrieving splits in redis."""
        adapter = _build_default_client({})
        try:
            cfg = DEFAULT_CONFIG.copy()
            cfg.update({'IPAddressesEnabled': False})
            self._put_impressions(adapter, get_metadata(cfg))

            imps = adapter.lrange('SPLITIO.impressions', 0, 2)
            assert len(imps) == 3
            for rawImpression in imps:
                impression = json.loads(rawImpression)
                assert impression['m']['i'] == 'NA'
                assert impression['m']['n'] == 'NA'
        finally:
            adapter.delete('SPLITIO.impressions')


class EventsStorageTests(object):
    """Redis Events storage e2e tests."""
    def _put_events(self, adapter, metadata):
        storage = RedisEventsStorage(adapter, metadata)
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
        """Test storing and retrieving splits in redis."""
        adapter = _build_default_client({})
        try:
            self._put_events(adapter, get_metadata({}))
            evts = adapter.lrange('SPLITIO.events', 0, 2)
            assert len(evts) == 3
            for rawEvent in evts:
                event = json.loads(rawEvent)
                assert event['m']['i'] != 'NA'
                assert event['m']['n'] != 'NA'
        finally:
            adapter.delete('SPLITIO.events')

    def test_put_fetch_contains_ip_address_disabled(self):
        """Test storing and retrieving splits in redis."""
        adapter = _build_default_client({})
        try:
            cfg = DEFAULT_CONFIG.copy()
            cfg.update({'IPAddressesEnabled': False})
            self._put_events(adapter, get_metadata(cfg))

            evts = adapter.lrange('SPLITIO.events', 0, 2)
            assert len(evts) == 3
            for rawEvent in evts:
                event = json.loads(rawEvent)
                assert event['m']['i'] == 'NA'
                assert event['m']['n'] == 'NA'
        finally:
            adapter.delete('SPLITIO.events')
