"""Localhost mode test module."""
# pylint: disable=no-self-use,line-too-long,protected-access

import os
import tempfile

from splitio.client import localhost
from splitio.sync.split import LocalSplitSynchronizer
from splitio.models.splits import Split
from splitio.models.grammar.matchers import AllKeysMatcher
from splitio.storage import SplitStorage


class LocalHostStoragesTests(object):
    """Localhost storages test cases."""

    def test_dummy_impression_storage(self):
        """Test that dummy impression storage never complains."""
        imp_storage = localhost.LocalhostImpressionsStorage()
        assert imp_storage.put() is None
        assert imp_storage.put('ads') is None
        assert imp_storage.put(3) is None
        assert imp_storage.put([2]) is None
        assert imp_storage.put(object) is None
        assert imp_storage.pop_many() is None
        assert imp_storage.pop_many('ads') is None
        assert imp_storage.pop_many(3) is None
        assert imp_storage.pop_many([2]) is None
        assert imp_storage.pop_many(object) is None

    def test_dummy_event_storage(self):
        """Test that dummy event storage never complains."""
        evt_storage = localhost.LocalhostEventsStorage()
        assert evt_storage.put() is None
        assert evt_storage.put('ads') is None
        assert evt_storage.put(3) is None
        assert evt_storage.put([2]) is None
        assert evt_storage.put(object) is None
        assert evt_storage.pop_many() is None
        assert evt_storage.pop_many('ads') is None
        assert evt_storage.pop_many(3) is None
        assert evt_storage.pop_many([2]) is None
        assert evt_storage.pop_many(object) is None

    def test_dummy_telemetry_storage(self):
        """Test that dummy telemetry storage never complains."""
        telemetry_storage = localhost.LocalhostTelemetryStorage()
        assert telemetry_storage.inc_latency() is None
        assert telemetry_storage.inc_latency('ads') is None
        assert telemetry_storage.inc_latency(3) is None
        assert telemetry_storage.inc_latency([2]) is None
        assert telemetry_storage.inc_latency(object) is None
        assert telemetry_storage.pop_latencies() is None
        assert telemetry_storage.pop_latencies('ads') is None
        assert telemetry_storage.pop_latencies(3) is None
        assert telemetry_storage.pop_latencies([2]) is None
        assert telemetry_storage.pop_latencies(object) is None
        assert telemetry_storage.inc_counter() is None
        assert telemetry_storage.inc_counter('ads') is None
        assert telemetry_storage.inc_counter(3) is None
        assert telemetry_storage.inc_counter([2]) is None
        assert telemetry_storage.inc_counter(object) is None
        assert telemetry_storage.pop_counters() is None
        assert telemetry_storage.pop_counters('ads') is None
        assert telemetry_storage.pop_counters(3) is None
        assert telemetry_storage.pop_counters([2]) is None
        assert telemetry_storage.pop_counters(object) is None
        assert telemetry_storage.put_gauge() is None
        assert telemetry_storage.put_gauge('ads') is None
        assert telemetry_storage.put_gauge(3) is None
        assert telemetry_storage.put_gauge([2]) is None
        assert telemetry_storage.put_gauge(object) is None
        assert telemetry_storage.pop_gauges() is None
        assert telemetry_storage.pop_gauges('ads') is None
        assert telemetry_storage.pop_gauges(3) is None
        assert telemetry_storage.pop_gauges([2]) is None
        assert telemetry_storage.pop_gauges(object) is None


class SplitFetchingTaskTests(object):
    """Localhost split fetching task test cases."""

    def test_make_all_keys_condition(self):
        """Test all keys-based condition construction."""
        cond = LocalSplitSynchronizer._make_all_keys_condition('on')
        assert cond['conditionType'] == 'WHITELIST'
        assert len(cond['partitions']) == 1
        assert cond['partitions'][0]['treatment'] == 'on'
        assert cond['partitions'][0]['size'] == 100
        assert len(cond['matcherGroup']['matchers']) == 1
        assert cond['matcherGroup']['matchers'][0]['matcherType'] == 'ALL_KEYS'
        assert cond['matcherGroup']['matchers'][0]['negate'] is False
        assert cond['matcherGroup']['combiner'] == 'AND'

    def test_make_whitelist_condition(self):
        """Test whitelist-based condition construction."""
        cond = LocalSplitSynchronizer._make_whitelist_condition(['key1', 'key2'], 'on')
        assert cond['conditionType'] == 'WHITELIST'
        assert len(cond['partitions']) == 1
        assert cond['partitions'][0]['treatment'] == 'on'
        assert cond['partitions'][0]['size'] == 100
        assert len(cond['matcherGroup']['matchers']) == 1
        assert cond['matcherGroup']['matchers'][0]['matcherType'] == 'WHITELIST'
        assert cond['matcherGroup']['matchers'][0]['whitelistMatcherData']['whitelist'] == ['key1', 'key2']
        assert cond['matcherGroup']['matchers'][0]['negate'] is False
        assert cond['matcherGroup']['combiner'] == 'AND'

    def test_parse_legacy_file(self):
        """Test that aprsing a legacy file works."""
        filename = os.path.join(os.path.dirname(__file__), 'files', 'file1.split')
        splits = LocalSplitSynchronizer._read_splits_from_legacy_file(filename)
        assert len(splits) == 2
        for split in splits.values():
            assert isinstance(split, Split)
        assert splits['split1'].name == 'split1'
        assert splits['split2'].name == 'split2'
        assert isinstance(splits['split1'].conditions[0].matchers[0], AllKeysMatcher)
        assert isinstance(splits['split2'].conditions[0].matchers[0], AllKeysMatcher)

    def test_parse_yaml_file(self):
        """Test that parsing a yaml file works."""
        filename = os.path.join(os.path.dirname(__file__), 'files', 'file2.yaml')
        splits = LocalSplitSynchronizer._read_splits_from_yaml_file(filename)
        assert len(splits) == 4
        for split in splits.values():
            assert isinstance(split, Split)
        assert splits['my_feature'].name == 'my_feature'
        assert splits['other_feature'].name == 'other_feature'
        assert splits['other_feature_2'].name == 'other_feature_2'
        assert splits['other_feature_3'].name == 'other_feature_3'

        # test that all_keys conditions are pushed to the bottom so that they don't override
        # whitelists
        condition_types = [
            [cond.matchers[0].__class__.__name__ for cond in split.conditions]
            for split in splits.values()
        ]
        assert all(
            'WhitelistMatcher' not in c[c.index('AllKeysMatcher'):] if 'AllKeysMatcher' in c else True
            for c in condition_types
        )

    def test_update_splits(self, mocker):
        """Test update spltis."""
        parse_legacy = mocker.Mock()
        parse_legacy.return_value = {}
        parse_yaml = mocker.Mock()
        parse_yaml.return_value = {}
        storage_mock = mocker.Mock(spec=SplitStorage)
        storage_mock.get_split_names.return_value = []

        parse_legacy.reset_mock()
        parse_yaml.reset_mock()
        sync = LocalSplitSynchronizer('something', storage_mock)
        sync._read_splits_from_legacy_file = parse_legacy
        sync._read_splits_from_yaml_file = parse_yaml
        sync.synchronize_splits()
        assert parse_legacy.mock_calls == [mocker.call('something')]
        assert parse_yaml.mock_calls == []

        parse_legacy.reset_mock()
        parse_yaml.reset_mock()
        sync = LocalSplitSynchronizer('something.yaml', storage_mock)
        sync._read_splits_from_legacy_file = parse_legacy
        sync._read_splits_from_yaml_file = parse_yaml
        sync.synchronize_splits()
        assert parse_legacy.mock_calls == []
        assert parse_yaml.mock_calls == [mocker.call('something.yaml')]

        parse_legacy.reset_mock()
        parse_yaml.reset_mock()
        sync = LocalSplitSynchronizer('something.yml', storage_mock)
        sync._read_splits_from_legacy_file = parse_legacy
        sync._read_splits_from_yaml_file = parse_yaml
        sync.synchronize_splits()
        assert parse_legacy.mock_calls == []
        assert parse_yaml.mock_calls == [mocker.call('something.yml')]

        parse_legacy.reset_mock()
        parse_yaml.reset_mock()
        sync = LocalSplitSynchronizer('something.YAML', storage_mock)
        sync._read_splits_from_legacy_file = parse_legacy
        sync._read_splits_from_yaml_file = parse_yaml
        sync.synchronize_splits()
        assert parse_legacy.mock_calls == []
        assert parse_yaml.mock_calls == [mocker.call('something.YAML')]

        parse_legacy.reset_mock()
        parse_yaml.reset_mock()
        sync = LocalSplitSynchronizer('yaml', storage_mock)
        sync._read_splits_from_legacy_file = parse_legacy
        sync._read_splits_from_yaml_file = parse_yaml
        sync.synchronize_splits()
        assert parse_legacy.mock_calls == [mocker.call('yaml')]
        assert parse_yaml.mock_calls == []
