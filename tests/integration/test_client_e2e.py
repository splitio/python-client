"""Client integration tests."""
# pylint: disable=protected-access,line-too-long,no-self-use
import json
import os
import threading
import time
import pytest
import unittest.mock as mocker
from redis import StrictRedis

from splitio.optional.loaders import asyncio
from splitio.exceptions import TimeoutException
from splitio.client.factory import get_factory, SplitFactory, get_factory_async, SplitFactoryAsync
from splitio.client.util import SdkMetadata
from splitio.storage.inmemmory import InMemoryEventStorage, InMemoryImpressionStorage, \
    InMemorySegmentStorage, InMemorySplitStorage, InMemoryTelemetryStorage, InMemorySplitStorageAsync,\
    InMemoryEventStorageAsync, InMemoryImpressionStorageAsync, InMemorySegmentStorageAsync, \
    InMemoryTelemetryStorageAsync
from splitio.storage.redis import RedisEventsStorage, RedisImpressionsStorage, \
    RedisSplitStorage, RedisSegmentStorage, RedisTelemetryStorage, RedisEventsStorageAsync,\
    RedisImpressionsStorageAsync, RedisSegmentStorageAsync, RedisSplitStorageAsync, RedisTelemetryStorageAsync
from splitio.storage.pluggable import PluggableEventsStorage, PluggableImpressionsStorage, PluggableSegmentStorage, \
    PluggableTelemetryStorage, PluggableSplitStorage, PluggableEventsStorageAsync, PluggableImpressionsStorageAsync, \
    PluggableSegmentStorageAsync, PluggableSplitStorageAsync, PluggableTelemetryStorageAsync
from splitio.storage.adapters.redis import build, RedisAdapter, RedisAdapterAsync, build_async
from splitio.models import splits, segments
from splitio.engine.impressions.impressions import Manager as ImpressionsManager, ImpressionsMode
from splitio.engine.impressions import set_classes, set_classes_async
from splitio.engine.impressions.strategies import StrategyDebugMode, StrategyOptimizedMode, StrategyNoneMode
from splitio.engine.telemetry import TelemetryStorageConsumer, TelemetryStorageProducer, TelemetryStorageConsumerAsync,\
    TelemetryStorageProducerAsync
from splitio.engine.impressions.manager import Counter as ImpressionsCounter
from splitio.engine.impressions.unique_keys_tracker import UniqueKeysTracker, UniqueKeysTrackerAsync
from splitio.recorder.recorder import StandardRecorder, PipelinedRecorder, StandardRecorderAsync, PipelinedRecorderAsync
from splitio.client.config import DEFAULT_CONFIG
from splitio.sync.synchronizer import SplitTasks, SplitSynchronizers, Synchronizer, RedisSynchronizer, SynchronizerAsync,\
RedisSynchronizerAsync
from splitio.sync.manager import Manager, RedisManager, ManagerAsync, RedisManagerAsync
from splitio.sync.synchronizer import PluggableSynchronizer, PluggableSynchronizerAsync
from splitio.sync.telemetry import RedisTelemetrySubmitter, RedisTelemetrySubmitterAsync

from tests.integration import splits_json
from tests.storage.test_pluggable import StorageMockAdapter, StorageMockAdapterAsync

def _validate_last_impressions(client, *to_validate):
    """Validate the last N impressions are present disregarding the order."""
    imp_storage = client._factory._get_storage('impressions')
    if isinstance(client._factory._get_storage('splits'), RedisSplitStorage) or isinstance(client._factory._get_storage('splits'), PluggableSplitStorage):
        if isinstance(client._factory._get_storage('splits'), RedisSplitStorage):
            redis_client = imp_storage._redis
            impressions_raw = [
                json.loads(redis_client.lpop(imp_storage.IMPRESSIONS_QUEUE_KEY))
                for _ in to_validate
            ]
        else:
            pluggable_adapter = imp_storage._pluggable_adapter
            results = pluggable_adapter.pop_items(imp_storage._impressions_queue_key)
            results = [] if results == None else results
            impressions_raw = [
                json.loads(i)
                for i in results
            ]
        as_tup_set = set(
            (i['i']['f'], i['i']['k'], i['i']['t'])
            for i in impressions_raw
        )
        assert as_tup_set == set(to_validate)
        time.sleep(0.2) # delay for redis to sync
    else:
        impressions = imp_storage.pop_many(len(to_validate))
        as_tup_set = set((i.feature_name, i.matching_key, i.treatment) for i in impressions)
        assert as_tup_set == set(to_validate)

def _validate_last_events(client, *to_validate):
    """Validate the last N impressions are present disregarding the order."""
    event_storage = client._factory._get_storage('events')
    if isinstance(client._factory._get_storage('splits'), RedisSplitStorage) or isinstance(client._factory._get_storage('splits'), PluggableSplitStorage):
        if isinstance(client._factory._get_storage('splits'), RedisSplitStorage):
            redis_client = event_storage._redis
            events_raw = [
                json.loads(redis_client.lpop(event_storage._EVENTS_KEY_TEMPLATE))
                for _ in to_validate
            ]
        else:
            pluggable_adapter = event_storage._pluggable_adapter
            events_raw = [
                json.loads(i)
                for i in pluggable_adapter.pop_items(event_storage._events_queue_key)
            ]
        as_tup_set = set(
            (i['e']['key'], i['e']['trafficTypeName'], i['e']['eventTypeId'], i['e']['value'], str(i['e']['properties']))
            for i in events_raw
        )
        assert as_tup_set == set(to_validate)
    else:
        events = event_storage.pop_many(len(to_validate))
        as_tup_set = set((i.key, i.traffic_type_name, i.event_type_id, i.value, str(i.properties)) for i in events)
        assert as_tup_set == set(to_validate)

def _get_treatment(factory):
    """Test client.get_treatment()."""
    try:
        client = factory.client()
    except:
        pass

    assert client.get_treatment('user1', 'sample_feature') == 'on'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('sample_feature', 'user1', 'on'))

    assert client.get_treatment('invalidKey', 'sample_feature') == 'off'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('sample_feature', 'invalidKey', 'off'))

    assert client.get_treatment('invalidKey', 'invalid_feature') == 'control'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client)  # No impressions should be present

    # testing a killed feature. No matter what the key, must return default treatment
    assert client.get_treatment('invalidKey', 'killed_feature') == 'defTreatment'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('killed_feature', 'invalidKey', 'defTreatment'))

    # testing ALL matcher
    assert client.get_treatment('invalidKey', 'all_feature') == 'on'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('all_feature', 'invalidKey', 'on'))

    # testing WHITELIST matcher
    assert client.get_treatment('whitelisted_user', 'whitelist_feature') == 'on'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('whitelist_feature', 'whitelisted_user', 'on'))
    assert client.get_treatment('unwhitelisted_user', 'whitelist_feature') == 'off'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('whitelist_feature', 'unwhitelisted_user', 'off'))

    #  testing INVALID matcher
    assert client.get_treatment('some_user_key', 'invalid_matcher_feature') == 'control'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client)  # No impressions should be present

    #  testing Dependency matcher
    assert client.get_treatment('somekey', 'dependency_test') == 'off'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('dependency_test', 'somekey', 'off'))

    #  testing boolean matcher
    assert client.get_treatment('True', 'boolean_test') == 'on'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('boolean_test', 'True', 'on'))

    #  testing regex matcher
    assert client.get_treatment('abc4', 'regex_test') == 'on'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('regex_test', 'abc4', 'on'))

def _get_treatment_with_config(factory):
    """Test client.get_treatment_with_config()."""
    try:
        client = factory.client()
    except:
        pass
    result = client.get_treatment_with_config('user1', 'sample_feature')
    assert result == ('on', '{"size":15,"test":20}')
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('sample_feature', 'user1', 'on'))

    result = client.get_treatment_with_config('invalidKey', 'sample_feature')
    assert result == ('off', None)
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('sample_feature', 'invalidKey', 'off'))

    result = client.get_treatment_with_config('invalidKey', 'invalid_feature')
    assert result == ('control', None)
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client)

    # testing a killed feature. No matter what the key, must return default treatment
    result = client.get_treatment_with_config('invalidKey', 'killed_feature')
    assert ('defTreatment', '{"size":15,"defTreatment":true}') == result
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('killed_feature', 'invalidKey', 'defTreatment'))

    # testing ALL matcher
    result = client.get_treatment_with_config('invalidKey', 'all_feature')
    assert result == ('on', None)
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('all_feature', 'invalidKey', 'on'))

def _get_treatments(factory):
    """Test client.get_treatments()."""
    try:
        client = factory.client()
    except:
        pass
    result = client.get_treatments('user1', ['sample_feature'])
    assert len(result) == 1
    assert result['sample_feature'] == 'on'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('sample_feature', 'user1', 'on'))

    result = client.get_treatments('invalidKey', ['sample_feature'])
    assert len(result) == 1
    assert result['sample_feature'] == 'off'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('sample_feature', 'invalidKey', 'off'))

    result = client.get_treatments('invalidKey', ['invalid_feature'])
    assert len(result) == 1
    assert result['invalid_feature'] == 'control'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client)

    # testing a killed feature. No matter what the key, must return default treatment
    result = client.get_treatments('invalidKey', ['killed_feature'])
    assert len(result) == 1
    assert result['killed_feature'] == 'defTreatment'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('killed_feature', 'invalidKey', 'defTreatment'))

    # testing ALL matcher
    result = client.get_treatments('invalidKey', ['all_feature'])
    assert len(result) == 1
    assert result['all_feature'] == 'on'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('all_feature', 'invalidKey', 'on'))

def _get_treatments_with_config(factory):
    """Test client.get_treatments_with_config()."""
    try:
        client = factory.client()
    except:
        pass

    result = client.get_treatments_with_config('user1', ['sample_feature'])
    assert len(result) == 1
    assert result['sample_feature'] == ('on', '{"size":15,"test":20}')
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('sample_feature', 'user1', 'on'))

    result = client.get_treatments_with_config('invalidKey', ['sample_feature'])
    assert len(result) == 1
    assert result['sample_feature'] == ('off', None)
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('sample_feature', 'invalidKey', 'off'))

    result = client.get_treatments_with_config('invalidKey', ['invalid_feature'])
    assert len(result) == 1
    assert result['invalid_feature'] == ('control', None)
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client)

    # testing a killed feature. No matter what the key, must return default treatment
    result = client.get_treatments_with_config('invalidKey', ['killed_feature'])
    assert len(result) == 1
    assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('killed_feature', 'invalidKey', 'defTreatment'))

    # testing ALL matcher
    result = client.get_treatments_with_config('invalidKey', ['all_feature'])
    assert len(result) == 1
    assert result['all_feature'] == ('on', None)
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('all_feature', 'invalidKey', 'on'))

def _get_treatments_by_flag_set(factory):
    """Test client.get_treatments_by_flag_set()."""
    try:
        client = factory.client()
    except:
        pass
    result = client.get_treatments_by_flag_set('user1', 'set1')
    assert len(result) == 2
    assert result == {'sample_feature': 'on', 'whitelist_feature': 'off'}
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('sample_feature', 'user1', 'on'), ('whitelist_feature', 'user1', 'off'))

    result = client.get_treatments_by_flag_set('invalidKey', 'invalid_set')
    assert len(result) == 0
    assert result == {}

    # testing a killed feature. No matter what the key, must return default treatment
    result = client.get_treatments_by_flag_set('invalidKey', 'set3')
    assert len(result) == 1
    assert result['killed_feature'] == 'defTreatment'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('killed_feature', 'invalidKey', 'defTreatment'))

    # testing ALL matcher
    result = client.get_treatments_by_flag_set('invalidKey', 'set4')
    assert len(result) == 1
    assert result['all_feature'] == 'on'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('all_feature', 'invalidKey', 'on'))

def _get_treatments_by_flag_sets(factory):
    """Test client.get_treatments_by_flag_sets()."""
    try:
        client = factory.client()
    except:
        pass
    result = client.get_treatments_by_flag_sets('user1', ['set1'])
    assert len(result) == 2
    assert result == {'sample_feature': 'on', 'whitelist_feature': 'off'}
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('sample_feature', 'user1', 'on'), ('whitelist_feature', 'user1', 'off'))

    result = client.get_treatments_by_flag_sets('invalidKey', ['invalid_set'])
    assert len(result) == 0
    assert result == {}

    result = client.get_treatments_by_flag_sets('invalidKey', [])
    assert len(result) == 0
    assert result == {}

    # testing a killed feature. No matter what the key, must return default treatment
    result = client.get_treatments_by_flag_sets('invalidKey', ['set3'])
    assert len(result) == 1
    assert result['killed_feature'] == 'defTreatment'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('killed_feature', 'invalidKey', 'defTreatment'))

    # testing ALL matcher
    result = client.get_treatments_by_flag_sets('user1', ['set4'])
    assert len(result) == 1
    assert result['all_feature'] == 'on'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('all_feature', 'user1', 'on'))

def _get_treatments_with_config_by_flag_set(factory):
    """Test client.get_treatments_with_config_by_flag_set()."""
    try:
        client = factory.client()
    except:
        pass
    result = client.get_treatments_with_config_by_flag_set('user1', 'set1')
    assert len(result) == 2
    assert result == {'sample_feature': ('on', '{"size":15,"test":20}'), 'whitelist_feature': ('off', None)}
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('sample_feature', 'user1', 'on'), ('whitelist_feature', 'user1', 'off'))

    result = client.get_treatments_with_config_by_flag_set('invalidKey', 'invalid_set')
    assert len(result) == 0
    assert result == {}

    # testing a killed feature. No matter what the key, must return default treatment
    result = client.get_treatments_with_config_by_flag_set('invalidKey', 'set3')
    assert len(result) == 1
    assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('killed_feature', 'invalidKey', 'defTreatment'))

    # testing ALL matcher
    result = client.get_treatments_with_config_by_flag_set('invalidKey', 'set4')
    assert len(result) == 1
    assert result['all_feature'] == ('on', None)
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('all_feature', 'invalidKey', 'on'))

def _get_treatments_with_config_by_flag_sets(factory):
    """Test client.get_treatments_with_config_by_flag_sets()."""
    try:
        client = factory.client()
    except:
        pass
    result = client.get_treatments_with_config_by_flag_sets('user1', ['set1'])
    assert len(result) == 2
    assert result == {'sample_feature': ('on', '{"size":15,"test":20}'), 'whitelist_feature': ('off', None)}
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('sample_feature', 'user1', 'on'), ('whitelist_feature', 'user1', 'off'))

    result = client.get_treatments_with_config_by_flag_sets('invalidKey', ['invalid_set'])
    assert len(result) == 0
    assert result == {}

    result = client.get_treatments_with_config_by_flag_sets('invalidKey', [])
    assert len(result) == 0
    assert result == {}

    # testing a killed feature. No matter what the key, must return default treatment
    result = client.get_treatments_with_config_by_flag_sets('invalidKey', ['set3'])
    assert len(result) == 1
    assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('killed_feature', 'invalidKey', 'defTreatment'))

    # testing ALL matcher
    result = client.get_treatments_with_config_by_flag_sets('user1', ['set4'])
    assert len(result) == 1
    assert result['all_feature'] == ('on', None)
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        _validate_last_impressions(client, ('all_feature', 'user1', 'on'))

def _track(factory):
    """Test client.track()."""
    try:
        client = factory.client()
    except:
        pass
    assert(client.track('user1', 'user', 'conversion', 1, {"prop1": "value1"}))
    assert(not client.track(None, 'user', 'conversion'))
    assert(not client.track('user1', None, 'conversion'))
    assert(not client.track('user1', 'user', None))
    _validate_last_events(
        client,
        ('user1', 'user', 'conversion', 1, "{'prop1': 'value1'}")
    )

def _manager_methods(factory):
    """Test manager.split/splits."""
    try:
        manager = factory.manager()
    except:
        pass
    result = manager.split('all_feature')
    assert result.name == 'all_feature'
    assert result.traffic_type is None
    assert result.killed is False
    assert len(result.treatments) == 2
    assert result.change_number == 123
    assert result.configs == {}

    result = manager.split('killed_feature')
    assert result.name == 'killed_feature'
    assert result.traffic_type is None
    assert result.killed is True
    assert len(result.treatments) == 2
    assert result.change_number == 123
    assert result.configs['defTreatment'] == '{"size":15,"defTreatment":true}'
    assert result.configs['off'] == '{"size":15,"test":20}'

    result = manager.split('sample_feature')
    assert result.name == 'sample_feature'
    assert result.traffic_type is None
    assert result.killed is False
    assert len(result.treatments) == 2
    assert result.change_number == 123
    assert result.configs['on'] == '{"size":15,"test":20}'

    assert len(manager.split_names()) == 7
    assert len(manager.splits()) == 7

class InMemoryDebugIntegrationTests(object):
    """Inmemory storage-based integration tests."""

    def setup_method(self):
        """Prepare storages with test data."""
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()

        split_fn = os.path.join(os.path.dirname(__file__), 'files', 'splitChanges.json')
        with open(split_fn, 'r') as flo:
            data = json.loads(flo.read())
        for split in data['splits']:
            split_storage.update([splits.from_raw(split)], [], 0)

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentEmployeesChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        segment_storage.put(segments.from_raw(data))

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentHumanBeignsChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        segment_storage.put(segments.from_raw(data))

        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': InMemoryImpressionStorage(5000, telemetry_runtime_producer),
            'events': InMemoryEventStorage(5000, telemetry_runtime_producer),
        }
        impmanager = ImpressionsManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = StandardRecorder(impmanager, storages['events'], storages['impressions'], telemetry_evaluation_producer, telemetry_runtime_producer)
        # Since we are passing None as SDK_Ready event, the factory will use the Redis telemetry call, using try catch to ignore the exception.
        try:
            self.factory = SplitFactory('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    None,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init
        except:
            pass

    def teardown_method(self):
        """Shut down the factory."""
        event = threading.Event()
        self.factory.destroy(event)
        event.wait()

    def test_get_treatment(self):
        """Test client.get_treatment()."""
        _get_treatment(self.factory)

    def test_get_treatment_with_config(self):
        """Test client.get_treatment_with_config()."""
        _get_treatment_with_config(self.factory)

    def test_get_treatments(self):
        _get_treatments(self.factory)
            # testing multiple splitNames
        client = self.factory.client()
        result = client.get_treatments('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == 'on'
        assert result['killed_feature'] == 'defTreatment'
        assert result['invalid_feature'] == 'control'
        assert result['sample_feature'] == 'off'
        _validate_last_impressions(
            client,
            ('all_feature', 'invalidKey', 'on'),
            ('killed_feature', 'invalidKey', 'defTreatment'),
            ('sample_feature', 'invalidKey', 'off')
        )

    def test_get_treatments_with_config(self):
        """Test client.get_treatments_with_config()."""
        _get_treatments_with_config(self.factory)
        # testing multiple splitNames
        client = self.factory.client()
        result = client.get_treatments_with_config('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == ('on', None)
        assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
        assert result['invalid_feature'] == ('control', None)
        assert result['sample_feature'] == ('off', None)
        _validate_last_impressions(
            client,
            ('all_feature', 'invalidKey', 'on'),
            ('killed_feature', 'invalidKey', 'defTreatment'),
            ('sample_feature', 'invalidKey', 'off'),
        )

    def test_get_treatments_by_flag_set(self):
        """Test client.get_treatments_by_flag_set()."""
        _get_treatments_by_flag_set(self.factory)

    def test_get_treatments_by_flag_sets(self):
        """Test client.get_treatments_by_flag_sets()."""
        _get_treatments_by_flag_sets(self.factory)
        client = self.factory.client()
        result = client.get_treatments_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': 'on',
                            'whitelist_feature': 'off',
                            'all_feature': 'on'
                            }
        _validate_last_impressions(client, ('sample_feature', 'user1', 'on'),
                                        ('whitelist_feature', 'user1', 'off'),
                                        ('all_feature', 'user1', 'on')
                                        )

    def test_get_treatments_with_config_by_flag_set(self):
        """Test client.get_treatments_with_config_by_flag_set()."""
        _get_treatments_with_config_by_flag_set(self.factory)

    def test_get_treatments_with_config_by_flag_sets(self):
        """Test client.get_treatments_with_config_by_flag_sets()."""
        _get_treatments_with_config_by_flag_sets(self.factory)
        client = self.factory.client()
        result = client.get_treatments_with_config_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': ('on', '{"size":15,"test":20}'),
                            'whitelist_feature': ('off', None),
                            'all_feature': ('on', None)
                            }
        _validate_last_impressions(client, ('sample_feature', 'user1', 'on'),
                                        ('whitelist_feature', 'user1', 'off'),
                                        ('all_feature', 'user1', 'on')
                                        )

    def test_track(self):
        """Test client.track()."""
        _track(self.factory)

    def test_manager_methods(self):
        """Test manager.split/splits."""
        _manager_methods(self.factory)


class InMemoryOptimizedIntegrationTests(object):
    """Inmemory storage-based integration tests."""

    def setup_method(self):
        """Prepare storages with test data."""
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()

        split_fn = os.path.join(os.path.dirname(__file__), 'files', 'splitChanges.json')
        with open(split_fn, 'r') as flo:
            data = json.loads(flo.read())
        for split in data['splits']:
            split_storage.update([splits.from_raw(split)], [], 0)

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentEmployeesChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        segment_storage.put(segments.from_raw(data))

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentHumanBeignsChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        segment_storage.put(segments.from_raw(data))

        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': InMemoryImpressionStorage(5000, telemetry_runtime_producer),
            'events': InMemoryEventStorage(5000, telemetry_runtime_producer),
        }
        impmanager = ImpressionsManager(StrategyOptimizedMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = StandardRecorder(impmanager, storages['events'], storages['impressions'], telemetry_evaluation_producer, telemetry_runtime_producer)
        self.factory = SplitFactory('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    None,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init

    def test_get_treatment(self):
        """Test client.get_treatment()."""
        _get_treatment(self.factory)

    def test_get_treatments(self):
        """Test client.get_treatments()."""
        _get_treatments(self.factory)
        # testing multiple splitNames
        client = self.factory.client()
        result = client.get_treatments('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == 'on'
        assert result['killed_feature'] == 'defTreatment'
        assert result['invalid_feature'] == 'control'
        assert result['sample_feature'] == 'off'
        assert self.factory._storages['impressions']._impressions.qsize() == 0

    def test_get_treatments_with_config(self):
        """Test client.get_treatments_with_config()."""
        _get_treatments_with_config(self.factory)
        # testing multiple splitNames
        client = self.factory.client()
        result = client.get_treatments_with_config('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == ('on', None)
        assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
        assert result['invalid_feature'] == ('control', None)
        assert result['sample_feature'] == ('off', None)
        _validate_last_impressions(client,)

    def test_get_treatments_by_flag_set(self):
        """Test client.get_treatments_by_flag_set()."""
        _get_treatments_by_flag_set(self.factory)

    def test_get_treatments_by_flag_sets(self):
        """Test client.get_treatments_by_flag_sets()."""
        _get_treatments_by_flag_sets(self.factory)
        client = self.factory.client()
        result = client.get_treatments_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': 'on',
                          'whitelist_feature': 'off',
                          'all_feature': 'on'
                          }
        _validate_last_impressions(client, )
        assert self.factory._storages['impressions']._impressions.qsize() == 0

    def test_get_treatments_with_config_by_flag_set(self):
        """Test client.get_treatments_with_config_by_flag_set()."""
        _get_treatments_with_config_by_flag_set(self.factory)

    def test_get_treatments_with_config_by_flag_sets(self):
        """Test client.get_treatments_with_config_by_flag_sets()."""
        _get_treatments_with_config_by_flag_sets(self.factory)
        client = self.factory.client()
        result = client.get_treatments_with_config_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': ('on', '{"size":15,"test":20}'),
                          'whitelist_feature': ('off', None),
                          'all_feature': ('on', None)
                          }
        _validate_last_impressions(client, )

    def test_manager_methods(self):
        """Test manager.split/splits."""
        _manager_methods(self.factory)

    def test_track(self):
        """Test client.track()."""
        _track(self.factory)

class RedisIntegrationTests(object):
    """Redis storage-based integration tests."""

    def setup_method(self):
        """Prepare storages with test data."""
        metadata = SdkMetadata('python-1.2.3', 'some_ip', 'some_name')
        redis_client = build(DEFAULT_CONFIG.copy())
        split_storage = RedisSplitStorage(redis_client)
        segment_storage = RedisSegmentStorage(redis_client)

        split_fn = os.path.join(os.path.dirname(__file__), 'files', 'splitChanges.json')
        with open(split_fn, 'r') as flo:
            data = json.loads(flo.read())
        for split in data['splits']:
            redis_client.set(split_storage._get_key(split['name']), json.dumps(split))
            if split.get('sets') is not None:
                for flag_set in split.get('sets'):
                    redis_client.sadd(split_storage._get_flag_set_key(flag_set), split['name'])
        redis_client.set(split_storage._FEATURE_FLAG_TILL_KEY, data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentEmployeesChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        redis_client.sadd(segment_storage._get_key(data['name']), *data['added'])
        redis_client.set(segment_storage._get_till_key(data['name']), data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentHumanBeignsChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        redis_client.sadd(segment_storage._get_key(data['name']), *data['added'])
        redis_client.set(segment_storage._get_till_key(data['name']), data['till'])

        telemetry_redis_storage = RedisTelemetryStorage(redis_client, metadata)
        telemetry_producer = TelemetryStorageProducer(telemetry_redis_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': RedisImpressionsStorage(redis_client, metadata),
            'events': RedisEventsStorage(redis_client, metadata),
        }
        impmanager = ImpressionsManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = PipelinedRecorder(redis_client.pipeline, impmanager, storages['events'],
                                    storages['impressions'], telemetry_redis_storage)
        self.factory = SplitFactory('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init

    def test_get_treatment(self):
        """Test client.get_treatment()."""
        _get_treatment(self.factory)

    def test_get_treatment_with_config(self):
        """Test client.get_treatment_with_config()."""
        _get_treatment_with_config(self.factory)

    def test_get_treatments(self):
        """Test client.get_treatments()."""
        _get_treatments(self.factory)
        client = self.factory.client()
        # testing multiple splitNames
        result = client.get_treatments('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == 'on'
        assert result['killed_feature'] == 'defTreatment'
        assert result['invalid_feature'] == 'control'
        assert result['sample_feature'] == 'off'
        _validate_last_impressions(
            client,
            ('all_feature', 'invalidKey', 'on'),
            ('killed_feature', 'invalidKey', 'defTreatment'),
            ('sample_feature', 'invalidKey', 'off')
        )

    def test_get_treatment_with_config(self):
        """Test client.get_treatment_with_config()."""
        _get_treatment_with_config(self.factory)

    def test_get_treatments_with_config(self):
        """Test client.get_treatments_with_config()."""
        _get_treatments_with_config(self.factory)
        client = self.factory.client()
        # testing multiple splitNames
        result = client.get_treatments_with_config('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == ('on', None)
        assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
        assert result['invalid_feature'] == ('control', None)
        assert result['sample_feature'] == ('off', None)
        _validate_last_impressions(
            client,
            ('all_feature', 'invalidKey', 'on'),
            ('killed_feature', 'invalidKey', 'defTreatment'),
            ('sample_feature', 'invalidKey', 'off'),
        )

    def test_get_treatments_by_flag_set(self):
        """Test client.get_treatments_by_flag_set()."""
        _get_treatments_by_flag_set(self.factory)

    def test_get_treatments_by_flag_sets(self):
        """Test client.get_treatments_by_flag_sets()."""
        _get_treatments_by_flag_sets(self.factory)
        client = self.factory.client()
        result = client.get_treatments_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': 'on',
                          'whitelist_feature': 'off',
                          'all_feature': 'on'
                          }
        _validate_last_impressions(client, ('sample_feature', 'user1', 'on'),
                                        ('whitelist_feature', 'user1', 'off'),
                                        ('all_feature', 'user1', 'on')
                                        )

    def test_get_treatments_with_config_by_flag_set(self):
        """Test client.get_treatments_with_config_by_flag_set()."""
        _get_treatments_with_config_by_flag_set(self.factory)

    def test_get_treatments_with_config_by_flag_sets(self):
        """Test client.get_treatments_with_config_by_flag_sets()."""
        _get_treatments_with_config_by_flag_sets(self.factory)
        client = self.factory.client()
        result = client.get_treatments_with_config_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': ('on', '{"size":15,"test":20}'),
                          'whitelist_feature': ('off', None),
                          'all_feature': ('on', None)
                          }
        _validate_last_impressions(client, ('sample_feature', 'user1', 'on'),
                                        ('whitelist_feature', 'user1', 'off'),
                                        ('all_feature', 'user1', 'on')
                                        )

    def test_track(self):
        """Test client.track()."""
        _track(self.factory)

    def test_manager_methods(self):
        """Test manager.split/splits."""
        _manager_methods(self.factory)

    def teardown_method(self):
        """Clear redis cache."""
        keys_to_delete = [
            "SPLITIO.segment.human_beigns",
            "SPLITIO.segment.employees.till",
            "SPLITIO.split.sample_feature",
            "SPLITIO.splits.till",
            "SPLITIO.split.killed_feature",
            "SPLITIO.split.all_feature",
            "SPLITIO.split.whitelist_feature",
            "SPLITIO.segment.employees",
            "SPLITIO.split.regex_test",
            "SPLITIO.segment.human_beigns.till",
            "SPLITIO.split.boolean_test",
            "SPLITIO.split.dependency_test",
            "SPLITIO.split.set.set1",
            "SPLITIO.split.set.set2",
            "SPLITIO.split.set.set3",
            "SPLITIO.split.set.set4"
        ]

        redis_client = RedisAdapter(StrictRedis())
        for key in keys_to_delete:
            redis_client.delete(key)

class RedisWithCacheIntegrationTests(RedisIntegrationTests):
    """Run the same tests as RedisIntegratioTests but with LRU/Expirable cache overlay."""

    def setup_method(self):
        """Prepare storages with test data."""
        metadata = SdkMetadata('python-1.2.3', 'some_ip', 'some_name')
        redis_client = build(DEFAULT_CONFIG.copy())
        split_storage = RedisSplitStorage(redis_client, True)
        segment_storage = RedisSegmentStorage(redis_client)

        split_fn = os.path.join(os.path.dirname(__file__), 'files', 'splitChanges.json')
        with open(split_fn, 'r') as flo:
            data = json.loads(flo.read())
        for split in data['splits']:
            redis_client.set(split_storage._get_key(split['name']), json.dumps(split))
        redis_client.set(split_storage._FEATURE_FLAG_TILL_KEY, data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentEmployeesChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        redis_client.sadd(segment_storage._get_key(data['name']), *data['added'])
        redis_client.set(segment_storage._get_till_key(data['name']), data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentHumanBeignsChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        redis_client.sadd(segment_storage._get_key(data['name']), *data['added'])
        redis_client.set(segment_storage._get_till_key(data['name']), data['till'])

        telemetry_redis_storage = RedisTelemetryStorage(redis_client, metadata)
        telemetry_producer = TelemetryStorageProducer(telemetry_redis_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': RedisImpressionsStorage(redis_client, metadata),
            'events': RedisEventsStorage(redis_client, metadata),
        }
        impmanager = ImpressionsManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = PipelinedRecorder(redis_client.pipeline, impmanager,
                                     storages['events'], storages['impressions'], telemetry_redis_storage)
        self.factory = SplitFactory('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init

class LocalhostIntegrationTests(object):  # pylint: disable=too-few-public-methods
    """Client & Manager integration tests."""

    def test_localhost_json_e2e(self):
        """Instantiate a client with a JSON file and issue get_treatment() calls."""
        self._update_temp_file(splits_json['splitChange2_1'])
        filename = os.path.join(os.path.dirname(__file__), 'files', 'split_changes_temp.json')
        self.factory = get_factory('localhost', config={'splitFile': filename})
        self.factory.block_until_ready(1)
        client = self.factory.client()

        # Tests 2
        assert self.factory.manager().split_names() == ["SPLIT_1"]
        assert client.get_treatment("key", "SPLIT_1") == 'off'

        # Tests 1
        self.factory._storages['splits'].update([], ['SPLIT_1'], -1)
        self._update_temp_file(splits_json['splitChange1_1'])
        self._synchronize_now()

        assert sorted(self.factory.manager().split_names()) == ["SPLIT_1", "SPLIT_2", "SPLIT_3"]
        assert client.get_treatment("key", "SPLIT_1", None) == 'off'
        assert client.get_treatment("key", "SPLIT_2", None) == 'on'

        self._update_temp_file(splits_json['splitChange1_2'])
        self._synchronize_now()

        assert sorted(self.factory.manager().split_names()) == ["SPLIT_1", "SPLIT_2", "SPLIT_3"]
        assert client.get_treatment("key", "SPLIT_1", None) == 'off'
        assert client.get_treatment("key", "SPLIT_2", None) == 'off'

        self._update_temp_file(splits_json['splitChange1_3'])
        self._synchronize_now()

        assert self.factory.manager().split_names() == ["SPLIT_2", "SPLIT_3"]
        assert client.get_treatment("key", "SPLIT_1", None) == 'control'
        assert client.get_treatment("key", "SPLIT_2", None) == 'on'

        # Tests 3
        self.factory._storages['splits'].update([], ['SPLIT_1'], -1)
        self._update_temp_file(splits_json['splitChange3_1'])
        self._synchronize_now()

        assert self.factory.manager().split_names() == ["SPLIT_2", "SPLIT_3"]
        assert client.get_treatment("key", "SPLIT_2", None) == 'on'

        self._update_temp_file(splits_json['splitChange3_2'])
        self._synchronize_now()

        assert self.factory.manager().split_names() == ["SPLIT_2", "SPLIT_3"]
        assert client.get_treatment("key", "SPLIT_2", None) == 'off'

        # Tests 4
        self.factory._storages['splits'].update([], ['SPLIT_2'], -1)
        self._update_temp_file(splits_json['splitChange4_1'])
        self._synchronize_now()

        assert sorted(self.factory.manager().split_names()) == ["SPLIT_1", "SPLIT_2", "SPLIT_3"]
        assert client.get_treatment("key", "SPLIT_1", None) == 'off'
        assert client.get_treatment("key", "SPLIT_2", None) == 'on'

        self._update_temp_file(splits_json['splitChange4_2'])
        self._synchronize_now()

        assert sorted(self.factory.manager().split_names()) == ["SPLIT_1", "SPLIT_2", "SPLIT_3"]
        assert client.get_treatment("key", "SPLIT_1", None) == 'off'
        assert client.get_treatment("key", "SPLIT_2", None) == 'off'

        self._update_temp_file(splits_json['splitChange4_3'])
        self._synchronize_now()

        assert sorted(self.factory.manager().split_names()) == ["SPLIT_2", "SPLIT_3"]
        assert client.get_treatment("key", "SPLIT_1", None) == 'control'
        assert client.get_treatment("key", "SPLIT_2", None) == 'on'

        # Tests 5
        self.factory._storages['splits'].update([], ['SPLIT_1', 'SPLIT_2'], -1)
        self._update_temp_file(splits_json['splitChange5_1'])
        self._synchronize_now()

        assert sorted(self.factory.manager().split_names()) == ["SPLIT_2", "SPLIT_3"]
        assert client.get_treatment("key", "SPLIT_2", None) == 'on'

        self._update_temp_file(splits_json['splitChange5_2'])
        self._synchronize_now()

        assert sorted(self.factory.manager().split_names()) == ["SPLIT_2", "SPLIT_3"]
        assert client.get_treatment("key", "SPLIT_2", None) == 'on'

        # Tests 6
        self.factory._storages['splits'].update([], ['SPLIT_2'], -1)
        self._update_temp_file(splits_json['splitChange6_1'])
        self._synchronize_now()

        assert sorted(self.factory.manager().split_names()) == ["SPLIT_1", "SPLIT_2", "SPLIT_3"]
        assert client.get_treatment("key", "SPLIT_1", None) == 'off'
        assert client.get_treatment("key", "SPLIT_2", None) == 'on'

        self._update_temp_file(splits_json['splitChange6_2'])
        self._synchronize_now()

        assert sorted(self.factory.manager().split_names()) == ["SPLIT_1", "SPLIT_2", "SPLIT_3"]
        assert client.get_treatment("key", "SPLIT_1", None) == 'off'
        assert client.get_treatment("key", "SPLIT_2", None) == 'off'

        self._update_temp_file(splits_json['splitChange6_3'])
        self._synchronize_now()

        assert sorted(self.factory.manager().split_names()) == ["SPLIT_2", "SPLIT_3"]
        assert client.get_treatment("key", "SPLIT_1", None) == 'control'
        assert client.get_treatment("key", "SPLIT_2", None) == 'on'

    def _update_temp_file(self, json_body):
        f = open(os.path.join(os.path.dirname(__file__), 'files','split_changes_temp.json'), 'w')
        f.write(json.dumps(json_body))
        f.close()

    def _synchronize_now(self):
        filename = os.path.join(os.path.dirname(__file__), 'files', 'split_changes_temp.json')
        self.factory._sync_manager._synchronizer._split_synchronizers._feature_flag_sync._filename = filename
        self.factory._sync_manager._synchronizer._split_synchronizers._feature_flag_sync.synchronize_splits()

    def test_incorrect_file_e2e(self):
        """Test initialize factory with a incorrect file name."""
        # TODO: secontion below is removed when legacu use BUR
        # legacy and yaml
        exception_raised = False
        factory = None
        try:
            factory = get_factory('localhost', config={'splitFile': 'filename'})
        except Exception as e:
            exception_raised = True

        assert(exception_raised)

        # json using BUR
        factory = get_factory('localhost', config={'splitFile': 'filename.json'})
        exception_raised = False
        try:
            factory.block_until_ready(1)
        except Exception as e:
            exception_raised = True

        assert(exception_raised)

        event = threading.Event()
        factory.destroy(event)
        event.wait()


    def test_localhost_e2e(self):
        """Instantiate a client with a YAML file and issue get_treatment() calls."""
        filename = os.path.join(os.path.dirname(__file__), 'files', 'file2.yaml')
        factory = get_factory('localhost', config={'splitFile': filename})
        factory.block_until_ready()
        client = factory.client()
        assert client.get_treatment_with_config('key', 'my_feature') == ('on', '{"desc" : "this applies only to ON treatment"}')
        assert client.get_treatment_with_config('only_key', 'my_feature') == (
            'off', '{"desc" : "this applies only to OFF and only for only_key. The rest will receive ON"}'
        )
        assert client.get_treatment_with_config('another_key', 'my_feature') == ('control', None)
        assert client.get_treatment_with_config('key2', 'other_feature') == ('on', None)
        assert client.get_treatment_with_config('key3', 'other_feature') == ('on', None)
        assert client.get_treatment_with_config('some_key', 'other_feature_2') == ('on', None)
        assert client.get_treatment_with_config('key_whitelist', 'other_feature_3') == ('on', None)
        assert client.get_treatment_with_config('any_other_key', 'other_feature_3') == ('off', None)

        manager = factory.manager()
        assert manager.split('my_feature').configs == {
            'on': '{"desc" : "this applies only to ON treatment"}',
            'off': '{"desc" : "this applies only to OFF and only for only_key. The rest will receive ON"}'
        }
        assert manager.split('other_feature').configs == {}
        assert manager.split('other_feature_2').configs == {}
        assert manager.split('other_feature_3').configs == {}
        event = threading.Event()
        factory.destroy(event)
        event.wait()


class PluggableIntegrationTests(object):
    """Pluggable storage-based integration tests."""

    def setup_method(self):
        """Prepare storages with test data."""
        metadata = SdkMetadata('python-1.2.3', 'some_ip', 'some_name')
        self.pluggable_storage_adapter = StorageMockAdapter()
        split_storage = PluggableSplitStorage(self.pluggable_storage_adapter)
        segment_storage = PluggableSegmentStorage(self.pluggable_storage_adapter)

        telemetry_pluggable_storage = PluggableTelemetryStorage(self.pluggable_storage_adapter, metadata)
        telemetry_producer = TelemetryStorageProducer(telemetry_pluggable_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': PluggableImpressionsStorage(self.pluggable_storage_adapter, metadata),
            'events': PluggableEventsStorage(self.pluggable_storage_adapter, metadata),
            'telemetry': telemetry_pluggable_storage
        }

        impmanager = ImpressionsManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = StandardRecorder(impmanager, storages['events'],
                                    storages['impressions'], telemetry_evaluation_producer, telemetry_runtime_producer)

        self.factory = SplitFactory('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    RedisManager(PluggableSynchronizer()),
                                    sdk_ready_flag=None,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init

        # Adding data to storage
        split_fn = os.path.join(os.path.dirname(__file__), 'files', 'splitChanges.json')
        with open(split_fn, 'r') as flo:
            data = json.loads(flo.read())
        for split in data['splits']:
            self.pluggable_storage_adapter.set(split_storage._prefix.format(feature_flag_name=split['name']), split)
            if split.get('sets') is not None:
                for flag_set in split.get('sets'):
                    self.pluggable_storage_adapter.push_items(split_storage._flag_set_prefix.format(flag_set=flag_set), split['name'])
        self.pluggable_storage_adapter.set(split_storage._feature_flag_till_prefix, data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentEmployeesChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        self.pluggable_storage_adapter.set(segment_storage._prefix.format(segment_name=data['name']), set(data['added']))
        self.pluggable_storage_adapter.set(segment_storage._segment_till_prefix.format(segment_name=data['name']), data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentHumanBeignsChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        self.pluggable_storage_adapter.set(segment_storage._prefix.format(segment_name=data['name']), set(data['added']))
        self.pluggable_storage_adapter.set(segment_storage._segment_till_prefix.format(segment_name=data['name']), data['till'])

    def test_get_treatment(self):
        """Test client.get_treatment()."""
        _get_treatment(self.factory)

    def test_get_treatment_with_config(self):
        """Test client.get_treatment_with_config()."""
        _get_treatment_with_config(self.factory)

    def test_get_treatments(self):
        """Test client.get_treatments()."""
        _get_treatments(self.factory)
        client = self.factory.client()
        # testing multiple splitNames
        result = client.get_treatments('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == 'on'
        assert result['killed_feature'] == 'defTreatment'
        assert result['invalid_feature'] == 'control'
        assert result['sample_feature'] == 'off'
        _validate_last_impressions(
            client,
            ('all_feature', 'invalidKey', 'on'),
            ('killed_feature', 'invalidKey', 'defTreatment'),
            ('sample_feature', 'invalidKey', 'off')
        )

    def test_get_treatment_with_config(self):
        """Test client.get_treatment_with_config()."""
        _get_treatment_with_config(self.factory)

    def test_get_treatments_with_config(self):
        """Test client.get_treatments_with_config()."""
        _get_treatments_with_config(self.factory)
        client = self.factory.client()
        # testing multiple splitNames
        result = client.get_treatments_with_config('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == ('on', None)
        assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
        assert result['invalid_feature'] == ('control', None)
        assert result['sample_feature'] == ('off', None)
        _validate_last_impressions(
            client,
            ('all_feature', 'invalidKey', 'on'),
            ('killed_feature', 'invalidKey', 'defTreatment'),
            ('sample_feature', 'invalidKey', 'off'),
        )

    def test_get_treatments_by_flag_set(self):
        """Test client.get_treatments_by_flag_set()."""
        _get_treatments_by_flag_set(self.factory)

    def test_get_treatments_by_flag_sets(self):
        """Test client.get_treatments_by_flag_sets()."""
        _get_treatments_by_flag_sets(self.factory)
        client = self.factory.client()
        result = client.get_treatments_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': 'on',
                          'whitelist_feature': 'off',
                          'all_feature': 'on'
                          }
        _validate_last_impressions(client, ('sample_feature', 'user1', 'on'),
                                        ('whitelist_feature', 'user1', 'off'),
                                        ('all_feature', 'user1', 'on')
                                        )

    def test_get_treatments_with_config_by_flag_set(self):
        """Test client.get_treatments_with_config_by_flag_set()."""
        _get_treatments_with_config_by_flag_set(self.factory)

    def test_get_treatments_with_config_by_flag_sets(self):
        """Test client.get_treatments_with_config_by_flag_sets()."""
        _get_treatments_with_config_by_flag_sets(self.factory)
        client = self.factory.client()
        result = client.get_treatments_with_config_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': ('on', '{"size":15,"test":20}'),
                          'whitelist_feature': ('off', None),
                          'all_feature': ('on', None)
                          }
        _validate_last_impressions(client, ('sample_feature', 'user1', 'on'),
                                        ('whitelist_feature', 'user1', 'off'),
                                        ('all_feature', 'user1', 'on')
                                        )

    def test_track(self):
        """Test client.track()."""
        _track(self.factory)

    def test_manager_methods(self):
        """Test manager.split/splits."""
        _manager_methods(self.factory)

    def teardown_method(self):
        """Clear pluggable cache."""
        keys_to_delete = [
            "SPLITIO.segment.human_beigns",
            "SPLITIO.segment.employees.till",
            "SPLITIO.split.sample_feature",
            "SPLITIO.splits.till",
            "SPLITIO.split.killed_feature",
            "SPLITIO.split.all_feature",
            "SPLITIO.split.whitelist_feature",
            "SPLITIO.segment.employees",
            "SPLITIO.split.regex_test",
            "SPLITIO.segment.human_beigns.till",
            "SPLITIO.split.boolean_test",
            "SPLITIO.split.dependency_test",
            "SPLITIO.split.set.set1",
            "SPLITIO.split.set.set2",
            "SPLITIO.split.set.set3",
            "SPLITIO.split.set.set4"
        ]
        for key in keys_to_delete:
            self.pluggable_storage_adapter.delete(key)

class PluggableOptimizedIntegrationTests(object):
    """Pluggable storage-based integration tests."""

    def setup_method(self):
        """Prepare storages with test data."""
        metadata = SdkMetadata('python-1.2.3', 'some_ip', 'some_name')
        self.pluggable_storage_adapter = StorageMockAdapter()
        split_storage = PluggableSplitStorage(self.pluggable_storage_adapter)
        segment_storage = PluggableSegmentStorage(self.pluggable_storage_adapter)

        telemetry_pluggable_storage = PluggableTelemetryStorage(self.pluggable_storage_adapter, metadata)
        telemetry_producer = TelemetryStorageProducer(telemetry_pluggable_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': PluggableImpressionsStorage(self.pluggable_storage_adapter, metadata),
            'events': PluggableEventsStorage(self.pluggable_storage_adapter, metadata),
            'telemetry': telemetry_pluggable_storage
        }

        impmanager = ImpressionsManager(StrategyOptimizedMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = StandardRecorder(impmanager, storages['events'],
                                    storages['impressions'], telemetry_evaluation_producer, telemetry_runtime_producer)

        self.factory = SplitFactory('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    RedisManager(PluggableSynchronizer()),
                                    sdk_ready_flag=None,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init

        # Adding data to storage
        split_fn = os.path.join(os.path.dirname(__file__), 'files', 'splitChanges.json')
        with open(split_fn, 'r') as flo:
            data = json.loads(flo.read())
        for split in data['splits']:
            if split.get('sets') is not None:
                for flag_set in split.get('sets'):
                    self.pluggable_storage_adapter.push_items(split_storage._flag_set_prefix.format(flag_set=flag_set), split['name'])
            self.pluggable_storage_adapter.set(split_storage._prefix.format(feature_flag_name=split['name']), split)
        self.pluggable_storage_adapter.set(split_storage._feature_flag_till_prefix, data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentEmployeesChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        self.pluggable_storage_adapter.set(segment_storage._prefix.format(segment_name=data['name']), set(data['added']))
        self.pluggable_storage_adapter.set(segment_storage._segment_till_prefix.format(segment_name=data['name']), data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentHumanBeignsChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        self.pluggable_storage_adapter.set(segment_storage._prefix.format(segment_name=data['name']), set(data['added']))
        self.pluggable_storage_adapter.set(segment_storage._segment_till_prefix.format(segment_name=data['name']), data['till'])

    def test_get_treatment(self):
        """Test client.get_treatment()."""
        _get_treatment(self.factory)
        client = self.factory.client()

        assert client.get_treatment('user1', 'sample_feature') == 'on'
        client.get_treatment('user1', 'sample_feature')
        client.get_treatment('user1', 'sample_feature')
        client.get_treatment('user1', 'sample_feature')
        assert self.pluggable_storage_adapter._keys['SPLITIO.impressions'] == []

    def test_get_treatment_with_config(self):
        """Test client.get_treatment_with_config()."""
        _get_treatment_with_config(self.factory)

    def test_get_treatments(self):
        """Test client.get_treatments()."""
        _get_treatments(self.factory)

    def test_get_treatment_with_config(self):
        """Test client.get_treatment_with_config()."""
        _get_treatment_with_config(self.factory)

    def test_get_treatments_with_config(self):
        """Test client.get_treatments_with_config()."""
        _get_treatments_with_config(self.factory)
        # testing multiple splitNames
        client = self.factory.client()
        result = client.get_treatments_with_config('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == ('on', None)
        assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
        assert result['invalid_feature'] == ('control', None)
        assert result['sample_feature'] == ('off', None)
        _validate_last_impressions(client,)

    def test_get_treatments_by_flag_set(self):
        """Test client.get_treatments_by_flag_set()."""
        _get_treatments_by_flag_set(self.factory)

    def test_get_treatments_by_flag_sets(self):
        """Test client.get_treatments_by_flag_sets()."""
        _get_treatments_by_flag_sets(self.factory)
        client = self.factory.client()
        result = client.get_treatments_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': 'on',
                          'whitelist_feature': 'off',
                          'all_feature': 'on'
                          }
        _validate_last_impressions(client, )

    def test_get_treatments_with_config_by_flag_set(self):
        """Test client.get_treatments_with_config_by_flag_set()."""
        _get_treatments_with_config_by_flag_set(self.factory)

    def test_get_treatments_with_config_by_flag_sets(self):
        """Test client.get_treatments_with_config_by_flag_sets()."""
        _get_treatments_with_config_by_flag_sets(self.factory)
        client = self.factory.client()
        result = client.get_treatments_with_config_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': ('on', '{"size":15,"test":20}'),
                          'whitelist_feature': ('off', None),
                          'all_feature': ('on', None)
                          }
        _validate_last_impressions(client, )

    def test_track(self):
        """Test client.track()."""
        _track(self.factory)

    def test_manager_methods(self):
        """Test manager.split/splits."""
        _manager_methods(self.factory)

    def teardown_method(self):
        """Clear pluggable cache."""
        keys_to_delete = [
            "SPLITIO.segment.human_beigns",
            "SPLITIO.segment.employees.till",
            "SPLITIO.split.sample_feature",
            "SPLITIO.splits.till",
            "SPLITIO.split.killed_feature",
            "SPLITIO.split.all_feature",
            "SPLITIO.split.whitelist_feature",
            "SPLITIO.segment.employees",
            "SPLITIO.split.regex_test",
            "SPLITIO.segment.human_beigns.till",
            "SPLITIO.split.boolean_test",
            "SPLITIO.split.dependency_test",
            "SPLITIO.split.set.set1",
            "SPLITIO.split.set.set2",
            "SPLITIO.split.set.set3",
            "SPLITIO.split.set.set4"
        ]
        for key in keys_to_delete:
            self.pluggable_storage_adapter.delete(key)

class PluggableNoneIntegrationTests(object):
    """Pluggable storage-based integration tests."""

    def setup_method(self):
        """Prepare storages with test data."""
        metadata = SdkMetadata('python-1.2.3', 'some_ip', 'some_name')
        self.pluggable_storage_adapter = StorageMockAdapter()
        split_storage = PluggableSplitStorage(self.pluggable_storage_adapter)
        segment_storage = PluggableSegmentStorage(self.pluggable_storage_adapter)

        telemetry_pluggable_storage = PluggableTelemetryStorage(self.pluggable_storage_adapter, metadata)
        telemetry_producer = TelemetryStorageProducer(telemetry_pluggable_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': PluggableImpressionsStorage(self.pluggable_storage_adapter, metadata),
            'events': PluggableEventsStorage(self.pluggable_storage_adapter, metadata),
            'telemetry': telemetry_pluggable_storage
        }
        imp_counter = ImpressionsCounter()
        unique_keys_tracker = UniqueKeysTracker()
        unique_keys_synchronizer, clear_filter_sync, self.unique_keys_task, \
        clear_filter_task, impressions_count_sync, impressions_count_task, \
        imp_strategy, none_strategy = set_classes('PLUGGABLE', ImpressionsMode.NONE, self.pluggable_storage_adapter, imp_counter, unique_keys_tracker)
        impmanager = ImpressionsManager(imp_strategy, none_strategy, telemetry_runtime_producer) # no listener

        recorder = StandardRecorder(impmanager, storages['events'],
                                    storages['impressions'], telemetry_evaluation_producer, telemetry_runtime_producer, unique_keys_tracker=unique_keys_tracker, imp_counter=imp_counter)

        synchronizers = SplitSynchronizers(None, None, None, None,
            impressions_count_sync,
            None,
            unique_keys_synchronizer,
            clear_filter_sync
        )

        tasks = SplitTasks(None, None, None, None,
            impressions_count_task,
            None,
            self.unique_keys_task,
            clear_filter_task
        )

        synchronizer = RedisSynchronizer(synchronizers, tasks)

        manager = RedisManager(synchronizer)
        manager.start()
        self.factory = SplitFactory('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    manager,
                                    sdk_ready_flag=None,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init

        # Adding data to storage
        split_fn = os.path.join(os.path.dirname(__file__), 'files', 'splitChanges.json')
        with open(split_fn, 'r') as flo:
            data = json.loads(flo.read())
        for split in data['splits']:
            if split.get('sets') is not None:
                for flag_set in split.get('sets'):
                    self.pluggable_storage_adapter.push_items(split_storage._flag_set_prefix.format(flag_set=flag_set), split['name'])
            self.pluggable_storage_adapter.set(split_storage._prefix.format(feature_flag_name=split['name']), split)
        self.pluggable_storage_adapter.set(split_storage._feature_flag_till_prefix, data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentEmployeesChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        self.pluggable_storage_adapter.set(segment_storage._prefix.format(segment_name=data['name']), set(data['added']))
        self.pluggable_storage_adapter.set(segment_storage._segment_till_prefix.format(segment_name=data['name']), data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentHumanBeignsChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        self.pluggable_storage_adapter.set(segment_storage._prefix.format(segment_name=data['name']), set(data['added']))
        self.pluggable_storage_adapter.set(segment_storage._segment_till_prefix.format(segment_name=data['name']), data['till'])
        self.client = self.factory.client()

    def test_get_treatment(self):
        """Test client.get_treatment()."""
        _get_treatment(self.factory)
        assert self.pluggable_storage_adapter._keys['SPLITIO.impressions'] == []

    def test_get_treatments(self):
        """Test client.get_treatments()."""
        _get_treatments(self.factory)
        result = self.client.get_treatments('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == 'on'
        assert result['killed_feature'] == 'defTreatment'
        assert result['invalid_feature'] == 'control'
        assert result['sample_feature'] == 'off'

        assert self.pluggable_storage_adapter._keys['SPLITIO.impressions'] == []

    def test_get_treatments_with_config(self):
        """Test client.get_treatments_with_config()."""
        _get_treatments_with_config(self.factory)
        result = self.client.get_treatments_with_config('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == ('on', None)
        assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
        assert result['invalid_feature'] == ('control', None)
        assert result['sample_feature'] == ('off', None)
        assert self.pluggable_storage_adapter._keys['SPLITIO.impressions'] == []

    def test_get_treatments_by_flag_set(self):
        """Test client.get_treatments_by_flag_set()."""
        _get_treatments_by_flag_set(self.factory)
        assert self.pluggable_storage_adapter._keys['SPLITIO.impressions'] == []

    def test_get_treatments_by_flag_sets(self):
        """Test client.get_treatments_by_flag_sets()."""
        _get_treatments_by_flag_sets(self.factory)
        result = self.client.get_treatments_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': 'on',
                            'whitelist_feature': 'off',
                            'all_feature': 'on'
                            }
        assert self.pluggable_storage_adapter._keys['SPLITIO.impressions'] == []

    def test_get_treatments_with_config_by_flag_set(self):
        """Test client.get_treatments_with_config_by_flag_set()."""
        _get_treatments_with_config_by_flag_set(self.factory)
        assert self.pluggable_storage_adapter._keys['SPLITIO.impressions'] == []

    def test_get_treatments_with_config_by_flag_sets(self):
        """Test client.get_treatments_with_config_by_flag_sets()."""
        _get_treatments_with_config_by_flag_sets(self.factory)
        result = self.client.get_treatments_with_config_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': ('on', '{"size":15,"test":20}'),
                            'whitelist_feature': ('off', None),
                            'all_feature': ('on', None)
                            }
        assert self.pluggable_storage_adapter._keys['SPLITIO.impressions'] == []

    def test_track(self):
        """Test client.track()."""
        _track(self.factory)

    def test_mtk(self):
        self.client.get_treatment('user1', 'sample_feature')
        self.client.get_treatment('invalidKey', 'sample_feature')
        self.client.get_treatment('invalidKey2', 'sample_feature')
        self.client.get_treatment('user22', 'invalidFeature')
        self.unique_keys_task._task.force_execution()
        time.sleep(1)

        assert(json.loads(self.pluggable_storage_adapter._keys['SPLITIO.uniquekeys'][0])["f"] =="sample_feature")
        assert(json.loads(self.pluggable_storage_adapter._keys['SPLITIO.uniquekeys'][0])["ks"].sort() ==
               ["invalidKey2", "invalidKey", "user1"].sort())
        event = threading.Event()
        self.factory.destroy(event)
        event.wait()

class InMemoryImpressionsToggleIntegrationTests(object):
    """InMemory storage-based impressions toggle integration tests."""

    def test_optimized(self):
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()

        split_storage.update([splits.from_raw(splits_json['splitChange1_1']['splits'][0]),
                              splits.from_raw(splits_json['splitChange1_1']['splits'][1]),
                              splits.from_raw(splits_json['splitChange1_1']['splits'][2])
                             ], [], -1)

        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': InMemoryImpressionStorage(5000, telemetry_runtime_producer),
            'events': InMemoryEventStorage(5000, telemetry_runtime_producer),
        }
        impmanager = ImpressionsManager(StrategyOptimizedMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = StandardRecorder(impmanager, storages['events'], storages['impressions'], telemetry_evaluation_producer, telemetry_runtime_producer, None, UniqueKeysTracker(), ImpressionsCounter())
        # Since we are passing None as SDK_Ready event, the factory will use the Redis telemetry call, using try catch to ignore the exception.
        try:
            factory = SplitFactory('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    None,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init
        except:
            pass

        try:
            client = factory.client()
        except:
            pass

        assert client.get_treatment('user1', 'SPLIT_1') == 'off'
        assert client.get_treatment('user1', 'SPLIT_2') == 'on'
        assert client.get_treatment('user1', 'SPLIT_3') == 'on'
        imp_storage = client._factory._get_storage('impressions')
        impressions = imp_storage.pop_many(10)
        assert len(impressions) == 2
        assert impressions[0].feature_name == 'SPLIT_1'
        assert impressions[1].feature_name == 'SPLIT_2'
        assert client._recorder._unique_keys_tracker._cache == {'SPLIT_3': {'user1'}}
        imps_count = client._recorder._imp_counter.pop_all()
        assert len(imps_count) == 1
        assert imps_count[0].feature == 'SPLIT_3'
        assert imps_count[0].count == 1

    def test_debug(self):
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()

        split_storage.update([splits.from_raw(splits_json['splitChange1_1']['splits'][0]),
                              splits.from_raw(splits_json['splitChange1_1']['splits'][1]),
                              splits.from_raw(splits_json['splitChange1_1']['splits'][2])
                             ], [], -1)

        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': InMemoryImpressionStorage(5000, telemetry_runtime_producer),
            'events': InMemoryEventStorage(5000, telemetry_runtime_producer),
        }
        impmanager = ImpressionsManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = StandardRecorder(impmanager, storages['events'], storages['impressions'], telemetry_evaluation_producer, telemetry_runtime_producer, None, UniqueKeysTracker(), ImpressionsCounter())
        # Since we are passing None as SDK_Ready event, the factory will use the Redis telemetry call, using try catch to ignore the exception.
        try:
            factory = SplitFactory('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    None,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init
        except:
            pass

        try:
            client = factory.client()
        except:
            pass

        assert client.get_treatment('user1', 'SPLIT_1') == 'off'
        assert client.get_treatment('user1', 'SPLIT_2') == 'on'
        assert client.get_treatment('user1', 'SPLIT_3') == 'on'
        imp_storage = client._factory._get_storage('impressions')
        impressions = imp_storage.pop_many(10)
        assert len(impressions) == 2
        assert impressions[0].feature_name == 'SPLIT_1'
        assert impressions[1].feature_name == 'SPLIT_2'
        assert client._recorder._unique_keys_tracker._cache == {'SPLIT_3': {'user1'}}
        imps_count = client._recorder._imp_counter.pop_all()
        assert len(imps_count) == 1
        assert imps_count[0].feature == 'SPLIT_3'
        assert imps_count[0].count == 1

    def test_none(self):
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()

        split_storage.update([splits.from_raw(splits_json['splitChange1_1']['splits'][0]),
                              splits.from_raw(splits_json['splitChange1_1']['splits'][1]),
                              splits.from_raw(splits_json['splitChange1_1']['splits'][2])
                             ], [], -1)

        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': InMemoryImpressionStorage(5000, telemetry_runtime_producer),
            'events': InMemoryEventStorage(5000, telemetry_runtime_producer),
        }
        impmanager = ImpressionsManager(StrategyNoneMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = StandardRecorder(impmanager, storages['events'], storages['impressions'], telemetry_evaluation_producer, telemetry_runtime_producer, None, UniqueKeysTracker(), ImpressionsCounter())
        # Since we are passing None as SDK_Ready event, the factory will use the Redis telemetry call, using try catch to ignore the exception.
        try:
            factory = SplitFactory('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    None,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init
        except:
            pass

        try:
            client = factory.client()
        except:
            pass

        assert client.get_treatment('user1', 'SPLIT_1') == 'off'
        assert client.get_treatment('user1', 'SPLIT_2') == 'on'
        assert client.get_treatment('user1', 'SPLIT_3') == 'on'
        imp_storage = client._factory._get_storage('impressions')
        impressions = imp_storage.pop_many(10)
        assert len(impressions) == 0
        assert client._recorder._unique_keys_tracker._cache == {'SPLIT_1': {'user1'}, 'SPLIT_2': {'user1'}, 'SPLIT_3': {'user1'}}
        imps_count = client._recorder._imp_counter.pop_all()
        assert len(imps_count) == 3
        assert imps_count[0].feature == 'SPLIT_1'
        assert imps_count[0].count == 1
        assert imps_count[1].feature == 'SPLIT_2'
        assert imps_count[1].count == 1
        assert imps_count[2].feature == 'SPLIT_3'
        assert imps_count[2].count == 1

class RedisImpressionsToggleIntegrationTests(object):
    """Run impression toggle tests for Redis."""

    def test_optimized(self):
        """Prepare storages with test data."""
        metadata = SdkMetadata('python-1.2.3', 'some_ip', 'some_name')
        redis_client = build(DEFAULT_CONFIG.copy())
        split_storage = RedisSplitStorage(redis_client, True)
        segment_storage = RedisSegmentStorage(redis_client)

        redis_client.set(split_storage._get_key(splits_json['splitChange1_1']['splits'][0]['name']), json.dumps(splits_json['splitChange1_1']['splits'][0]))
        redis_client.set(split_storage._get_key(splits_json['splitChange1_1']['splits'][1]['name']), json.dumps(splits_json['splitChange1_1']['splits'][1]))
        redis_client.set(split_storage._get_key(splits_json['splitChange1_1']['splits'][2]['name']), json.dumps(splits_json['splitChange1_1']['splits'][2]))
        redis_client.set(split_storage._FEATURE_FLAG_TILL_KEY, -1)

        telemetry_redis_storage = RedisTelemetryStorage(redis_client, metadata)
        telemetry_producer = TelemetryStorageProducer(telemetry_redis_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': RedisImpressionsStorage(redis_client, metadata),
            'events': RedisEventsStorage(redis_client, metadata),
        }
        impmanager = ImpressionsManager(StrategyOptimizedMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = PipelinedRecorder(redis_client.pipeline, impmanager,
                                     storages['events'], storages['impressions'], telemetry_redis_storage, unique_keys_tracker=UniqueKeysTracker(), imp_counter=ImpressionsCounter())
        factory = SplitFactory('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init

        try:
            client = factory.client()
        except:
            pass

        assert client.get_treatment('user1', 'SPLIT_1') == 'off'
        assert client.get_treatment('user2', 'SPLIT_2') == 'on'
        assert client.get_treatment('user3', 'SPLIT_3') == 'on'
        time.sleep(0.2)

        imp_storage = factory._storages['impressions']
        impressions = []
        while True:
            impression = redis_client.lpop(imp_storage.IMPRESSIONS_QUEUE_KEY)
            if impression is None:
                break
            impressions.append(json.loads(impression))

        assert len(impressions) == 2
        assert impressions[0]['i']['f'] == 'SPLIT_1'
        assert impressions[1]['i']['f'] == 'SPLIT_2'
        assert client._recorder._unique_keys_tracker._cache == {'SPLIT_3': {'user3'}}
        imps_count = client._recorder._imp_counter.pop_all()
        assert len(imps_count) == 1
        assert imps_count[0].feature == 'SPLIT_3'
        assert imps_count[0].count == 1
        self.clear_cache()
        client.destroy()

    def test_debug(self):
        """Prepare storages with test data."""
        metadata = SdkMetadata('python-1.2.3', 'some_ip', 'some_name')
        redis_client = build(DEFAULT_CONFIG.copy())
        split_storage = RedisSplitStorage(redis_client, True)
        segment_storage = RedisSegmentStorage(redis_client)

        redis_client.set(split_storage._get_key(splits_json['splitChange1_1']['splits'][0]['name']), json.dumps(splits_json['splitChange1_1']['splits'][0]))
        redis_client.set(split_storage._get_key(splits_json['splitChange1_1']['splits'][1]['name']), json.dumps(splits_json['splitChange1_1']['splits'][1]))
        redis_client.set(split_storage._get_key(splits_json['splitChange1_1']['splits'][2]['name']), json.dumps(splits_json['splitChange1_1']['splits'][2]))
        redis_client.set(split_storage._FEATURE_FLAG_TILL_KEY, -1)

        telemetry_redis_storage = RedisTelemetryStorage(redis_client, metadata)
        telemetry_producer = TelemetryStorageProducer(telemetry_redis_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': RedisImpressionsStorage(redis_client, metadata),
            'events': RedisEventsStorage(redis_client, metadata),
        }
        impmanager = ImpressionsManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = PipelinedRecorder(redis_client.pipeline, impmanager,
                                     storages['events'], storages['impressions'], telemetry_redis_storage, unique_keys_tracker=UniqueKeysTracker(), imp_counter=ImpressionsCounter())
        factory = SplitFactory('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init

        try:
            client = factory.client()
        except:
            pass

        assert client.get_treatment('user1', 'SPLIT_1') == 'off'
        assert client.get_treatment('user2', 'SPLIT_2') == 'on'
        assert client.get_treatment('user3', 'SPLIT_3') == 'on'
        time.sleep(0.2)

        imp_storage = factory._storages['impressions']
        impressions = []
        while True:
            impression = redis_client.lpop(imp_storage.IMPRESSIONS_QUEUE_KEY)
            if impression is None:
                break
            impressions.append(json.loads(impression))

        assert len(impressions) == 2
        assert impressions[0]['i']['f'] == 'SPLIT_1'
        assert impressions[1]['i']['f'] == 'SPLIT_2'
        assert client._recorder._unique_keys_tracker._cache == {'SPLIT_3': {'user3'}}
        imps_count = client._recorder._imp_counter.pop_all()
        assert len(imps_count) == 1
        assert imps_count[0].feature == 'SPLIT_3'
        assert imps_count[0].count == 1
        self.clear_cache()
        client.destroy()

    def test_none(self):
        """Prepare storages with test data."""
        metadata = SdkMetadata('python-1.2.3', 'some_ip', 'some_name')
        redis_client = build(DEFAULT_CONFIG.copy())
        split_storage = RedisSplitStorage(redis_client, True)
        segment_storage = RedisSegmentStorage(redis_client)

        redis_client.set(split_storage._get_key(splits_json['splitChange1_1']['splits'][0]['name']), json.dumps(splits_json['splitChange1_1']['splits'][0]))
        redis_client.set(split_storage._get_key(splits_json['splitChange1_1']['splits'][1]['name']), json.dumps(splits_json['splitChange1_1']['splits'][1]))
        redis_client.set(split_storage._get_key(splits_json['splitChange1_1']['splits'][2]['name']), json.dumps(splits_json['splitChange1_1']['splits'][2]))
        redis_client.set(split_storage._FEATURE_FLAG_TILL_KEY, -1)

        telemetry_redis_storage = RedisTelemetryStorage(redis_client, metadata)
        telemetry_producer = TelemetryStorageProducer(telemetry_redis_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': RedisImpressionsStorage(redis_client, metadata),
            'events': RedisEventsStorage(redis_client, metadata),
        }
        impmanager = ImpressionsManager(StrategyNoneMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = PipelinedRecorder(redis_client.pipeline, impmanager,
                                     storages['events'], storages['impressions'], telemetry_redis_storage, unique_keys_tracker=UniqueKeysTracker(), imp_counter=ImpressionsCounter())
        factory = SplitFactory('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init

        try:
            client = factory.client()
        except:
            pass

        assert client.get_treatment('user1', 'SPLIT_1') == 'off'
        assert client.get_treatment('user2', 'SPLIT_2') == 'on'
        assert client.get_treatment('user3', 'SPLIT_3') == 'on'
        time.sleep(0.2)

        imp_storage = factory._storages['impressions']
        impressions = []
        while True:
            impression = redis_client.lpop(imp_storage.IMPRESSIONS_QUEUE_KEY)
            if impression is None:
                break
            impressions.append(json.loads(impression))

        assert len(impressions) == 0
        assert client._recorder._unique_keys_tracker._cache == {'SPLIT_1': {'user1'}, 'SPLIT_2': {'user2'}, 'SPLIT_3': {'user3'}}
        imps_count = client._recorder._imp_counter.pop_all()
        assert len(imps_count) == 3
        assert imps_count[0].feature == 'SPLIT_1'
        assert imps_count[0].count == 1
        assert imps_count[1].feature == 'SPLIT_2'
        assert imps_count[1].count == 1
        assert imps_count[2].feature == 'SPLIT_3'
        assert imps_count[2].count == 1
        self.clear_cache()
        client.destroy()

    def clear_cache(self):
        """Clear redis cache."""
        keys_to_delete = [
            "SPLITIO.split.SPLIT_3",
            "SPLITIO.splits.till",
            "SPLITIO.split.SPLIT_2",
            "SPLITIO.split.SPLIT_1",
            "SPLITIO.telemetry.latencies"
        ]

        redis_client = RedisAdapter(StrictRedis())
        for key in keys_to_delete:
            redis_client.delete(key)

class InMemoryIntegrationAsyncTests(object):
    """Inmemory storage-based integration tests."""

    def setup_method(self):
        self.setup_task = asyncio.get_event_loop().create_task(self._setup_method())

    async def _setup_method(self):
        """Prepare storages with test data."""
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()

        split_fn = os.path.join(os.path.dirname(__file__), 'files', 'splitChanges.json')
        with open(split_fn, 'r') as flo:
            data = json.loads(flo.read())
        for split in data['splits']:
            await split_storage.update([splits.from_raw(split)], [], -1)

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentEmployeesChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        await segment_storage.put(segments.from_raw(data))

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentHumanBeignsChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        await segment_storage.put(segments.from_raw(data))

        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': InMemoryImpressionStorageAsync(5000, telemetry_runtime_producer),
            'events': InMemoryEventStorageAsync(5000, telemetry_runtime_producer),
        }
        impmanager = ImpressionsManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = StandardRecorderAsync(impmanager, storages['events'], storages['impressions'], telemetry_evaluation_producer, telemetry_runtime_producer)
        # Since we are passing None as SDK_Ready event, the factory will use the Redis telemetry call, using try catch to ignore the exception.
        try:
            self.factory = SplitFactoryAsync('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    None,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init
        except:
            pass
        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(self.factory).ready = ready_property

    @pytest.mark.asyncio
    async def test_get_treatment(self):
        """Test client.get_treatment()."""
        await _get_treatment_async(self.factory)

    @pytest.mark.asyncio
    async def test_get_treatment_with_config(self):
        """Test client.get_treatment_with_config()."""
        await _get_treatment_with_config_async(self.factory)

    @pytest.mark.asyncio
    async def test_get_treatments(self):
        await _get_treatments_async(self.factory)
            # testing multiple splitNames
        client = self.factory.client()
        result = await client.get_treatments('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == 'on'
        assert result['killed_feature'] == 'defTreatment'
        assert result['invalid_feature'] == 'control'
        assert result['sample_feature'] == 'off'
        await _validate_last_impressions_async(
            client,
            ('all_feature', 'invalidKey', 'on'),
            ('killed_feature', 'invalidKey', 'defTreatment'),
            ('sample_feature', 'invalidKey', 'off')
        )

    @pytest.mark.asyncio
    async def test_get_treatments_with_config(self):
        """Test client.get_treatments_with_config()."""
        await _get_treatments_with_config_async(self.factory)
        # testing multiple splitNames
        client = self.factory.client()
        result = await client.get_treatments_with_config('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == ('on', None)
        assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
        assert result['invalid_feature'] == ('control', None)
        assert result['sample_feature'] == ('off', None)
        await _validate_last_impressions_async(
            client,
            ('all_feature', 'invalidKey', 'on'),
            ('killed_feature', 'invalidKey', 'defTreatment'),
            ('sample_feature', 'invalidKey', 'off'),
        )

    @pytest.mark.asyncio
    async def test_get_treatments_by_flag_set(self):
        """Test client.get_treatments_by_flag_set()."""
        await _get_treatments_by_flag_set_async(self.factory)

    @pytest.mark.asyncio
    async def test_get_treatments_by_flag_sets(self):
        """Test client.get_treatments_by_flag_sets()."""
        await _get_treatments_by_flag_sets_async(self.factory)
        client = self.factory.client()
        result = await client.get_treatments_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': 'on',
                            'whitelist_feature': 'off',
                            'all_feature': 'on'
                            }
        await _validate_last_impressions_async(client, ('sample_feature', 'user1', 'on'),
                                        ('whitelist_feature', 'user1', 'off'),
                                        ('all_feature', 'user1', 'on')
                                        )

    @pytest.mark.asyncio
    async def test_get_treatments_with_config_by_flag_set(self):
        """Test client.get_treatments_with_config_by_flag_set()."""
        await _get_treatments_with_config_by_flag_set_async(self.factory)

    @pytest.mark.asyncio
    async def test_get_treatments_with_config_by_flag_sets(self):
        """Test client.get_treatments_with_config_by_flag_sets()."""
        await _get_treatments_with_config_by_flag_sets_async(self.factory)
        client = self.factory.client()
        result = await client.get_treatments_with_config_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': ('on', '{"size":15,"test":20}'),
                            'whitelist_feature': ('off', None),
                            'all_feature': ('on', None)
                            }
        await _validate_last_impressions_async(client, ('sample_feature', 'user1', 'on'),
                                        ('whitelist_feature', 'user1', 'off'),
                                        ('all_feature', 'user1', 'on')
                                        )

    @pytest.mark.asyncio
    async def test_track(self):
        """Test client.track()."""
        await _track_async(self.factory)

    @pytest.mark.asyncio
    async def test_manager_methods(self):
        """Test manager.split/splits."""
        await _manager_methods_async(self.factory)
        await self.factory.destroy()

class InMemoryOptimizedIntegrationAsyncTests(object):
    """Inmemory storage-based integration tests."""

    def setup_method(self):
        self.setup_task = asyncio.get_event_loop().create_task(self._setup_method())

    async def _setup_method(self):
        """Prepare storages with test data."""
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()

        split_fn = os.path.join(os.path.dirname(__file__), 'files', 'splitChanges.json')
        with open(split_fn, 'r') as flo:
            data = json.loads(flo.read())
        for split in data['splits']:
            await split_storage.update([splits.from_raw(split)], [], -1)

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentEmployeesChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        await segment_storage.put(segments.from_raw(data))

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentHumanBeignsChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        await segment_storage.put(segments.from_raw(data))

        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': InMemoryImpressionStorageAsync(5000, telemetry_runtime_producer),
            'events': InMemoryEventStorageAsync(5000, telemetry_runtime_producer),
        }
        impmanager = ImpressionsManager(StrategyOptimizedMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = StandardRecorderAsync(impmanager, storages['events'], storages['impressions'], telemetry_evaluation_producer, telemetry_runtime_producer,
                                         imp_counter = ImpressionsCounter())
        # Since we are passing None as SDK_Ready event, the factory will use the Redis telemetry call, using try catch to ignore the exception.
        try:
            self.factory = SplitFactoryAsync('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    None,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init
        except:
            pass
        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(self.factory).ready = ready_property

    @pytest.mark.asyncio
    async def test_get_treatment(self):
        """Test client.get_treatment()."""
        await _get_treatment_async(self.factory)

    @pytest.mark.asyncio
    async def test_get_treatments(self):
        """Test client.get_treatments()."""
        await _get_treatments_async(self.factory)
        # testing multiple splitNames
        client = self.factory.client()
        result = await client.get_treatments('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == 'on'
        assert result['killed_feature'] == 'defTreatment'
        assert result['invalid_feature'] == 'control'
        assert result['sample_feature'] == 'off'
        assert self.factory._storages['impressions']._impressions.qsize() == 0

    @pytest.mark.asyncio
    async def test_get_treatments_with_config(self):
        """Test client.get_treatments_with_config()."""
        await _get_treatments_with_config_async(self.factory)
        # testing multiple splitNames
        client = self.factory.client()
        result = await client.get_treatments_with_config('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == ('on', None)
        assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
        assert result['invalid_feature'] == ('control', None)
        assert result['sample_feature'] == ('off', None)
        await _validate_last_impressions_async(client,)

    @pytest.mark.asyncio
    async def test_get_treatments_by_flag_set(self):
        """Test client.get_treatments_by_flag_set()."""
        await _get_treatments_by_flag_set_async(self.factory)

    @pytest.mark.asyncio
    async def test_get_treatments_by_flag_sets(self):
        """Test client.get_treatments_by_flag_sets()."""
        await _get_treatments_by_flag_sets_async(self.factory)
        client = self.factory.client()
        result = await client.get_treatments_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': 'on',
                          'whitelist_feature': 'off',
                          'all_feature': 'on'
                          }
        await _validate_last_impressions_async(client, )
        assert self.factory._storages['impressions']._impressions.qsize() == 0

    @pytest.mark.asyncio
    async def test_get_treatments_with_config_by_flag_set(self):
        """Test client.get_treatments_with_config_by_flag_set()."""
        await _get_treatments_with_config_by_flag_set_async(self.factory)

    @pytest.mark.asyncio
    async def test_get_treatments_with_config_by_flag_sets(self):
        """Test client.get_treatments_with_config_by_flag_sets()."""
        await _get_treatments_with_config_by_flag_sets_async(self.factory)
        client = self.factory.client()
        result = await client.get_treatments_with_config_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': ('on', '{"size":15,"test":20}'),
                          'whitelist_feature': ('off', None),
                          'all_feature': ('on', None)
                          }
        await _validate_last_impressions_async(client, )

    @pytest.mark.asyncio
    async def test_manager_methods(self):
        """Test manager.split/splits."""
        await _manager_methods_async(self.factory)

    @pytest.mark.asyncio
    async def test_track(self):
        """Test client.track()."""
        await _track_async(self.factory)
        await self.factory.destroy()

class RedisIntegrationAsyncTests(object):
    """Redis storage-based integration tests."""

    def setup_method(self):
        self.setup_task = asyncio.get_event_loop().create_task(self._setup_method())

    async def _setup_method(self):
        """Prepare storages with test data."""
        metadata = SdkMetadata('python-1.2.3', 'some_ip', 'some_name')
        redis_client = await build_async(DEFAULT_CONFIG.copy())
        await self._clear_cache(redis_client)

        split_storage = RedisSplitStorageAsync(redis_client)
        segment_storage = RedisSegmentStorageAsync(redis_client)

        split_fn = os.path.join(os.path.dirname(__file__), 'files', 'splitChanges.json')
        with open(split_fn, 'r') as flo:
            data = json.loads(flo.read())
        for split in data['splits']:
            await redis_client.set(split_storage._get_key(split['name']), json.dumps(split))
            if split.get('sets') is not None:
                for flag_set in split.get('sets'):
                    await redis_client.sadd(split_storage._get_flag_set_key(flag_set), split['name'])

        await redis_client.set(split_storage._FEATURE_FLAG_TILL_KEY, data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentEmployeesChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        await redis_client.sadd(segment_storage._get_key(data['name']), *data['added'])
        await redis_client.set(segment_storage._get_till_key(data['name']), data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentHumanBeignsChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        await redis_client.sadd(segment_storage._get_key(data['name']), *data['added'])
        await redis_client.set(segment_storage._get_till_key(data['name']), data['till'])

        telemetry_redis_storage = await RedisTelemetryStorageAsync.create(redis_client, metadata)
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_redis_storage)
        telemetry_submitter = RedisTelemetrySubmitterAsync(telemetry_redis_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': RedisImpressionsStorageAsync(redis_client, metadata),
            'events': RedisEventsStorageAsync(redis_client, metadata),
        }
        impmanager = ImpressionsManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = PipelinedRecorderAsync(redis_client.pipeline, impmanager, storages['events'],
                                    storages['impressions'], telemetry_redis_storage)
        self.factory = SplitFactoryAsync('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    telemetry_submitter=telemetry_submitter
                                    )  # pylint:disable=attribute-defined-outside-init
        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(self.factory).ready = ready_property

    @pytest.mark.asyncio
    async def test_get_treatment(self):
        """Test client.get_treatment()."""
        await self.setup_task
        await _get_treatment_async(self.factory)
        await self.factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatment_with_config(self):
        """Test client.get_treatment_with_config()."""
        await self.setup_task
        await _get_treatment_with_config_async(self.factory)
        await self.factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments(self):
        # testing multiple splitNames
        await self.setup_task
        await _get_treatments_async(self.factory)
        client = self.factory.client()
        result = await client.get_treatments('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == 'on'
        assert result['killed_feature'] == 'defTreatment'
        assert result['invalid_feature'] == 'control'
        assert result['sample_feature'] == 'off'
        await _validate_last_impressions_async(
            client,
            ('all_feature', 'invalidKey', 'on'),
            ('killed_feature', 'invalidKey', 'defTreatment'),
            ('sample_feature', 'invalidKey', 'off')
        )
        await self.factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_with_config(self):
        """Test client.get_treatments_with_config()."""
        await self.setup_task
        await _get_treatments_with_config_async(self.factory)
        # testing multiple splitNames
        client = self.factory.client()
        result = await client.get_treatments_with_config('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == ('on', None)
        assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
        assert result['invalid_feature'] == ('control', None)
        assert result['sample_feature'] == ('off', None)
        await _validate_last_impressions_async(
            client,
            ('all_feature', 'invalidKey', 'on'),
            ('killed_feature', 'invalidKey', 'defTreatment'),
            ('sample_feature', 'invalidKey', 'off'),
        )
        await self.factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_by_flag_set(self):
        """Test client.get_treatments_by_flag_set()."""
        await self.setup_task
        await _get_treatments_by_flag_set_async(self.factory)
        await self.factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_by_flag_sets(self):
        """Test client.get_treatments_by_flag_sets()."""
        await self.setup_task
        await _get_treatments_by_flag_sets_async(self.factory)
        client = self.factory.client()
        result = await client.get_treatments_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': 'on',
                            'whitelist_feature': 'off',
                            'all_feature': 'on'
                            }
        await _validate_last_impressions_async(client, ('sample_feature', 'user1', 'on'),
                                        ('whitelist_feature', 'user1', 'off'),
                                        ('all_feature', 'user1', 'on')
                                        )
        await self.factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_with_config_by_flag_set(self):
        """Test client.get_treatments_with_config_by_flag_set()."""
        await self.setup_task
        await _get_treatments_with_config_by_flag_set_async(self.factory)
        await self.factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_with_config_by_flag_sets(self):
        """Test client.get_treatments_with_config_by_flag_sets()."""
        await self.setup_task
        await _get_treatments_with_config_by_flag_sets_async(self.factory)
        client = self.factory.client()
        result = await client.get_treatments_with_config_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': ('on', '{"size":15,"test":20}'),
                            'whitelist_feature': ('off', None),
                            'all_feature': ('on', None)
                            }
        await _validate_last_impressions_async(client, ('sample_feature', 'user1', 'on'),
                                        ('whitelist_feature', 'user1', 'off'),
                                        ('all_feature', 'user1', 'on')
                                        )
        await self.factory.destroy()

    @pytest.mark.asyncio
    async def test_track(self):
        """Test client.track()."""
        await self.setup_task
        await _track_async(self.factory)
        await self.factory.destroy()

    @pytest.mark.asyncio
    async def test_manager_methods(self):
        """Test manager.split/splits."""
        await self.setup_task
        await _manager_methods_async(self.factory)
        await self.factory.destroy()
        await self._clear_cache(self.factory._storages['splits'].redis)

    async def _clear_cache(self, redis_client):
        """Clear redis cache."""
        keys_to_delete = [
            "SPLITIO.split.sample_feature",
            "SPLITIO.split.killed_feature",
            "SPLITIO.split.regex_test",
            "SPLITIO.segment.employees",
            "SPLITIO.segment.human_beigns.till",
            "SPLITIO.segment.human_beigns",
            "SPLITIO.impressions",
            "SPLITIO.split.boolean_test",
            "SPLITIO.splits.till",
            "SPLITIO.split.all_feature",
            "SPLITIO.segment.employees.till",
            "SPLITIO.split.whitelist_feature",
            "SPLITIO.telemetry.latencies",
            "SPLITIO.split.dependency_test"
        ]
        for key in keys_to_delete:
            await redis_client.delete(key)

class RedisWithCacheIntegrationAsyncTests(RedisIntegrationAsyncTests):
    """Run the same tests as RedisIntegratioTests but with LRU/Expirable cache overlay."""

    def setup_method(self):
        self.setup_task = asyncio.get_event_loop().create_task(self._setup_method())

    async def _setup_method(self):
        """Prepare storages with test data."""
        metadata = SdkMetadata('python-1.2.3', 'some_ip', 'some_name')
        redis_client = await build_async(DEFAULT_CONFIG.copy())
        await self._clear_cache(redis_client)

        split_storage = RedisSplitStorageAsync(redis_client, True)
        segment_storage = RedisSegmentStorageAsync(redis_client)

        split_fn = os.path.join(os.path.dirname(__file__), 'files', 'splitChanges.json')
        with open(split_fn, 'r') as flo:
            data = json.loads(flo.read())
        for split in data['splits']:
            await redis_client.set(split_storage._get_key(split['name']), json.dumps(split))
            if split.get('sets') is not None:
                for flag_set in split.get('sets'):
                    await redis_client.sadd(split_storage._get_flag_set_key(flag_set), split['name'])
        await redis_client.set(split_storage._FEATURE_FLAG_TILL_KEY, data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentEmployeesChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        await redis_client.sadd(segment_storage._get_key(data['name']), *data['added'])
        await redis_client.set(segment_storage._get_till_key(data['name']), data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentHumanBeignsChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        await redis_client.sadd(segment_storage._get_key(data['name']), *data['added'])
        await redis_client.set(segment_storage._get_till_key(data['name']), data['till'])

        telemetry_redis_storage = await RedisTelemetryStorageAsync.create(redis_client, metadata)
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_redis_storage)
        telemetry_submitter = RedisTelemetrySubmitterAsync(telemetry_redis_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': RedisImpressionsStorageAsync(redis_client, metadata),
            'events': RedisEventsStorageAsync(redis_client, metadata),
        }
        impmanager = ImpressionsManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = PipelinedRecorderAsync(redis_client.pipeline, impmanager, storages['events'],
                                    storages['impressions'], telemetry_redis_storage)
        self.factory = SplitFactoryAsync('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    telemetry_submitter=telemetry_submitter
                                    )  # pylint:disable=attribute-defined-outside-init
        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(self.factory).ready = ready_property


class LocalhostIntegrationAsyncTests(object):  # pylint: disable=too-few-public-methods
    """Client & Manager integration tests."""

    @pytest.mark.asyncio
    async def test_localhost_json_e2e(self):
        """Instantiate a client with a JSON file and issue get_treatment() calls."""
        self._update_temp_file(splits_json['splitChange2_1'])
        filename = os.path.join(os.path.dirname(__file__), 'files', 'split_changes_temp.json')
        self.factory = await get_factory_async('localhost', config={'splitFile': filename})
        await self.factory.block_until_ready(1)
        client = self.factory.client()

        # Tests 2
        assert await self.factory.manager().split_names() == ["SPLIT_1"]
        assert await client.get_treatment("key", "SPLIT_1") == 'off'

        # Tests 1
        await self.factory._storages['splits'].update([], ['SPLIT_1'], -1)
        self._update_temp_file(splits_json['splitChange1_1'])
        await self._synchronize_now()

        assert sorted(await self.factory.manager().split_names()) == ["SPLIT_1", "SPLIT_2", "SPLIT_3"]
        assert await client.get_treatment("key", "SPLIT_1", None) == 'off'
        assert await client.get_treatment("key", "SPLIT_2", None) == 'on'

        self._update_temp_file(splits_json['splitChange1_2'])
        await self._synchronize_now()

        assert sorted(await self.factory.manager().split_names()) == ["SPLIT_1", "SPLIT_2", "SPLIT_3"]
        assert await client.get_treatment("key", "SPLIT_1", None) == 'off'
        assert await client.get_treatment("key", "SPLIT_2", None) == 'off'

        self._update_temp_file(splits_json['splitChange1_3'])
        await self._synchronize_now()

        assert sorted(await self.factory.manager().split_names()) == ["SPLIT_2", "SPLIT_3"]
        assert await client.get_treatment("key", "SPLIT_1", None) == 'control'
        assert await client.get_treatment("key", "SPLIT_2", None) == 'on'

        # Tests 3
        await self.factory._storages['splits'].update([], ['SPLIT_1'], -1)
        self._update_temp_file(splits_json['splitChange3_1'])
        await self._synchronize_now()

        assert sorted(await self.factory.manager().split_names()) == ["SPLIT_2", "SPLIT_3"]
        assert await client.get_treatment("key", "SPLIT_2", None) == 'on'

        self._update_temp_file(splits_json['splitChange3_2'])
        await self._synchronize_now()

        assert sorted(await self.factory.manager().split_names()) == ["SPLIT_2", "SPLIT_3"]
        assert await client.get_treatment("key", "SPLIT_2", None) == 'off'

        # Tests 4
        await self.factory._storages['splits'].update([], ['SPLIT_2'], -1)
        self._update_temp_file(splits_json['splitChange4_1'])
        await self._synchronize_now()

        assert sorted(await self.factory.manager().split_names()) == ["SPLIT_1", "SPLIT_2", "SPLIT_3"]
        assert await client.get_treatment("key", "SPLIT_1", None) == 'off'
        assert await client.get_treatment("key", "SPLIT_2", None) == 'on'

        self._update_temp_file(splits_json['splitChange4_2'])
        await self._synchronize_now()

        assert sorted(await self.factory.manager().split_names()) == ["SPLIT_1", "SPLIT_2", "SPLIT_3"]
        assert await client.get_treatment("key", "SPLIT_1", None) == 'off'
        assert await client.get_treatment("key", "SPLIT_2", None) == 'off'

        self._update_temp_file(splits_json['splitChange4_3'])
        await self._synchronize_now()

        assert sorted(await self.factory.manager().split_names()) == ["SPLIT_2", "SPLIT_3"]
        assert await client.get_treatment("key", "SPLIT_1", None) == 'control'
        assert await client.get_treatment("key", "SPLIT_2", None) == 'on'

        # Tests 5
        await self.factory._storages['splits'].update([], ['SPLIT_1', 'SPLIT_2'], -1)
        self._update_temp_file(splits_json['splitChange5_1'])
        await self._synchronize_now()

        assert sorted(await self.factory.manager().split_names()) == ["SPLIT_2", "SPLIT_3"]
        assert await client.get_treatment("key", "SPLIT_2", None) == 'on'

        self._update_temp_file(splits_json['splitChange5_2'])
        await self._synchronize_now()

        assert sorted(await self.factory.manager().split_names()) == ["SPLIT_2", "SPLIT_3"]
        assert await client.get_treatment("key", "SPLIT_2", None) == 'on'

        # Tests 6
        await self.factory._storages['splits'].update([], ['SPLIT_2'], -1)
        self._update_temp_file(splits_json['splitChange6_1'])
        await self._synchronize_now()

        assert sorted(await self.factory.manager().split_names()) == ["SPLIT_1", "SPLIT_2", "SPLIT_3"]
        assert await client.get_treatment("key", "SPLIT_1", None) == 'off'
        assert await client.get_treatment("key", "SPLIT_2", None) == 'on'

        self._update_temp_file(splits_json['splitChange6_2'])
        await self._synchronize_now()

        assert sorted(await self.factory.manager().split_names()) == ["SPLIT_1", "SPLIT_2", "SPLIT_3"]
        assert await client.get_treatment("key", "SPLIT_1", None) == 'off'
        assert await client.get_treatment("key", "SPLIT_2", None) == 'off'

        self._update_temp_file(splits_json['splitChange6_3'])
        await self._synchronize_now()

        assert sorted(await self.factory.manager().split_names()) == ["SPLIT_2", "SPLIT_3"]
        assert await client.get_treatment("key", "SPLIT_1", None) == 'control'
        assert await client.get_treatment("key", "SPLIT_2", None) == 'on'

    def _update_temp_file(self, json_body):
        f = open(os.path.join(os.path.dirname(__file__), 'files','split_changes_temp.json'), 'w')
        f.write(json.dumps(json_body))
        f.close()

    async def _synchronize_now(self):
        filename = os.path.join(os.path.dirname(__file__), 'files', 'split_changes_temp.json')
        self.factory._sync_manager._synchronizer._split_synchronizers._feature_flag_sync._filename = filename
        await self.factory._sync_manager._synchronizer._split_synchronizers._feature_flag_sync.synchronize_splits()

    @pytest.mark.asyncio
    async def test_incorrect_file_e2e(self):
        """Test initialize factory with a incorrect file name."""
        # TODO: secontion below is removed when legacu use BUR
        # legacy and yaml
        exception_raised = False
        factory = None
        try:
            factory = await get_factory_async('localhost', config={'splitFile': 'filename'})
        except Exception as e:
            exception_raised = True

        assert(exception_raised)

        # json using BUR
        factory = await get_factory_async('localhost', config={'splitFile': 'filename.json'})
        exception_raised = False
        try:
            await factory.block_until_ready(1)
        except Exception as e:
            exception_raised = True

        assert(exception_raised)
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_localhost_e2e(self):
        """Instantiate a client with a YAML file and issue get_treatment() calls."""
        filename = os.path.join(os.path.dirname(__file__), 'files', 'file2.yaml')
        factory = await get_factory_async('localhost', config={'splitFile': filename})
        await factory.block_until_ready()
        client = factory.client()
        assert await client.get_treatment_with_config('key', 'my_feature') == ('on', '{"desc" : "this applies only to ON treatment"}')
        assert await client.get_treatment_with_config('only_key', 'my_feature') == (
            'off', '{"desc" : "this applies only to OFF and only for only_key. The rest will receive ON"}'
        )
        assert await client.get_treatment_with_config('another_key', 'my_feature') == ('control', None)
        assert await client.get_treatment_with_config('key2', 'other_feature') == ('on', None)
        assert await client.get_treatment_with_config('key3', 'other_feature') == ('on', None)
        assert await client.get_treatment_with_config('some_key', 'other_feature_2') == ('on', None)
        assert await client.get_treatment_with_config('key_whitelist', 'other_feature_3') == ('on', None)
        assert await client.get_treatment_with_config('any_other_key', 'other_feature_3') == ('off', None)

        manager = factory.manager()
        split = await manager.split('my_feature')
        assert split.configs == {
            'on': '{"desc" : "this applies only to ON treatment"}',
            'off': '{"desc" : "this applies only to OFF and only for only_key. The rest will receive ON"}'
        }
        split = await manager.split('other_feature')
        assert split.configs == {}
        split = await manager.split('other_feature_2')
        assert split.configs == {}
        split = await manager.split('other_feature_3')
        assert split.configs == {}
        await factory.destroy()


class PluggableIntegrationAsyncTests(object):
    """Pluggable storage-based integration tests."""
    def setup_method(self):
        self.setup_task = asyncio.get_event_loop().create_task(self._setup_method())

    async def _setup_method(self):
        """Prepare storages with test data."""
        metadata = SdkMetadata('python-1.2.3', 'some_ip', 'some_name')
        self.pluggable_storage_adapter = StorageMockAdapterAsync()
        split_storage = PluggableSplitStorageAsync(self.pluggable_storage_adapter, 'myprefix')
        segment_storage = PluggableSegmentStorageAsync(self.pluggable_storage_adapter, 'myprefix')

        telemetry_pluggable_storage = await PluggableTelemetryStorageAsync.create(self.pluggable_storage_adapter, metadata, 'myprefix')
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_pluggable_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_submitter = RedisTelemetrySubmitterAsync(telemetry_pluggable_storage)

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': PluggableImpressionsStorageAsync(self.pluggable_storage_adapter, metadata),
            'events': PluggableEventsStorageAsync(self.pluggable_storage_adapter, metadata),
            'telemetry': telemetry_pluggable_storage
        }

        impmanager = ImpressionsManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = StandardRecorderAsync(impmanager, storages['events'],
                                    storages['impressions'],
                                    telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_runtime_producer)

        self.factory = SplitFactoryAsync('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    RedisManagerAsync(PluggableSynchronizerAsync()),
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    telemetry_submitter=telemetry_submitter
                                    )  # pylint:disable=attribute-defined-outside-init
        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(self.factory).ready = ready_property

        # Adding data to storage
        split_fn = os.path.join(os.path.dirname(__file__), 'files', 'splitChanges.json')
        with open(split_fn, 'r') as flo:
            data = json.loads(flo.read())
        for split in data['splits']:
            await self.pluggable_storage_adapter.set(split_storage._prefix.format(feature_flag_name=split['name']), split)
            for flag_set in split.get('sets'):
                await self.pluggable_storage_adapter.push_items(split_storage._flag_set_prefix.format(flag_set=flag_set), split['name'])
        await self.pluggable_storage_adapter.set(split_storage._feature_flag_till_prefix, data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentEmployeesChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        await self.pluggable_storage_adapter.set(segment_storage._prefix.format(segment_name=data['name']), set(data['added']))
        await self.pluggable_storage_adapter.set(segment_storage._segment_till_prefix.format(segment_name=data['name']), data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentHumanBeignsChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        await self.pluggable_storage_adapter.set(segment_storage._prefix.format(segment_name=data['name']), set(data['added']))
        await self.pluggable_storage_adapter.set(segment_storage._segment_till_prefix.format(segment_name=data['name']), data['till'])
        await self.factory.block_until_ready(1)

    @pytest.mark.asyncio
    async def test_get_treatment(self):
        """Test client.get_treatment()."""
        await self.setup_task
        await _get_treatment_async(self.factory)
        await self.factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatment_with_config(self):
        """Test client.get_treatment_with_config()."""
        await self.setup_task
        await _get_treatment_with_config_async(self.factory)
        await self.factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments(self):
        # testing multiple splitNames
        await self.setup_task
        await _get_treatments_async(self.factory)
        client = self.factory.client()
        result = await client.get_treatments('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == 'on'
        assert result['killed_feature'] == 'defTreatment'
        assert result['invalid_feature'] == 'control'
        assert result['sample_feature'] == 'off'
        await _validate_last_impressions_async(
            client,
            ('all_feature', 'invalidKey', 'on'),
            ('killed_feature', 'invalidKey', 'defTreatment'),
            ('sample_feature', 'invalidKey', 'off')
        )
        await self.factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_with_config(self):
        """Test client.get_treatments_with_config()."""
        await self.setup_task
        await _get_treatments_with_config_async(self.factory)
        # testing multiple splitNames
        client = self.factory.client()
        result = await client.get_treatments_with_config('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == ('on', None)
        assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
        assert result['invalid_feature'] == ('control', None)
        assert result['sample_feature'] == ('off', None)
        await _validate_last_impressions_async(
            client,
            ('all_feature', 'invalidKey', 'on'),
            ('killed_feature', 'invalidKey', 'defTreatment'),
            ('sample_feature', 'invalidKey', 'off'),
        )
        await self.factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_by_flag_set(self):
        """Test client.get_treatments_by_flag_set()."""
        await self.setup_task
        await _get_treatments_by_flag_set_async(self.factory)
        await self.factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_by_flag_sets(self):
        """Test client.get_treatments_by_flag_sets()."""
        await self.setup_task
        await _get_treatments_by_flag_sets_async(self.factory)
        client = self.factory.client()
        result = await client.get_treatments_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': 'on',
                            'whitelist_feature': 'off',
                            'all_feature': 'on'
                            }
        await _validate_last_impressions_async(client, ('sample_feature', 'user1', 'on'),
                                        ('whitelist_feature', 'user1', 'off'),
                                        ('all_feature', 'user1', 'on')
                                        )
        await self.factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_with_config_by_flag_set(self):
        """Test client.get_treatments_with_config_by_flag_set()."""
        await self.setup_task
        await _get_treatments_with_config_by_flag_set_async(self.factory)
        await self.factory.destroy()

    @pytest.mark.asyncio
    async def test_get_treatments_with_config_by_flag_sets(self):
        """Test client.get_treatments_with_config_by_flag_sets()."""
        await self.setup_task
        await _get_treatments_with_config_by_flag_sets_async(self.factory)
        client = self.factory.client()
        result = await client.get_treatments_with_config_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': ('on', '{"size":15,"test":20}'),
                            'whitelist_feature': ('off', None),
                            'all_feature': ('on', None)
                            }
        await _validate_last_impressions_async(client, ('sample_feature', 'user1', 'on'),
                                        ('whitelist_feature', 'user1', 'off'),
                                        ('all_feature', 'user1', 'on')
                                        )
        await self.factory.destroy()
        await self._teardown_method()

    @pytest.mark.asyncio
    async def test_track(self):
        """Test client.track()."""
        await self.setup_task
        await _track_async(self.factory)
        await self.factory.destroy()
        await self._teardown_method()

    @pytest.mark.asyncio
    async def test_manager_methods(self):
        """Test manager.split/splits."""
        await self.setup_task
        await _manager_methods_async(self.factory)
        await self.factory.destroy()
        await self._teardown_method()

    async def _teardown_method(self):
        """Clear pluggable cache."""
        keys_to_delete = [
            "SPLITIO.segment.human_beigns",
            "SPLITIO.segment.employees.till",
            "SPLITIO.split.sample_feature",
            "SPLITIO.splits.till",
            "SPLITIO.split.killed_feature",
            "SPLITIO.split.all_feature",
            "SPLITIO.split.whitelist_feature",
            "SPLITIO.segment.employees",
            "SPLITIO.split.regex_test",
            "SPLITIO.segment.human_beigns.till",
            "SPLITIO.split.boolean_test",
            "SPLITIO.split.dependency_test"
        ]

        for key in keys_to_delete:
            await self.pluggable_storage_adapter.delete(key)


class PluggableOptimizedIntegrationAsyncTests(object):
    """Pluggable storage-based optimized integration tests."""
    def setup_method(self):
        self.setup_task = asyncio.get_event_loop().create_task(self._setup_method())

    async def _setup_method(self):
        """Prepare storages with test data."""
        metadata = SdkMetadata('python-1.2.3', 'some_ip', 'some_name')
        self.pluggable_storage_adapter = StorageMockAdapterAsync()
        split_storage = PluggableSplitStorageAsync(self.pluggable_storage_adapter)
        segment_storage = PluggableSegmentStorageAsync(self.pluggable_storage_adapter)

        telemetry_pluggable_storage = await PluggableTelemetryStorageAsync.create(self.pluggable_storage_adapter, metadata)
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_pluggable_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_submitter = RedisTelemetrySubmitterAsync(telemetry_pluggable_storage)

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': PluggableImpressionsStorageAsync(self.pluggable_storage_adapter, metadata),
            'events': PluggableEventsStorageAsync(self.pluggable_storage_adapter, metadata),
            'telemetry': telemetry_pluggable_storage
        }

        impmanager = ImpressionsManager(StrategyOptimizedMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = StandardRecorderAsync(impmanager, storages['events'],
                                    storages['impressions'],
                                    telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_runtime_producer,
                                    imp_counter=ImpressionsCounter())

        self.factory = SplitFactoryAsync('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    RedisManagerAsync(PluggableSynchronizerAsync()),
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    telemetry_submitter=telemetry_submitter
                                    )  # pylint:disable=attribute-defined-outside-init

        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(self.factory).ready = ready_property

        # Adding data to storage
        split_fn = os.path.join(os.path.dirname(__file__), 'files', 'splitChanges.json')
        with open(split_fn, 'r') as flo:
            data = json.loads(flo.read())
        for split in data['splits']:
            await self.pluggable_storage_adapter.set(split_storage._prefix.format(feature_flag_name=split['name']), split)
            for flag_set in split.get('sets'):
                await self.pluggable_storage_adapter.push_items(split_storage._flag_set_prefix.format(flag_set=flag_set), split['name'])
        await self.pluggable_storage_adapter.set(split_storage._feature_flag_till_prefix, data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentEmployeesChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        await self.pluggable_storage_adapter.set(segment_storage._prefix.format(segment_name=data['name']), set(data['added']))
        await self.pluggable_storage_adapter.set(segment_storage._segment_till_prefix.format(segment_name=data['name']), data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentHumanBeignsChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        await self.pluggable_storage_adapter.set(segment_storage._prefix.format(segment_name=data['name']), set(data['added']))
        await self.pluggable_storage_adapter.set(segment_storage._segment_till_prefix.format(segment_name=data['name']), data['till'])
        await self.factory.block_until_ready(1)

    @pytest.mark.asyncio
    async def test_get_treatment(self):
        """Test client.get_treatment()."""
        await self.setup_task
        await _get_treatment_async(self.factory)
        await self.factory.destroy()
        await self._teardown_method()

    @pytest.mark.asyncio
    async def test_get_treatments(self):
        """Test client.get_treatments()."""
        await self.setup_task
        await _get_treatments_async(self.factory)
        # testing multiple splitNames
        client = self.factory.client()
        result = await client.get_treatments('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == 'on'
        assert result['killed_feature'] == 'defTreatment'
        assert result['invalid_feature'] == 'control'
        assert result['sample_feature'] == 'off'
        assert len(self.pluggable_storage_adapter._keys['SPLITIO.impressions']) == 0
        await self.factory.destroy()
        await self._teardown_method()

    @pytest.mark.asyncio
    async def test_get_treatments_with_config(self):
        """Test client.get_treatments_with_config()."""
        await self.setup_task
        await _get_treatments_with_config_async(self.factory)
        # testing multiple splitNames
        client = self.factory.client()
        result = await client.get_treatments_with_config('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == ('on', None)
        assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
        assert result['invalid_feature'] == ('control', None)
        assert result['sample_feature'] == ('off', None)
        await _validate_last_impressions_async(client,)
        await self.factory.destroy()
        await self._teardown_method()

    @pytest.mark.asyncio
    async def test_get_treatments_by_flag_set(self):
        """Test client.get_treatments_by_flag_set()."""
        await self.setup_task
        await _get_treatments_by_flag_set_async(self.factory)
        await self.factory.destroy()
        await self._teardown_method()

    @pytest.mark.asyncio
    async def test_get_treatments_by_flag_sets(self):
        """Test client.get_treatments_by_flag_sets()."""
        await self.setup_task
        await _get_treatments_by_flag_sets_async(self.factory)
        client = self.factory.client()
        result = await client.get_treatments_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': 'on',
                          'whitelist_feature': 'off',
                          'all_feature': 'on'
                          }
        await _validate_last_impressions_async(client, )
        assert self.pluggable_storage_adapter._keys.get('SPLITIO.impressions') == None
        await self.factory.destroy()
        await self._teardown_method()

    @pytest.mark.asyncio
    async def test_get_treatments_with_config_by_flag_set(self):
        """Test client.get_treatments_with_config_by_flag_set()."""
        await self.setup_task
        await _get_treatments_with_config_by_flag_set_async(self.factory)
        await self.factory.destroy()
        await self._teardown_method()

    @pytest.mark.asyncio
    async def test_get_treatments_with_config_by_flag_sets(self):
        """Test client.get_treatments_with_config_by_flag_sets()."""
        await self.setup_task
        await _get_treatments_with_config_by_flag_sets_async(self.factory)
        client = self.factory.client()
        result = await client.get_treatments_with_config_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': ('on', '{"size":15,"test":20}'),
                          'whitelist_feature': ('off', None),
                          'all_feature': ('on', None)
                          }
        await _validate_last_impressions_async(client, )
        await self.factory.destroy()
        await self._teardown_method()

    @pytest.mark.asyncio
    async def test_manager_methods(self):
        """Test manager.split/splits."""
        await self.setup_task
        await _manager_methods_async(self.factory)
        await self.factory.destroy()
        await self._teardown_method()

    @pytest.mark.asyncio
    async def test_track(self):
        """Test client.track()."""
        await self.setup_task
        await _track_async(self.factory)
        await self.factory.destroy()
        await self._teardown_method()

    async def _teardown_method(self):
        """Clear pluggable cache."""
        keys_to_delete = [
            "SPLITIO.segment.human_beigns",
            "SPLITIO.segment.employees.till",
            "SPLITIO.split.sample_feature",
            "SPLITIO.splits.till",
            "SPLITIO.split.killed_feature",
            "SPLITIO.split.all_feature",
            "SPLITIO.split.whitelist_feature",
            "SPLITIO.segment.employees",
            "SPLITIO.split.regex_test",
            "SPLITIO.segment.human_beigns.till",
            "SPLITIO.split.boolean_test",
            "SPLITIO.split.dependency_test"
        ]

        for key in keys_to_delete:
            await self.pluggable_storage_adapter.delete(key)

class PluggableNoneIntegrationAsyncTests(object):
    """Pluggable storage-based integration tests."""

    def setup_method(self):
        self.setup_task = asyncio.get_event_loop().create_task(self._setup_method())

    async def _setup_method(self):
        """Prepare storages with test data."""
        metadata = SdkMetadata('python-1.2.3', 'some_ip', 'some_name')
        self.pluggable_storage_adapter = StorageMockAdapterAsync()
        split_storage = PluggableSplitStorageAsync(self.pluggable_storage_adapter)
        segment_storage = PluggableSegmentStorageAsync(self.pluggable_storage_adapter)

        telemetry_pluggable_storage = await PluggableTelemetryStorageAsync.create(self.pluggable_storage_adapter, metadata)
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_pluggable_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': PluggableImpressionsStorageAsync(self.pluggable_storage_adapter, metadata),
            'events': PluggableEventsStorageAsync(self.pluggable_storage_adapter, metadata),
            'telemetry': telemetry_pluggable_storage
        }
        imp_counter = ImpressionsCounter()
        unique_keys_tracker = UniqueKeysTrackerAsync()
        unique_keys_synchronizer, clear_filter_sync, self.unique_keys_task, \
        clear_filter_task, impressions_count_sync, impressions_count_task, \
        imp_strategy, none_strategy = set_classes_async('PLUGGABLE', ImpressionsMode.NONE, self.pluggable_storage_adapter, imp_counter, unique_keys_tracker)
        impmanager = ImpressionsManager(imp_strategy, none_strategy, telemetry_runtime_producer) # no listener

        recorder = StandardRecorderAsync(impmanager, storages['events'],
                                    storages['impressions'], telemetry_evaluation_producer, telemetry_runtime_producer, unique_keys_tracker=unique_keys_tracker, imp_counter=imp_counter)

        synchronizers = SplitSynchronizers(None, None, None, None,
            impressions_count_sync,
            None,
            unique_keys_synchronizer,
            clear_filter_sync
        )

        tasks = SplitTasks(None, None, None, None,
            impressions_count_task,
            None,
            self.unique_keys_task,
            clear_filter_task
        )

        synchronizer = RedisSynchronizerAsync(synchronizers, tasks)

        manager = RedisManagerAsync(synchronizer)
        manager.start()
        self.factory = SplitFactoryAsync('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    manager,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init

        # Adding data to storage
        split_fn = os.path.join(os.path.dirname(__file__), 'files', 'splitChanges.json')
        with open(split_fn, 'r') as flo:
            data = json.loads(flo.read())
        for split in data['splits']:
            await self.pluggable_storage_adapter.set(split_storage._prefix.format(feature_flag_name=split['name']), split)
            for flag_set in split.get('sets'):
                await self.pluggable_storage_adapter.push_items(split_storage._flag_set_prefix.format(flag_set=flag_set), split['name'])
        await self.pluggable_storage_adapter.set(split_storage._feature_flag_till_prefix, data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentEmployeesChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        await self.pluggable_storage_adapter.set(segment_storage._prefix.format(segment_name=data['name']), set(data['added']))
        await self.pluggable_storage_adapter.set(segment_storage._segment_till_prefix.format(segment_name=data['name']), data['till'])

        segment_fn = os.path.join(os.path.dirname(__file__), 'files', 'segmentHumanBeignsChanges.json')
        with open(segment_fn, 'r') as flo:
            data = json.loads(flo.read())
        await self.pluggable_storage_adapter.set(segment_storage._prefix.format(segment_name=data['name']), set(data['added']))
        await self.pluggable_storage_adapter.set(segment_storage._segment_till_prefix.format(segment_name=data['name']), data['till'])
        await self.factory.block_until_ready(1)

    @pytest.mark.asyncio
    async def test_get_treatment(self):
        """Test client.get_treatment()."""
        await self.setup_task
        await _get_treatment_async(self.factory)
        assert self.pluggable_storage_adapter._keys['SPLITIO.impressions'] == []
        await self.factory.destroy()
        await self._teardown_method()

    @pytest.mark.asyncio
    async def test_get_treatments(self):
        """Test client.get_treatments()."""
        await self.setup_task
        await _get_treatments_async(self.factory)
        client = self.factory.client()
        result = await client.get_treatments('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == 'on'
        assert result['killed_feature'] == 'defTreatment'
        assert result['invalid_feature'] == 'control'
        assert result['sample_feature'] == 'off'
        assert self.pluggable_storage_adapter._keys['SPLITIO.impressions'] == []
        await self.factory.destroy()
        await self._teardown_method()

    @pytest.mark.asyncio
    async def test_get_treatments_with_config(self):
        """Test client.get_treatments_with_config()."""
        await self.setup_task
        await _get_treatments_with_config_async(self.factory)
        client = self.factory.client()
        result = await client.get_treatments_with_config('invalidKey', [
            'all_feature',
            'killed_feature',
            'invalid_feature',
            'sample_feature'
        ])
        assert len(result) == 4
        assert result['all_feature'] == ('on', None)
        assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
        assert result['invalid_feature'] == ('control', None)
        assert result['sample_feature'] == ('off', None)
        assert self.pluggable_storage_adapter._keys['SPLITIO.impressions'] == []
        await self.factory.destroy()
        await self._teardown_method()

    @pytest.mark.asyncio
    async def test_get_treatments_by_flag_set(self):
        """Test client.get_treatments_by_flag_set()."""
        await self.setup_task
        await _get_treatments_by_flag_set_async(self.factory)
        assert self.pluggable_storage_adapter._keys['SPLITIO.impressions'] == []
        await self.factory.destroy()
        await self._teardown_method()

    @pytest.mark.asyncio
    async def test_get_treatments_by_flag_sets(self):
        """Test client.get_treatments_by_flag_sets()."""
        await self.setup_task
        await _get_treatments_by_flag_sets_async(self.factory)
        client = self.factory.client()
        result = await client.get_treatments_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': 'on',
                            'whitelist_feature': 'off',
                            'all_feature': 'on'
                            }
        assert self.pluggable_storage_adapter._keys['SPLITIO.impressions'] == []
        await self.factory.destroy()
        await self._teardown_method()

    @pytest.mark.asyncio
    async def test_get_treatments_with_config_by_flag_set(self):
        """Test client.get_treatments_with_config_by_flag_set()."""
        await self.setup_task
        await _get_treatments_with_config_by_flag_set_async(self.factory)
        assert self.pluggable_storage_adapter._keys['SPLITIO.impressions'] == []
        await self.factory.destroy()
        await self._teardown_method()

    @pytest.mark.asyncio
    async def test_get_treatments_with_config_by_flag_sets(self):
        """Test client.get_treatments_with_config_by_flag_sets()."""
        await self.setup_task
        await _get_treatments_with_config_by_flag_sets_async(self.factory)
        client = self.factory.client()
        result = await client.get_treatments_with_config_by_flag_sets('user1', ['set1', 'set2', 'set4'])
        assert len(result) == 3
        assert result == {'sample_feature': ('on', '{"size":15,"test":20}'),
                            'whitelist_feature': ('off', None),
                            'all_feature': ('on', None)
                            }
        assert self.pluggable_storage_adapter._keys['SPLITIO.impressions'] == []
        await self.factory.destroy()
        await self._teardown_method()

    @pytest.mark.asyncio
    async def test_track(self):
        """Test client.track()."""
        await self.setup_task
        await _track_async(self.factory)
        await self.factory.destroy()
        await self._teardown_method()

    @pytest.mark.asyncio
    async def test_mtk(self):
        await self.setup_task
        client = self.factory.client()
        await client.get_treatment('user1', 'sample_feature')
        await client.get_treatment('invalidKey', 'sample_feature')
        await client.get_treatment('invalidKey2', 'sample_feature')
        await client.get_treatment('user22', 'invalidFeature')
        self.unique_keys_task._task.force_execution()
        await asyncio.sleep(1)

        assert(json.loads(self.pluggable_storage_adapter._keys['SPLITIO.uniquekeys'][0])["f"] =="sample_feature")
        assert(json.loads(self.pluggable_storage_adapter._keys['SPLITIO.uniquekeys'][0])["ks"].sort() ==
               ["invalidKey2", "invalidKey", "user1"].sort())
        await self.factory.destroy()
        await self._teardown_method()

    async def _teardown_method(self):
        """Clear pluggable cache."""
        keys_to_delete = [
            "SPLITIO.segment.human_beigns",
            "SPLITIO.segment.employees.till",
            "SPLITIO.split.sample_feature",
            "SPLITIO.splits.till",
            "SPLITIO.split.killed_feature",
            "SPLITIO.split.all_feature",
            "SPLITIO.split.whitelist_feature",
            "SPLITIO.segment.employees",
            "SPLITIO.split.regex_test",
            "SPLITIO.segment.human_beigns.till",
            "SPLITIO.split.boolean_test",
            "SPLITIO.split.dependency_test"
        ]

        for key in keys_to_delete:
            await self.pluggable_storage_adapter.delete(key)

class InMemoryImpressionsToggleIntegrationAsyncTests(object):
    """InMemory storage-based impressions toggle integration tests."""

    @pytest.mark.asyncio
    async def test_optimized(self):
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()

        await split_storage.update([splits.from_raw(splits_json['splitChange1_1']['splits'][0]),
                              splits.from_raw(splits_json['splitChange1_1']['splits'][1]),
                              splits.from_raw(splits_json['splitChange1_1']['splits'][2])
                             ], [], -1)

        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': InMemoryImpressionStorageAsync(5000, telemetry_runtime_producer),
            'events': InMemoryEventStorageAsync(5000, telemetry_runtime_producer),
        }
        impmanager = ImpressionsManager(StrategyOptimizedMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = StandardRecorderAsync(impmanager, storages['events'], storages['impressions'], telemetry_evaluation_producer, telemetry_runtime_producer, None, UniqueKeysTrackerAsync(), ImpressionsCounter())
        # Since we are passing None as SDK_Ready event, the factory will use the Redis telemetry call, using try catch to ignore the exception.
        try:
            factory = SplitFactoryAsync('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    None,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init
        except:
            pass
        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(factory).ready = ready_property

        try:
            client = factory.client()
        except:
            pass

        assert await client.get_treatment('user1', 'SPLIT_1') == 'off'
        assert await client.get_treatment('user1', 'SPLIT_2') == 'on'
        assert await client.get_treatment('user1', 'SPLIT_3') == 'on'
        imp_storage = client._factory._get_storage('impressions')
        impressions = await imp_storage.pop_many(10)
        assert len(impressions) == 2
        assert impressions[0].feature_name == 'SPLIT_1'
        assert impressions[1].feature_name == 'SPLIT_2'
        assert client._recorder._unique_keys_tracker._cache == {'SPLIT_3': {'user1'}}
        imps_count = client._recorder._imp_counter.pop_all()
        assert len(imps_count) == 1
        assert imps_count[0].feature == 'SPLIT_3'
        assert imps_count[0].count == 1
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_debug(self):
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()

        await split_storage.update([splits.from_raw(splits_json['splitChange1_1']['splits'][0]),
                              splits.from_raw(splits_json['splitChange1_1']['splits'][1]),
                              splits.from_raw(splits_json['splitChange1_1']['splits'][2])
                             ], [], -1)

        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': InMemoryImpressionStorageAsync(5000, telemetry_runtime_producer),
            'events': InMemoryEventStorageAsync(5000, telemetry_runtime_producer),
        }
        impmanager = ImpressionsManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = StandardRecorderAsync(impmanager, storages['events'], storages['impressions'], telemetry_evaluation_producer, telemetry_runtime_producer, None, UniqueKeysTrackerAsync(), ImpressionsCounter())
        # Since we are passing None as SDK_Ready event, the factory will use the Redis telemetry call, using try catch to ignore the exception.
        try:
            factory = SplitFactoryAsync('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    None,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init
        except:
            pass
        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(factory).ready = ready_property

        try:
            client = factory.client()
        except:
            pass

        assert await client.get_treatment('user1', 'SPLIT_1') == 'off'
        assert await client.get_treatment('user1', 'SPLIT_2') == 'on'
        assert await client.get_treatment('user1', 'SPLIT_3') == 'on'
        imp_storage = client._factory._get_storage('impressions')
        impressions = await imp_storage.pop_many(10)
        assert len(impressions) == 2
        assert impressions[0].feature_name == 'SPLIT_1'
        assert impressions[1].feature_name == 'SPLIT_2'
        assert client._recorder._unique_keys_tracker._cache == {'SPLIT_3': {'user1'}}
        imps_count = client._recorder._imp_counter.pop_all()
        assert len(imps_count) == 1
        assert imps_count[0].feature == 'SPLIT_3'
        assert imps_count[0].count == 1
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_none(self):
        split_storage = InMemorySplitStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()

        await split_storage.update([splits.from_raw(splits_json['splitChange1_1']['splits'][0]),
                              splits.from_raw(splits_json['splitChange1_1']['splits'][1]),
                              splits.from_raw(splits_json['splitChange1_1']['splits'][2])
                             ], [], -1)

        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': InMemoryImpressionStorageAsync(5000, telemetry_runtime_producer),
            'events': InMemoryEventStorageAsync(5000, telemetry_runtime_producer),
        }
        impmanager = ImpressionsManager(StrategyNoneMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = StandardRecorderAsync(impmanager, storages['events'], storages['impressions'], telemetry_evaluation_producer, telemetry_runtime_producer, None, UniqueKeysTrackerAsync(), ImpressionsCounter())
        # Since we are passing None as SDK_Ready event, the factory will use the Redis telemetry call, using try catch to ignore the exception.
        try:
            factory = SplitFactoryAsync('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    None,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init
        except:
            pass
        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(factory).ready = ready_property

        try:
            client = factory.client()
        except:
            pass

        assert await client.get_treatment('user1', 'SPLIT_1') == 'off'
        assert await client.get_treatment('user1', 'SPLIT_2') == 'on'
        assert await client.get_treatment('user1', 'SPLIT_3') == 'on'
        imp_storage = client._factory._get_storage('impressions')
        impressions = await imp_storage.pop_many(10)
        assert len(impressions) == 0
        assert client._recorder._unique_keys_tracker._cache == {'SPLIT_1': {'user1'}, 'SPLIT_2': {'user1'}, 'SPLIT_3': {'user1'}}
        imps_count = client._recorder._imp_counter.pop_all()
        assert len(imps_count) == 3
        assert imps_count[0].feature == 'SPLIT_1'
        assert imps_count[0].count == 1
        assert imps_count[1].feature == 'SPLIT_2'
        assert imps_count[1].count == 1
        assert imps_count[2].feature == 'SPLIT_3'
        assert imps_count[2].count == 1
        await factory.destroy()

class RedisImpressionsToggleIntegrationAsyncTests(object):
    """Run impression toggle tests for Redis."""

    @pytest.mark.asyncio
    async def test_optimized(self):
        """Prepare storages with test data."""
        metadata = SdkMetadata('python-1.2.3', 'some_ip', 'some_name')
        redis_client = await build_async(DEFAULT_CONFIG.copy())
        split_storage = RedisSplitStorageAsync(redis_client, True)
        segment_storage = RedisSegmentStorageAsync(redis_client)

        await redis_client.set(split_storage._get_key(splits_json['splitChange1_1']['splits'][0]['name']), json.dumps(splits_json['splitChange1_1']['splits'][0]))
        await redis_client.set(split_storage._get_key(splits_json['splitChange1_1']['splits'][1]['name']), json.dumps(splits_json['splitChange1_1']['splits'][1]))
        await redis_client.set(split_storage._get_key(splits_json['splitChange1_1']['splits'][2]['name']), json.dumps(splits_json['splitChange1_1']['splits'][2]))
        await redis_client.set(split_storage._FEATURE_FLAG_TILL_KEY, -1)

        telemetry_redis_storage = await RedisTelemetryStorageAsync.create(redis_client, metadata)
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_redis_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': RedisImpressionsStorageAsync(redis_client, metadata),
            'events': RedisEventsStorageAsync(redis_client, metadata),
        }
        impmanager = ImpressionsManager(StrategyOptimizedMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = PipelinedRecorderAsync(redis_client.pipeline, impmanager,
                                     storages['events'], storages['impressions'], telemetry_redis_storage, unique_keys_tracker=UniqueKeysTracker(), imp_counter=ImpressionsCounter())
        factory = SplitFactoryAsync('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init
        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(factory).ready = ready_property

        try:
            client = factory.client()
        except:
            pass

        assert await client.get_treatment('user1', 'SPLIT_1') == 'off'
        assert await client.get_treatment('user2', 'SPLIT_2') == 'on'
        assert await client.get_treatment('user3', 'SPLIT_3') == 'on'
        await asyncio.sleep(0.2)

        imp_storage = factory._storages['impressions']
        impressions = []
        while True:
            impression = await redis_client.lpop(imp_storage.IMPRESSIONS_QUEUE_KEY)
            if impression is None:
                break
            impressions.append(json.loads(impression))

        assert len(impressions) == 2
        assert impressions[0]['i']['f'] == 'SPLIT_1'
        assert impressions[1]['i']['f'] == 'SPLIT_2'
        assert client._recorder._unique_keys_tracker._cache == {'SPLIT_3': {'user3'}}
        imps_count = client._recorder._imp_counter.pop_all()
        assert len(imps_count) == 1
        assert imps_count[0].feature == 'SPLIT_3'
        assert imps_count[0].count == 1
        await self.clear_cache()
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_debug(self):
        """Prepare storages with test data."""
        metadata = SdkMetadata('python-1.2.3', 'some_ip', 'some_name')
        redis_client = await build_async(DEFAULT_CONFIG.copy())
        split_storage = RedisSplitStorageAsync(redis_client, True)
        segment_storage = RedisSegmentStorageAsync(redis_client)

        await redis_client.set(split_storage._get_key(splits_json['splitChange1_1']['splits'][0]['name']), json.dumps(splits_json['splitChange1_1']['splits'][0]))
        await redis_client.set(split_storage._get_key(splits_json['splitChange1_1']['splits'][1]['name']), json.dumps(splits_json['splitChange1_1']['splits'][1]))
        await redis_client.set(split_storage._get_key(splits_json['splitChange1_1']['splits'][2]['name']), json.dumps(splits_json['splitChange1_1']['splits'][2]))
        await redis_client.set(split_storage._FEATURE_FLAG_TILL_KEY, -1)

        telemetry_redis_storage = await RedisTelemetryStorageAsync.create(redis_client, metadata)
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_redis_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': RedisImpressionsStorageAsync(redis_client, metadata),
            'events': RedisEventsStorageAsync(redis_client, metadata),
        }
        impmanager = ImpressionsManager(StrategyDebugMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = PipelinedRecorderAsync(redis_client.pipeline, impmanager,
                                     storages['events'], storages['impressions'], telemetry_redis_storage, unique_keys_tracker=UniqueKeysTracker(), imp_counter=ImpressionsCounter())
        factory = SplitFactoryAsync('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init
        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(factory).ready = ready_property

        try:
            client = factory.client()
        except:
            pass

        assert await client.get_treatment('user1', 'SPLIT_1') == 'off'
        assert await client.get_treatment('user2', 'SPLIT_2') == 'on'
        assert await client.get_treatment('user3', 'SPLIT_3') == 'on'
        await asyncio.sleep(0.2)

        imp_storage = factory._storages['impressions']
        impressions = []
        while True:
            impression = await redis_client.lpop(imp_storage.IMPRESSIONS_QUEUE_KEY)
            if impression is None:
                break
            impressions.append(json.loads(impression))

        assert len(impressions) == 2
        assert impressions[0]['i']['f'] == 'SPLIT_1'
        assert impressions[1]['i']['f'] == 'SPLIT_2'
        assert client._recorder._unique_keys_tracker._cache == {'SPLIT_3': {'user3'}}
        imps_count = client._recorder._imp_counter.pop_all()
        assert len(imps_count) == 1
        assert imps_count[0].feature == 'SPLIT_3'
        assert imps_count[0].count == 1
        await self.clear_cache()
        await factory.destroy()

    @pytest.mark.asyncio
    async def test_none(self):
        """Prepare storages with test data."""
        metadata = SdkMetadata('python-1.2.3', 'some_ip', 'some_name')
        redis_client = await build_async(DEFAULT_CONFIG.copy())
        split_storage = RedisSplitStorageAsync(redis_client, True)
        segment_storage = RedisSegmentStorageAsync(redis_client)

        await redis_client.set(split_storage._get_key(splits_json['splitChange1_1']['splits'][0]['name']), json.dumps(splits_json['splitChange1_1']['splits'][0]))
        await redis_client.set(split_storage._get_key(splits_json['splitChange1_1']['splits'][1]['name']), json.dumps(splits_json['splitChange1_1']['splits'][1]))
        await redis_client.set(split_storage._get_key(splits_json['splitChange1_1']['splits'][2]['name']), json.dumps(splits_json['splitChange1_1']['splits'][2]))
        await redis_client.set(split_storage._FEATURE_FLAG_TILL_KEY, -1)

        telemetry_redis_storage = await RedisTelemetryStorageAsync.create(redis_client, metadata)
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_redis_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': RedisImpressionsStorageAsync(redis_client, metadata),
            'events': RedisEventsStorageAsync(redis_client, metadata),
        }
        impmanager = ImpressionsManager(StrategyNoneMode(), StrategyNoneMode(), telemetry_runtime_producer) # no listener
        recorder = PipelinedRecorderAsync(redis_client.pipeline, impmanager,
                                     storages['events'], storages['impressions'], telemetry_redis_storage, unique_keys_tracker=UniqueKeysTracker(), imp_counter=ImpressionsCounter())
        factory = SplitFactoryAsync('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init
        ready_property = mocker.PropertyMock()
        ready_property.return_value = True
        type(factory).ready = ready_property

        try:
            client = factory.client()
        except:
            pass

        assert await client.get_treatment('user1', 'SPLIT_1') == 'off'
        assert await client.get_treatment('user2', 'SPLIT_2') == 'on'
        assert await client.get_treatment('user3', 'SPLIT_3') == 'on'
        await asyncio.sleep(0.2)

        imp_storage = factory._storages['impressions']
        impressions = []
        while True:
            impression = await redis_client.lpop(imp_storage.IMPRESSIONS_QUEUE_KEY)
            if impression is None:
                break
            impressions.append(json.loads(impression))

        assert len(impressions) == 0
        assert client._recorder._unique_keys_tracker._cache == {'SPLIT_1': {'user1'}, 'SPLIT_2': {'user2'}, 'SPLIT_3': {'user3'}}
        imps_count = client._recorder._imp_counter.pop_all()
        assert len(imps_count) == 3
        assert imps_count[0].feature == 'SPLIT_1'
        assert imps_count[0].count == 1
        assert imps_count[1].feature == 'SPLIT_2'
        assert imps_count[1].count == 1
        assert imps_count[2].feature == 'SPLIT_3'
        assert imps_count[2].count == 1
        await self.clear_cache()
        await factory.destroy()

    async def clear_cache(self):
        """Clear redis cache."""
        keys_to_delete = [
            "SPLITIO.split.SPLIT_3",
            "SPLITIO.splits.till",
            "SPLITIO.split.SPLIT_2",
            "SPLITIO.split.SPLIT_1",
            "SPLITIO.telemetry.latencies"
        ]

        redis_client = await build_async(DEFAULT_CONFIG.copy())
        for key in keys_to_delete:
            await redis_client.delete(key)

async def _validate_last_impressions_async(client, *to_validate):
    """Validate the last N impressions are present disregarding the order."""
    imp_storage = client._factory._get_storage('impressions')
    if isinstance(client._factory._get_storage('splits'), RedisSplitStorageAsync) or isinstance(client._factory._get_storage('splits'), PluggableSplitStorageAsync):
        if isinstance(client._factory._get_storage('splits'), RedisSplitStorageAsync):
            redis_client = imp_storage._redis
            impressions_raw = [
                json.loads(await redis_client.lpop(imp_storage.IMPRESSIONS_QUEUE_KEY))
                for _ in to_validate
            ]
        else:
            pluggable_adapter = imp_storage._pluggable_adapter
            results = await pluggable_adapter.pop_items(imp_storage._impressions_queue_key)
            results = [] if results == None else results
            impressions_raw = [
                json.loads(i)
                for i in results
            ]
        as_tup_set = set(
            (i['i']['f'], i['i']['k'], i['i']['t'])
            for i in impressions_raw
        )
        assert as_tup_set == set(to_validate)
        await asyncio.sleep(0.2) # delay for redis to sync
    else:
        impressions = await imp_storage.pop_many(len(to_validate))
        as_tup_set = set((i.feature_name, i.matching_key, i.treatment) for i in impressions)
        assert as_tup_set == set(to_validate)

async def _validate_last_events_async(client, *to_validate):
    """Validate the last N impressions are present disregarding the order."""
    event_storage = client._factory._get_storage('events')
    if isinstance(client._factory._get_storage('splits'), RedisSplitStorageAsync) or isinstance(client._factory._get_storage('splits'), PluggableSplitStorageAsync):
        if isinstance(client._factory._get_storage('splits'), RedisSplitStorageAsync):
            redis_client = event_storage._redis
            events_raw = [
                json.loads(await redis_client.lpop(event_storage._EVENTS_KEY_TEMPLATE))
                for _ in to_validate
            ]
        else:
            pluggable_adapter = event_storage._pluggable_adapter
            events_raw = [
                json.loads(i)
                for i in await pluggable_adapter.pop_items(event_storage._events_queue_key)
            ]
        as_tup_set = set(
            (i['e']['key'], i['e']['trafficTypeName'], i['e']['eventTypeId'], i['e']['value'], str(i['e']['properties']))
            for i in events_raw
        )
        assert as_tup_set == set(to_validate)
    else:
        events = await event_storage.pop_many(len(to_validate))
        as_tup_set = set((i.key, i.traffic_type_name, i.event_type_id, i.value, str(i.properties)) for i in events)
        assert as_tup_set == set(to_validate)

async def _get_treatment_async(factory):
    """Test client.get_treatment()."""
    try:
        client = factory.client()
    except:
        pass

    assert await client.get_treatment('user1', 'sample_feature') == 'on'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('sample_feature', 'user1', 'on'))

    assert await client.get_treatment('invalidKey', 'sample_feature') == 'off'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('sample_feature', 'invalidKey', 'off'))

    assert await client.get_treatment('invalidKey', 'invalid_feature') == 'control'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client)  # No impressions should be present

    # testing a killed feature. No matter what the key, must return default treatment
    assert await client.get_treatment('invalidKey', 'killed_feature') == 'defTreatment'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('killed_feature', 'invalidKey', 'defTreatment'))

    # testing ALL matcher
    assert await client.get_treatment('invalidKey', 'all_feature') == 'on'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('all_feature', 'invalidKey', 'on'))

    # testing WHITELIST matcher
    assert await client.get_treatment('whitelisted_user', 'whitelist_feature') == 'on'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('whitelist_feature', 'whitelisted_user', 'on'))
    assert await client.get_treatment('unwhitelisted_user', 'whitelist_feature') == 'off'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('whitelist_feature', 'unwhitelisted_user', 'off'))

    #  testing INVALID matcher
    assert await client.get_treatment('some_user_key', 'invalid_matcher_feature') == 'control'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client)  # No impressions should be present

    #  testing Dependency matcher
    assert await client.get_treatment('somekey', 'dependency_test') == 'off'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('dependency_test', 'somekey', 'off'))

    #  testing boolean matcher
    assert await client.get_treatment('True', 'boolean_test') == 'on'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('boolean_test', 'True', 'on'))

    #  testing regex matcher
    assert await client.get_treatment('abc4', 'regex_test') == 'on'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('regex_test', 'abc4', 'on'))

async def _get_treatment_with_config_async(factory):
    """Test client.get_treatment_with_config()."""
    try:
        client = factory.client()
    except:
        pass
    result = await client.get_treatment_with_config('user1', 'sample_feature')
    assert result == ('on', '{"size":15,"test":20}')
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('sample_feature', 'user1', 'on'))

    result = await client.get_treatment_with_config('invalidKey', 'sample_feature')
    assert result == ('off', None)
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('sample_feature', 'invalidKey', 'off'))

    result = await client.get_treatment_with_config('invalidKey', 'invalid_feature')
    assert result == ('control', None)
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client)

    # testing a killed feature. No matter what the key, must return default treatment
    result = await client.get_treatment_with_config('invalidKey', 'killed_feature')
    assert ('defTreatment', '{"size":15,"defTreatment":true}') == result
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('killed_feature', 'invalidKey', 'defTreatment'))

    # testing ALL matcher
    result = await client.get_treatment_with_config('invalidKey', 'all_feature')
    assert result == ('on', None)
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('all_feature', 'invalidKey', 'on'))

async def _get_treatments_async(factory):
    """Test client.get_treatments()."""
    try:
        client = factory.client()
    except:
        pass
    result = await client.get_treatments('user1', ['sample_feature'])
    assert len(result) == 1
    assert result['sample_feature'] == 'on'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('sample_feature', 'user1', 'on'))

    result = await client.get_treatments('invalidKey', ['sample_feature'])
    assert len(result) == 1
    assert result['sample_feature'] == 'off'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('sample_feature', 'invalidKey', 'off'))

    result = await client.get_treatments('invalidKey', ['invalid_feature'])
    assert len(result) == 1
    assert result['invalid_feature'] == 'control'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client)

    # testing a killed feature. No matter what the key, must return default treatment
    result = await client.get_treatments('invalidKey', ['killed_feature'])
    assert len(result) == 1
    assert result['killed_feature'] == 'defTreatment'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('killed_feature', 'invalidKey', 'defTreatment'))

    # testing ALL matcher
    result = await client.get_treatments('invalidKey', ['all_feature'])
    assert len(result) == 1
    assert result['all_feature'] == 'on'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('all_feature', 'invalidKey', 'on'))

async def _get_treatments_with_config_async(factory):
    """Test client.get_treatments_with_config()."""
    try:
        client = factory.client()
    except:
        pass

    result = await client.get_treatments_with_config('user1', ['sample_feature'])
    assert len(result) == 1
    assert result['sample_feature'] == ('on', '{"size":15,"test":20}')
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('sample_feature', 'user1', 'on'))

    result = await client.get_treatments_with_config('invalidKey', ['sample_feature'])
    assert len(result) == 1
    assert result['sample_feature'] == ('off', None)
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('sample_feature', 'invalidKey', 'off'))

    result = await client.get_treatments_with_config('invalidKey', ['invalid_feature'])
    assert len(result) == 1
    assert result['invalid_feature'] == ('control', None)
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client)

    # testing a killed feature. No matter what the key, must return default treatment
    result = await client.get_treatments_with_config('invalidKey', ['killed_feature'])
    assert len(result) == 1
    assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('killed_feature', 'invalidKey', 'defTreatment'))

    # testing ALL matcher
    result = await client.get_treatments_with_config('invalidKey', ['all_feature'])
    assert len(result) == 1
    assert result['all_feature'] == ('on', None)
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('all_feature', 'invalidKey', 'on'))

async def _get_treatments_by_flag_set_async(factory):
    """Test client.get_treatments_by_flag_set()."""
    try:
        client = factory.client()
    except:
        pass
    result = await client.get_treatments_by_flag_set('user1', 'set1')
    assert len(result) == 2
    assert result == {'sample_feature': 'on', 'whitelist_feature': 'off'}
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('sample_feature', 'user1', 'on'), ('whitelist_feature', 'user1', 'off'))

    result = await client.get_treatments_by_flag_set('invalidKey', 'invalid_set')
    assert len(result) == 0
    assert result == {}

    # testing a killed feature. No matter what the key, must return default treatment
    result = await client.get_treatments_by_flag_set('invalidKey', 'set3')
    assert len(result) == 1
    assert result['killed_feature'] == 'defTreatment'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('killed_feature', 'invalidKey', 'defTreatment'))

    # testing ALL matcher
    result = await client.get_treatments_by_flag_set('invalidKey', 'set4')
    assert len(result) == 1
    assert result['all_feature'] == 'on'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('all_feature', 'invalidKey', 'on'))

async def _get_treatments_by_flag_sets_async(factory):
    """Test client.get_treatments_by_flag_sets()."""
    try:
        client = factory.client()
    except:
        pass
    result = await client.get_treatments_by_flag_sets('user1', ['set1'])
    assert len(result) == 2
    assert result == {'sample_feature': 'on', 'whitelist_feature': 'off'}
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('sample_feature', 'user1', 'on'), ('whitelist_feature', 'user1', 'off'))

    result = await client.get_treatments_by_flag_sets('invalidKey', ['invalid_set'])
    assert len(result) == 0
    assert result == {}

    result = await client.get_treatments_by_flag_sets('invalidKey', [])
    assert len(result) == 0
    assert result == {}

    # testing a killed feature. No matter what the key, must return default treatment
    result = await client.get_treatments_by_flag_sets('invalidKey', ['set3'])
    assert len(result) == 1
    assert result['killed_feature'] == 'defTreatment'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('killed_feature', 'invalidKey', 'defTreatment'))

    # testing ALL matcher
    result = await client.get_treatments_by_flag_sets('user1', ['set4'])
    assert len(result) == 1
    assert result['all_feature'] == 'on'
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('all_feature', 'user1', 'on'))

async def _get_treatments_with_config_by_flag_set_async(factory):
    """Test client.get_treatments_with_config_by_flag_set()."""
    try:
        client = factory.client()
    except:
        pass
    result = await client.get_treatments_with_config_by_flag_set('user1', 'set1')
    assert len(result) == 2
    assert result == {'sample_feature': ('on', '{"size":15,"test":20}'), 'whitelist_feature': ('off', None)}
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('sample_feature', 'user1', 'on'), ('whitelist_feature', 'user1', 'off'))

    result = await client.get_treatments_with_config_by_flag_set('invalidKey', 'invalid_set')
    assert len(result) == 0
    assert result == {}

    # testing a killed feature. No matter what the key, must return default treatment
    result = await client.get_treatments_with_config_by_flag_set('invalidKey', 'set3')
    assert len(result) == 1
    assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('killed_feature', 'invalidKey', 'defTreatment'))

    # testing ALL matcher
    result = await client.get_treatments_with_config_by_flag_set('invalidKey', 'set4')
    assert len(result) == 1
    assert result['all_feature'] == ('on', None)
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('all_feature', 'invalidKey', 'on'))

async def _get_treatments_with_config_by_flag_sets_async(factory):
    """Test client.get_treatments_with_config_by_flag_sets()."""
    try:
        client = factory.client()
    except:
        pass
    result = await client.get_treatments_with_config_by_flag_sets('user1', ['set1'])
    assert len(result) == 2
    assert result == {'sample_feature': ('on', '{"size":15,"test":20}'), 'whitelist_feature': ('off', None)}
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('sample_feature', 'user1', 'on'), ('whitelist_feature', 'user1', 'off'))

    result = await client.get_treatments_with_config_by_flag_sets('invalidKey', ['invalid_set'])
    assert len(result) == 0
    assert result == {}

    result = await client.get_treatments_with_config_by_flag_sets('invalidKey', [])
    assert len(result) == 0
    assert result == {}

    # testing a killed feature. No matter what the key, must return default treatment
    result = await client.get_treatments_with_config_by_flag_sets('invalidKey', ['set3'])
    assert len(result) == 1
    assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('killed_feature', 'invalidKey', 'defTreatment'))

    # testing ALL matcher
    result = await client.get_treatments_with_config_by_flag_sets('user1', ['set4'])
    assert len(result) == 1
    assert result['all_feature'] == ('on', None)
    if not isinstance(factory._recorder._impressions_manager._strategy, StrategyNoneMode):
        await _validate_last_impressions_async(client, ('all_feature', 'user1', 'on'))

async def _track_async(factory):
    """Test client.track()."""
    try:
        client = factory.client()
    except:
        pass
    assert(await client.track('user1', 'user', 'conversion', 1, {"prop1": "value1"}))
    assert(not await client.track(None, 'user', 'conversion'))
    assert(not await client.track('user1', None, 'conversion'))
    assert(not await client.track('user1', 'user', None))
    await _validate_last_events_async(
        client,
        ('user1', 'user', 'conversion', 1, "{'prop1': 'value1'}")
    )

async def _manager_methods_async(factory):
    """Test manager.split/splits."""
    try:
        manager = factory.manager()
    except:
        pass
    result = await manager.split('all_feature')
    assert result.name == 'all_feature'
    assert result.traffic_type is None
    assert result.killed is False
    assert len(result.treatments) == 2
    assert result.change_number == 123
    assert result.configs == {}

    result = await manager.split('killed_feature')
    assert result.name == 'killed_feature'
    assert result.traffic_type is None
    assert result.killed is True
    assert len(result.treatments) == 2
    assert result.change_number == 123
    assert result.configs['defTreatment'] == '{"size":15,"defTreatment":true}'
    assert result.configs['off'] == '{"size":15,"test":20}'

    result = await manager.split('sample_feature')
    assert result.name == 'sample_feature'
    assert result.traffic_type is None
    assert result.killed is False
    assert len(result.treatments) == 2
    assert result.change_number == 123
    assert result.configs['on'] == '{"size":15,"test":20}'

    assert len(await manager.split_names()) == 7
    assert len(await manager.splits()) == 7
