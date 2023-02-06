"""Client integration tests."""
# pylint: disable=protected-access,line-too-long,no-self-use
import json
import os
import threading
import time
import pytest

from redis import StrictRedis

from splitio.exceptions import TimeoutException
from splitio.client.factory import get_factory, SplitFactory
from splitio.client.util import SdkMetadata
from splitio.storage.inmemmory import InMemoryEventStorage, InMemoryImpressionStorage, \
    InMemorySegmentStorage, InMemorySplitStorage, InMemoryTelemetryStorage
from splitio.storage.redis import RedisEventsStorage, RedisImpressionsStorage, \
    RedisSplitStorage, RedisSegmentStorage, RedisTelemetryStorage
from splitio.storage.adapters.redis import build, RedisAdapter
from splitio.models import splits, segments
from splitio.engine.impressions.impressions import Manager as ImpressionsManager, ImpressionsMode
from splitio.engine.impressions.strategies import StrategyDebugMode, StrategyOptimizedMode
from splitio.engine.impressions.manager import Counter
from splitio.engine.telemetry import TelemetryStorageConsumer, TelemetryStorageProducer
from splitio.engine.impressions.manager import Counter as ImpressionsCounter
from splitio.recorder.recorder import StandardRecorder, PipelinedRecorder
from splitio.client.config import DEFAULT_CONFIG
from splitio.sync.synchronizer import SplitTasks, SplitSynchronizers, Synchronizer
from splitio.sync.manager import Manager
from tests.integration import splits_json
class InMemoryIntegrationTests(object):
    """Inmemory storage-based integration tests."""

    def setup_method(self):
        """Prepare storages with test data."""
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()

        split_fn = os.path.join(os.path.dirname(__file__), 'files', 'splitChanges.json')
        with open(split_fn, 'r') as flo:
            data = json.loads(flo.read())
        for split in data['splits']:
            split_storage.put(splits.from_raw(split))

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
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': InMemoryImpressionStorage(5000, telemetry_runtime_producer),
            'events': InMemoryEventStorage(5000, telemetry_runtime_producer),
        }
        impmanager = ImpressionsManager(StrategyDebugMode(), telemetry_runtime_producer) # no listener
        recorder = StandardRecorder(impmanager, storages['events'], storages['impressions'], telemetry_evaluation_producer)
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

    def _validate_last_impressions(self, client, *to_validate):
        """Validate the last N impressions are present disregarding the order."""
        imp_storage = client._factory._get_storage('impressions')
        impressions = imp_storage.pop_many(len(to_validate))
        as_tup_set = set((i.feature_name, i.matching_key, i.treatment) for i in impressions)
        assert as_tup_set == set(to_validate)

    def _validate_last_events(self, client, *to_validate):
        """Validate the last N impressions are present disregarding the order."""
        event_storage = client._factory._get_storage('events')
        events = event_storage.pop_many(len(to_validate))
        as_tup_set = set((i.key, i.traffic_type_name, i.event_type_id, i.value, str(i.properties)) for i in events)
        assert as_tup_set == set(to_validate)

    def test_get_treatment(self):
        """Test client.get_treatment()."""
        try:
            client = self.factory.client()
        except:
            pass

        assert client.get_treatment('user1', 'sample_feature') == 'on'
        self._validate_last_impressions(client, ('sample_feature', 'user1', 'on'))

        assert client.get_treatment('invalidKey', 'sample_feature') == 'off'
        self._validate_last_impressions(client, ('sample_feature', 'invalidKey', 'off'))

        assert client.get_treatment('invalidKey', 'invalid_feature') == 'control'
        self._validate_last_impressions(client)  # No impressions should be present

        # testing a killed feature. No matter what the key, must return default treatment
        assert client.get_treatment('invalidKey', 'killed_feature') == 'defTreatment'
        self._validate_last_impressions(client, ('killed_feature', 'invalidKey', 'defTreatment'))

        # testing ALL matcher
        assert client.get_treatment('invalidKey', 'all_feature') == 'on'
        self._validate_last_impressions(client, ('all_feature', 'invalidKey', 'on'))

        # testing WHITELIST matcher
        assert client.get_treatment('whitelisted_user', 'whitelist_feature') == 'on'
        self._validate_last_impressions(client, ('whitelist_feature', 'whitelisted_user', 'on'))
        assert client.get_treatment('unwhitelisted_user', 'whitelist_feature') == 'off'
        self._validate_last_impressions(client, ('whitelist_feature', 'unwhitelisted_user', 'off'))

        #  testing INVALID matcher
        assert client.get_treatment('some_user_key', 'invalid_matcher_feature') == 'control'
        self._validate_last_impressions(client)  # No impressions should be present

        #  testing Dependency matcher
        assert client.get_treatment('somekey', 'dependency_test') == 'off'
        self._validate_last_impressions(client, ('dependency_test', 'somekey', 'off'))

        #  testing boolean matcher
        assert client.get_treatment('True', 'boolean_test') == 'on'
        self._validate_last_impressions(client, ('boolean_test', 'True', 'on'))

        #  testing regex matcher
        assert client.get_treatment('abc4', 'regex_test') == 'on'
        self._validate_last_impressions(client, ('regex_test', 'abc4', 'on'))

    def test_get_treatment_with_config(self):
        """Test client.get_treatment_with_config()."""
        try:
            client = self.factory.client()
        except:
            pass
        result = client.get_treatment_with_config('user1', 'sample_feature')
        assert result == ('on', '{"size":15,"test":20}')
        self._validate_last_impressions(client, ('sample_feature', 'user1', 'on'))

        result = client.get_treatment_with_config('invalidKey', 'sample_feature')
        assert result == ('off', None)
        self._validate_last_impressions(client, ('sample_feature', 'invalidKey', 'off'))

        result = client.get_treatment_with_config('invalidKey', 'invalid_feature')
        assert result == ('control', None)
        self._validate_last_impressions(client)

        # testing a killed feature. No matter what the key, must return default treatment
        result = client.get_treatment_with_config('invalidKey', 'killed_feature')
        assert ('defTreatment', '{"size":15,"defTreatment":true}') == result
        self._validate_last_impressions(client, ('killed_feature', 'invalidKey', 'defTreatment'))

        # testing ALL matcher
        result = client.get_treatment_with_config('invalidKey', 'all_feature')
        assert result == ('on', None)
        self._validate_last_impressions(client, ('all_feature', 'invalidKey', 'on'))

    def test_get_treatments(self):
        """Test client.get_treatments()."""
        try:
            client = self.factory.client()
        except:
            pass
        result = client.get_treatments('user1', ['sample_feature'])
        assert len(result) == 1
        assert result['sample_feature'] == 'on'
        self._validate_last_impressions(client, ('sample_feature', 'user1', 'on'))

        result = client.get_treatments('invalidKey', ['sample_feature'])
        assert len(result) == 1
        assert result['sample_feature'] == 'off'
        self._validate_last_impressions(client, ('sample_feature', 'invalidKey', 'off'))

        result = client.get_treatments('invalidKey', ['invalid_feature'])
        assert len(result) == 1
        assert result['invalid_feature'] == 'control'
        self._validate_last_impressions(client)

        # testing a killed feature. No matter what the key, must return default treatment
        result = client.get_treatments('invalidKey', ['killed_feature'])
        assert len(result) == 1
        assert result['killed_feature'] == 'defTreatment'
        self._validate_last_impressions(client, ('killed_feature', 'invalidKey', 'defTreatment'))

        # testing ALL matcher
        result = client.get_treatments('invalidKey', ['all_feature'])
        assert len(result) == 1
        assert result['all_feature'] == 'on'
        self._validate_last_impressions(client, ('all_feature', 'invalidKey', 'on'))

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
        self._validate_last_impressions(
            client,
            ('all_feature', 'invalidKey', 'on'),
            ('killed_feature', 'invalidKey', 'defTreatment'),
            ('sample_feature', 'invalidKey', 'off')
        )

    def test_get_treatments_with_config(self):
        """Test client.get_treatments_with_config()."""
        try:
            client = self.factory.client()
        except:
            pass

        result = client.get_treatments_with_config('user1', ['sample_feature'])
        assert len(result) == 1
        assert result['sample_feature'] == ('on', '{"size":15,"test":20}')
        self._validate_last_impressions(client, ('sample_feature', 'user1', 'on'))

        result = client.get_treatments_with_config('invalidKey', ['sample_feature'])
        assert len(result) == 1
        assert result['sample_feature'] == ('off', None)
        self._validate_last_impressions(client, ('sample_feature', 'invalidKey', 'off'))

        result = client.get_treatments_with_config('invalidKey', ['invalid_feature'])
        assert len(result) == 1
        assert result['invalid_feature'] == ('control', None)
        self._validate_last_impressions(client)

        # testing a killed feature. No matter what the key, must return default treatment
        result = client.get_treatments_with_config('invalidKey', ['killed_feature'])
        assert len(result) == 1
        assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
        self._validate_last_impressions(client, ('killed_feature', 'invalidKey', 'defTreatment'))

        # testing ALL matcher
        result = client.get_treatments_with_config('invalidKey', ['all_feature'])
        assert len(result) == 1
        assert result['all_feature'] == ('on', None)
        self._validate_last_impressions(client, ('all_feature', 'invalidKey', 'on'))

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
        self._validate_last_impressions(
            client,
            ('all_feature', 'invalidKey', 'on'),
            ('killed_feature', 'invalidKey', 'defTreatment'),
            ('sample_feature', 'invalidKey', 'off'),
        )

    def test_track(self):
        """Test client.track()."""
        try:
            client = self.factory.client()
        except:
            pass
        assert(client.track('user1', 'user', 'conversion', 1, {"prop1": "value1"}))
        assert(not client.track(None, 'user', 'conversion'))
        assert(not client.track('user1', None, 'conversion'))
        assert(not client.track('user1', 'user', None))
        self._validate_last_events(
            client,
            ('user1', 'user', 'conversion', 1, "{'prop1': 'value1'}")
        )

    def test_manager_methods(self):
        """Test manager.split/splits."""
        try:
            manager = self.factory.manager()
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
            split_storage.put(splits.from_raw(split))

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
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_evaluation_producer = telemetry_producer.get_telemetry_evaluation_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': InMemoryImpressionStorage(5000, telemetry_runtime_producer),
            'events': InMemoryEventStorage(5000, telemetry_runtime_producer),
        }
        impmanager = ImpressionsManager(StrategyOptimizedMode(ImpressionsCounter()), telemetry_runtime_producer) # no listener
        recorder = StandardRecorder(impmanager, storages['events'], storages['impressions'], telemetry_evaluation_producer)
        self.factory = SplitFactory('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    None,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init

    def _validate_last_impressions(self, client, *to_validate):
        """Validate the last N impressions are present disregarding the order."""
        imp_storage = client._factory._get_storage('impressions')
        impressions = imp_storage.pop_many(len(to_validate))
        as_tup_set = set((i.feature_name, i.matching_key, i.treatment) for i in impressions)
        assert as_tup_set == set(to_validate)

    def _validate_last_events(self, client, *to_validate):
        """Validate the last N impressions are present disregarding the order."""
        event_storage = client._factory._get_storage('events')
        events = event_storage.pop_many(len(to_validate))
        as_tup_set = set((i.key, i.traffic_type_name, i.event_type_id, i.value, str(i.properties)) for i in events)
        assert as_tup_set == set(to_validate)

    def test_get_treatment(self):
        """Test client.get_treatment()."""
        client = self.factory.client()

        assert client.get_treatment('user1', 'sample_feature') == 'on'
        self._validate_last_impressions(client, ('sample_feature', 'user1', 'on'))
        client.get_treatment('user1', 'sample_feature')
        client.get_treatment('user1', 'sample_feature')
        client.get_treatment('user1', 'sample_feature')

        # Only one impression was added, and popped when validating, the rest were ignored
        assert self.factory._storages['impressions']._impressions.qsize() == 0

        assert client.get_treatment('invalidKey', 'sample_feature') == 'off'
        self._validate_last_impressions(client, ('sample_feature', 'invalidKey', 'off'))

        assert client.get_treatment('invalidKey', 'invalid_feature') == 'control'
        self._validate_last_impressions(client)  # No impressions should be present

        # testing a killed feature. No matter what the key, must return default treatment
        assert client.get_treatment('invalidKey', 'killed_feature') == 'defTreatment'
        self._validate_last_impressions(client, ('killed_feature', 'invalidKey', 'defTreatment'))

        # testing ALL matcher
        assert client.get_treatment('invalidKey', 'all_feature') == 'on'
        self._validate_last_impressions(client, ('all_feature', 'invalidKey', 'on'))

        # testing WHITELIST matcher
        assert client.get_treatment('whitelisted_user', 'whitelist_feature') == 'on'
        self._validate_last_impressions(client, ('whitelist_feature', 'whitelisted_user', 'on'))
        assert client.get_treatment('unwhitelisted_user', 'whitelist_feature') == 'off'
        self._validate_last_impressions(client, ('whitelist_feature', 'unwhitelisted_user', 'off'))

        #  testing INVALID matcher
        assert client.get_treatment('some_user_key', 'invalid_matcher_feature') == 'control'
        self._validate_last_impressions(client)  # No impressions should be present

        #  testing Dependency matcher
        assert client.get_treatment('somekey', 'dependency_test') == 'off'
        self._validate_last_impressions(client, ('dependency_test', 'somekey', 'off'))

        #  testing boolean matcher
        assert client.get_treatment('True', 'boolean_test') == 'on'
        self._validate_last_impressions(client, ('boolean_test', 'True', 'on'))

        #  testing regex matcher
        assert client.get_treatment('abc4', 'regex_test') == 'on'
        self._validate_last_impressions(client, ('regex_test', 'abc4', 'on'))

    def test_get_treatments(self):
        """Test client.get_treatments()."""
        client = self.factory.client()

        result = client.get_treatments('user1', ['sample_feature'])
        assert len(result) == 1
        assert result['sample_feature'] == 'on'
        self._validate_last_impressions(client, ('sample_feature', 'user1', 'on'))

        result = client.get_treatments('invalidKey', ['sample_feature'])
        assert len(result) == 1
        assert result['sample_feature'] == 'off'
        self._validate_last_impressions(client, ('sample_feature', 'invalidKey', 'off'))

        result = client.get_treatments('invalidKey', ['invalid_feature'])
        assert len(result) == 1
        assert result['invalid_feature'] == 'control'
        self._validate_last_impressions(client)

        # testing a killed feature. No matter what the key, must return default treatment
        result = client.get_treatments('invalidKey', ['killed_feature'])
        assert len(result) == 1
        assert result['killed_feature'] == 'defTreatment'
        self._validate_last_impressions(client, ('killed_feature', 'invalidKey', 'defTreatment'))

        # testing ALL matcher
        result = client.get_treatments('invalidKey', ['all_feature'])
        assert len(result) == 1
        assert result['all_feature'] == 'on'
        self._validate_last_impressions(client, ('all_feature', 'invalidKey', 'on'))

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
        assert self.factory._storages['impressions']._impressions.qsize() == 0

    def test_get_treatments_with_config(self):
        """Test client.get_treatments_with_config()."""
        client = self.factory.client()

        result = client.get_treatments_with_config('user1', ['sample_feature'])
        assert len(result) == 1
        assert result['sample_feature'] == ('on', '{"size":15,"test":20}')
        self._validate_last_impressions(client, ('sample_feature', 'user1', 'on'))

        result = client.get_treatments_with_config('invalidKey', ['sample_feature'])
        assert len(result) == 1
        assert result['sample_feature'] == ('off', None)
        self._validate_last_impressions(client, ('sample_feature', 'invalidKey', 'off'))

        result = client.get_treatments_with_config('invalidKey', ['invalid_feature'])
        assert len(result) == 1
        assert result['invalid_feature'] == ('control', None)
        self._validate_last_impressions(client)

        # testing a killed feature. No matter what the key, must return default treatment
        result = client.get_treatments_with_config('invalidKey', ['killed_feature'])
        assert len(result) == 1
        assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
        self._validate_last_impressions(client, ('killed_feature', 'invalidKey', 'defTreatment'))

        # testing ALL matcher
        result = client.get_treatments_with_config('invalidKey', ['all_feature'])
        assert len(result) == 1
        assert result['all_feature'] == ('on', None)
        self._validate_last_impressions(client, ('all_feature', 'invalidKey', 'on'))

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
        assert self.factory._storages['impressions']._impressions.qsize() == 0

    def test_manager_methods(self):
        """Test manager.split/splits."""
        manager = self.factory.manager()
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

    def test_track(self):
        """Test client.track()."""
        client = self.factory.client()
        assert(client.track('user1', 'user', 'conversion', 1, {"prop1": "value1"}))
        assert(not client.track(None, 'user', 'conversion'))
        assert(not client.track('user1', None, 'conversion'))
        assert(not client.track('user1', 'user', None))
        self._validate_last_events(
            client,
            ('user1', 'user', 'conversion', 1, "{'prop1': 'value1'}")
        )

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
        redis_client.set(split_storage._SPLIT_TILL_KEY, data['till'])

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
        telemetry_consumer = TelemetryStorageConsumer(telemetry_redis_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': RedisImpressionsStorage(redis_client, metadata),
            'events': RedisEventsStorage(redis_client, metadata),
        }
        impmanager = ImpressionsManager(StrategyDebugMode(), telemetry_runtime_producer) # no listener
        recorder = PipelinedRecorder(redis_client.pipeline, impmanager, storages['events'],
                                    storages['impressions'], telemetry_redis_storage)
        self.factory = SplitFactory('some_api_key',
                                    storages,
                                    True,
                                    recorder,
                                    telemetry_producer=telemetry_producer,
                                    telemetry_init_producer=telemetry_producer.get_telemetry_init_producer(),
                                    )  # pylint:disable=attribute-defined-outside-init

    def _validate_last_events(self, client, *to_validate):
        """Validate the last N impressions are present disregarding the order."""
        event_storage = client._factory._get_storage('events')
        redis_client = event_storage._redis
        events_raw = [
            json.loads(redis_client.lpop(event_storage._EVENTS_KEY_TEMPLATE))
            for _ in to_validate
        ]
        as_tup_set = set(
            (i['e']['key'], i['e']['trafficTypeName'], i['e']['eventTypeId'], i['e']['value'], str(i['e']['properties']))
            for i in events_raw
        )
        assert as_tup_set == set(to_validate)

    def _validate_last_impressions(self, client, *to_validate):
        """Validate the last N impressions are present disregarding the order."""
        imp_storage = client._factory._get_storage('impressions')
        redis_client = imp_storage._redis
        impressions_raw = [
            json.loads(redis_client.lpop(imp_storage.IMPRESSIONS_QUEUE_KEY))
            for _ in to_validate
        ]
        as_tup_set = set(
            (i['i']['f'], i['i']['k'], i['i']['t'])
            for i in impressions_raw
        )

        assert as_tup_set == set(to_validate)

    def test_get_treatment(self):
        """Test client.get_treatment()."""
        client = self.factory.client()

        assert client.get_treatment('user1', 'sample_feature') == 'on'
        self._validate_last_impressions(client, ('sample_feature', 'user1', 'on'))

        assert client.get_treatment('invalidKey', 'sample_feature') == 'off'
        self._validate_last_impressions(client, ('sample_feature', 'invalidKey', 'off'))

        assert client.get_treatment('invalidKey', 'invalid_feature') == 'control'
        self._validate_last_impressions(client)

        # testing a killed feature. No matter what the key, must return default treatment
        assert client.get_treatment('invalidKey', 'killed_feature') == 'defTreatment'
        self._validate_last_impressions(client, ('killed_feature', 'invalidKey', 'defTreatment'))

        # testing ALL matcher
        assert client.get_treatment('invalidKey', 'all_feature') == 'on'
        self._validate_last_impressions(client, ('all_feature', 'invalidKey', 'on'))

        # testing WHITELIST matcher
        assert client.get_treatment('whitelisted_user', 'whitelist_feature') == 'on'
        self._validate_last_impressions(client, ('whitelist_feature', 'whitelisted_user', 'on'))
        assert client.get_treatment('unwhitelisted_user', 'whitelist_feature') == 'off'
        self._validate_last_impressions(client, ('whitelist_feature', 'unwhitelisted_user', 'off'))

        #  testing INVALID matcher
        assert client.get_treatment('some_user_key', 'invalid_matcher_feature') == 'control'
        self._validate_last_impressions(client)

        #  testing Dependency matcher
        assert client.get_treatment('somekey', 'dependency_test') == 'off'
        self._validate_last_impressions(client, ('dependency_test', 'somekey', 'off'))

        #  testing boolean matcher
        assert client.get_treatment('True', 'boolean_test') == 'on'
        self._validate_last_impressions(client, ('boolean_test', 'True', 'on'))

        #  testing regex matcher
        assert client.get_treatment('abc4', 'regex_test') == 'on'
        self._validate_last_impressions(client, ('regex_test', 'abc4', 'on'))

    def test_get_treatment_with_config(self):
        """Test client.get_treatment_with_config()."""
        client = self.factory.client()

        result = client.get_treatment_with_config('user1', 'sample_feature')
        assert result == ('on', '{"size":15,"test":20}')
        self._validate_last_impressions(client, ('sample_feature', 'user1', 'on'))

        result = client.get_treatment_with_config('invalidKey', 'sample_feature')
        assert result == ('off', None)
        self._validate_last_impressions(client, ('sample_feature', 'invalidKey', 'off'))

        result = client.get_treatment_with_config('invalidKey', 'invalid_feature')
        assert result == ('control', None)
        self._validate_last_impressions(client)

        # testing a killed feature. No matter what the key, must return default treatment
        result = client.get_treatment_with_config('invalidKey', 'killed_feature')
        assert ('defTreatment', '{"size":15,"defTreatment":true}') == result
        self._validate_last_impressions(client, ('killed_feature', 'invalidKey', 'defTreatment'))

        # testing ALL matcher
        result = client.get_treatment_with_config('invalidKey', 'all_feature')
        assert result == ('on', None)
        self._validate_last_impressions(client, ('all_feature', 'invalidKey', 'on'))

    def test_get_treatments(self):
        """Test client.get_treatments()."""
        client = self.factory.client()

        result = client.get_treatments('user1', ['sample_feature'])
        assert len(result) == 1
        assert result['sample_feature'] == 'on'
        self._validate_last_impressions(client, ('sample_feature', 'user1', 'on'))

        result = client.get_treatments('invalidKey', ['sample_feature'])
        assert len(result) == 1
        assert result['sample_feature'] == 'off'
        self._validate_last_impressions(client, ('sample_feature', 'invalidKey', 'off'))

        result = client.get_treatments('invalidKey', ['invalid_feature'])
        assert len(result) == 1
        assert result['invalid_feature'] == 'control'
        self._validate_last_impressions(client)

        # testing a killed feature. No matter what the key, must return default treatment
        result = client.get_treatments('invalidKey', ['killed_feature'])
        assert len(result) == 1
        assert result['killed_feature'] == 'defTreatment'
        self._validate_last_impressions(client, ('killed_feature', 'invalidKey', 'defTreatment'))

        # testing ALL matcher
        result = client.get_treatments('invalidKey', ['all_feature'])
        assert len(result) == 1
        assert result['all_feature'] == 'on'
        self._validate_last_impressions(client, ('all_feature', 'invalidKey', 'on'))

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
        self._validate_last_impressions(
            client,
            ('all_feature', 'invalidKey', 'on'),
            ('killed_feature', 'invalidKey', 'defTreatment'),
            ('sample_feature', 'invalidKey', 'off')
        )

    def test_get_treatments_with_config(self):
        """Test client.get_treatments_with_config()."""
        client = self.factory.client()

        result = client.get_treatments_with_config('user1', ['sample_feature'])
        assert len(result) == 1
        assert result['sample_feature'] == ('on', '{"size":15,"test":20}')
        self._validate_last_impressions(client, ('sample_feature', 'user1', 'on'))

        result = client.get_treatments_with_config('invalidKey', ['sample_feature'])
        assert len(result) == 1
        assert result['sample_feature'] == ('off', None)
        self._validate_last_impressions(client, ('sample_feature', 'invalidKey', 'off'))

        result = client.get_treatments_with_config('invalidKey', ['invalid_feature'])
        assert len(result) == 1
        assert result['invalid_feature'] == ('control', None)
        self._validate_last_impressions(client)

        # testing a killed feature. No matter what the key, must return default treatment
        result = client.get_treatments_with_config('invalidKey', ['killed_feature'])
        assert len(result) == 1
        assert result['killed_feature'] == ('defTreatment', '{"size":15,"defTreatment":true}')
        self._validate_last_impressions(client, ('killed_feature', 'invalidKey', 'defTreatment'))

        # testing ALL matcher
        result = client.get_treatments_with_config('invalidKey', ['all_feature'])
        assert len(result) == 1
        assert result['all_feature'] == ('on', None)
        self._validate_last_impressions(client, ('all_feature', 'invalidKey', 'on'))

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
        self._validate_last_impressions(
            client,
            ('all_feature', 'invalidKey', 'on'),
            ('killed_feature', 'invalidKey', 'defTreatment'),
            ('sample_feature', 'invalidKey', 'off'),
        )

    def test_track(self):
        """Test client.track()."""
        client = self.factory.client()
        assert(client.track('user1', 'user', 'conversion', 1, {"prop1": "value1"}))
        assert(not client.track(None, 'user', 'conversion'))
        assert(not client.track('user1', None, 'conversion'))
        assert(not client.track('user1', 'user', None))
        self._validate_last_events(
            client,
            ('user1', 'user', 'conversion', 1, "{'prop1': 'value1'}")
        )

    def test_manager_methods(self):
        """Test manager.split/splits."""
        try:
            manager = self.factory.manager()
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
            "SPLITIO.split.dependency_test"
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
        redis_client.set(split_storage._SPLIT_TILL_KEY, data['till'])

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
        telemetry_consumer = TelemetryStorageConsumer(telemetry_redis_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()

        storages = {
            'splits': split_storage,
            'segments': segment_storage,
            'impressions': RedisImpressionsStorage(redis_client, metadata),
            'events': RedisEventsStorage(redis_client, metadata),
        }
        impmanager = ImpressionsManager(StrategyDebugMode(), telemetry_runtime_producer) # no listener
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

    def test_incorrect_file_e2e(self):
        factory = get_factory('localhost', config={'splitFile': 'filename'})
        exception_raised = False
        try:
            factory.block_until_ready(1)
        except TimeoutException as e:
            exception_raised = True

        assert(exception_raised)

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

        # hack to increase isolation and prevent conflicts with other tests
#        thread = factory._sync_manager._synchronizer._split_tasks.split_task._task._thread
#        if thread is not None and thread.is_alive():
#            thread.join()

    def test_localhost_json_e2e(self):
        """Instantiate a client with a JSON file and issue get_treatment() calls."""
        filename = os.path.join(os.path.dirname(__file__), 'files', 'split_changes_temp.json')
        factory = get_factory('localhost', config={'splitFile': filename})
        factory.block_until_ready()
        client = factory.client()

        factory._sync_manager._synchronizer._split_synchronizers._split_sync._split_storage.set_change_number(-1)
        self._update_temp_file(splits_json['splitChange1_1'])
        self._synchronize_now(factory)

        assert factory.manager().split_names() == ["SPLIT_2", "SPLIT_1"]
        assert client.get_treatment("key", "SPLIT_1", None) == 'off'
        assert client.get_treatment("key", "SPLIT_2", None) == 'on'

        self._update_temp_file(splits_json['splitChange1_2'])
        self._synchronize_now(factory)

        assert factory.manager().split_names() == ["SPLIT_2", "SPLIT_1"]
        assert client.get_treatment("key", "SPLIT_1", None) == 'off'
        assert client.get_treatment("key", "SPLIT_2", None) == 'off'

        self._update_temp_file(splits_json['splitChange1_3'])
        self._synchronize_now(factory)

        assert factory.manager().split_names() == ["SPLIT_2"]
        assert client.get_treatment("key", "SPLIT_1", None) == 'control'
        assert client.get_treatment("key", "SPLIT_2", None) == 'on'

        # Tests 2 - Enable after Sanitization is added
#        factory._sync_manager._synchronizer._split_synchronizers._split_sync._split_storage.set_change_number(-1)
#        self._update_temp_file(splits_json['splitChange2_1'])
#        self._synchronize_now(factory)

#        assert factory.manager().split_names() == ["SPLIT_1"]
#        assert client.get_treatment("key", "SPLIT_1", None) == 'on'

        # Tests 3
        factory._sync_manager._synchronizer._split_synchronizers._split_sync._split_storage.set_change_number(-1)
        self._update_temp_file(splits_json['splitChange3_1'])
        self._synchronize_now(factory)

        assert factory.manager().split_names() == ["SPLIT_2"]
        assert client.get_treatment("key", "SPLIT_2", None) == 'on'

        self._update_temp_file(splits_json['splitChange3_2'])
        self._synchronize_now(factory)

        assert factory.manager().split_names() == ["SPLIT_2"]
        assert client.get_treatment("key", "SPLIT_2", None) == 'off'

        # Tests 4
        factory._sync_manager._synchronizer._split_synchronizers._split_sync._split_storage.set_change_number(-1)
        self._update_temp_file(splits_json['splitChange4_1'])
        self._synchronize_now(factory)

        assert factory.manager().split_names() == ["SPLIT_2", "SPLIT_1"]
        assert client.get_treatment("key", "SPLIT_1", None) == 'off'
        assert client.get_treatment("key", "SPLIT_2", None) == 'on'

        self._update_temp_file(splits_json['splitChange4_2'])
        self._synchronize_now(factory)

        assert factory.manager().split_names() == ["SPLIT_2", "SPLIT_1"]
        assert client.get_treatment("key", "SPLIT_1", None) == 'off'
        assert client.get_treatment("key", "SPLIT_2", None) == 'off'

        self._update_temp_file(splits_json['splitChange4_3'])
        self._synchronize_now(factory)

        assert factory.manager().split_names() == ["SPLIT_2"]
        assert client.get_treatment("key", "SPLIT_1", None) == 'control'
        assert client.get_treatment("key", "SPLIT_2", None) == 'on'

        # Tests 5
        factory._sync_manager._synchronizer._split_synchronizers._split_sync._split_storage.set_change_number(-1)
        self._update_temp_file(splits_json['splitChange5_1'])
        self._synchronize_now(factory)

        assert factory.manager().split_names() == ["SPLIT_2"]
        assert client.get_treatment("key", "SPLIT_2", None) == 'on'

        self._update_temp_file(splits_json['splitChange5_2'])
        self._synchronize_now(factory)

        assert factory.manager().split_names() == ["SPLIT_2"]
        assert client.get_treatment("key", "SPLIT_2", None) == 'on'

        # Tests 6
        factory._sync_manager._synchronizer._split_synchronizers._split_sync._split_storage.set_change_number(-1)
        self._update_temp_file(splits_json['splitChange6_1'])
        self._synchronize_now(factory)

        assert factory.manager().split_names() == ["SPLIT_2", "SPLIT_1"]
        assert client.get_treatment("key", "SPLIT_1", None) == 'off'
        assert client.get_treatment("key", "SPLIT_2", None) == 'on'

        self._update_temp_file(splits_json['splitChange6_2'])
        self._synchronize_now(factory)

        assert factory.manager().split_names() == ["SPLIT_2", "SPLIT_1"]
        assert client.get_treatment("key", "SPLIT_1", None) == 'off'
        assert client.get_treatment("key", "SPLIT_2", None) == 'off'

        self._update_temp_file(splits_json['splitChange6_3'])
        self._synchronize_now(factory)

        assert factory.manager().split_names() == ["SPLIT_2"]
        assert client.get_treatment("key", "SPLIT_1", None) == 'control'
        assert client.get_treatment("key", "SPLIT_2", None) == 'on'

    def _update_temp_file(self, json_body):
        f = open(os.path.join(os.path.dirname(__file__), 'files','split_changes_temp.json'), 'w')
        f.write(json.dumps(json_body))
        f.close()

    def _synchronize_now(self, factory):
        filename = os.path.join(os.path.dirname(__file__), 'files', 'split_changes_temp.json')
        factory._sync_manager._synchronizer._split_synchronizers._split_sync._filename = filename
        factory._sync_manager._synchronizer._split_synchronizers._split_sync.synchronize_splits()
