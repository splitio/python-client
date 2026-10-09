"""Split Worker tests."""

import pytest
import os
import json
import copy
import queue
import time

from splitio_commons.util.backoff import Backoff
from splitio_commons.api import APIException
from splitio_commons.api.commons import FetchOptions
from splitio_commons.storage import DefinitionStorage, RuleBasedSegmentsStorage
from splitio.storage.inmemory import InMemorySplitStorage, InMemorySplitStorageAsync
from splitio_commons.storage.inmemmory import InMemoryRuleBasedSegmentStorage, InMemoryRuleBasedSegmentStorageAsync
from splitio_commons.storage import FlagSetsFilter
from splitio.models.splits import Split
from splitio_commons.models.rule_based_segments import RuleBasedSegment
from splitio.sync.split import SplitSynchronizer, SplitSynchronizerAsync, LocalSplitSynchronizer, LocalSplitSynchronizerAsync, LocalhostMode
from splitio.optional.loaders import aiofiles, asyncio
from tests.integration import splits_json, rbsegments_json

splits_raw = [{
    'changeNumber': 123,
    'trafficTypeName': 'user',
    'name': 'some_name',
    'trafficAllocation': 100,
    'trafficAllocationSeed': 123456,
    'seed': 321654,
    'status': 'ACTIVE',
    'killed': False,
    'defaultTreatment': 'off',
    'algo': 2,
    'conditions': [
        {
            'partitions': [
                {'treatment': 'on', 'size': 50},
                {'treatment': 'off', 'size': 50}
            ],
            'contitionType': 'WHITELIST',
            'label': 'some_label',
            'matcherGroup': {
                'matchers': [
                    {
                        'matcherType': 'WHITELIST',
                        'whitelistMatcherData': {
                            'whitelist': ['k1', 'k2', 'k3']
                        },
                        'negate': False,
                    }
                ],
                'combiner': 'AND'
            }
        }
    ],
    'sets': ['set1', 'set2']
}]

json_body = {
    "ff": {
        "t":1675095324253,
        "s":-1,
        'd': [{
        'changeNumber': 123,
        'trafficTypeName': 'user',
        'name': 'some_name',
        'trafficAllocation': 100,
        'trafficAllocationSeed': 123456,
        'seed': 321654,
        'status': 'ACTIVE',
        'killed': False,
        'defaultTreatment': 'off',
        'algo': 2,
        'conditions': [
            {
                'partitions': [
                    {'treatment': 'on', 'size': 50},
                    {'treatment': 'off', 'size': 50}
                ],
                'contitionType': 'WHITELIST',
                'label': 'some_label',
                'matcherGroup': {
                    'matchers': [
                        {
                            'matcherType': 'WHITELIST',
                            'whitelistMatcherData': {
                                'whitelist': ['k1', 'k2', 'k3']
                            },
                            'negate': False,
                        }
                    ],
                    'combiner': 'AND'
                }
          },
          {
            "conditionType": "ROLLOUT",
            "matcherGroup": {
              "combiner": "AND",
              "matchers": [
                {
                  "keySelector": {
                    "trafficType": "user"
                  },
                  "matcherType": "IN_RULE_BASED_SEGMENT",
                  "negate": False,
                  "userDefinedSegmentMatcherData": {
                    "segmentName": "sample_rule_based_segment"
                  }
                }
              ]
            },
            "partitions": [
              {
                "treatment": "on",
                "size": 100
              },
              {
                "treatment": "off",
                "size": 0
              }
            ],
            "label": "in rule based segment sample_rule_based_segment"
          },            
        ],
        'sets': ['set1', 'set2']}]
    },
    "rbs":  {
    "t": 1675095324253,
    "s": -1,
    "d": [
      {
        "changeNumber": 5,
        "name": "sample_rule_based_segment",
        "status": "ACTIVE",
        "trafficTypeName": "user",
        "excluded":{
          "keys":["mauro@split.io","gaston@split.io"],
          "segments":[]
        },
        "conditions": [
          {
            "matcherGroup": {
              "combiner": "AND",
              "matchers": [
                {
                  "keySelector": {
                    "trafficType": "user",
                    "attribute": "email"
                  },
                  "matcherType": "ENDS_WITH",
                  "negate": False,
                  "whitelistMatcherData": {
                    "whitelist": [
                      "@split.io"
                    ]
                  }
                }
              ]
            }
          }
        ]
      }
    ]
  }
}

class LocalSplitsSynchronizerTests(object):
    """Split synchronizer test cases."""

    payload = copy.deepcopy(json_body)

    def test_synchronize_definitions_error(self, mocker):
        """Test that if fetching splits fails at some_point, the task will continue running."""
        storage = mocker.Mock(spec=DefinitionStorage)
        rbs_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)
        split_synchronizer = LocalSplitSynchronizer("/incorrect_file", storage, rbs_storage)

        with pytest.raises(Exception):
            split_synchronizer.synchronize_definitions(1)

    def test_synchronize_definitions(self, mocker):
        """Test split sync."""
        events_queue = queue.Queue()
        storage = InMemorySplitStorage()
        rbs_storage = InMemoryRuleBasedSegmentStorage()

        def read_splits_from_json_file(*args, **kwargs):
                return self.payload

        split_synchronizer = LocalSplitSynchronizer("split.json", storage, rbs_storage, LocalhostMode.JSON)
        split_synchronizer._read_feature_flags_from_json_file = read_splits_from_json_file

        split_synchronizer.synchronize_definitions()
        inserted_split = storage.get(self.payload["ff"]["d"][0]['name'])
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == 'some_name'

        # Should sync when changenumber is not changed
        self.payload["ff"]["d"][0]['killed'] = True
        self.payload["ff"]["d"][0]['changeNumber'] = 124
        split_synchronizer.synchronize_definitions()
        inserted_split = storage.get(self.payload["ff"]["d"][0]['name'])
        assert inserted_split.killed

        # Should not sync when changenumber is less than stored
        self.payload["ff"]["t"] = 122
        self.payload["ff"]["d"][0]['killed'] = False
        split_synchronizer.synchronize_definitions()
        inserted_split = storage.get(self.payload["ff"]["d"][0]['name'])
        assert inserted_split.killed

        # Should sync when changenumber is higher than stored
        self.payload["ff"]["t"] = 1675095324999
        self.payload["ff"]["d"][0]['changeNumber'] = 125
        split_synchronizer._current_json_sha = "-1"
        split_synchronizer.synchronize_definitions()
        inserted_split = storage.get(self.payload["ff"]["d"][0]['name'])
        assert inserted_split.killed == False

        # Should sync when till is default (-1)
        self.payload["ff"]["t"] = -1
        split_synchronizer._current_json_sha = "-1"
        self.payload["ff"]["d"][0]['killed'] = True
        self.payload["ff"]["d"][0]['changeNumber'] = 126
        split_synchronizer.synchronize_definitions()
        inserted_split = storage.get(self.payload["ff"]["d"][0]['name'])
        assert inserted_split.killed == True

    def test_sync_flag_sets_with_config_sets(self, mocker):
        """Test split sync with flag sets."""
        events_queue = queue.Queue()
        storage = InMemorySplitStorage(['set1', 'set2'])
        rbs_storage = InMemoryRuleBasedSegmentStorage()
        
        split = self.payload["ff"]["d"][0].copy()
        split['name'] = 'second'
        splits1 = [self.payload["ff"]["d"][0].copy(), split]
        splits2 = self.payload["ff"]["d"].copy()
        splits3 = self.payload["ff"]["d"].copy()
        splits4 = self.payload["ff"]["d"].copy()

        self.called = 0
        def read_feature_flags_from_json_file(*args, **kwargs):
            self.called += 1
            if self.called == 1:
                splits1[0]['changeNumber'] = 124
                return {"ff": {"d": splits1, "t": 124, "s": -1}, "rbs": {"d": [], "t": -1, "s": -1}}
            elif self.called == 2:
                splits2[0]['changeNumber'] = 125
                splits2[0]['sets'] = ['set3']
                return {"ff": {"d": splits2, "t": 125, "s": -1}, "rbs": {"d": [], "t": -1, "s": -1}}
            elif self.called == 3:
                splits3[0]['changeNumber'] = 126
                splits3[0]['sets'] = ['set1']
                return {"ff": {"d": splits3, "t": 12434, "s": -1}, "rbs": {"d": [], "t": -1, "s": -1}}
            splits4[0]['sets'] = ['set6']
            splits4[0]['name'] = 'new_split'
            splits4[0]['changeNumber'] = 127
            return {"ff": {"d": splits4, "t": 12438, "s": -1}, "rbs": {"d": [], "t": -1, "s": -1}}

        split_synchronizer = LocalSplitSynchronizer("split.json", storage, rbs_storage, LocalhostMode.JSON)
        split_synchronizer._read_feature_flags_from_json_file = read_feature_flags_from_json_file

        split_synchronizer.synchronize_definitions()
        assert isinstance(storage.get('some_name'), Split)

        split_synchronizer.synchronize_definitions(125)
        assert storage.get('some_name') == None

        split_synchronizer.synchronize_definitions(12434)
        assert isinstance(storage.get('some_name'), Split)

        split_synchronizer.synchronize_definitions(12438)
        assert storage.get('new_name') == None

    def test_sync_flag_sets_without_config_sets(self, mocker):
        """Test split sync with flag sets."""
        events_queue = queue.Queue()
        storage = InMemorySplitStorage()
        rbs_storage = InMemoryRuleBasedSegmentStorage()

        split = self.payload["ff"]["d"][0].copy()
        split['name'] = 'second'
        splits1 = [self.payload["ff"]["d"][0].copy(), split]
        splits2 = self.payload["ff"]["d"].copy()
        splits3 = self.payload["ff"]["d"].copy()
        splits4 = self.payload["ff"]["d"].copy()

        self.called = 0
        def read_feature_flags_from_json_file(*args, **kwargs):
            self.called += 1
            if self.called == 1:
                return {"ff": {"d": splits1, "t": 123, "s": -1}, "rbs": {"d": [], "t": -1, "s": -1}}
            elif self.called == 2:
                splits2[0]['sets'] = ['set3']
                return {"ff": {"d": splits2, "t": 124, "s": -1}, "rbs": {"d": [], "t": -1, "s": -1}}
            elif self.called == 3:
                splits3[0]['sets'] = ['set1']
                return {"ff": {"d": splits3, "t": 12434, "s": -1}, "rbs": {"d": [], "t": -1, "s": -1}}
            splits4[0]['sets'] = ['set6']
            splits4[0]['name'] = 'third_split'
            return {"ff": {"d": splits4, "t": 12438, "s": -1}, "rbs": {"d": [], "t": -1, "s": -1}}

        split_synchronizer = LocalSplitSynchronizer("split.json", storage, rbs_storage, LocalhostMode.JSON)
        split_synchronizer._read_feature_flags_from_json_file = read_feature_flags_from_json_file

        split_synchronizer.synchronize_definitions()
        assert isinstance(storage.get('new_split'), Split)

        split_synchronizer.synchronize_definitions(124)
        assert isinstance(storage.get('new_split'), Split)

        split_synchronizer.synchronize_definitions(12434)
        assert isinstance(storage.get('new_split'), Split)

        split_synchronizer.synchronize_definitions(12438)
        assert isinstance(storage.get('third_split'), Split)

    def test_reading_json(self, mocker):
        """Test reading json file."""
        f = open("./splits.json", "w")
        f.write(json.dumps(self.payload))
        f.close()
        events_queue = queue.Queue()
        storage = InMemorySplitStorage()
        rbs_storage = InMemoryRuleBasedSegmentStorage()
        split_synchronizer = LocalSplitSynchronizer("./splits.json", storage, rbs_storage, LocalhostMode.JSON)
        split_synchronizer.synchronize_definitions()

        inserted_split = storage.get(self.payload['ff']['d'][0]['name'])
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == self.payload['ff']['d'][0]['name']

        inserted_rbs = rbs_storage.get(self.payload['rbs']['d'][0]['name'])
        assert isinstance(inserted_rbs, RuleBasedSegment)
        assert inserted_rbs.name == self.payload['rbs']['d'][0]['name']

        os.remove("./splits.json")

    def test_json_elements_sanitization(self, mocker):
        """Test sanitization."""
        split_synchronizer = LocalSplitSynchronizer(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())

        # check no changes if all elements exist with valid values
        parsed = {"ff": {"d": [], "s": -1, "t": -1}, "rbs": {"d": [], "s": -1, "t": -1}}
        assert (split_synchronizer._sanitize_json_elements(parsed) == parsed)

        # check set since to -1 when is None
        parsed2 = parsed.copy()
        parsed2['ff']['s'] = None
        assert (split_synchronizer._sanitize_json_elements(parsed2) == parsed)

        # check no changes if since > -1
        parsed2 = parsed.copy()
        parsed2['ff']['s'] = 12
        assert (split_synchronizer._sanitize_json_elements(parsed2) == parsed)

        # check set till to -1 when is None
        parsed2 = parsed.copy()
        parsed2['ff']['t'] = None
        assert (split_synchronizer._sanitize_json_elements(parsed2) == parsed)

        # check add since when missing
        parsed2 = {"ff": {"d": [], "t": -1}, "rbs": {"d": [], "s": -1, "t": -1}}
        assert (split_synchronizer._sanitize_json_elements(parsed2) == parsed)

        # check add till when missing
        parsed2 = {"ff": {"d": [], "s": -1}, "rbs": {"d": [], "s": -1, "t": -1}}
        assert (split_synchronizer._sanitize_json_elements(parsed2) == parsed)

        # check add splits when missing
        parsed2 = {"ff": {"s": -1, "t": -1}, "rbs": {"d": [], "s": -1, "t": -1}}
        assert (split_synchronizer._sanitize_json_elements(parsed2) == parsed)

        # check add since when missing
        parsed2 = {"ff": {"d": [], "t": -1}, "rbs": {"d": [], "t": -1}}
        assert (split_synchronizer._sanitize_json_elements(parsed2) == parsed)

        # check add till when missing
        parsed2 = {"ff": {"d": [], "s": -1}, "rbs": {"d": [], "s": -1}}
        assert (split_synchronizer._sanitize_json_elements(parsed2) == parsed)

        # check add splits when missing
        parsed2 = {"ff": {"s": -1, "t": -1}, "rbs": {"s": -1, "t": -1}}
        assert (split_synchronizer._sanitize_json_elements(parsed2) == parsed)

    def test_elements_sanitization(self, mocker):
        """Test sanitization."""
        split_synchronizer = LocalSplitSynchronizer(mocker.Mock(), mocker.Mock(), mocker.Mock(), mocker.Mock())

        # No changes when split structure is good
        assert (split_synchronizer._sanitize_feature_flag_elements(splits_json["splitChange1_1"]['ff']['d']) == splits_json["splitChange1_1"]['ff']['d'])

        # test 'trafficTypeName' value None
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]['trafficTypeName'] = None
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == splits_json["splitChange1_1"]['ff']['d'])

        # test 'trafficAllocation' value None
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]['trafficAllocation'] = None
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == splits_json["splitChange1_1"]['ff']['d'])

        # test 'trafficAllocation' valid value should not change
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]['trafficAllocation'] = 50
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == split)

        # test 'trafficAllocation' invalid value should change
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]['trafficAllocation'] = 110
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == splits_json["splitChange1_1"]['ff']['d'])

        # test 'trafficAllocationSeed' is set to millisec epoch when None
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]['trafficAllocationSeed'] = None
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['trafficAllocationSeed'] > 0)

        # test 'trafficAllocationSeed' is set to millisec epoch when 0
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]['trafficAllocationSeed'] = 0
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['trafficAllocationSeed'] > 0)

        # test 'seed' is set to millisec epoch when None
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]['seed'] = None
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['seed'] > 0)

        # test 'seed' is set to millisec epoch when its 0
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]['seed'] = 0
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['seed'] > 0)

        # test 'status' is set to ACTIVE when None
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]['status'] = None
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == splits_json["splitChange1_1"]['ff']['d'])

        # test 'status' is set to ACTIVE when incorrect
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]['status'] = 'ww'
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == splits_json["splitChange1_1"]['ff']['d'])

        # test ''killed' is set to False when incorrect
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]['killed'] = None
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == splits_json["splitChange1_1"]['ff']['d'])

        # test 'defaultTreatment' is set to on when None
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]['defaultTreatment'] = None
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['defaultTreatment'] == 'control')

        # test 'defaultTreatment' is set to on when its empty
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]['defaultTreatment'] = ' '
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['defaultTreatment'] == 'control')

        # test 'changeNumber' is set to 0 when None
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]['changeNumber'] = None
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['changeNumber'] == 0)

        # test 'changeNumber' is set to 0 when invalid
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]['changeNumber'] = -33
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['changeNumber'] == 0)

        # test 'algo' is set to 2 when None
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]['algo'] = None
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['algo'] == 2)

        # test 'algo' is set to 2 when higher than 2
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]['algo'] = 3
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['algo'] == 2)

        # test 'algo' is set to 2 when lower than 2
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]['algo'] = 1
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['algo'] == 2)

        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        del split[0]['prerequisites']
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['prerequisites'] == [])

        # test 'status' is set to ACTIVE when None
        rbs = copy.deepcopy(json_body["rbs"]["d"])
        rbs[0]['status'] = None
        assert (split_synchronizer._sanitize_rb_segment_elements(rbs)[0]['status'] == 'ACTIVE')

        # test 'changeNumber' is set to 0 when invalid
        rbs = copy.deepcopy(json_body["rbs"]["d"])
        rbs[0]['changeNumber'] = -2
        assert (split_synchronizer._sanitize_rb_segment_elements(rbs)[0]['changeNumber'] == 0)

        rbs = copy.deepcopy(json_body["rbs"]["d"])
        del rbs[0]['conditions']
        assert (len(split_synchronizer._sanitize_rb_segment_elements(rbs)[0]['conditions']) == 1)

    def test_condition_sanitization(self, mocker):
        """Test sanitization."""
        split_synchronizer = LocalSplitSynchronizer(mocker.Mock(), mocker.Mock(), mocker.Mock())

        # test missing all conditions with default rule set to 100% off
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        target_split = splits_json["splitChange1_1"]['ff']['d'].copy()
        target_split[0]["conditions"][0]['partitions'][0]['size'] = 0
        target_split[0]["conditions"][0]['partitions'][1]['size'] = 100
        del split[0]["conditions"]
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == target_split)

        # test missing ALL_KEYS condition matcher with default rule set to 100% off
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        target_split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]["conditions"][0]["matcherGroup"]["matchers"][0]["matcherType"] = "IN_STR"
        target_split = split.copy()
        target_split[0]["conditions"].append(splits_json["splitChange1_1"]['ff']['d'][0]["conditions"][0])
        target_split[0]["conditions"][1]['partitions'][0]['size'] = 0
        target_split[0]["conditions"][1]['partitions'][1]['size'] = 100
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == target_split)

        # test missing ROLLOUT condition type with default rule set to 100% off
        split = splits_json["splitChange1_1"]['ff']['d'].copy()
        target_split = splits_json["splitChange1_1"]['ff']['d'].copy()
        split[0]["conditions"][0]["conditionType"] = "NOT"
        target_split = split.copy()
        target_split[0]["conditions"].append(splits_json["splitChange1_1"]['ff']['d'][0]["conditions"][0])
        target_split[0]["conditions"][1]['partitions'][0]['size'] = 0
        target_split[0]["conditions"][1]['partitions'][1]['size'] = 100
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == target_split)

class LocalSplitsSynchronizerAsyncTests(object):
    """Split synchronizer test cases."""

    payload = copy.deepcopy(json_body)

    @pytest.mark.asyncio
    async def test_synchronize_definitions_error(self, mocker):
        """Test that if fetching splits fails at some_point, the task will continue running."""
        storage = mocker.Mock(spec=DefinitionStorage)
        rbs_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)
        split_synchronizer = LocalSplitSynchronizerAsync("/incorrect_file", storage, rbs_storage)

        with pytest.raises(Exception):
            await split_synchronizer.synchronize_definitions(1)

    @pytest.mark.asyncio
    async def test_synchronize_definitions(self, mocker):
        """Test split sync."""
        internal_events_queue = asyncio.Queue()
        storage = InMemorySplitStorageAsync()
        rbs_storage = InMemoryRuleBasedSegmentStorageAsync()

        async def read_splits_from_json_file(*args, **kwargs):
            return self.payload

        split_synchronizer = LocalSplitSynchronizerAsync("split.json", storage, rbs_storage, LocalhostMode.JSON)
        split_synchronizer._read_feature_flags_from_json_file = read_splits_from_json_file

        await split_synchronizer.synchronize_definitions()
        inserted_split = await storage.get(self.payload["ff"]["d"][0]['name'])
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == 'some_name'

        # Should sync when changenumber is not changed
        self.payload["ff"]["d"][0]['killed'] = True
        self.payload["ff"]["d"][0]['changeNumber'] = 125
        await split_synchronizer.synchronize_definitions()
        inserted_split = await storage.get(self.payload["ff"]["d"][0]['name'])
        assert inserted_split.killed

        # Should not sync when changenumber is less than stored
        self.payload["ff"]["t"] = 122
        self.payload["ff"]["d"][0]['killed'] = False
        self.payload["ff"]["d"][0]['changeNumber'] = 126
        await split_synchronizer.synchronize_definitions()
        inserted_split = await storage.get(self.payload["ff"]["d"][0]['name'])
        assert inserted_split.killed

        # Should sync when changenumber is higher than stored
        self.payload["ff"]["t"] = 1675095324999
        split_synchronizer._current_json_sha = "-1"
        await split_synchronizer.synchronize_definitions()
        inserted_split = await storage.get(self.payload["ff"]["d"][0]['name'])
        assert inserted_split.killed == False

        # Should sync when till is default (-1)
        self.payload["ff"]["t"] = -1
        split_synchronizer._current_json_sha = "-1"
        self.payload["ff"]["d"][0]['killed'] = True
        self.payload["ff"]["d"][0]['changeNumber'] = 127
        await split_synchronizer.synchronize_definitions()
        inserted_split = await storage.get(self.payload["ff"]["d"][0]['name'])
        assert inserted_split.killed == True

    @pytest.mark.asyncio
    async def test_sync_flag_sets_with_config_sets(self, mocker):
        """Test split sync with flag sets."""
        internal_events_queue = asyncio.Queue()
        storage = InMemorySplitStorageAsync(['set1', 'set2'])
        rbs_storage = InMemoryRuleBasedSegmentStorageAsync()
        
        split = self.payload["ff"]["d"][0].copy()
        split['name'] = 'second'
        splits1 = [self.payload["ff"]["d"][0].copy(), split]
        splits2 = self.payload["ff"]["d"].copy()
        splits3 = self.payload["ff"]["d"].copy()
        splits4 = self.payload["ff"]["d"].copy()

        self.called = 0
        async def read_feature_flags_from_json_file(*args, **kwargs):
            self.called += 1
            if self.called == 1:
                return {"ff": {"d": splits1, "t": 123, "s": -1}, "rbs": {"d": [], "t": -1, "s": -1}}
            elif self.called == 2:
                splits2[0]['sets'] = ['set3']
                return {"ff": {"d": splits2, "t": 124, "s": -1}, "rbs": {"d": [], "t": -1, "s": -1}}
            elif self.called == 3:
                splits3[0]['sets'] = ['set1']
                return {"ff": {"d": splits3, "t": 12434, "s": -1}, "rbs": {"d": [], "t": -1, "s": -1}}            
            splits4[0]['sets'] = ['set6']
            splits4[0]['name'] = 'new_split'
            return {"ff": {"d": splits4, "t": 12438, "s": -1}, "rbs": {"d": [], "t": -1, "s": -1}}

        split_synchronizer = LocalSplitSynchronizerAsync("split.json", storage, rbs_storage, LocalhostMode.JSON)
        split_synchronizer._read_feature_flags_from_json_file = read_feature_flags_from_json_file

        await split_synchronizer.synchronize_definitions()
        assert isinstance(await storage.get('some_name'), Split)

        await split_synchronizer.synchronize_definitions(124)
        assert await storage.get('some_name') == None

        await split_synchronizer.synchronize_definitions(12434)
        assert isinstance(await storage.get('some_name'), Split)

        await split_synchronizer.synchronize_definitions(12438)
        assert await storage.get('new_name') == None

    @pytest.mark.asyncio
    async def test_sync_flag_sets_without_config_sets(self, mocker):
        """Test split sync with flag sets."""
        internal_events_queue = asyncio.Queue()
        storage = InMemorySplitStorageAsync()
        rbs_storage = InMemoryRuleBasedSegmentStorageAsync()
        
        split = self.payload["ff"]["d"][0].copy()
        split['name'] = 'second'
        splits1 = [self.payload["ff"]["d"][0].copy(), split]
        splits2 = self.payload["ff"]["d"].copy()
        splits3 = self.payload["ff"]["d"].copy()
        splits4 = self.payload["ff"]["d"].copy()

        self.called = 0
        async def read_feature_flags_from_json_file(*args, **kwargs):
            self.called += 1
            if self.called == 1:
                return {"ff": {"d": splits1, "t": 123, "s": -1}, "rbs": {"d": [], "t": -1, "s": -1}}
            elif self.called == 2:
                return {"ff": {"d": splits2, "t": 124, "s": -1}, "rbs": {"d": [], "t": -1, "s": -1}}
            elif self.called == 3:
                splits3[0]['sets'] = ['set1']
                return {"ff": {"d": splits3, "t": 12434, "s": -1}, "rbs": {"d": [], "t": -1, "s": -1}}
            splits4[0]['sets'] = ['set6']
            splits4[0]['name'] = 'third_split'
            return {"ff": {"d": splits4, "t": 12438, "s": -1}, "rbs": {"d": [], "t": -1, "s": -1}}

        split_synchronizer = LocalSplitSynchronizerAsync("split.json", storage, rbs_storage, LocalhostMode.JSON)
        split_synchronizer._read_feature_flags_from_json_file = read_feature_flags_from_json_file

        await split_synchronizer.synchronize_definitions()
        assert isinstance(await storage.get('new_split'), Split)

        await split_synchronizer.synchronize_definitions(124)
        assert isinstance(await storage.get('new_split'), Split)

        await split_synchronizer.synchronize_definitions(12434)
        assert isinstance(await storage.get('new_split'), Split)

        await split_synchronizer.synchronize_definitions(12438)
        assert isinstance(await storage.get('third_split'), Split)

    @pytest.mark.asyncio
    async def test_reading_json(self, mocker):
        """Test reading json file."""
        async with aiofiles.open("./splits.json", "w") as f:
            await f.write(json.dumps(self.payload))
        internal_events_queue = asyncio.Queue()
        storage = InMemorySplitStorageAsync()
        rbs_storage = InMemoryRuleBasedSegmentStorageAsync()
        split_synchronizer = LocalSplitSynchronizerAsync("./splits.json", storage, rbs_storage, LocalhostMode.JSON)
        await split_synchronizer.synchronize_definitions()

        inserted_split = await storage.get(self.payload['ff']['d'][0]['name'])
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == self.payload['ff']['d'][0]['name']

        inserted_rbs = await rbs_storage.get(self.payload['rbs']['d'][0]['name'])
        assert isinstance(inserted_rbs, RuleBasedSegment)
        assert inserted_rbs.name == self.payload['rbs']['d'][0]['name']

        os.remove("./splits.json")
