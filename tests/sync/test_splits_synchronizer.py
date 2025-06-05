"""Split Worker tests."""

import pytest
import os
import json
import copy

from splitio.util.backoff import Backoff
from splitio.api import APIException
from splitio.api.commons import FetchOptions
from splitio.storage import SplitStorage, RuleBasedSegmentsStorage
from splitio.storage.inmemmory import InMemorySplitStorage, InMemorySplitStorageAsync, InMemoryRuleBasedSegmentStorage, InMemoryRuleBasedSegmentStorageAsync
from splitio.storage import FlagSetsFilter
from splitio.models.splits import Split
from splitio.models.rule_based_segments import RuleBasedSegment
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

class SplitsSynchronizerTests(object):
    """Split synchronizer test cases."""

    splits = copy.deepcopy(splits_raw)

    def test_synchronize_splits_error(self, mocker):
        """Test that if fetching splits fails at some_point, the task will continue running."""
        storage = mocker.Mock(spec=InMemorySplitStorage)
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorage)
        api = mocker.Mock()

        def run(x, y, c):
            raise APIException("something broke")
        run._calls = 0
        api.fetch_splits.side_effect = run
        storage.get_change_number.return_value = -1
        rbs_storage.get_change_number.return_value = -1
        
        class flag_set_filter():
            def should_filter():
                return False

            def intersect(sets):
                return True
        storage.flag_set_filter = flag_set_filter
        storage.flag_set_filter.flag_sets = {}
        storage.flag_set_filter.sorted_flag_sets = []

        split_synchronizer = SplitSynchronizer(api, storage, rbs_storage)

        with pytest.raises(APIException):
            split_synchronizer.synchronize_splits(1)

    def test_synchronize_splits(self, mocker):
        """Test split sync."""
        storage = mocker.Mock(spec=InMemorySplitStorage)
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorage)

        def change_number_mock():
            change_number_mock._calls += 1
            if change_number_mock._calls == 1:
                return -1
            return 123
        
        def rbs_change_number_mock():
            rbs_change_number_mock._calls += 1
            if rbs_change_number_mock._calls == 1:
                return -1
            return 123
        
        change_number_mock._calls = 0
        rbs_change_number_mock._calls = 0
        storage.get_change_number.side_effect = change_number_mock
        rbs_storage.get_change_number.side_effect = rbs_change_number_mock
        
        class flag_set_filter():
            def should_filter():
                return False

            def intersect(sets):
                return True
            
        storage.flag_set_filter = flag_set_filter
        storage.flag_set_filter.flag_sets = {}
        storage.flag_set_filter.sorted_flag_sets = []

        api = mocker.Mock()
        def get_changes(*args, **kwargs):
            get_changes.called += 1

            if get_changes.called == 1:
                return json_body
            else:
                return {
                    "ff": {
                        "t":123,
                        "s":123,
                        'd': []
                    },
                    "rbs":  {
                        "t": 5,
                        "s": 5,
                        "d": []
                    }               
                }
                
        get_changes.called = 0
        api.fetch_splits.side_effect = get_changes

        split_synchronizer = SplitSynchronizer(api, storage, rbs_storage)
        split_synchronizer.synchronize_splits()
    
        assert api.fetch_splits.mock_calls[0][1][0] == -1
        assert api.fetch_splits.mock_calls[0][1][2].cache_control_headers == True
        assert api.fetch_splits.mock_calls[1][1][0] == 123
        assert api.fetch_splits.mock_calls[1][1][1] == 123
        assert api.fetch_splits.mock_calls[1][1][2].cache_control_headers == True

        inserted_split = storage.update.mock_calls[0][1][0][0]
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == 'some_name'

        inserted_rbs = rbs_storage.update.mock_calls[0][1][0][0]
        assert isinstance(inserted_rbs, RuleBasedSegment)
        assert inserted_rbs.name == 'sample_rule_based_segment'

    def test_not_called_on_till(self, mocker):
        """Test that sync is not called when till is less than previous changenumber"""
        storage = mocker.Mock(spec=InMemorySplitStorage)
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorage)

        class flag_set_filter():
            def should_filter():
                return False

            def intersect(sets):
                return True
        storage.flag_set_filter = flag_set_filter
        storage.flag_set_filter.flag_sets = {}
        storage.flag_set_filter.sorted_flag_sets = []

        def change_number_mock():
            return 2
        storage.get_change_number.side_effect = change_number_mock
        rbs_storage.get_change_number.side_effect = change_number_mock

        def get_changes(*args, **kwargs):
            get_changes.called += 1
            return None

        get_changes.called = 0

        api = mocker.Mock()
        api.fetch_splits.side_effect = get_changes

        split_synchronizer = SplitSynchronizer(api, storage, rbs_storage)
        split_synchronizer.synchronize_splits(1)

        assert get_changes.called == 0

    def test_synchronize_splits_cdn(self, mocker):
        """Test split sync with bypassing cdn."""
        mocker.patch('splitio.sync.split._ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES', new=3)

        storage = mocker.Mock(spec=InMemorySplitStorage)
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorage)

        def change_number_mock():
            change_number_mock._calls += 1
            if change_number_mock._calls == 1:
                return -1
            elif change_number_mock._calls >= 2 and change_number_mock._calls <= 3:
                return 123
            elif change_number_mock._calls <= 7:
                return 1234
            return 12345 # Return proper cn for CDN Bypass

        def rbs_change_number_mock():
            rbs_change_number_mock._calls += 1
            if rbs_change_number_mock._calls == 1:
                return -1
            elif change_number_mock._calls >= 2 and change_number_mock._calls <= 3:
                return 555
            elif change_number_mock._calls <= 9:
                return 555
            return 666 # Return proper cn for CDN Bypass
        
        change_number_mock._calls = 0
        rbs_change_number_mock._calls = 0
        storage.get_change_number.side_effect = change_number_mock
        rbs_storage.get_change_number.side_effect = rbs_change_number_mock

        api = mocker.Mock()
        rbs_1 = copy.deepcopy(json_body['rbs']['d'])
        def get_changes(*args, **kwargs):
            get_changes.called += 1
            if get_changes.called == 1:
                return { 'ff': { 'd': self.splits, 's': -1, 't': 123 }, 
                        'rbs':  {"t": 555, "s": -1, "d": rbs_1}}
            elif get_changes.called == 2:
                return { 'ff': { 'd': [], 's': 123, 't': 123 },
                        'rbs':  {"t": 555, "s": 555, "d": []}}
            elif get_changes.called == 3:
                return { 'ff': { 'd': [], 's': 123, 't': 1234 },
                        'rbs':  {"t": 555, "s": 555, "d": []}}
            elif get_changes.called >= 4 and get_changes.called <= 6:
                return { 'ff': { 'd': [], 's': 1234, 't': 1234 },
                        'rbs':  {"t": 555, "s": 555, "d": []}}
            elif get_changes.called == 7:
                return { 'ff': { 'd': [], 's': 1234, 't': 12345 },
                        'rbs':  {"t": 555, "s": 555, "d": []}}
            elif get_changes.called == 8:
                return { 'ff': { 'd': [], 's': 12345, 't': 12345 },
                        'rbs':  {"t": 555, "s": 555, "d": []}}
            rbs_1[0]['excluded']['keys'] = ['bilal@split.io']
            return { 'ff': { 'd': [], 's': 12345, 't': 12345 },
                        'rbs':  {"t": 666, "s": 666, "d": rbs_1}}

        get_changes.called = 0
        api.fetch_splits.side_effect = get_changes

        class flag_set_filter():
            def should_filter():
                return False

            def intersect(sets):
                return True

        storage.flag_set_filter = flag_set_filter
        storage.flag_set_filter.flag_sets = {}
        storage.flag_set_filter.sorted_flag_sets = []

        split_synchronizer = SplitSynchronizer(api, storage, rbs_storage)
        split_synchronizer._backoff = Backoff(1, 1)
        split_synchronizer.synchronize_splits()

        assert api.fetch_splits.mock_calls[0][1][0] == -1
        assert api.fetch_splits.mock_calls[0][1][2].cache_control_headers == True
        assert api.fetch_splits.mock_calls[1][1][0] == 123
        assert api.fetch_splits.mock_calls[1][1][2].cache_control_headers == True

        split_synchronizer._backoff = Backoff(1, 0.1)
        split_synchronizer.synchronize_splits(12345)
        assert api.fetch_splits.mock_calls[3][1][0] == 1234
        assert api.fetch_splits.mock_calls[3][1][2].cache_control_headers == True
        assert len(api.fetch_splits.mock_calls) == 8 # 2 ok + BACKOFF(2 since==till + 2 re-attempts) + CDN(2 since==till)

        inserted_split = storage.update.mock_calls[0][1][0][0]
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == 'some_name'
        inserted_rbs = rbs_storage.update.mock_calls[0][1][0][0]
        assert inserted_rbs.excluded.get_excluded_keys() == ["mauro@split.io","gaston@split.io"]

        split_synchronizer._backoff = Backoff(1, 0.1)
        split_synchronizer.synchronize_splits(None, 666)
        inserted_rbs = rbs_storage.update.mock_calls[8][1][0][0]
        assert inserted_rbs.excluded.get_excluded_keys() == ['bilal@split.io']
        
    def test_sync_flag_sets_with_config_sets(self, mocker):
        """Test split sync with flag sets."""
        storage = InMemorySplitStorage(['set1', 'set2'])
        rbs_storage = InMemoryRuleBasedSegmentStorage()
        
        split = copy.deepcopy(self.splits[0])
        split['name'] = 'second'
        splits1 = [self.splits[0].copy(), split]
        splits2 = copy.deepcopy(self.splits)
        splits3 = copy.deepcopy(self.splits)
        splits4 = copy.deepcopy(self.splits)
        api = mocker.Mock()
        def get_changes(*args, **kwargs):
            get_changes.called += 1
            if get_changes.called == 1:
                return { 'ff': { 'd': splits1, 's': 123, 't': 123 },
                        'rbs':  {'t': 123, 's': 123, 'd': []}}                        
            elif get_changes.called == 2:
                splits2[0]['sets'] = ['set3']
                return { 'ff': { 'd': splits2, 's': 124, 't': 124 },
                        'rbs':  {'t': 124, 's': 124, 'd': []}}                        
            elif get_changes.called == 3:
                splits3[0]['sets'] = ['set1']
                return { 'ff': { 'd': splits3, 's': 12434, 't': 12434 },
                        'rbs':  {'t': 12434, 's': 12434, 'd': []}}                        
            splits4[0]['sets'] = ['set6']
            splits4[0]['name'] = 'new_split'
            return { 'ff': { 'd': splits4, 's': 12438, 't': 12438 },
                        'rbs':  {'t': 12438, 's': 12438, 'd': []}}                        
        get_changes.called = 0
        api.fetch_splits.side_effect = get_changes

        split_synchronizer = SplitSynchronizer(api, storage, rbs_storage)
        split_synchronizer._backoff = Backoff(1, 1)
        split_synchronizer.synchronize_splits()
        assert isinstance(storage.get('some_name'), Split)

        split_synchronizer.synchronize_splits(124)
        assert storage.get('some_name') == None

        split_synchronizer.synchronize_splits(12434)
        assert isinstance(storage.get('some_name'), Split)

        split_synchronizer.synchronize_splits(12438)
        assert storage.get('new_name') == None

    def test_sync_flag_sets_without_config_sets(self, mocker):
        """Test split sync with flag sets."""
        storage = InMemorySplitStorage()
        rbs_storage = InMemoryRuleBasedSegmentStorage()
        split = copy.deepcopy(self.splits[0])
        split['name'] = 'second'
        splits1 = [self.splits[0].copy(), split]
        splits2 = copy.deepcopy(self.splits)
        splits3 = copy.deepcopy(self.splits)
        splits4 = copy.deepcopy(self.splits)
        api = mocker.Mock()
        def get_changes(*args, **kwargs):
            get_changes.called += 1
            if get_changes.called == 1:
                return { 'ff': { 'd': splits1, 's': 123, 't': 123 },
                        'rbs':  {"t": 123, "s": 123, "d": []}}                        
            elif get_changes.called == 2:
                splits2[0]['sets'] = ['set3']
                return { 'ff': { 'd': splits2, 's': 124, 't': 124 },
                        'rbs':  {"t": 124, "s": 124, "d": []}}                        
            elif get_changes.called == 3:
                splits3[0]['sets'] = ['set1']
                return { 'ff': { 'd': splits3, 's': 12434, 't': 12434 },
                        'rbs':  {"t": 12434, "s": 12434, "d": []}}                        
            splits4[0]['sets'] = ['set6']
            splits4[0]['name'] = 'third_split'
            return { 'ff': { 'd': splits4, 's': 12438, 't': 12438 },
                        'rbs':  {"t": 12438, "s": 12438, "d": []}}                        
        get_changes.called = 0
        api.fetch_splits.side_effect = get_changes

        split_synchronizer = SplitSynchronizer(api, storage, rbs_storage)
        split_synchronizer._backoff = Backoff(1, 1)
        split_synchronizer.synchronize_splits()
        assert isinstance(storage.get('some_name'), Split)

        split_synchronizer.synchronize_splits(124)
        assert isinstance(storage.get('some_name'), Split)

        split_synchronizer.synchronize_splits(12434)
        assert isinstance(storage.get('some_name'), Split)

        split_synchronizer.synchronize_splits(12438)
        assert isinstance(storage.get('third_split'), Split)

class SplitsSynchronizerAsyncTests(object):
    """Split synchronizer test cases."""

    splits = copy.deepcopy(splits_raw)

    @pytest.mark.asyncio
    async def test_synchronize_splits_error(self, mocker):
        """Test that if fetching splits fails at some_point, the task will continue running."""
        storage = mocker.Mock(spec=InMemorySplitStorageAsync)
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorageAsync)
        api = mocker.Mock()

        async def run(x, y, c):
            raise APIException("something broke")
        run._calls = 0
        api.fetch_splits = run

        async def get_change_number(*args):
            return -1
        storage.get_change_number = get_change_number        
        rbs_storage.get_change_number = get_change_number
        
        class flag_set_filter():
            def should_filter():
                return False

            def intersect(sets):
                return True
        storage.flag_set_filter = flag_set_filter
        storage.flag_set_filter.flag_sets = {}
        storage.flag_set_filter.sorted_flag_sets = []

        split_synchronizer = SplitSynchronizerAsync(api, storage, rbs_storage)

        with pytest.raises(APIException):
            await split_synchronizer.synchronize_splits(1)

    @pytest.mark.asyncio
    async def test_synchronize_splits(self, mocker):
        """Test split sync."""
        storage = mocker.Mock(spec=InMemorySplitStorageAsync)
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorageAsync)
        
        async def change_number_mock():
            change_number_mock._calls += 1
            if change_number_mock._calls == 1:
                return -1
            return 123
        async def rbs_change_number_mock():
            rbs_change_number_mock._calls += 1
            if rbs_change_number_mock._calls == 1:
                return -1
            return 123

        change_number_mock._calls = 0
        rbs_change_number_mock._calls = 0        
        storage.get_change_number = change_number_mock
        rbs_storage.get_change_number.side_effect = rbs_change_number_mock
        
        class flag_set_filter():
            def should_filter():
                return False

            def intersect(sets):
                return True
        storage.flag_set_filter = flag_set_filter
        storage.flag_set_filter.flag_sets = {}
        storage.flag_set_filter.sorted_flag_sets = []

        self.parsed_split = None
        async def update(parsed_split, deleted, chanhe_number):
            if len(parsed_split) > 0:
                self.parsed_split = parsed_split
        storage.update = update

        self.parsed_rbs = None
        async def update(parsed_rbs, deleted, chanhe_number):
            if len(parsed_rbs) > 0:
                self.parsed_rbs = parsed_rbs
        rbs_storage.update = update

        self.clear = False
        async def clear():
            self.clear = True
        storage.clear = clear

        self.clear2 = False
        async def clear():
            self.clear2 = True
        rbs_storage.clear = clear
        
        api = mocker.Mock()
        self.change_number_1 = None
        self.fetch_options_1 = None
        self.change_number_2 = None
        self.fetch_options_2 = None
        async def get_changes(change_number, rbs_change_number, fetch_options):
            get_changes.called += 1
            if get_changes.called == 1:
                self.change_number_1 = change_number
                self.fetch_options_1 = fetch_options
                return json_body
            else:
                self.change_number_2 = change_number
                self.fetch_options_2 = fetch_options
                return {
                    "ff": {
                        "t":123,
                        "s":123,
                        'd': []
                    },
                    "rbs":  {
                        "t": 123,
                        "s": 123,
                        "d": []
                    }               
                }
        get_changes.called = 0
        api.fetch_splits = get_changes
        api.clear_storage.return_value = False

        split_synchronizer = SplitSynchronizerAsync(api, storage, rbs_storage)
        await split_synchronizer.synchronize_splits()

        assert (-1, FetchOptions(True)._cache_control_headers) == (self.change_number_1, self.fetch_options_1._cache_control_headers)
        assert (123, FetchOptions(True)._cache_control_headers) == (self.change_number_2, self.fetch_options_2._cache_control_headers)
        inserted_split = self.parsed_split[0]
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == 'some_name'

        inserted_rbs = self.parsed_rbs[0]
        assert isinstance(inserted_rbs, RuleBasedSegment)
        assert inserted_rbs.name == 'sample_rule_based_segment'


    @pytest.mark.asyncio
    async def test_not_called_on_till(self, mocker):
        """Test that sync is not called when till is less than previous changenumber"""
        storage = mocker.Mock(spec=InMemorySplitStorageAsync)
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorageAsync)
                
        class flag_set_filter():
            def should_filter():
                return False

            def intersect(sets):
                return True
        storage.flag_set_filter = flag_set_filter
        storage.flag_set_filter.flag_sets = {}
        storage.flag_set_filter.sorted_flag_sets = []

        async def change_number_mock():
            return 2
        storage.get_change_number = change_number_mock
        rbs_storage.get_change_number.side_effect = change_number_mock
        
        async def get_changes(*args, **kwargs):
            get_changes.called += 1
            return None
        get_changes.called = 0
        api = mocker.Mock()
        api.fetch_splits = get_changes

        split_synchronizer = SplitSynchronizerAsync(api, storage, rbs_storage)
        await split_synchronizer.synchronize_splits(1)
        assert get_changes.called == 0

    @pytest.mark.asyncio
    async def test_synchronize_splits_cdn(self, mocker):
        """Test split sync with bypassing cdn."""
        mocker.patch('splitio.sync.split._ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES', new=3)
        storage = mocker.Mock(spec=InMemorySplitStorageAsync)
        rbs_storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorageAsync)
        async def change_number_mock():
            change_number_mock._calls += 1
            if change_number_mock._calls == 1:
                return -1
            elif change_number_mock._calls >= 2 and change_number_mock._calls <= 3:
                return 123
            elif change_number_mock._calls <= 7:
                return 1234
            return 12345 # Return proper cn for CDN Bypass
        async def rbs_change_number_mock():
            rbs_change_number_mock._calls += 1
            if rbs_change_number_mock._calls == 1:
                return -1
            elif change_number_mock._calls >= 2 and change_number_mock._calls <= 3:
                return 555
            elif change_number_mock._calls <= 9:
                return 555
            return 666 # Return proper cn for CDN Bypass

        change_number_mock._calls = 0
        rbs_change_number_mock._calls = 0
        storage.get_change_number = change_number_mock
        rbs_storage.get_change_number = rbs_change_number_mock
        
        self.parsed_split = None
        async def update(parsed_split, deleted, change_number):
            if len(parsed_split) > 0:
                self.parsed_split = parsed_split
        storage.update = update

        self.parsed_rbs = None
        async def rbs_update(parsed, deleted, change_number):
            if len(parsed) > 0:
                self.parsed_rbs = parsed
        rbs_storage.update = rbs_update

        api = mocker.Mock()
        self.change_number_1 = None
        self.fetch_options_1 = None
        self.change_number_2 = None
        self.fetch_options_2 = None
        self.change_number_3 = None
        self.fetch_options_3 = None
        rbs_1 = copy.deepcopy(json_body['rbs']['d'])

        async def get_changes(change_number, rbs_change_number, fetch_options):
            get_changes.called += 1
            if get_changes.called == 1:
                self.change_number_1 = change_number
                self.fetch_options_1 = fetch_options
                return { 'ff': { 'd': self.splits, 's': -1, 't': 123 }, 
                        'rbs':  {"t": 555, "s": -1, "d": rbs_1}}
            elif get_changes.called == 2:
                self.change_number_2 = change_number
                self.fetch_options_2 = fetch_options
                return { 'ff': { 'd': [], 's': 123, 't': 123 },
                        'rbs':  {"t": 555, "s": 555, "d": []}}
            elif get_changes.called == 3:
                return { 'ff': { 'd': [], 's': 123, 't': 1234 },
                        'rbs':  {"t": 555, "s": 555, "d": []}}                    
            elif get_changes.called >= 4 and get_changes.called <= 6:
                return { 'ff': { 'd': [], 's': 1234, 't': 1234 },
                        'rbs':  {"t": 555, "s": 555, "d": []}}                        
            elif get_changes.called == 7:
                return { 'ff': { 'd': [], 's': 1234, 't': 12345 },
                        'rbs':  {"t": 555, "s": 555, "d": []}}      
            elif get_changes.called == 8:                                  
                self.change_number_3 = change_number
                self.fetch_options_3 = fetch_options
                return { 'ff': { 'd': [], 's': 12345, 't': 12345 },
                        'rbs':  {"t": 555, "s": 555, "d": []}}
            rbs_1[0]['excluded']['keys'] = ['bilal@split.io']
            return { 'ff': { 'd': [], 's': 12345, 't': 12345 },
                        'rbs':  {"t": 666, "s": 666, "d": rbs_1}}
    
        get_changes.called = 0
        api.fetch_splits = get_changes

        class flag_set_filter():
            def should_filter():
                return False

            def intersect(sets):
                return True

        storage.flag_set_filter = flag_set_filter
        storage.flag_set_filter.flag_sets = {}
        storage.flag_set_filter.sorted_flag_sets = []

        self.clear = False
        async def clear():
            self.clear = True
        storage.clear = clear

        self.clear2 = False
        async def clear():
            self.clear2 = True
        rbs_storage.clear = clear

        split_synchronizer = SplitSynchronizerAsync(api, storage, rbs_storage)
        split_synchronizer._backoff = Backoff(1, 1)
        await split_synchronizer.synchronize_splits()

        assert (-1, FetchOptions(True).cache_control_headers) == (self.change_number_1, self.fetch_options_1.cache_control_headers)
        assert (123, FetchOptions(True).cache_control_headers) == (self.change_number_2, self.fetch_options_2.cache_control_headers)

        split_synchronizer._backoff = Backoff(1, 0.1)
        await split_synchronizer.synchronize_splits(12345)
        assert (12345, True, 1234) == (self.change_number_3, self.fetch_options_3.cache_control_headers, self.fetch_options_3.change_number)
        assert get_changes.called == 8 # 2 ok + BACKOFF(2 since==till + 2 re-attempts) + CDN(2 since==till)

        inserted_split = self.parsed_split[0]
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == 'some_name'
        inserted_rbs = self.parsed_rbs[0]
        assert inserted_rbs.excluded.get_excluded_keys() == ["mauro@split.io","gaston@split.io"]

        split_synchronizer._backoff = Backoff(1, 0.1)
        await split_synchronizer.synchronize_splits(None, 666)
        inserted_rbs = self.parsed_rbs[0]
        assert inserted_rbs.excluded.get_excluded_keys() == ['bilal@split.io']
        
    @pytest.mark.asyncio
    async def test_sync_flag_sets_with_config_sets(self, mocker):
        """Test split sync with flag sets."""
        storage = InMemorySplitStorageAsync(['set1', 'set2'])
        rbs_storage = InMemoryRuleBasedSegmentStorageAsync()
        
        split = self.splits[0].copy()
        split['name'] = 'second'
        splits1 = [self.splits[0].copy(), split]
        splits2 = self.splits.copy()
        splits3 = self.splits.copy()
        splits4 = self.splits.copy()
        api = mocker.Mock()
        async def get_changes(*args, **kwargs):
            get_changes.called += 1
            if get_changes.called == 1:
                return { 'ff': { 'd': splits1, 's': 123, 't': 123 },
                        'rbs':  {'t': 123, 's': 123, 'd': []}}                        
            elif get_changes.called == 2:
                splits2[0]['sets'] = ['set3']
                return { 'ff': { 'd': splits2, 's': 124, 't': 124 },
                        'rbs':  {'t': 124, 's': 124, 'd': []}}                        
            elif get_changes.called == 3:
                splits3[0]['sets'] = ['set1']
                return { 'ff': { 'd': splits3, 's': 12434, 't': 12434 },
                        'rbs':  {'t': 12434, 's': 12434, 'd': []}}                        
            splits4[0]['sets'] = ['set6']
            splits4[0]['name'] = 'new_split'
            return { 'ff': { 'd': splits4, 's': 12438, 't': 12438 },
                        'rbs':  {'t': 12438, 's': 12438, 'd': []}}                        

        get_changes.called = 0
        api.fetch_splits = get_changes

        split_synchronizer = SplitSynchronizerAsync(api, storage, rbs_storage)
        split_synchronizer._backoff = Backoff(1, 1)
        await split_synchronizer.synchronize_splits()
        assert isinstance(await storage.get('some_name'), Split)

        await split_synchronizer.synchronize_splits(124)
        assert await storage.get('some_name') == None

        await split_synchronizer.synchronize_splits(12434)
        assert isinstance(await storage.get('some_name'), Split)

        await split_synchronizer.synchronize_splits(12438)
        assert await storage.get('new_name') == None

    @pytest.mark.asyncio
    async def test_sync_flag_sets_without_config_sets(self, mocker):
        """Test split sync with flag sets."""
        storage = InMemorySplitStorageAsync()
        rbs_storage = InMemoryRuleBasedSegmentStorageAsync()
        split = self.splits[0].copy()
        split['name'] = 'second'
        splits1 = [self.splits[0].copy(), split]
        splits2 = self.splits.copy()
        splits3 = self.splits.copy()
        splits4 = self.splits.copy()
        api = mocker.Mock()
        async def get_changes(*args, **kwargs):
            get_changes.called += 1
            if get_changes.called == 1:
                return { 'ff': { 'd': splits1, 's': 123, 't': 123 },
                        'rbs':  {"t": 123, "s": 123, "d": []}}                        
            elif get_changes.called == 2:
                splits2[0]['sets'] = ['set3']
                return { 'ff': { 'd': splits2, 's': 124, 't': 124 },
                        'rbs':  {"t": 124, "s": 124, "d": []}}                        
            elif get_changes.called == 3:
                splits3[0]['sets'] = ['set1']
                return { 'ff': { 'd': splits3, 's': 12434, 't': 12434 },
                        'rbs':  {"t": 12434, "s": 12434, "d": []}}                        
            splits4[0]['sets'] = ['set6']
            splits4[0]['name'] = 'third_split'
            return { 'ff': { 'd': splits4, 's': 12438, 't': 12438 },
                        'rbs':  {"t": 12438, "s": 12438, "d": []}}                        
        get_changes.called = 0
        api.fetch_splits.side_effect = get_changes

        split_synchronizer = SplitSynchronizerAsync(api, storage, rbs_storage)
        split_synchronizer._backoff = Backoff(1, 1)
        await split_synchronizer.synchronize_splits()
        assert isinstance(await storage.get('new_split'), Split)

        await split_synchronizer.synchronize_splits(124)
        assert isinstance(await storage.get('new_split'), Split)

        await split_synchronizer.synchronize_splits(12434)
        assert isinstance(await storage.get('new_split'), Split)

        await split_synchronizer.synchronize_splits(12438)
        assert isinstance(await storage.get('third_split'), Split)

class LocalSplitsSynchronizerTests(object):
    """Split synchronizer test cases."""

    payload = copy.deepcopy(json_body)

    def test_synchronize_splits_error(self, mocker):
        """Test that if fetching splits fails at some_point, the task will continue running."""
        storage = mocker.Mock(spec=SplitStorage)
        rbs_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)
        split_synchronizer = LocalSplitSynchronizer("/incorrect_file", storage, rbs_storage)

        with pytest.raises(Exception):
            split_synchronizer.synchronize_splits(1)

    def test_synchronize_splits(self, mocker):
        """Test split sync."""
        storage = InMemorySplitStorage()
        rbs_storage = InMemoryRuleBasedSegmentStorage()

        def read_splits_from_json_file(*args, **kwargs):
                return self.payload

        split_synchronizer = LocalSplitSynchronizer("split.json", storage, rbs_storage, LocalhostMode.JSON)
        split_synchronizer._read_feature_flags_from_json_file = read_splits_from_json_file

        split_synchronizer.synchronize_splits()
        inserted_split = storage.get(self.payload["ff"]["d"][0]['name'])
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == 'some_name'

        # Should sync when changenumber is not changed
        self.payload["ff"]["d"][0]['killed'] = True
        split_synchronizer.synchronize_splits()
        inserted_split = storage.get(self.payload["ff"]["d"][0]['name'])
        assert inserted_split.killed

        # Should not sync when changenumber is less than stored
        self.payload["ff"]["t"] = 122
        self.payload["ff"]["d"][0]['killed'] = False
        split_synchronizer.synchronize_splits()
        inserted_split = storage.get(self.payload["ff"]["d"][0]['name'])
        assert inserted_split.killed

        # Should sync when changenumber is higher than stored
        self.payload["ff"]["t"] = 1675095324999
        split_synchronizer._current_json_sha = "-1"
        split_synchronizer.synchronize_splits()
        inserted_split = storage.get(self.payload["ff"]["d"][0]['name'])
        assert inserted_split.killed == False

        # Should sync when till is default (-1)
        self.payload["ff"]["t"] = -1
        split_synchronizer._current_json_sha = "-1"
        self.payload["ff"]["d"][0]['killed'] = True
        split_synchronizer.synchronize_splits()
        inserted_split = storage.get(self.payload["ff"]["d"][0]['name'])
        assert inserted_split.killed == True

    def test_sync_flag_sets_with_config_sets(self, mocker):
        """Test split sync with flag sets."""
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

        split_synchronizer = LocalSplitSynchronizer("split.json", storage, rbs_storage, LocalhostMode.JSON)
        split_synchronizer._read_feature_flags_from_json_file = read_feature_flags_from_json_file

        split_synchronizer.synchronize_splits()
        assert isinstance(storage.get('some_name'), Split)

        split_synchronizer.synchronize_splits(124)
        assert storage.get('some_name') == None

        split_synchronizer.synchronize_splits(12434)
        assert isinstance(storage.get('some_name'), Split)

        split_synchronizer.synchronize_splits(12438)
        assert storage.get('new_name') == None

    def test_sync_flag_sets_without_config_sets(self, mocker):
        """Test split sync with flag sets."""
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

        split_synchronizer.synchronize_splits()
        assert isinstance(storage.get('new_split'), Split)

        split_synchronizer.synchronize_splits(124)
        assert isinstance(storage.get('new_split'), Split)

        split_synchronizer.synchronize_splits(12434)
        assert isinstance(storage.get('new_split'), Split)

        split_synchronizer.synchronize_splits(12438)
        assert isinstance(storage.get('third_split'), Split)

    def test_reading_json(self, mocker):
        """Test reading json file."""
        f = open("./splits.json", "w")
        f.write(json.dumps(self.payload))
        f.close()
        storage = InMemorySplitStorage()
        rbs_storage = InMemoryRuleBasedSegmentStorage()
        split_synchronizer = LocalSplitSynchronizer("./splits.json", storage, rbs_storage, LocalhostMode.JSON)
        split_synchronizer.synchronize_splits()

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
    async def test_synchronize_splits_error(self, mocker):
        """Test that if fetching splits fails at some_point, the task will continue running."""
        storage = mocker.Mock(spec=SplitStorage)
        rbs_storage = mocker.Mock(spec=RuleBasedSegmentsStorage)
        split_synchronizer = LocalSplitSynchronizerAsync("/incorrect_file", storage, rbs_storage)

        with pytest.raises(Exception):
            await split_synchronizer.synchronize_splits(1)

    @pytest.mark.asyncio
    async def test_synchronize_splits(self, mocker):
        """Test split sync."""
        storage = InMemorySplitStorageAsync()
        rbs_storage = InMemoryRuleBasedSegmentStorageAsync()

        async def read_splits_from_json_file(*args, **kwargs):
            return self.payload

        split_synchronizer = LocalSplitSynchronizerAsync("split.json", storage, rbs_storage, LocalhostMode.JSON)
        split_synchronizer._read_feature_flags_from_json_file = read_splits_from_json_file

        await split_synchronizer.synchronize_splits()
        inserted_split = await storage.get(self.payload["ff"]["d"][0]['name'])
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == 'some_name'

        # Should sync when changenumber is not changed
        self.payload["ff"]["d"][0]['killed'] = True
        await split_synchronizer.synchronize_splits()
        inserted_split = await storage.get(self.payload["ff"]["d"][0]['name'])
        assert inserted_split.killed

        # Should not sync when changenumber is less than stored
        self.payload["ff"]["t"] = 122
        self.payload["ff"]["d"][0]['killed'] = False
        await split_synchronizer.synchronize_splits()
        inserted_split = await storage.get(self.payload["ff"]["d"][0]['name'])
        assert inserted_split.killed

        # Should sync when changenumber is higher than stored
        self.payload["ff"]["t"] = 1675095324999
        split_synchronizer._current_json_sha = "-1"
        await split_synchronizer.synchronize_splits()
        inserted_split = await storage.get(self.payload["ff"]["d"][0]['name'])
        assert inserted_split.killed == False

        # Should sync when till is default (-1)
        self.payload["ff"]["t"] = -1
        split_synchronizer._current_json_sha = "-1"
        self.payload["ff"]["d"][0]['killed'] = True
        await split_synchronizer.synchronize_splits()
        inserted_split = await storage.get(self.payload["ff"]["d"][0]['name'])
        assert inserted_split.killed == True

    @pytest.mark.asyncio
    async def test_sync_flag_sets_with_config_sets(self, mocker):
        """Test split sync with flag sets."""
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

        await split_synchronizer.synchronize_splits()
        assert isinstance(await storage.get('some_name'), Split)

        await split_synchronizer.synchronize_splits(124)
        assert await storage.get('some_name') == None

        await split_synchronizer.synchronize_splits(12434)
        assert isinstance(await storage.get('some_name'), Split)

        await split_synchronizer.synchronize_splits(12438)
        assert await storage.get('new_name') == None

    @pytest.mark.asyncio
    async def test_sync_flag_sets_without_config_sets(self, mocker):
        """Test split sync with flag sets."""
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

        await split_synchronizer.synchronize_splits()
        assert isinstance(await storage.get('new_split'), Split)

        await split_synchronizer.synchronize_splits(124)
        assert isinstance(await storage.get('new_split'), Split)

        await split_synchronizer.synchronize_splits(12434)
        assert isinstance(await storage.get('new_split'), Split)

        await split_synchronizer.synchronize_splits(12438)
        assert isinstance(await storage.get('third_split'), Split)

    @pytest.mark.asyncio
    async def test_reading_json(self, mocker):
        """Test reading json file."""
        async with aiofiles.open("./splits.json", "w") as f:
            await f.write(json.dumps(self.payload))
        storage = InMemorySplitStorageAsync()
        rbs_storage = InMemoryRuleBasedSegmentStorageAsync()        
        split_synchronizer = LocalSplitSynchronizerAsync("./splits.json", storage, rbs_storage, LocalhostMode.JSON)
        await split_synchronizer.synchronize_splits()

        inserted_split = await storage.get(self.payload['ff']['d'][0]['name'])
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == self.payload['ff']['d'][0]['name']

        inserted_rbs = await rbs_storage.get(self.payload['rbs']['d'][0]['name'])
        assert isinstance(inserted_rbs, RuleBasedSegment)
        assert inserted_rbs.name == self.payload['rbs']['d'][0]['name']

        os.remove("./splits.json")
