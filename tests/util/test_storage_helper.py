"""Storage Helper tests."""
import pytest

from splitio.util.storage_helper import update_feature_flag_storage, get_valid_flag_sets, combine_valid_flag_sets, \
    update_rule_based_segment_storage, update_rule_based_segment_storage_async, update_feature_flag_storage_async 
from splitio.storage.inmemmory import InMemorySplitStorage, InMemoryRuleBasedSegmentStorage, InMemoryRuleBasedSegmentStorageAsync, \
    InMemorySplitStorageAsync
from splitio.models import splits, rule_based_segments
from splitio.storage import FlagSetsFilter
from tests.sync.test_splits_synchronizer import splits_raw as split_sample

class StorageHelperTests(object):

    rbs = rule_based_segments.from_raw({
        "changeNumber": 123,
        "name": "sample_rule_based_segment",
        "status": "ACTIVE",
        "trafficTypeName": "user",
        "excluded":{
        "keys":["mauro@split.io","gaston@split.io"],
        "segments":['excluded_segment']
        },
        "conditions": [
            {"matcherGroup": {
                "combiner": "AND",
                "matchers": [
                    {
                        "matcherType": "IN_SEGMENT",
                        "negate": False,
                        "userDefinedSegmentMatcherData": {
                            "segmentName": "employees"
                        },
                        "whitelistMatcherData": None
                    }
                ]
            },
            }
        ]
    })

    def test_update_feature_flag_storage(self, mocker):
        storage = mocker.Mock(spec=InMemorySplitStorage)
        split = splits.from_raw(split_sample[0])

        self.added = []
        self.deleted = []
        self.change_number = 0
        def update(to_add, to_delete, change_number):
            self.added = to_add
            self.deleted = to_delete
            self.change_number = change_number
        storage.update = update

        def is_flag_set_exist(flag_set):
            return False
        storage.is_flag_set_exist = is_flag_set_exist

        class flag_set_filter():
            def should_filter():
                return False
            def intersect(sets):
                return True
        storage.flag_set_filter = flag_set_filter
        storage.flag_set_filter.flag_sets = {}

        update_feature_flag_storage(storage, [split], 123)
        assert self.added[0] == split
        assert self.deleted == []
        assert self.change_number == 123

        class flag_set_filter2():
            def should_filter():
                return True
            def intersect(sets):
                return False
        storage.flag_set_filter = flag_set_filter2
        storage.flag_set_filter.flag_sets = set({'set1', 'set2'})

        update_feature_flag_storage(storage, [split], 123)
        assert self.added == []
        assert self.deleted[0] == split.name

        class flag_set_filter3():
            def should_filter():
                return True
            def intersect(sets):
                return True
        storage.flag_set_filter = flag_set_filter3
        storage.flag_set_filter.flag_sets = set({'set1', 'set2'})

        def is_flag_set_exist2(flag_set):
            return True
        storage.is_flag_set_exist = is_flag_set_exist2
        update_feature_flag_storage(storage, [split], 123)
        assert self.added[0] == split
        assert self.deleted == []

        split_json = split_sample[0]
        split_json['conditions'].append({
                    "matcherGroup": {
                        "combiner": "AND",
                        "matchers": [
                            {
                                "matcherType": "IN_SEGMENT",
                                "negate": False,
                                "userDefinedSegmentMatcherData": {
                                    "segmentName": "segment1"
                                },
                                "whitelistMatcherData": None
                            }
                        ]
                    },
                    "partitions": [
                        {
                            "treatment": "on",
                            "size": 30
                        },
                        {
                            "treatment": "off",
                            "size": 70
                        }
                    ]
                }
        )

        split = splits.from_raw(split_json)
        storage.config_flag_sets_used = 0
        assert update_feature_flag_storage(storage, [split], 123) == {'segment1'}

    def test_get_valid_flag_sets(self):
        flag_sets = ['set1', 'set2']
        config_flag_sets = FlagSetsFilter([])
        assert get_valid_flag_sets(flag_sets, config_flag_sets) == ['set1', 'set2']

        config_flag_sets = FlagSetsFilter(['set1'])
        assert get_valid_flag_sets(flag_sets, config_flag_sets) == ['set1']

        flag_sets = ['set2', 'set3']
        config_flag_sets = FlagSetsFilter(['set1', 'set2'])
        assert get_valid_flag_sets(flag_sets, config_flag_sets) == ['set2']

        flag_sets = ['set3', 'set4']
        config_flag_sets = FlagSetsFilter(['set1', 'set2'])
        assert get_valid_flag_sets(flag_sets, config_flag_sets) == []

        flag_sets = []
        config_flag_sets = FlagSetsFilter(['set1', 'set2'])
        assert get_valid_flag_sets(flag_sets, config_flag_sets) == []

    def test_combine_valid_flag_sets(self):
        results_set = [{'set1', 'set2'}, {'set2', 'set3'}]
        assert combine_valid_flag_sets(results_set) == {'set1', 'set2', 'set3'}

        results_set = [{}, {'set2', 'set3'}]
        assert combine_valid_flag_sets(results_set) == {'set2', 'set3'}

        results_set = ['set1', {'set2', 'set3'}]
        assert combine_valid_flag_sets(results_set) == {'set2', 'set3'}
        
    def test_update_rule_base_segment_storage(self, mocker):
        storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorage)
        self.added = []
        self.deleted = []
        self.change_number = 0
        def update(to_add, to_delete, change_number):
            self.added = to_add
            self.deleted = to_delete
            self.change_number = change_number
        storage.update = update

        segments = update_rule_based_segment_storage(storage, [self.rbs], 123)
        assert self.added[0] == self.rbs
        assert self.deleted == []
        assert self.change_number == 123
        assert segments == {'excluded_segment', 'employees'}
        
    @pytest.mark.asyncio
    async def test_update_rule_base_segment_storage_async(self, mocker):
        storage = mocker.Mock(spec=InMemoryRuleBasedSegmentStorageAsync)
        self.added = []
        self.deleted = []
        self.change_number = 0
        async def update(to_add, to_delete, change_number):
            self.added = to_add
            self.deleted = to_delete
            self.change_number = change_number
        storage.update = update

        segments = await update_rule_based_segment_storage_async(storage, [self.rbs], 123)
        assert self.added[0] == self.rbs
        assert self.deleted == []
        assert self.change_number == 123
        assert segments == {'excluded_segment', 'employees'}        
        
    @pytest.mark.asyncio
    async def test_update_feature_flag_storage_async(self, mocker):
        storage = mocker.Mock(spec=InMemorySplitStorageAsync)
        split = splits.from_raw(split_sample[0])

        self.added = []
        self.deleted = []
        self.change_number = 0
        async def get(flag_name):
            return None
        storage.get = get
        
        async def update(to_add, to_delete, change_number):
            self.added = to_add
            self.deleted = to_delete
            self.change_number = change_number
        storage.update = update

        async def is_flag_set_exist(flag_set):
            return False
        storage.is_flag_set_exist = is_flag_set_exist

        class flag_set_filter():
            def should_filter():
                return False
            def intersect(sets):
                return True
        storage.flag_set_filter = flag_set_filter
        storage.flag_set_filter.flag_sets = {}

        await update_feature_flag_storage_async(storage, [split], 123)
        assert self.added[0] == split
        assert self.deleted == []
        assert self.change_number == 123

        class flag_set_filter2():
            def should_filter():
                return True
            def intersect(sets):
                return False
        storage.flag_set_filter = flag_set_filter2
        storage.flag_set_filter.flag_sets = set({'set1', 'set2'})

        async def get(flag_name):
            return split
        storage.get = get

        await update_feature_flag_storage_async(storage, [split], 123)
        assert self.added == []
        assert self.deleted[0] == split.name

        class flag_set_filter3():
            def should_filter():
                return True
            def intersect(sets):
                return True
        storage.flag_set_filter = flag_set_filter3
        storage.flag_set_filter.flag_sets = set({'set1', 'set2'})

        async def is_flag_set_exist2(flag_set):
            return True
        storage.is_flag_set_exist = is_flag_set_exist2
        await update_feature_flag_storage_async(storage, [split], 123)
        assert self.added[0] == split
        assert self.deleted == []

        split_json = split_sample[0]
        split_json['conditions'].append({
                    "matcherGroup": {
                        "combiner": "AND",
                        "matchers": [
                            {
                                "matcherType": "IN_SEGMENT",
                                "negate": False,
                                "userDefinedSegmentMatcherData": {
                                    "segmentName": "segment1"
                                },
                                "whitelistMatcherData": None
                            }
                        ]
                    },
                    "partitions": [
                        {
                            "treatment": "on",
                            "size": 30
                        },
                        {
                            "treatment": "off",
                            "size": 70
                        }
                    ]
                }
        )

        split = splits.from_raw(split_json)
        storage.config_flag_sets_used = 0
        assert await update_feature_flag_storage_async(storage, [split], 123) == {'segment1'}