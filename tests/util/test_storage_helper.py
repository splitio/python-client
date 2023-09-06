"""Storage Helper tests."""

from splitio.util.storage_helper import update_feature_flag_storage, get_valid_flag_sets
from splitio.storage.inmemmory import InMemorySplitStorage
from splitio.models import splits
from tests.sync.test_splits_synchronizer import splits as split_sample

class StorageHelperTests(object):

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

        storage.config_flag_sets_used = 0
        update_feature_flag_storage(storage, [split], 123)
        assert self.added[0] == split
        assert self.deleted == []
        assert self.change_number == 123

        storage.config_flag_sets_used = 2
        update_feature_flag_storage(storage, [split], 123)
        assert self.added == []
        assert self.deleted[0] == split.name

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

    def test_get_valid_flag_sets(self, mocker):
        flag_sets = ['set1', 'set2']
        config_flag_sets = []
        assert get_valid_flag_sets(flag_sets, config_flag_sets) == ['set1', 'set2']

        config_flag_sets = ['set1']
        assert get_valid_flag_sets(flag_sets, config_flag_sets) == ['set1']

        flag_sets = ['set2', 'set3']
        config_flag_sets = ['set1', 'set2']
        assert get_valid_flag_sets(flag_sets, config_flag_sets) == ['set2']

        flag_sets = ['set3', 'set4']
        config_flag_sets = ['set1', 'set2']
        assert get_valid_flag_sets(flag_sets, config_flag_sets) == []

        flag_sets = []
        config_flag_sets = ['set1', 'set2']
        assert get_valid_flag_sets(flag_sets, config_flag_sets) == []
