"""Split Worker tests."""

import pytest
import os
import json

from splitio.util.backoff import Backoff
from splitio.api import APIException
from splitio.api.commons import FetchOptions
from splitio.storage import SplitStorage
from splitio.storage.inmemmory import InMemorySplitStorage
from splitio.models.splits import Split
from splitio.sync.split import SplitSynchronizer, LocalSplitSynchronizer, LocalhostMode
from tests.integration import splits_json

splits = [{
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


class SplitsSynchronizerTests(object):
    """Split synchronizer test cases."""

    def test_synchronize_splits_error(self, mocker):
        """Test that if fetching splits fails at some_point, the task will continue running."""
        storage = mocker.Mock(spec=InMemorySplitStorage)
        api = mocker.Mock()

        def run(x, c):
            raise APIException("something broke")
        run._calls = 0
        api.fetch_splits.side_effect = run
        storage.get_change_number.return_value = -1

        split_synchronizer = SplitSynchronizer(api, storage)

        with pytest.raises(APIException):
            split_synchronizer.synchronize_splits(1)

    def test_synchronize_splits(self, mocker):
        """Test split sync."""
        storage = mocker.Mock(spec=InMemorySplitStorage)

        def change_number_mock():
            change_number_mock._calls += 1
            if change_number_mock._calls == 1:
                return -1
            return 123
        change_number_mock._calls = 0
        storage.get_change_number.side_effect = change_number_mock

        class flag_set_filter():
            def should_filter():
                return False

            def intersect(sets):
                return True

        storage.flag_set_filter = flag_set_filter

        api = mocker.Mock()
        def get_changes(*args, **kwargs):
            get_changes.called += 1

            if get_changes.called == 1:
                return {
                    'splits': splits,
                    'since': -1,
                    'till': 123
                }
            else:
                return {
                    'splits': [],
                    'since': 123,
                    'till': 123
                }
        get_changes.called = 0
        api.fetch_splits.side_effect = get_changes

        split_synchronizer = SplitSynchronizer(api, storage)
        split_synchronizer.synchronize_splits()

        assert mocker.call(-1, FetchOptions(True)) in api.fetch_splits.mock_calls
        assert mocker.call(123, FetchOptions(True)) in api.fetch_splits.mock_calls

        inserted_split = storage.update.mock_calls[0][1][0][0]
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == 'some_name'

    def test_not_called_on_till(self, mocker):
        """Test that sync is not called when till is less than previous changenumber"""
        storage = mocker.Mock(spec=InMemorySplitStorage)

        def change_number_mock():
            return 2
        storage.get_change_number.side_effect = change_number_mock

        def get_changes(*args, **kwargs):
            get_changes.called += 1
            return None

        get_changes.called = 0

        api = mocker.Mock()
        api.fetch_splits.side_effect = get_changes

        split_synchronizer = SplitSynchronizer(api, storage)
        split_synchronizer.synchronize_splits(1)

        assert get_changes.called == 0

    def test_synchronize_splits_cdn(self, mocker):
        """Test split sync with bypassing cdn."""
        mocker.patch('splitio.sync.split._ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES', new=3)

        storage = mocker.Mock(spec=InMemorySplitStorage)

        def change_number_mock():
            change_number_mock._calls += 1
            if change_number_mock._calls == 1:
                return -1
            elif change_number_mock._calls >= 2 and change_number_mock._calls <= 3:
                return 123
            elif change_number_mock._calls <= 7:
                return 1234
            return 12345 # Return proper cn for CDN Bypass
        change_number_mock._calls = 0
        storage.get_change_number.side_effect = change_number_mock

        api = mocker.Mock()
        def get_changes(*args, **kwargs):
            get_changes.called += 1
            if get_changes.called == 1:
                return { 'splits': splits, 'since': -1, 'till': 123 }
            elif get_changes.called == 2:
                return { 'splits': [], 'since': 123, 'till': 123 }
            elif get_changes.called == 3:
                return { 'splits': [], 'since': 123, 'till': 1234 }
            elif get_changes.called >= 4 and get_changes.called <= 6:
                return { 'splits': [], 'since': 1234, 'till': 1234 }
            elif get_changes.called == 7:
                return { 'splits': [], 'since': 1234, 'till': 12345 }
            return { 'splits': [], 'since': 12345, 'till': 12345 }
        get_changes.called = 0
        api.fetch_splits.side_effect = get_changes

        class flag_set_filter():
            def should_filter():
                return False

            def intersect(sets):
                return True

        storage.flag_set_filter = flag_set_filter

        split_synchronizer = SplitSynchronizer(api, storage)
        split_synchronizer._backoff = Backoff(1, 1)
        split_synchronizer.synchronize_splits()

        assert mocker.call(-1, FetchOptions(True)) in api.fetch_splits.mock_calls
        assert mocker.call(123, FetchOptions(True)) in api.fetch_splits.mock_calls

        split_synchronizer._backoff = Backoff(1, 0.1)
        split_synchronizer.synchronize_splits(12345)
        assert mocker.call(12345, FetchOptions(True, 1234)) in api.fetch_splits.mock_calls
        assert len(api.fetch_splits.mock_calls) == 8 # 2 ok + BACKOFF(2 since==till + 2 re-attempts) + CDN(2 since==till)

        inserted_split = storage.update.mock_calls[0][1][0][0]
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == 'some_name'

    def test_sync_flag_sets_with_config_sets(self, mocker):
        """Test split sync with flag sets."""
        storage = InMemorySplitStorage(['set1', 'set2'])

        split = splits[0].copy()
        split['name'] = 'second'
        splits1 = [splits[0].copy(), split]
        splits2 = splits.copy()
        splits3 = splits.copy()
        splits4 = splits.copy()
        api = mocker.Mock()
        def get_changes(*args, **kwargs):
            get_changes.called += 1
            if get_changes.called == 1:
                return { 'splits': splits1, 'since': 123, 'till': 123 }
            elif get_changes.called == 2:
                splits2[0]['sets'] = ['set3']
                return { 'splits': splits2, 'since': 124, 'till': 124 }
            elif get_changes.called == 3:
                splits3[0]['sets'] = ['set1']
                return { 'splits': splits3, 'since': 12434, 'till': 12434 }
            splits4[0]['sets'] = ['set6']
            splits4[0]['name'] = 'new_split'
            return { 'splits': splits4, 'since': 12438, 'till': 12438 }
        get_changes.called = 0
        api.fetch_splits.side_effect = get_changes

        split_synchronizer = SplitSynchronizer(api, storage)
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

        split = splits[0].copy()
        split['name'] = 'second'
        splits1 = [splits[0].copy(), split]
        splits2 = splits.copy()
        splits3 = splits.copy()
        splits4 = splits.copy()
        api = mocker.Mock()
        def get_changes(*args, **kwargs):
            get_changes.called += 1
            if get_changes.called == 1:
                return { 'splits': splits1, 'since': 123, 'till': 123 }
            elif get_changes.called == 2:
                splits2[0]['sets'] = ['set3']
                return { 'splits': splits2, 'since': 124, 'till': 124 }
            elif get_changes.called == 3:
                splits3[0]['sets'] = ['set1']
                return { 'splits': splits3, 'since': 12434, 'till': 12434 }
            splits4[0]['sets'] = ['set6']
            splits4[0]['name'] = 'third_split'
            return { 'splits': splits4, 'since': 12438, 'till': 12438 }
        get_changes.called = 0
        api.fetch_splits.side_effect = get_changes

        split_synchronizer = SplitSynchronizer(api, storage)
        split_synchronizer._backoff = Backoff(1, 1)
        split_synchronizer.synchronize_splits()
        assert isinstance(storage.get('new_split'), Split)

        split_synchronizer.synchronize_splits(124)
        assert isinstance(storage.get('new_split'), Split)

        split_synchronizer.synchronize_splits(12434)
        assert isinstance(storage.get('new_split'), Split)

        split_synchronizer.synchronize_splits(12438)
        assert isinstance(storage.get('third_split'), Split)

class LocalSplitsSynchronizerTests(object):
    """Split synchronizer test cases."""

    def test_synchronize_splits_error(self, mocker):
        """Test that if fetching splits fails at some_point, the task will continue running."""
        storage = mocker.Mock(spec=SplitStorage)
        split_synchronizer = LocalSplitSynchronizer("/incorrect_file", storage)

        with pytest.raises(Exception):
            split_synchronizer.synchronize_splits(1)

    def test_synchronize_splits(self, mocker):
        """Test split sync."""
        storage = InMemorySplitStorage()

        till = 123
        splits = [{
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
            ]
        }]

        def read_feature_flags_from_json_file(*args, **kwargs):
                return splits, till

        split_synchronizer = LocalSplitSynchronizer("split.json", storage, LocalhostMode.JSON)
        split_synchronizer._read_feature_flags_from_json_file = read_feature_flags_from_json_file

        split_synchronizer.synchronize_splits()
        inserted_split = storage.get(splits[0]['name'])
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == 'some_name'

        # Should sync when changenumber is not changed
        splits[0]['killed'] = True
        split_synchronizer.synchronize_splits()
        inserted_split = storage.get(splits[0]['name'])
        assert inserted_split.killed

        # Should not sync when changenumber is less than stored
        till = 122
        splits[0]['killed'] = False
        split_synchronizer.synchronize_splits()
        inserted_split = storage.get(splits[0]['name'])
        assert inserted_split.killed

        # Should sync when changenumber is higher than stored
        till = 124
        split_synchronizer._current_json_sha = "-1"
        split_synchronizer.synchronize_splits()
        inserted_split = storage.get(splits[0]['name'])
        assert inserted_split.killed == False

        # Should sync when till is default (-1)
        till = -1
        split_synchronizer._current_json_sha = "-1"
        splits[0]['killed'] = True
        split_synchronizer.synchronize_splits()
        inserted_split = storage.get(splits[0]['name'])
        assert inserted_split.killed == True

    def test_reading_json(self, mocker):
        """Test reading json file."""
        f = open("./splits.json", "w")
        json_body = {'splits': [{
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
            ]
        }],
        "till":1675095324253,
        "since":-1,
        }

        f.write(json.dumps(json_body))
        f.close()
        storage = InMemorySplitStorage()
        split_synchronizer = LocalSplitSynchronizer("./splits.json", storage, LocalhostMode.JSON)
        split_synchronizer.synchronize_splits()

        inserted_split = storage.get(json_body['splits'][0]['name'])
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == 'some_name'

        os.remove("./splits.json")

    def test_json_elements_sanitization(self, mocker):
        """Test sanitization."""
        split_synchronizer = LocalSplitSynchronizer(mocker.Mock(), mocker.Mock(), mocker.Mock())

        # check no changes if all elements exist with valid values
        parsed = {"splits": [], "since": -1, "till": -1}
        assert (split_synchronizer._sanitize_json_elements(parsed) == parsed)

        # check set since to -1 when is None
        parsed2 = parsed.copy()
        parsed2['since'] = None
        assert (split_synchronizer._sanitize_json_elements(parsed2) == parsed)

        # check no changes if since > -1
        parsed2 = parsed.copy()
        parsed2['since'] = 12
        assert (split_synchronizer._sanitize_json_elements(parsed2) == parsed)

        # check set till to -1 when is None
        parsed2 = parsed.copy()
        parsed2['till'] = None
        assert (split_synchronizer._sanitize_json_elements(parsed2) == parsed)

        # check add since when missing
        parsed2 = {"splits": [], "till": -1}
        assert (split_synchronizer._sanitize_json_elements(parsed2) == parsed)

        # check add till when missing
        parsed2 = {"splits": [], "since": -1}
        assert (split_synchronizer._sanitize_json_elements(parsed2) == parsed)

        # check add splits when missing
        parsed2 = {"since": -1, "till": -1}
        assert (split_synchronizer._sanitize_json_elements(parsed2) == parsed)

    def test_split_elements_sanitization(self, mocker):
        """Test sanitization."""
        split_synchronizer = LocalSplitSynchronizer(mocker.Mock(), mocker.Mock(), mocker.Mock())

        # No changes when split structure is good
        assert (split_synchronizer._sanitize_feature_flag_elements(splits_json["splitChange1_1"]["splits"]) == splits_json["splitChange1_1"]["splits"])

        # test 'trafficTypeName' value None
        split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]['trafficTypeName'] = None
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == splits_json["splitChange1_1"]["splits"])

        # test 'trafficAllocation' value None
        split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]['trafficAllocation'] = None
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == splits_json["splitChange1_1"]["splits"])

        # test 'trafficAllocation' valid value should not change
        split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]['trafficAllocation'] = 50
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == split)

        # test 'trafficAllocation' invalid value should change
        split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]['trafficAllocation'] = 110
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == splits_json["splitChange1_1"]["splits"])

        # test 'trafficAllocationSeed' is set to millisec epoch when None
        split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]['trafficAllocationSeed'] = None
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['trafficAllocationSeed'] > 0)

        # test 'trafficAllocationSeed' is set to millisec epoch when 0
        split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]['trafficAllocationSeed'] = 0
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['trafficAllocationSeed'] > 0)

        # test 'seed' is set to millisec epoch when None
        split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]['seed'] = None
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['seed'] > 0)

        # test 'seed' is set to millisec epoch when its 0
        split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]['seed'] = 0
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['seed'] > 0)

        # test 'status' is set to ACTIVE when None
        split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]['status'] = None
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == splits_json["splitChange1_1"]["splits"])

        # test 'status' is set to ACTIVE when incorrect
        split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]['status'] = 'ww'
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == splits_json["splitChange1_1"]["splits"])

        # test ''killed' is set to False when incorrect
        split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]['killed'] = None
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == splits_json["splitChange1_1"]["splits"])

        # test 'defaultTreatment' is set to on when None
        split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]['defaultTreatment'] = None
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['defaultTreatment'] == 'control')

        # test 'defaultTreatment' is set to on when its empty
        split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]['defaultTreatment'] = ' '
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['defaultTreatment'] == 'control')

        # test 'changeNumber' is set to 0 when None
        split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]['changeNumber'] = None
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['changeNumber'] == 0)

        # test 'changeNumber' is set to 0 when invalid
        split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]['changeNumber'] = -33
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['changeNumber'] == 0)

        # test 'algo' is set to 2 when None
        split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]['algo'] = None
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['algo'] == 2)

        # test 'algo' is set to 2 when higher than 2
        split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]['algo'] = 3
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['algo'] == 2)

        # test 'algo' is set to 2 when lower than 2
        split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]['algo'] = 1
        assert (split_synchronizer._sanitize_feature_flag_elements(split)[0]['algo'] == 2)

    def test_split_condition_sanitization(self, mocker):
        """Test sanitization."""
        split_synchronizer = LocalSplitSynchronizer(mocker.Mock(), mocker.Mock(), mocker.Mock())

        # test missing all conditions with default rule set to 100% off
        split = splits_json["splitChange1_1"]["splits"].copy()
        target_split = splits_json["splitChange1_1"]["splits"].copy()
        target_split[0]["conditions"][0]['partitions'][0]['size'] = 0
        target_split[0]["conditions"][0]['partitions'][1]['size'] = 100
        del split[0]["conditions"]
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == target_split)

        # test missing ALL_KEYS condition matcher with default rule set to 100% off
        split = splits_json["splitChange1_1"]["splits"].copy()
        target_split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]["conditions"][0]["matcherGroup"]["matchers"][0]["matcherType"] = "IN_STR"
        target_split = split.copy()
        target_split[0]["conditions"].append(splits_json["splitChange1_1"]["splits"][0]["conditions"][0])
        target_split[0]["conditions"][1]['partitions'][0]['size'] = 0
        target_split[0]["conditions"][1]['partitions'][1]['size'] = 100
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == target_split)

        # test missing ROLLOUT condition type with default rule set to 100% off
        split = splits_json["splitChange1_1"]["splits"].copy()
        target_split = splits_json["splitChange1_1"]["splits"].copy()
        split[0]["conditions"][0]["conditionType"] = "NOT"
        target_split = split.copy()
        target_split[0]["conditions"].append(splits_json["splitChange1_1"]["splits"][0]["conditions"][0])
        target_split[0]["conditions"][1]['partitions'][0]['size'] = 0
        target_split[0]["conditions"][1]['partitions'][1]['size'] = 100
        assert (split_synchronizer._sanitize_feature_flag_elements(split) == target_split)
