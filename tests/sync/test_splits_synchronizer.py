"""Split Worker tests."""

import pytest

from splitio.util.backoff import Backoff
from splitio.api import APIException
from splitio.api.commons import FetchOptions
from splitio.storage import SplitStorage
from splitio.models.splits import Split


class SplitsSynchronizerTests(object):
    """Split synchronizer test cases."""

    def test_synchronize_splits_error(self, mocker):
        """Test that if fetching splits fails at some_point, the task will continue running."""
        storage = mocker.Mock(spec=SplitStorage)
        api = mocker.Mock()

        def run(x, c):
            raise APIException("something broke")
        run._calls = 0
        api.fetch_splits.side_effect = run
        storage.get_change_number.return_value = -1

        from splitio.sync.split import SplitSynchronizer
        split_synchronizer = SplitSynchronizer(api, storage)

        with pytest.raises(APIException):
            split_synchronizer.synchronize_splits(1)

    def test_synchronize_splits(self, mocker):
        """Test split sync."""
        storage = mocker.Mock(spec=SplitStorage)

        def change_number_mock():
            change_number_mock._calls += 1
            if change_number_mock._calls == 1:
                return -1
            return 123
        change_number_mock._calls = 0
        storage.get_change_number.side_effect = change_number_mock

        api = mocker.Mock()
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

        from splitio.sync.split import SplitSynchronizer
        split_synchronizer = SplitSynchronizer(api, storage)
        split_synchronizer.synchronize_splits()

        assert mocker.call(-1, FetchOptions(True)) in api.fetch_splits.mock_calls
        assert mocker.call(123, FetchOptions(True)) in api.fetch_splits.mock_calls

        inserted_split = storage.put.mock_calls[0][1][0]
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == 'some_name'

    def test_not_called_on_till(self, mocker):
        """Test that sync is not called when till is less than previous changenumber"""
        storage = mocker.Mock(spec=SplitStorage)

        def change_number_mock():
            return 2
        storage.get_change_number.side_effect = change_number_mock

        def get_changes(*args, **kwargs):
            get_changes.called += 1
            return None

        get_changes.called = 0

        api = mocker.Mock()
        api.fetch_splits.side_effect = get_changes

        from splitio.sync.split import SplitSynchronizer
        split_synchronizer = SplitSynchronizer(api, storage)
        split_synchronizer.synchronize_splits(1)

        assert get_changes.called == 0

    def test_synchronize_splits_cdn(self, mocker):
        """Test split sync with bypassing cdn."""
        mocker.patch('splitio.sync.split._ON_DEMAND_FETCH_BACKOFF_MAX_RETRIES', new=3)
        from splitio.sync.split import SplitSynchronizer

        storage = mocker.Mock(spec=SplitStorage)

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

        split_synchronizer = SplitSynchronizer(api, storage)
        split_synchronizer._backoff = Backoff(1, 1)
        split_synchronizer.synchronize_splits()

        assert mocker.call(-1, FetchOptions(True)) in api.fetch_splits.mock_calls
        assert mocker.call(123, FetchOptions(True)) in api.fetch_splits.mock_calls

        split_synchronizer._backoff = Backoff(1, 0.1)
        split_synchronizer.synchronize_splits(12345)
        assert mocker.call(12345, FetchOptions(True, 1234)) in api.fetch_splits.mock_calls
        assert len(api.fetch_splits.mock_calls) == 8 # 2 ok + BACKOFF(2 since==till + 2 re-attempts) + CDN(2 since==till)

        inserted_split = storage.put.mock_calls[0][1][0]
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == 'some_name'
