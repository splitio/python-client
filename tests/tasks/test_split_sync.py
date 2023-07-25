"""Split syncrhonization task test module."""
import threading
import time
import pytest

from splitio.api import APIException
from splitio.api.commons import FetchOptions
from splitio.tasks import split_sync
from splitio.storage import SplitStorage
from splitio.models.splits import Split
from splitio.sync.split import SplitSynchronizer, SplitSynchronizerAsync
from splitio.optional.loaders import asyncio

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


class SplitSynchronizationTests(object):
    """Split synchronization task test cases."""

    def test_normal_operation(self, mocker):
        """Test the normal operation flow."""
        storage = mocker.Mock(spec=SplitStorage)

        def change_number_mock():
            change_number_mock._calls += 1
            if change_number_mock._calls == 1:
                return -1
            return 123
        change_number_mock._calls = 0
        storage.get_change_number.side_effect = change_number_mock

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

        fetch_options = FetchOptions(True)
        api.fetch_splits.side_effect = get_changes
        split_synchronizer = SplitSynchronizer(api, storage)
        task = split_sync.SplitSynchronizationTask(split_synchronizer.synchronize_splits, 0.5)
        task.start()
        time.sleep(0.7)
        assert task.is_running()
        stop_event = threading.Event()
        task.stop(stop_event)
        stop_event.wait()
        assert not task.is_running()
        assert mocker.call(-1, fetch_options) in api.fetch_splits.mock_calls
        assert mocker.call(123, fetch_options) in api.fetch_splits.mock_calls

        inserted_split = storage.put.mock_calls[0][1][0]
        assert isinstance(inserted_split, Split)
        assert inserted_split.name == 'some_name'

    def test_that_errors_dont_stop_task(self, mocker):
        """Test that if fetching splits fails at some_point, the task will continue running."""
        storage = mocker.Mock(spec=SplitStorage)
        api = mocker.Mock()

        def run(x):
            run._calls += 1
            if run._calls == 1:
                return {'splits': [], 'since': -1, 'till': -1}
            if run._calls == 2:
                return {'splits': [], 'since': -1, 'till': -1}
            raise APIException("something broke")
        run._calls = 0
        api.fetch_splits.side_effect = run
        storage.get_change_number.return_value = -1

        split_synchronizer = SplitSynchronizer(api, storage)
        task = split_sync.SplitSynchronizationTask(split_synchronizer.synchronize_splits, 0.5)
        task.start()
        time.sleep(0.1)
        assert task.is_running()
        time.sleep(1)
        assert task.is_running()
        task.stop()


class SplitSynchronizationAsyncTests(object):
    """Split synchronization task async test cases."""

    @pytest.mark.asyncio
    async def test_normal_operation(self, mocker):
        """Test the normal operation flow."""
        storage = mocker.Mock(spec=SplitStorage)

        async def change_number_mock():
            change_number_mock._calls += 1
            if change_number_mock._calls == 1:
                return -1
            return 123
        change_number_mock._calls = 0
        storage.get_change_number = change_number_mock

        api = mocker.Mock()
        self.change_number = []
        self.fetch_options = []
        async def get_changes(change_number, fetch_options):
            self.change_number.append(change_number)
            self.fetch_options.append(fetch_options)
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
        api.fetch_splits = get_changes
        get_changes.called = 0
        self.inserted_split = None
        async def put(split):
            self.inserted_split = split
        storage.put = put

        fetch_options = FetchOptions(True)
        split_synchronizer = SplitSynchronizerAsync(api, storage)
        task = split_sync.SplitSynchronizationTaskAsync(split_synchronizer.synchronize_splits, 0.5)
        task.start()
        await asyncio.sleep(0.7)
        assert task.is_running()
        await task.stop()
        assert not task.is_running()
        assert (self.change_number[0], self.fetch_options[0])  == (-1, fetch_options)
        assert (self.change_number[1], self.fetch_options[1])  == (123, fetch_options)
        assert isinstance(self.inserted_split, Split)
        assert self.inserted_split.name == 'some_name'

    @pytest.mark.asyncio
    async def test_that_errors_dont_stop_task(self, mocker):
        """Test that if fetching splits fails at some_point, the task will continue running."""
        storage = mocker.Mock(spec=SplitStorage)
        api = mocker.Mock()

        async def run(x):
            run._calls += 1
            if run._calls == 1:
                return {'splits': [], 'since': -1, 'till': -1}
            if run._calls == 2:
                return {'splits': [], 'since': -1, 'till': -1}
            raise APIException("something broke")
        run._calls = 0
        api.fetch_splits = run

        async def get_change_number():
            return -1
        storage.get_change_number = get_change_number

        split_synchronizer = SplitSynchronizerAsync(api, storage)
        task = split_sync.SplitSynchronizationTaskAsync(split_synchronizer.synchronize_splits, 0.5)
        task.start()
        await asyncio.sleep(0.1)
        assert task.is_running()
        await asyncio.sleep(1)
        assert task.is_running()
        await task.stop()
