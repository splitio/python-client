"""Split Worker tests."""

from splitio.engine.impressions.adapters import InMemorySenderAdapter
from splitio.engine.impressions.unique_keys_tracker import UniqueKeysTracker
from splitio.sync.unique_keys import UniqueKeysSynchronizer, ClearFilterSynchronizer
import unittest.mock as mock

class UniqueKeysSynchronizerTests(object):
    """ImpressionsCount synchronizer test cases."""

    def test_sync_unique_keys_chunks(self, mocker):
        total_mtks = 5010 # Use number higher than 5000, which is the default max_bulk_size
        unique_keys_tracker = UniqueKeysTracker()
        for i in range(0 , total_mtks):
            unique_keys_tracker.track('key'+str(i)+'', 'feature1')
        sender_adapter = InMemorySenderAdapter(mocker.Mock())
        unique_keys_synchronizer = UniqueKeysSynchronizer(sender_adapter, unique_keys_tracker)
        cache, cache_size = unique_keys_synchronizer._uniqe_keys_tracker.get_cache_info_and_pop_all()
        assert(cache_size > unique_keys_synchronizer._max_bulk_size)

        bulks = unique_keys_synchronizer._split_cache_to_bulks(cache)
        assert(len(bulks) == int(total_mtks / unique_keys_synchronizer._max_bulk_size) + 1)
        for i in range(0 , int(total_mtks / unique_keys_synchronizer._max_bulk_size)):
            if i > int(total_mtks / unique_keys_synchronizer._max_bulk_size):
                assert(len(bulks[i]['feature1']) == (total_mtks - unique_keys_synchronizer._max_bulk_size))
            else:
                assert(len(bulks[i]['feature1']) == unique_keys_synchronizer._max_bulk_size)

    @mock.patch('splitio.engine.impressions.adapters.InMemorySenderAdapter.record_unique_keys')
    def test_sync_unique_keys_send_all(self, mtk_mocker):
        mtk_mocker.side_effect = self.mocked_record_unique_keys

        total_mtks = 5010 # Use number higher than 5000, which is the default max_bulk_size
        unique_keys_tracker = UniqueKeysTracker()
        for i in range(0 , total_mtks):
            unique_keys_tracker.track('key'+str(i)+'', 'feature1')
        sender_adapter = InMemorySenderAdapter(mock.Mock())
        unique_keys_synchronizer = UniqueKeysSynchronizer(sender_adapter, unique_keys_tracker)
        unique_keys_synchronizer.send_all()
        assert(mtk_mocker.call_count == int(total_mtks / unique_keys_synchronizer._max_bulk_size) + 1)

    def mocked_record_unique_keys(self, cache):
        return mock.Mock()

    def test_clear_all_filter(self, mocker):
        unique_keys_tracker = UniqueKeysTracker()
        total_mtks = 50
        for i in range(0 , total_mtks):
            unique_keys_tracker.track('key'+str(i)+'', 'feature1')

        clear_filter_sync = ClearFilterSynchronizer(unique_keys_tracker)
        clear_filter_sync.clear_all()
        for i in range(0 , total_mtks):
            assert(not unique_keys_tracker._filter.contains('feature1key'+str(i)))