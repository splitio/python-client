"""Unit tests for the cache module"""
from __future__ import absolute_import, division, print_function, unicode_literals

try:
    from unittest import mock
except ImportError:
    # Python 2
    import mock

from unittest import TestCase

from splitio.cache import (InMemorySplitCache, InMemorySegmentCache, InMemoryImpressionsCache)
from splitio.test.utils import MockUtilsMixin


class InMemorySegmentCacheTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_segment_name = mock.MagicMock()
        self.some_change_number = mock.MagicMock()
        self.segment_cache = InMemorySegmentCache()
        self.entries_mock = self.patch_object(self.segment_cache, '_entries')
        self.existing_entries = {'key_set': {'some_key_1', 'some_key_2'}, 'change_number': -1}
        self.entries_mock.__getitem__.return_value = self.existing_entries

    def test_add_keys_to_segment_sets_keys_to_union(self):
        """Tests that add_keys_to_segment sets keys to union of old and new keys"""
        self.segment_cache.add_keys_to_segment(self.some_segment_name, {'some_key_2', 'some_key_3'})
        self.assertSetEqual({'some_key_1', 'some_key_2', 'some_key_3'},
                            self.existing_entries['key_set'])

    def test_remove_keys_from_segment_set_keys_to_difference(self):
        """Tests that remove_from_segment sets keys to difference of old and new keys"""
        self.segment_cache.remove_keys_from_segment(self.some_segment_name,
                                                    {'some_key_2', 'some_key_3'})
        self.assertSetEqual({'some_key_1'}, self.existing_entries['key_set'])

    def test_is_in_segment_calls_in_on_entries(self):
        """Tests that is_in_segment checks if key in internal set"""
        self.assertTrue(self.segment_cache.is_in_segment(self.some_segment_name, 'some_key_1'))
        self.assertFalse(self.segment_cache.is_in_segment(self.some_segment_name, 'some_key_3'))

    def test_set_change_number_sets_change_number_for_segment(self):
        """Tests that set_change_number sets the change number for the segment"""
        self.segment_cache.set_change_number(self.some_segment_name, self.some_change_number)
        self.assertIn('change_number', self.existing_entries)
        self.assertEqual(self.some_change_number, self.existing_entries['change_number'])

    def test_get_change_number_returns_existing_change_number(self):
        """Tests that get_change_number resturns the current change number for the segment"""
        self.assertEqual(self.existing_entries['change_number'],
                         self.segment_cache.get_change_number(self.some_segment_name))


class InMemorySplitCacheTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_split_name = mock.MagicMock()
        self.some_split = mock.MagicMock()
        self.split_cache = InMemorySplitCache()
        self.some_change_number = mock.MagicMock()
        self.entries_mock = self.patch_object(self.split_cache, '_entries')

    def test_add_split_calls_entries_setitem(self):
        """Tests that add_split calls __setitem__ on entries"""
        self.split_cache.add_split(self.some_split_name, self.some_split)
        self.entries_mock.__setitem__.assert_called_once_with(self.some_split_name,
                                                              self.some_split)

    def test_remove_split_calls_entries_pop(self):
        """Tests that remove_split calls pop on entries"""
        self.split_cache.remove_split(self.some_split_name)
        self.entries_mock.pop.assert_called_once_with(self.some_split_name, None)

    def test_get_split_calls_get(self):
        """Tests that get_split calls get on entries"""
        self.split_cache.get_split(self.some_split_name)
        self.entries_mock.get.assert_called_once_with(self.some_split_name, None)

    def test_get_split_returns_get_result(self):
        """Tests that get_split returns the result of calling get on entries"""
        self.assertEqual(self.entries_mock.get.return_value,
                         self.split_cache.get_split(self.some_split_name))

    def test_set_change_number_sets_change_number(self):
        """Test that set_change_number sets the change number"""
        self.split_cache.set_change_number(self.some_change_number)
        self.assertEqual(self.some_change_number, self.split_cache._change_number)

    def test_get_change_number_returns_change_number(self):
        """Test that get_change_number returns the change number"""
        self.split_cache.set_change_number(self.some_change_number)
        self.split_cache._change_number = self.some_change_number
        self.assertEqual(self.some_change_number, self.split_cache.get_change_number())


class InMemoryImpressionsCacheInitTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_impressions = mock.MagicMock()
        self.impressions_mock = mock.MagicMock()
        self.defaultdict_mock = self.patch('splitio.cache.defaultdict',
                                           return_value=self.impressions_mock)

    def test_init_initializes_impressions_with_impressions_parameter(self):
        """Tests that __init__ updates the _impressions field with the impressions parameter"""
        InMemoryImpressionsCache(impressions=self.some_impressions)
        self.impressions_mock.update.assert_called_once_with(self.some_impressions)


class InMemoryImpressionsCacheTests(TestCase, MockUtilsMixin):
    def setUp(self):
        self.some_impression = mock.MagicMock()
        self.rlock_mock = self.patch('splitio.cache.RLock')
        self.deepcopy_mock = self.patch('splitio.cache.deepcopy')
        self.impressions_mock = mock.MagicMock()
        self.defaultdict_mock = self.patch('splitio.cache.defaultdict',
                                           return_value=self.impressions_mock)
        self.impressions_cache = InMemoryImpressionsCache()
        self.defaultdict_mock.reset_mock()

    def test_add_impression_appends_impression(self):
        """Test that add_impression appends impression to existing list"""
        self.impressions_cache.add_impression(self.some_impression)
        self.impressions_mock.__getitem__.assert_called_once_with(self.some_impression.feature)
        self.impressions_mock.__getitem__.return_value.append.assert_called_once_with(
            self.some_impression)

    def test_fetch_all_returns_impressions_copy(self):
        """Test that fetch all returns a copy of the impressions"""
        result = self.impressions_cache.fetch_all()
        self.deepcopy_mock.assert_called_once_with(self.impressions_mock)
        self.assertEqual(self.deepcopy_mock.return_value, result)

    def test_clear_resets_impressions(self):
        """Test that clear resets impressions"""
        self.impressions_cache.clear()
        self.assertEqual(self.defaultdict_mock.return_value, self.impressions_cache._impressions)

    def test_fetch_all_and_clear_returns_impressions_copy(self):
        """Test that fetch_all_and_clear returns impressions copy"""
        result = self.impressions_cache.fetch_all_and_clear()
        self.deepcopy_mock.assert_called_once_with(self.impressions_mock)
        self.assertEqual(self.deepcopy_mock.return_value, result)

    def test_fetch_all_and_clear_clears_impressions(self):
        """Test that fetch_all_and_clear clears impressions"""
        result = self.impressions_cache.fetch_all_and_clear()
        self.deepcopy_mock.assert_called_once_with(self.impressions_mock)
        self.assertEqual(self.deepcopy_mock.return_value, result)
