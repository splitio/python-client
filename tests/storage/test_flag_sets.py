from splitio.storage import FlagSetsFilter
from splitio.storage.inmemmory import FlagSets

class FlagSetsFilterTests(object):
    """Flag sets filter storage tests."""
    def test_without_initial_set(self):
        flag_set = FlagSets()
        assert flag_set.sets_feature_flag_map == {}

        flag_set.add_flag_set('set1')
        assert flag_set.get_flag_set('set1') == set({})
        assert flag_set.flag_set_exist('set1') == True
        assert flag_set.flag_set_exist('set2') == False

        flag_set.add_feature_flag_to_flag_set('set1', 'split1')
        assert flag_set.get_flag_set('set1') == {'split1'}
        flag_set.add_feature_flag_to_flag_set('set1', 'split2')
        assert flag_set.get_flag_set('set1') == {'split1', 'split2'}
        flag_set.remove_feature_flag_to_flag_set('set1', 'split1')
        assert flag_set.get_flag_set('set1') == {'split2'}
        flag_set.remove_flag_set('set2')
        assert flag_set.sets_feature_flag_map == {'set1': set({'split2'})}
        flag_set.remove_flag_set('set1')
        assert flag_set.sets_feature_flag_map == {}
        assert flag_set.flag_set_exist('set1') == False

    def test_with_initial_set(self):
        flag_set = FlagSets(['set1', 'set2'])
        assert flag_set.sets_feature_flag_map == {'set1': set(), 'set2': set()}

        flag_set.add_flag_set('set1')
        assert flag_set.get_flag_set('set1') == set({})
        assert flag_set.flag_set_exist('set1') == True
        assert flag_set.flag_set_exist('set2') == True

        flag_set.add_feature_flag_to_flag_set('set1', 'split1')
        assert flag_set.get_flag_set('set1') == {'split1'}
        flag_set.add_feature_flag_to_flag_set('set1', 'split2')
        assert flag_set.get_flag_set('set1') == {'split1', 'split2'}
        flag_set.remove_feature_flag_to_flag_set('set1', 'split1')
        assert flag_set.get_flag_set('set1') == {'split2'}
        flag_set.remove_flag_set('set2')
        assert flag_set.sets_feature_flag_map == {'set1': set({'split2'})}
        flag_set.remove_flag_set('set1')
        assert flag_set.sets_feature_flag_map == {}
        assert flag_set.flag_set_exist('set1') == False

    def test_flag_set_filter(self):
        flag_set_filter = FlagSetsFilter()
        assert flag_set_filter.flag_sets == set()
        assert not flag_set_filter.should_filter

        flag_set_filter = FlagSetsFilter(['set1', 'set2'])
        assert flag_set_filter.flag_sets == set({'set1', 'set2'})
        assert flag_set_filter.should_filter
        assert flag_set_filter.intersect(set({'set1', 'set2'}))
        assert flag_set_filter.intersect(set({'set1', 'set2', 'set5'}))
        assert not flag_set_filter.intersect(set({'set4'}))
        assert not flag_set_filter.set_exist('set4')
        assert flag_set_filter.set_exist('set1')
