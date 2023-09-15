from splitio.storage import FlagSetsFilter

class FlagSetsFilterTests(object):
    """Flag sets filter storage tests."""

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
