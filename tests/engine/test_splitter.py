"""Splitter test module."""

from splitio.models.grammar.partitions import Partition
from splitio.engine.splitters import Splitter, CONTROL


class SplitterTests(object):
    """Tests for engine/splitter."""

    def test_get_treatment(self, mocker):
        """Test get_treatment method on all possible outputs."""
        splitter = Splitter()

        # no partitions returns control
        assert splitter.get_treatment('key', 123, [], 1) == CONTROL
        # single partition returns that treatment
        assert splitter.get_treatment('key', 123, [Partition('on', 100)], 1) == 'on'
        # multiple partitions call hash_functions
        splitter.get_treatment_for_bucket = lambda x,y: 'on'
        partitions = [Partition('on', 50), Partition('off', 50)]
        assert splitter.get_treatment('key', 123, partitions, 1) == 'on'

    def test_get_bucket(self, mocker):
        """Test get_bucket method."""
        get_hash_fn_mock = mocker.Mock()
        hash_fn = mocker.Mock()
        hash_fn.return_value = 1
        get_hash_fn_mock.side_effect = lambda x: hash_fn
        mocker.patch('splitio.engine.splitters.get_hash_fn', new=get_hash_fn_mock)
        splitter = Splitter()
        splitter.get_bucket(1, 123, 1)
        assert get_hash_fn_mock.mock_calls == [mocker.call(1)]
        assert hash_fn.mock_calls == [mocker.call(1, 123)]

    def test_treatment_for_bucket(self, mocker):
        """Test treatment for bucket method."""
        splitter = Splitter()
        assert splitter.get_treatment_for_bucket(0, []) == CONTROL
        assert splitter.get_treatment_for_bucket(-1, []) == CONTROL
        assert splitter.get_treatment_for_bucket(101, [Partition('a', 100)]) == CONTROL
        assert splitter.get_treatment_for_bucket(1, [Partition('a', 100)]) == 'a'
        assert splitter.get_treatment_for_bucket(100, [Partition('a', 100)]) == 'a'
        assert splitter.get_treatment_for_bucket(50, [Partition('a', 50), Partition('b', 50)]) == 'a'
        assert splitter.get_treatment_for_bucket(51, [Partition('a', 50), Partition('b', 50)]) == 'b'







