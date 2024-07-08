"""In-Memory storage test module."""
# pylint: disable=no-self-use
import random
import pytest

from splitio.models.splits import Split
from splitio.models.segments import Segment
from splitio.models.impressions import Impression
from splitio.models.events import Event, EventWrapper
import splitio.models.telemetry as ModelTelemetry
from splitio.engine.telemetry import TelemetryStorageProducer, TelemetryStorageProducerAsync
from splitio.storage.inmemmory import InMemorySplitStorage, InMemorySegmentStorage, InMemorySegmentStorageAsync, InMemorySplitStorageAsync, \
    InMemoryImpressionStorage, InMemoryEventStorage, InMemoryTelemetryStorage, InMemoryImpressionStorageAsync, InMemoryEventStorageAsync, \
    InMemoryTelemetryStorageAsync, FlagSets

class FlagSetsFilterTests(object):
    """Flag sets filter storage tests."""
    def test_without_initial_set(self):
        flag_set = FlagSets()
        assert flag_set.sets_feature_flag_map == {}

        flag_set._add_flag_set('set1')
        assert flag_set.get_flag_set('set1') == set({})
        assert flag_set.flag_set_exist('set1') == True
        assert flag_set.flag_set_exist('set2') == False

        flag_set.add_feature_flag_to_flag_set('set1', 'split1')
        assert flag_set.get_flag_set('set1') == {'split1'}
        flag_set.add_feature_flag_to_flag_set('set1', 'split2')
        assert flag_set.get_flag_set('set1') == {'split1', 'split2'}
        flag_set.remove_feature_flag_to_flag_set('set1', 'split1')
        assert flag_set.get_flag_set('set1') == {'split2'}
        flag_set._remove_flag_set('set2')
        assert flag_set.sets_feature_flag_map == {'set1': set({'split2'})}
        flag_set._remove_flag_set('set1')
        assert flag_set.sets_feature_flag_map == {}
        assert flag_set.flag_set_exist('set1') == False

    def test_with_initial_set(self):
        flag_set = FlagSets(['set1', 'set2'])
        assert flag_set.sets_feature_flag_map == {'set1': set(), 'set2': set()}

        flag_set._add_flag_set('set1')
        assert flag_set.get_flag_set('set1') == set({})
        assert flag_set.flag_set_exist('set1') == True
        assert flag_set.flag_set_exist('set2') == True

        flag_set.add_feature_flag_to_flag_set('set1', 'split1')
        assert flag_set.get_flag_set('set1') == {'split1'}
        flag_set.add_feature_flag_to_flag_set('set1', 'split2')
        assert flag_set.get_flag_set('set1') == {'split1', 'split2'}
        flag_set.remove_feature_flag_to_flag_set('set1', 'split1')
        assert flag_set.get_flag_set('set1') == {'split2'}
        flag_set._remove_flag_set('set2')
        assert flag_set.sets_feature_flag_map == {'set1': set({'split2'})}
        flag_set._remove_flag_set('set1')
        assert flag_set.sets_feature_flag_map == {}
        assert flag_set.flag_set_exist('set1') == False

class InMemorySplitStorageTests(object):
    """In memory split storage test cases."""

    def test_storing_retrieving_splits(self, mocker):
        """Test storing and retrieving splits works."""
        storage = InMemorySplitStorage()

        split = mocker.Mock(spec=Split)
        name_property = mocker.PropertyMock()
        name_property.return_value = 'some_split'
        type(split).name = name_property
        sets_property = mocker.PropertyMock()
        sets_property.return_value = ['set_1']
        type(split).sets = sets_property

        storage.update([split], [], -1)
        assert storage.get('some_split') == split
        assert storage.get_split_names() == ['some_split']
        assert storage.get_all_splits() == [split]
        assert storage.get('nonexistant_split') is None

        storage.update([], ['some_split'], -1)
        assert storage.get('some_split') is None

    def test_get_splits(self, mocker):
        """Test retrieving a list of passed splits."""
        split1 = mocker.Mock()
        name1_prop = mocker.PropertyMock()
        name1_prop.return_value = 'split1'
        type(split1).name = name1_prop
        sets_property = mocker.PropertyMock()
        sets_property.return_value = ['set_1']
        type(split1).sets = sets_property

        split2 = mocker.Mock()
        name2_prop = mocker.PropertyMock()
        name2_prop.return_value = 'split2'
        type(split2).name = name2_prop
        type(split2).sets = sets_property

        storage = InMemorySplitStorage()
        storage.update([split1, split2], [], -1)

        splits = storage.fetch_many(['split1', 'split2', 'split3'])
        assert len(splits) == 3
        assert splits['split1'].name == 'split1'
        assert splits['split1'].sets == ['set_1']
        assert splits['split2'].name == 'split2'
        assert splits['split2'].sets == ['set_1']
        assert 'split3' in splits

    def test_store_get_changenumber(self):
        """Test that storing and retrieving change numbers works."""
        storage = InMemorySplitStorage()
        assert storage.get_change_number() == -1
        storage.update([], [], 5)
        assert storage.get_change_number() == 5

    def test_get_split_names(self, mocker):
        """Test retrieving a list of all split names."""
        split1 = mocker.Mock()
        name1_prop = mocker.PropertyMock()
        name1_prop.return_value = 'split1'
        type(split1).name = name1_prop
        sets_property = mocker.PropertyMock()
        sets_property.return_value = ['set_1']
        type(split1).sets = sets_property

        split2 = mocker.Mock()
        name2_prop = mocker.PropertyMock()
        name2_prop.return_value = 'split2'
        type(split2).name = name2_prop
        type(split2).sets = sets_property

        storage = InMemorySplitStorage()
        storage.update([split1, split2], [], -1)

        assert set(storage.get_split_names()) == set(['split1', 'split2'])

    def test_get_all_splits(self, mocker):
        """Test retrieving a list of all split names."""
        split1 = mocker.Mock()
        name1_prop = mocker.PropertyMock()
        name1_prop.return_value = 'split1'
        type(split1).name = name1_prop
        sets_property = mocker.PropertyMock()
        sets_property.return_value = ['set_1']
        type(split1).sets = sets_property

        split2 = mocker.Mock()
        name2_prop = mocker.PropertyMock()
        name2_prop.return_value = 'split2'
        type(split2).name = name2_prop
        type(split2).sets = sets_property

        storage = InMemorySplitStorage()
        storage.update([split1, split2], [], -1)

        all_splits = storage.get_all_splits()
        assert next(s for s in all_splits if s.name == 'split1')
        assert next(s for s in all_splits if s.name == 'split2')

    def test_is_valid_traffic_type(self, mocker):
        """Test that traffic type validation works properly."""
        split1 = mocker.Mock()
        name1_prop = mocker.PropertyMock()
        name1_prop.return_value = 'split1'
        type(split1).name = name1_prop
        split2 = mocker.Mock()
        name2_prop = mocker.PropertyMock()
        name2_prop.return_value = 'split2'
        type(split2).name = name2_prop
        split3 = mocker.Mock()
        tt_user = mocker.PropertyMock()
        tt_user.return_value = 'user'
        tt_account = mocker.PropertyMock()
        tt_account.return_value = 'account'
        name3_prop = mocker.PropertyMock()
        name3_prop.return_value = 'split3'
        type(split3).name = name3_prop
        type(split1).traffic_type_name = tt_user
        type(split2).traffic_type_name = tt_account
        type(split3).traffic_type_name = tt_user
        sets_property = mocker.PropertyMock()
        sets_property.return_value = []
        type(split1).sets = sets_property
        type(split2).sets = sets_property
        type(split3).sets = sets_property

        storage = InMemorySplitStorage()

        storage.update([split1], [], -1)
        assert storage.is_valid_traffic_type('user') is True
        assert storage.is_valid_traffic_type('account') is False

        storage.update([split2], [], -1)
        assert storage.is_valid_traffic_type('user') is True
        assert storage.is_valid_traffic_type('account') is True

        storage.update([split3], [], -1)
        assert storage.is_valid_traffic_type('user') is True
        assert storage.is_valid_traffic_type('account') is True

        storage.update([], ['split1'], -1)
        assert storage.is_valid_traffic_type('user') is True
        assert storage.is_valid_traffic_type('account') is True

        storage.update([], ['split2'], -1)
        assert storage.is_valid_traffic_type('user') is True
        assert storage.is_valid_traffic_type('account') is False

        storage.update([], ['split3'], -1)
        assert storage.is_valid_traffic_type('user') is False
        assert storage.is_valid_traffic_type('account') is False

    def test_traffic_type_inc_dec_logic(self, mocker):
        """Test that adding/removing split, handles traffic types correctly."""
        storage = InMemorySplitStorage()

        split1 = mocker.Mock()
        name1_prop = mocker.PropertyMock()
        name1_prop.return_value = 'split1'
        type(split1).name = name1_prop
        split2 = mocker.Mock()
        name2_prop = mocker.PropertyMock()
        name2_prop.return_value = 'split1'
        type(split2).name = name2_prop
        sets_property = mocker.PropertyMock()
        sets_property.return_value = None
        type(split1).sets = sets_property
        type(split2).sets = sets_property

        tt_user = mocker.PropertyMock()
        tt_user.return_value = 'user'
        tt_account = mocker.PropertyMock()
        tt_account.return_value = 'account'
        sets_property = mocker.PropertyMock()
        sets_property.return_value = ['set_1']
        type(split1).sets = sets_property
        type(split2).sets = sets_property
        type(split1).traffic_type_name = tt_user
        type(split2).traffic_type_name = tt_account

        storage.update([split1], [], -1)
        assert storage.is_valid_traffic_type('user') is True
        assert storage.is_valid_traffic_type('account') is False

        storage.update([split2], [], -1)
        assert storage.is_valid_traffic_type('user') is False
        assert storage.is_valid_traffic_type('account') is True

    def test_kill_locally(self):
        """Test kill local."""
        storage = InMemorySplitStorage()

        split = Split('some_split', 123456789, False, 'some', 'traffic_type',
                      'ACTIVE', 1)
        storage.update([split], [], 1)

        storage.kill_locally('test', 'default_treatment', 2)
        assert storage.get('test') is None

        storage.kill_locally('some_split', 'default_treatment', 0)
        assert storage.get('some_split').change_number == 1
        assert storage.get('some_split').killed is False
        assert storage.get('some_split').default_treatment == 'some'

        storage.kill_locally('some_split', 'default_treatment', 3)
        assert storage.get('some_split').change_number == 3

    def test_flag_sets_with_config_sets(self):
        storage = InMemorySplitStorage(['set10', 'set02', 'set05'])
        assert storage.flag_set_filter.flag_sets == {'set10', 'set02', 'set05'}
        assert storage.flag_set_filter.should_filter

        assert storage.flag_set.sets_feature_flag_map == {'set10': set(), 'set02': set(), 'set05': set()}

        split1 = Split('split1', 123456789, False, 'some', 'traffic_type',
                      'ACTIVE', 1, sets=['set10', 'set02'])
        split2 = Split('split2', 123456789, False, 'some', 'traffic_type',
                      'ACTIVE', 1, sets=['set05', 'set02'])
        split3 = Split('split3', 123456789, False, 'some', 'traffic_type',
                      'ACTIVE', 1, sets=['set04', 'set05'])
        storage.update([split1], [], 1)
        assert storage.get_feature_flags_by_sets(['set10']) == ['split1']
        assert storage.get_feature_flags_by_sets(['set02']) == ['split1']
        assert storage.get_feature_flags_by_sets(['set02', 'set10']) == ['split1']
        assert storage.is_flag_set_exist('set10')
        assert storage.is_flag_set_exist('set02')
        assert not storage.is_flag_set_exist('set03')

        storage.update([split2], [], 1)
        assert storage.get_feature_flags_by_sets(['set05']) == ['split2']
        assert sorted(storage.get_feature_flags_by_sets(['set02', 'set05'])) == ['split1', 'split2']
        assert storage.is_flag_set_exist('set05')

        storage.update([], [split2.name], 1)
        assert storage.is_flag_set_exist('set05')
        assert storage.get_feature_flags_by_sets(['set02']) == ['split1']
        assert storage.get_feature_flags_by_sets(['set05']) == []

        split1 = Split('split1', 123456789, False, 'some', 'traffic_type',
                      'ACTIVE', 1, sets=['set02'])
        storage.update([split1], [], 1)
        assert storage.is_flag_set_exist('set10')
        assert storage.get_feature_flags_by_sets(['set02']) == ['split1']

        storage.update([], [split1.name], 1)
        assert storage.get_feature_flags_by_sets(['set02']) == []
        assert storage.flag_set.sets_feature_flag_map == {'set10': set(), 'set02': set(), 'set05': set()}

        storage.update([split3], [], 1)
        assert storage.get_feature_flags_by_sets(['set05']) == ['split3']
        assert not storage.is_flag_set_exist('set04')

    def test_flag_sets_withut_config_sets(self):
        storage = InMemorySplitStorage()
        assert storage.flag_set_filter.flag_sets == set({})
        assert not storage.flag_set_filter.should_filter

        assert storage.flag_set.sets_feature_flag_map == {}

        split1 = Split('split1', 123456789, False, 'some', 'traffic_type',
                      'ACTIVE', 1, sets=['set10', 'set02'])
        split2 = Split('split2', 123456789, False, 'some', 'traffic_type',
                      'ACTIVE', 1, sets=['set05', 'set02'])
        split3 = Split('split3', 123456789, False, 'some', 'traffic_type',
                      'ACTIVE', 1, sets=['set04', 'set05'])
        storage.update([split1], [], 1)
        assert storage.get_feature_flags_by_sets(['set10']) == ['split1']
        assert storage.get_feature_flags_by_sets(['set02']) == ['split1']
        assert storage.is_flag_set_exist('set10')
        assert storage.is_flag_set_exist('set02')
        assert not storage.is_flag_set_exist('set03')

        storage.update([split2], [], 1)
        assert storage.get_feature_flags_by_sets(['set05']) == ['split2']
        assert sorted(storage.get_feature_flags_by_sets(['set02', 'set05'])) == ['split1', 'split2']
        assert storage.is_flag_set_exist('set05')

        storage.update([], [split2.name], 1)
        assert not storage.is_flag_set_exist('set05')
        assert storage.get_feature_flags_by_sets(['set02']) == ['split1']

        split1 = Split('split1', 123456789, False, 'some', 'traffic_type',
                      'ACTIVE', 1, sets=['set02'])
        storage.update([split1], [], 1)
        assert not storage.is_flag_set_exist('set10')
        assert storage.get_feature_flags_by_sets(['set02']) == ['split1']

        storage.update([], [split1.name], 1)
        assert storage.get_feature_flags_by_sets(['set02']) == []
        assert storage.flag_set.sets_feature_flag_map == {}

        storage.update([split3], [], 1)
        assert storage.get_feature_flags_by_sets(['set05']) == ['split3']
        assert storage.get_feature_flags_by_sets(['set04', 'set05']) == ['split3']

class InMemorySplitStorageAsyncTests(object):
    """In memory split storage test cases."""

    @pytest.mark.asyncio
    async def test_storing_retrieving_splits(self, mocker):
        """Test storing and retrieving splits works."""
        storage = InMemorySplitStorageAsync()

        split = mocker.Mock(spec=Split)
        name_property = mocker.PropertyMock()
        name_property.return_value = 'some_split'
        type(split).name = name_property
        sets_property = mocker.PropertyMock()
        sets_property.return_value = ['set_1']
        type(split).sets = sets_property

        await storage.update([split], [], -1)
        assert await storage.get('some_split') == split
        assert await storage.get_split_names() == ['some_split']
        assert await storage.get_all_splits() == [split]
        assert await storage.get('nonexistant_split') is None

        await storage.update([], ['some_split'], -1)
        assert await storage.get('some_split') is None

    @pytest.mark.asyncio
    async def test_get_splits(self, mocker):
        """Test retrieving a list of passed splits."""
        split1 = mocker.Mock()
        name1_prop = mocker.PropertyMock()
        name1_prop.return_value = 'split1'
        type(split1).name = name1_prop
        split2 = mocker.Mock()
        name2_prop = mocker.PropertyMock()
        name2_prop.return_value = 'split2'
        type(split2).name = name2_prop
        sets_property = mocker.PropertyMock()
        sets_property.return_value = ['set_1']
        type(split1).sets = sets_property
        type(split2).sets = sets_property

        storage = InMemorySplitStorageAsync()
        await storage.update([split1, split2], [], -1)

        splits = await storage.fetch_many(['split1', 'split2', 'split3'])
        assert len(splits) == 3
        assert splits['split1'].name == 'split1'
        assert splits['split2'].name == 'split2'
        assert 'split3' in splits

    @pytest.mark.asyncio
    async def test_store_get_changenumber(self):
        """Test that storing and retrieving change numbers works."""
        storage = InMemorySplitStorageAsync()
        assert await storage.get_change_number() == -1
        await storage.update([], [], 5)
        assert await storage.get_change_number() == 5

    @pytest.mark.asyncio
    async def test_get_split_names(self, mocker):
        """Test retrieving a list of all split names."""
        split1 = mocker.Mock()
        name1_prop = mocker.PropertyMock()
        name1_prop.return_value = 'split1'
        type(split1).name = name1_prop
        split2 = mocker.Mock()
        name2_prop = mocker.PropertyMock()
        name2_prop.return_value = 'split2'
        type(split2).name = name2_prop
        sets_property = mocker.PropertyMock()
        sets_property.return_value = ['set_1']
        type(split1).sets = sets_property
        type(split2).sets = sets_property

        storage = InMemorySplitStorageAsync()
        await storage.update([split1, split2], [], -1)

        assert set(await storage.get_split_names()) == set(['split1', 'split2'])

    @pytest.mark.asyncio
    async def test_get_all_splits(self, mocker):
        """Test retrieving a list of all split names."""
        split1 = mocker.Mock()
        name1_prop = mocker.PropertyMock()
        name1_prop.return_value = 'split1'
        type(split1).name = name1_prop
        split2 = mocker.Mock()
        name2_prop = mocker.PropertyMock()
        name2_prop.return_value = 'split2'
        type(split2).name = name2_prop
        sets_property = mocker.PropertyMock()
        sets_property.return_value = ['set_1']
        type(split1).sets = sets_property
        type(split2).sets = sets_property

        storage = InMemorySplitStorageAsync()
        await storage.update([split1, split2], [], -1)

        all_splits = await storage.get_all_splits()
        assert next(s for s in all_splits if s.name == 'split1')
        assert next(s for s in all_splits if s.name == 'split2')

    @pytest.mark.asyncio
    async def test_is_valid_traffic_type(self, mocker):
        """Test that traffic type validation works properly."""
        split1 = mocker.Mock()
        name1_prop = mocker.PropertyMock()
        name1_prop.return_value = 'split1'
        type(split1).name = name1_prop
        split2 = mocker.Mock()
        name2_prop = mocker.PropertyMock()
        name2_prop.return_value = 'split2'
        type(split2).name = name2_prop
        split3 = mocker.Mock()
        tt_user = mocker.PropertyMock()
        tt_user.return_value = 'user'
        tt_account = mocker.PropertyMock()
        tt_account.return_value = 'account'
        name3_prop = mocker.PropertyMock()
        name3_prop.return_value = 'split3'
        type(split3).name = name3_prop
        type(split1).traffic_type_name = tt_user
        type(split2).traffic_type_name = tt_account
        type(split3).traffic_type_name = tt_user
        sets_property = mocker.PropertyMock()
        sets_property.return_value = ['set_1']
        type(split1).sets = sets_property
        type(split2).sets = sets_property
        type(split3).sets = sets_property

        storage = InMemorySplitStorageAsync()

        await storage.update([split1], [], -1)
        assert await storage.is_valid_traffic_type('user') is True
        assert await storage.is_valid_traffic_type('account') is False

        await storage.update([split2], [], -1)
        assert await storage.is_valid_traffic_type('user') is True
        assert await storage.is_valid_traffic_type('account') is True

        await storage.update([split3], [], -1)
        assert await storage.is_valid_traffic_type('user') is True
        assert await storage.is_valid_traffic_type('account') is True

        await storage.update([], ['split1'], -1)
        assert await storage.is_valid_traffic_type('user') is True
        assert await storage.is_valid_traffic_type('account') is True

        await storage.update([], ['split2'], -1)
        assert await storage.is_valid_traffic_type('user') is True
        assert await storage.is_valid_traffic_type('account') is False

        await storage.update([], ['split3'], -1)
        assert await storage.is_valid_traffic_type('user') is False
        assert await storage.is_valid_traffic_type('account') is False

    @pytest.mark.asyncio
    async def test_traffic_type_inc_dec_logic(self, mocker):
        """Test that adding/removing split, handles traffic types correctly."""
        storage = InMemorySplitStorageAsync()

        split1 = mocker.Mock()
        name1_prop = mocker.PropertyMock()
        name1_prop.return_value = 'split1'
        type(split1).name = name1_prop

        split2 = mocker.Mock()
        name2_prop = mocker.PropertyMock()
        name2_prop.return_value = 'split1'
        type(split2).name = name2_prop
        tt_user = mocker.PropertyMock()
        tt_user.return_value = 'user'
        tt_account = mocker.PropertyMock()
        tt_account.return_value = 'account'
        type(split1).traffic_type_name = tt_user
        type(split2).traffic_type_name = tt_account
        sets_property = mocker.PropertyMock()
        sets_property.return_value = ['set_1']
        type(split1).sets = sets_property
        type(split2).sets = sets_property

        await storage.update([split1], [], -1)
        assert await storage.is_valid_traffic_type('user') is True
        assert await storage.is_valid_traffic_type('account') is False

        await storage.update([split2], [], -1)
        assert await storage.is_valid_traffic_type('user') is False
        assert await storage.is_valid_traffic_type('account') is True

    @pytest.mark.asyncio
    async def test_kill_locally(self):
        """Test kill local."""
        storage = InMemorySplitStorageAsync()

        split = Split('some_split', 123456789, False, 'some', 'traffic_type',
                      'ACTIVE', 1)
        await storage.update([split], [], 1)

        await storage.kill_locally('test', 'default_treatment', 2)
        assert await storage.get('test') is None

        await storage.kill_locally('some_split', 'default_treatment', 0)
        split = await storage.get('some_split')
        assert split.change_number == 1
        assert split.killed is False
        assert split.default_treatment == 'some'

        await storage.kill_locally('some_split', 'default_treatment', 3)
        split = await storage.get('some_split')
        assert split.change_number == 3

    @pytest.mark.asyncio
    async def test_flag_sets_with_config_sets(self):
        storage = InMemorySplitStorageAsync(['set10', 'set02', 'set05'])
        assert storage.flag_set_filter.flag_sets == {'set10', 'set02', 'set05'}
        assert storage.flag_set_filter.should_filter

        assert storage.flag_set.sets_feature_flag_map == {'set10': set(), 'set02': set(), 'set05': set()}

        split1 = Split('split1', 123456789, False, 'some', 'traffic_type',
                      'ACTIVE', 1, sets=['set10', 'set02'])
        split2 = Split('split2', 123456789, False, 'some', 'traffic_type',
                      'ACTIVE', 1, sets=['set05', 'set02'])
        split3 = Split('split3', 123456789, False, 'some', 'traffic_type',
                      'ACTIVE', 1, sets=['set04', 'set05'])
        await storage.update([split1], [], 1)
        assert await storage.get_feature_flags_by_sets(['set10']) == ['split1']
        assert await storage.get_feature_flags_by_sets(['set02']) == ['split1']
        assert await storage.get_feature_flags_by_sets(['set02', 'set10']) == ['split1']
        assert await storage.is_flag_set_exist('set10')
        assert await storage.is_flag_set_exist('set02')
        assert not await storage.is_flag_set_exist('set03')

        await storage.update([split2], [], 1)
        assert await storage.get_feature_flags_by_sets(['set05']) == ['split2']
        assert sorted(await storage.get_feature_flags_by_sets(['set02', 'set05'])) == ['split1', 'split2']
        assert await storage.is_flag_set_exist('set05')

        await storage.update([], [split2.name], 1)
        assert await storage.is_flag_set_exist('set05')
        assert await storage.get_feature_flags_by_sets(['set02']) == ['split1']
        assert await storage.get_feature_flags_by_sets(['set05']) == []

        split1 = Split('split1', 123456789, False, 'some', 'traffic_type',
                      'ACTIVE', 1, sets=['set02'])
        await storage.update([split1], [], 1)
        assert await storage.is_flag_set_exist('set10')
        assert await storage.get_feature_flags_by_sets(['set02']) == ['split1']

        await storage.update([], [split1.name], 1)
        assert await storage.get_feature_flags_by_sets(['set02']) == []
        assert storage.flag_set.sets_feature_flag_map == {'set10': set(), 'set02': set(), 'set05': set()}

        await storage.update([split3], [], 1)
        assert await storage.get_feature_flags_by_sets(['set05']) == ['split3']
        assert not await storage.is_flag_set_exist('set04')

    @pytest.mark.asyncio
    async def test_flag_sets_withut_config_sets(self):
        storage = InMemorySplitStorageAsync()
        assert storage.flag_set_filter.flag_sets == set({})
        assert not storage.flag_set_filter.should_filter

        assert storage.flag_set.sets_feature_flag_map == {}

        split1 = Split('split1', 123456789, False, 'some', 'traffic_type',
                      'ACTIVE', 1, sets=['set10', 'set02'])
        split2 = Split('split2', 123456789, False, 'some', 'traffic_type',
                      'ACTIVE', 1, sets=['set05', 'set02'])
        split3 = Split('split3', 123456789, False, 'some', 'traffic_type',
                      'ACTIVE', 1, sets=['set04', 'set05'])
        await storage.update([split1], [], 1)
        assert await storage.get_feature_flags_by_sets(['set10']) == ['split1']
        assert await storage.get_feature_flags_by_sets(['set02']) == ['split1']
        assert await storage.is_flag_set_exist('set10')
        assert await storage.is_flag_set_exist('set02')
        assert not await storage.is_flag_set_exist('set03')

        await storage.update([split2], [], 1)
        assert await storage.get_feature_flags_by_sets(['set05']) == ['split2']
        assert sorted(await storage.get_feature_flags_by_sets(['set02', 'set05'])) == ['split1', 'split2']
        assert await storage.is_flag_set_exist('set05')

        await storage.update([], [split2.name], 1)
        assert not await storage.is_flag_set_exist('set05')
        assert await storage.get_feature_flags_by_sets(['set02']) == ['split1']

        split1 = Split('split1', 123456789, False, 'some', 'traffic_type',
                      'ACTIVE', 1, sets=['set02'])
        await storage.update([split1], [], 1)
        assert not await storage.is_flag_set_exist('set10')
        assert await storage.get_feature_flags_by_sets(['set02']) == ['split1']

        await storage.update([], [split1.name], 1)
        assert await storage.get_feature_flags_by_sets(['set02']) == []
        assert storage.flag_set.sets_feature_flag_map == {}

        await storage.update([split3], [], 1)
        assert await storage.get_feature_flags_by_sets(['set05']) == ['split3']
        assert await storage.get_feature_flags_by_sets(['set04', 'set05']) == ['split3']

class InMemorySegmentStorageTests(object):
    """In memory segment storage tests."""

    def test_segment_storage_retrieval(self, mocker):
        """Test storing and retrieving segments."""
        storage = InMemorySegmentStorage()
        segment = mocker.Mock(spec=Segment)
        name_property = mocker.PropertyMock()
        name_property.return_value = 'some_segment'
        type(segment).name = name_property

        storage.put(segment)
        assert storage.get('some_segment') == segment
        assert storage.get('nonexistant-segment') is None

    def test_change_number(self, mocker):
        """Test storing and retrieving segment changeNumber."""
        storage = InMemorySegmentStorage()
        storage.set_change_number('some_segment', 123)
        # Change number is not updated if segment doesn't exist
        assert storage.get_change_number('some_segment') is None
        assert storage.get_change_number('nonexistant-segment') is None

        # Change number is updated if segment does exist.
        storage = InMemorySegmentStorage()
        segment = mocker.Mock(spec=Segment)
        name_property = mocker.PropertyMock()
        name_property.return_value = 'some_segment'
        type(segment).name = name_property
        storage.put(segment)
        storage.set_change_number('some_segment', 123)
        assert storage.get_change_number('some_segment') == 123

    def test_segment_contains(self, mocker):
        """Test using storage to determine whether a key belongs to a segment."""
        storage = InMemorySegmentStorage()
        segment = mocker.Mock(spec=Segment)
        name_property = mocker.PropertyMock()
        name_property.return_value = 'some_segment'
        type(segment).name = name_property
        storage.put(segment)

        storage.segment_contains('some_segment', 'abc')
        assert segment.contains.mock_calls[0] == mocker.call('abc')

    def test_segment_update(self):
        """Test updating a segment."""
        storage = InMemorySegmentStorage()
        segment = Segment('some_segment', ['key1', 'key2', 'key3'], 123)
        storage.put(segment)
        assert storage.get('some_segment') == segment

        storage.update('some_segment', ['key4', 'key5'], ['key2', 'key3'], 456)
        assert storage.segment_contains('some_segment', 'key1')
        assert storage.segment_contains('some_segment', 'key4')
        assert storage.segment_contains('some_segment', 'key5')
        assert not storage.segment_contains('some_segment', 'key2')
        assert not storage.segment_contains('some_segment', 'key3')
        assert storage.get_change_number('some_segment') == 456


class InMemorySegmentStorageAsyncTests(object):
    """In memory segment storage tests."""

    @pytest.mark.asyncio
    async def test_segment_storage_retrieval(self, mocker):
        """Test storing and retrieving segments."""
        storage = InMemorySegmentStorageAsync()
        segment = mocker.Mock(spec=Segment)
        name_property = mocker.PropertyMock()
        name_property.return_value = 'some_segment'
        type(segment).name = name_property

        await storage.put(segment)
        assert await storage.get('some_segment') == segment
        assert await storage.get('nonexistant-segment') is None

    @pytest.mark.asyncio
    async def test_change_number(self, mocker):
        """Test storing and retrieving segment changeNumber."""
        storage = InMemorySegmentStorageAsync()
        await storage.set_change_number('some_segment', 123)
        # Change number is not updated if segment doesn't exist
        assert await storage.get_change_number('some_segment') is None
        assert await storage.get_change_number('nonexistant-segment') is None

        # Change number is updated if segment does exist.
        storage = InMemorySegmentStorageAsync()
        segment = mocker.Mock(spec=Segment)
        name_property = mocker.PropertyMock()
        name_property.return_value = 'some_segment'
        type(segment).name = name_property
        await storage.put(segment)
        await storage.set_change_number('some_segment', 123)
        assert await storage.get_change_number('some_segment') == 123

    @pytest.mark.asyncio
    async def test_segment_contains(self, mocker):
        """Test using storage to determine whether a key belongs to a segment."""
        storage = InMemorySegmentStorageAsync()
        segment = mocker.Mock(spec=Segment)
        name_property = mocker.PropertyMock()
        name_property.return_value = 'some_segment'
        type(segment).name = name_property
        await storage.put(segment)

        await storage.segment_contains('some_segment', 'abc')
        assert segment.contains.mock_calls[0] == mocker.call('abc')

    @pytest.mark.asyncio
    async def test_segment_update(self):
        """Test updating a segment."""
        storage = InMemorySegmentStorageAsync()
        segment = Segment('some_segment', ['key1', 'key2', 'key3'], 123)
        await storage.put(segment)
        assert await storage.get('some_segment') == segment

        await storage.update('some_segment', ['key4', 'key5'], ['key2', 'key3'], 456)
        assert await storage.segment_contains('some_segment', 'key1')
        assert await storage.segment_contains('some_segment', 'key4')
        assert await storage.segment_contains('some_segment', 'key5')
        assert not await storage.segment_contains('some_segment', 'key2')
        assert not await storage.segment_contains('some_segment', 'key3')
        assert await storage.get_change_number('some_segment') == 456


class InMemoryImpressionsStorageTests(object):
    """InMemory impressions storage test cases."""

    def test_push_pop_impressions(self, mocker):
        """Test pushing and retrieving impressions."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        storage = InMemoryImpressionStorage(100, telemetry_runtime_producer)
        storage.put([Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])
        storage.put([Impression('key2', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])
        storage.put([Impression('key3', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])
        assert(telemetry_storage._counters._impressions_queued == 3)

        # Assert impressions are retrieved in the same order they are inserted.
        assert storage.pop_many(1) == [
            Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert storage.pop_many(1) == [
            Impression('key2', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert storage.pop_many(1) == [
            Impression('key3', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]

        # Assert inserting multiple impressions at once works and maintains order.
        impressions = [
            Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654),
            Impression('key2', 'feature1', 'on', 'l1', 123456, 'b1', 321654),
            Impression('key3', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert storage.put(impressions)

        # Assert impressions are retrieved in the same order they are inserted.
        assert storage.pop_many(1) == [
            Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert storage.pop_many(1) == [
            Impression('key2', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert storage.pop_many(1) == [
            Impression('key3', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]

    def test_queue_full_hook(self, mocker):
        """Test queue_full_hook is executed when the queue is full."""
        storage = InMemoryImpressionStorage(100, mocker.Mock())
        queue_full_hook = mocker.Mock()
        storage.set_queue_full_hook(queue_full_hook)
        impressions = [
            Impression('key%d' % i, 'feature1', 'on', 'l1', 123456, 'b1', 321654)
            for i in range(0, 101)
        ]
        storage.put(impressions)
        assert queue_full_hook.mock_calls == mocker.call()

    def test_clear(self, mocker):
        """Test clear method."""
        storage = InMemoryImpressionStorage(100, mocker.Mock())
        storage.put([Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])

        assert storage._impressions.qsize() == 1
        storage.clear()
        assert storage._impressions.qsize() == 0

    def test_impressions_dropped(self, mocker):
        """Test pushing and retrieving impressions."""
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        storage = InMemoryImpressionStorage(2, telemetry_runtime_producer)
        storage.put([Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])
        storage.put([Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])
        storage.put([Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])
        assert(telemetry_storage._counters._impressions_dropped == 1)
        assert(telemetry_storage._counters._impressions_queued == 2)


class InMemoryImpressionsStorageAsyncTests(object):
    """InMemory impressions async storage test cases."""

    @pytest.mark.asyncio
    async def test_push_pop_impressions(self, mocker):
        """Test pushing and retrieving impressions."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        storage = InMemoryImpressionStorageAsync(100, telemetry_runtime_producer)
        await storage.put([Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])
        await storage.put([Impression('key2', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])
        await storage.put([Impression('key3', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])
        assert(telemetry_storage._counters._impressions_queued == 3)

        # Assert impressions are retrieved in the same order they are inserted.
        assert await storage.pop_many(1) == [
            Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert await storage.pop_many(1) == [
            Impression('key2', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert await storage.pop_many(1) == [
            Impression('key3', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]

        # Assert inserting multiple impressions at once works and maintains order.
        impressions = [
            Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654),
            Impression('key2', 'feature1', 'on', 'l1', 123456, 'b1', 321654),
            Impression('key3', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert await storage.put(impressions)

        # Assert impressions are retrieved in the same order they are inserted.
        assert await storage.pop_many(1) == [
            Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert await storage.pop_many(1) == [
            Impression('key2', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert await storage.pop_many(1) == [
            Impression('key3', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]

    @pytest.mark.asyncio
    async def test_queue_full_hook(self, mocker):
        """Test queue_full_hook is executed when the queue is full."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        storage = InMemoryImpressionStorageAsync(100, telemetry_runtime_producer)
        self.hook_called = False
        async def queue_full_hook():
            self.hook_called = True

        storage.set_queue_full_hook(queue_full_hook)
        impressions = [
            Impression('key%d' % i, 'feature1', 'on', 'l1', 123456, 'b1', 321654)
            for i in range(0, 101)
        ]
        await storage.put(impressions)
        await queue_full_hook()
        assert self.hook_called == True

    @pytest.mark.asyncio
    async def test_clear(self, mocker):
        """Test clear method."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        storage = InMemoryImpressionStorageAsync(100, telemetry_runtime_producer)
        await storage.put([Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])
        assert storage._impressions.qsize() == 1
        await storage.clear()
        assert storage._impressions.qsize() == 0

    @pytest.mark.asyncio
    async def test_impressions_dropped(self, mocker):
        """Test pushing and retrieving impressions."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        storage = InMemoryImpressionStorageAsync(2, telemetry_runtime_producer)
        await storage.put([Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])
        await storage.put([Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])
        await storage.put([Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])
        assert(telemetry_storage._counters._impressions_dropped == 1)
        assert(telemetry_storage._counters._impressions_queued == 2)


class InMemoryEventsStorageTests(object):
    """InMemory events storage test cases."""

    def test_push_pop_events(self, mocker):
        """Test pushing and retrieving events."""
        storage = InMemoryEventStorage(100, mocker.Mock())
        storage.put([EventWrapper(
            event=Event('key1', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])
        storage.put([EventWrapper(
            event=Event('key2', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])
        storage.put([EventWrapper(
            event=Event('key3', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])

        # Assert impressions are retrieved in the same order they are inserted.
        assert storage.pop_many(1) == [Event('key1', 'user', 'purchase', 3.5, 123456, None)]
        assert storage.pop_many(1) == [Event('key2', 'user', 'purchase', 3.5, 123456, None)]
        assert storage.pop_many(1) == [Event('key3', 'user', 'purchase', 3.5, 123456, None)]

        # Assert inserting multiple impressions at once works and maintains order.
        events = [
            EventWrapper(
                event=Event('key1', 'user', 'purchase', 3.5, 123456, None),
                size=1024,
            ),
            EventWrapper(
                event=Event('key2', 'user', 'purchase', 3.5, 123456, None),
                size=1024,
            ),
            EventWrapper(
                event=Event('key3', 'user', 'purchase', 3.5, 123456, None),
                size=1024,
            ),
        ]
        assert storage.put(events)

        # Assert events are retrieved in the same order they are inserted.
        assert storage.pop_many(1) == [Event('key1', 'user', 'purchase', 3.5, 123456, None)]
        assert storage.pop_many(1) == [Event('key2', 'user', 'purchase', 3.5, 123456, None)]
        assert storage.pop_many(1) == [Event('key3', 'user', 'purchase', 3.5, 123456, None)]

    def test_queue_full_hook(self, mocker):
        """Test queue_full_hook is executed when the queue is full."""
        storage = InMemoryEventStorage(100, mocker.Mock())
        queue_full_hook = mocker.Mock()
        storage.set_queue_full_hook(queue_full_hook)
        events = [EventWrapper(event=Event('key%d' % i, 'user', 'purchase', 12.5, 321654, None), size=1024) for i in range(0, 101)]
        storage.put(events)
        assert queue_full_hook.mock_calls == [mocker.call()]

    def test_queue_full_hook_properties(self, mocker):
        """Test queue_full_hook is executed when the queue is full regarding properties."""
        storage = InMemoryEventStorage(200, mocker.Mock())
        queue_full_hook = mocker.Mock()
        storage.set_queue_full_hook(queue_full_hook)
        events = [EventWrapper(event=Event('key%d' % i, 'user', 'purchase', 12.5, 1, None),  size=32768) for i in range(160)]
        storage.put(events)
        assert queue_full_hook.mock_calls == [mocker.call()]

    def test_clear(self, mocker):
        """Test clear method."""
        storage = InMemoryEventStorage(100, mocker.Mock())
        storage.put([EventWrapper(
            event=Event('key1', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])

        assert storage._events.qsize() == 1
        storage.clear()
        assert storage._events.qsize() == 0

    def test_event_telemetry(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        storage = InMemoryEventStorage(2, telemetry_runtime_producer)
        storage.put([EventWrapper(
            event=Event('key1', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])
        storage.put([EventWrapper(
            event=Event('key1', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])
        storage.put([EventWrapper(
            event=Event('key1', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])
        assert(telemetry_storage._counters._events_dropped == 1)
        assert(telemetry_storage._counters._events_queued == 2)


class InMemoryEventsStorageAsyncTests(object):
    """InMemory events async storage test cases."""

    @pytest.mark.asyncio
    async def test_push_pop_events(self, mocker):
        """Test pushing and retrieving events."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        storage = InMemoryEventStorageAsync(100, telemetry_runtime_producer)
        await storage.put([EventWrapper(
            event=Event('key1', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])
        await storage.put([EventWrapper(
            event=Event('key2', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])
        await storage.put([EventWrapper(
            event=Event('key3', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])

        # Assert impressions are retrieved in the same order they are inserted.
        assert await storage.pop_many(1) == [Event('key1', 'user', 'purchase', 3.5, 123456, None)]
        assert await storage.pop_many(1) == [Event('key2', 'user', 'purchase', 3.5, 123456, None)]
        assert await storage.pop_many(1) == [Event('key3', 'user', 'purchase', 3.5, 123456, None)]

        # Assert inserting multiple impressions at once works and maintains order.
        events = [
            EventWrapper(
                event=Event('key1', 'user', 'purchase', 3.5, 123456, None),
                size=1024,
            ),
            EventWrapper(
                event=Event('key2', 'user', 'purchase', 3.5, 123456, None),
                size=1024,
            ),
            EventWrapper(
                event=Event('key3', 'user', 'purchase', 3.5, 123456, None),
                size=1024,
            ),
        ]
        assert await storage.put(events)

        # Assert events are retrieved in the same order they are inserted.
        assert await storage.pop_many(1) == [Event('key1', 'user', 'purchase', 3.5, 123456, None)]
        assert await storage.pop_many(1) == [Event('key2', 'user', 'purchase', 3.5, 123456, None)]
        assert await storage.pop_many(1) == [Event('key3', 'user', 'purchase', 3.5, 123456, None)]

    @pytest.mark.asyncio
    async def test_queue_full_hook(self, mocker):
        """Test queue_full_hook is executed when the queue is full."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        storage = InMemoryEventStorageAsync(100, telemetry_runtime_producer)
        self.called = False
        async def queue_full_hook():
            self.called = True

        storage.set_queue_full_hook(queue_full_hook)
        events = [EventWrapper(event=Event('key%d' % i, 'user', 'purchase', 12.5, 321654, None), size=1024) for i in range(0, 101)]
        await storage.put(events)
        assert self.called == True

    @pytest.mark.asyncio
    async def test_queue_full_hook_properties(self, mocker):
        """Test queue_full_hook is executed when the queue is full regarding properties."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        storage = InMemoryEventStorageAsync(200, telemetry_runtime_producer)
        self.called = False
        async def queue_full_hook():
            self.called = True
        storage.set_queue_full_hook(queue_full_hook)
        events = [EventWrapper(event=Event('key%d' % i, 'user', 'purchase', 12.5, 1, None),  size=32768) for i in range(160)]
        await storage.put(events)
        assert self.called == True

    @pytest.mark.asyncio
    async def test_clear(self, mocker):
        """Test clear method."""
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        storage = InMemoryEventStorageAsync(100, telemetry_runtime_producer)
        await storage.put([EventWrapper(
            event=Event('key1', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])

        assert storage._events.qsize() == 1
        await storage.clear()
        assert storage._events.qsize() == 0

    @pytest.mark.asyncio
    async def test_event_telemetry(self, mocker):
        telemetry_storage = await InMemoryTelemetryStorageAsync.create()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        storage = InMemoryEventStorageAsync(2, telemetry_runtime_producer)
        await storage.put([EventWrapper(
            event=Event('key1', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])
        await storage.put([EventWrapper(
            event=Event('key1', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])
        await storage.put([EventWrapper(
            event=Event('key1', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])
        assert(telemetry_storage._counters._events_dropped == 1)
        assert(telemetry_storage._counters._events_queued == 2)


class InMemoryTelemetryStorageTests(object):
    """InMemory telemetry storage test cases."""

    def test_resets(self):
        storage = InMemoryTelemetryStorage()

        assert(storage._counters._impressions_queued == 0)
        assert(storage._counters._impressions_deduped == 0)
        assert(storage._counters._impressions_dropped == 0)
        assert(storage._counters._events_dropped == 0)
        assert(storage._counters._events_queued == 0)
        assert(storage._counters._auth_rejections == 0)
        assert(storage._counters._token_refreshes == 0)

        assert(storage._method_exceptions.pop_all() == {'methodExceptions': {'treatment': 0, 'treatments': 0, 'treatment_with_config': 0, 'treatments_with_config': 0, 'treatments_by_flag_set': 0, 'treatments_by_flag_sets': 0, 'treatments_with_config_by_flag_set': 0, 'treatments_with_config_by_flag_sets': 0, 'track': 0}})
        assert(storage._last_synchronization.get_all() == {'lastSynchronizations': {'split': 0, 'segment': 0, 'impression': 0, 'impressionCount': 0, 'event': 0, 'telemetry': 0, 'token': 0}})
        assert(storage._http_sync_errors.pop_all() == {'httpErrors': {'split': {}, 'segment': {}, 'impression': {}, 'impressionCount': {}, 'event': {}, 'telemetry': {}, 'token': {}}})
        assert(storage._tel_config.get_stats() == {
                'bT':0,
                'nR':0,
                'tR': 0,
                'oM': None,
                'sT': None,
                'sE': None,
                'rR': {'sp': 0, 'se': 0, 'im': 0, 'ev': 0, 'te': 0},
                'uO': {'s': False, 'e': False, 'a': False, 'st': False, 't': False},
                'iQ': 0,
                'eQ': 0,
                'iM': None,
                'iL': False,
                'hp': None,
                'aF': 0,
                'rF': 0,
                'fsT': 0,
                'fsI': 0
            })
        assert(storage._streaming_events.pop_streaming_events() == {'streamingEvents': []})
        assert(storage._tags == [])

        assert(storage._method_latencies.pop_all() == {'methodLatencies': {'treatment': [0] * 23, 'treatments': [0] * 23, 'treatment_with_config': [0] * 23, 'treatments_with_config': [0] * 23, 'treatments_by_flag_set': [0] * 23, 'treatments_by_flag_sets': [0] * 23, 'treatments_with_config_by_flag_set': [0] * 23, 'treatments_with_config_by_flag_sets': [0] * 23, 'track': [0] * 23}})
        assert(storage._http_latencies.pop_all() == {'httpLatencies': {'split': [0] * 23, 'segment': [0] * 23, 'impression': [0] * 23, 'impressionCount': [0] * 23, 'event': [0] * 23, 'telemetry': [0] * 23, 'token': [0] * 23}})

    def test_record_config(self):
        storage = InMemoryTelemetryStorage()
        config = {'operationMode': 'standalone',
                  'streamingEnabled': True,
                  'impressionsQueueSize': 100,
                  'eventsQueueSize': 200,
                  'impressionsMode': 'DEBUG',''
                  'impressionListener': None,
                  'featuresRefreshRate': 30,
                  'segmentsRefreshRate': 30,
                  'impressionsRefreshRate': 60,
                  'eventsPushRate': 60,
                  'metricsRefreshRate': 10,
                  'storageType': None
                  }
        storage.record_config(config, {}, 2, 1)
        storage.record_active_and_redundant_factories(1, 0)
        assert(storage._tel_config.get_stats() == {'oM': 0,
            'sT': storage._tel_config._get_storage_type(config['operationMode'], config['storageType']),
            'sE': config['streamingEnabled'],
            'rR': {'sp': 30, 'se': 30, 'im': 60, 'ev': 60, 'te': 10},
            'uO':  {'s': False, 'e': False, 'a': False, 'st': False, 't': False},
            'iQ': config['impressionsQueueSize'],
            'eQ': config['eventsQueueSize'],
            'iM': storage._tel_config._get_impressions_mode(config['impressionsMode']),
            'iL': True if config['impressionListener'] is not None else False,
            'hp': storage._tel_config._check_if_proxy_detected(),
            'bT': 0,
            'tR': 0,
            'nR': 0,
            'aF': 1,
            'rF': 0,
            'fsT': 2,
            'fsI': 1}
            )

    def test_record_counters(self):
        storage = InMemoryTelemetryStorage()

        storage.record_ready_time(10)
        assert(storage._tel_config._time_until_ready == 10)

        storage.add_tag('tag')
        assert('tag' in storage._tags)
        [storage.add_tag('tag') for i in range(1, 25)]
        assert(len(storage._tags) == 10)

        storage.record_bur_time_out()
        storage.record_bur_time_out()
        assert(storage._tel_config.get_bur_time_outs() == 2)

        storage.record_not_ready_usage()
        storage.record_not_ready_usage()
        assert(storage._tel_config.get_non_ready_usage() == 2)

        storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT)
        assert(storage._method_exceptions._treatment == 1)

        storage.record_impression_stats(ModelTelemetry.CounterConstants.IMPRESSIONS_QUEUED, 5)
        assert(storage._counters.get_counter_stats(ModelTelemetry.CounterConstants.IMPRESSIONS_QUEUED) == 5)

        storage.record_event_stats(ModelTelemetry.CounterConstants.EVENTS_DROPPED, 6)
        assert(storage._counters.get_counter_stats(ModelTelemetry.CounterConstants.EVENTS_DROPPED) == 6)

        storage.record_successful_sync(ModelTelemetry.HTTPExceptionsAndLatencies.SEGMENT, 10)
        assert(storage._last_synchronization._segment == 10)

        storage.record_sync_error(ModelTelemetry.HTTPExceptionsAndLatencies.SEGMENT, '500')
        assert(storage._http_sync_errors._segment['500'] == 1)

        storage.record_auth_rejections()
        storage.record_auth_rejections()
        assert(storage._counters.pop_auth_rejections() == 2)

        storage.record_token_refreshes()
        storage.record_token_refreshes()
        assert(storage._counters.pop_token_refreshes() == 2)

        storage.record_streaming_event((ModelTelemetry.StreamingEventTypes.CONNECTION_ESTABLISHED, 'split', 1234))
        assert(storage._streaming_events.pop_streaming_events() == {'streamingEvents': [{'e': ModelTelemetry.StreamingEventTypes.CONNECTION_ESTABLISHED.value, 'd': 'split', 't': 1234}]})
        [storage.record_streaming_event((ModelTelemetry.StreamingEventTypes.CONNECTION_ESTABLISHED, 'split', 1234)) for i in range(1, 25)]
        assert(len(storage._streaming_events._streaming_events) == 20)

        storage.record_session_length(20)
        assert(storage._counters.get_session_length() == 20)

    def test_record_latencies(self):
        storage = InMemoryTelemetryStorage()

        for method in ModelTelemetry.MethodExceptionsAndLatencies:
            if self._get_method_latency(method, storage) == None:
                continue
            storage.record_latency(method, 50)
            assert(self._get_method_latency(method, storage)[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            storage.record_latency(method, 50000000)
            assert(self._get_method_latency(method, storage)[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            for j in range(10):
                latency = random.randint(1001, 4987885)
                current_count = self._get_method_latency(method, storage)[ModelTelemetry.get_latency_bucket_index(latency)]
                [storage.record_latency(method, latency) for i in range(2)]
                assert(self._get_method_latency(method, storage)[ModelTelemetry.get_latency_bucket_index(latency)] == 2 + current_count)

        for resource in ModelTelemetry.HTTPExceptionsAndLatencies:
            if self._get_http_latency(resource, storage) == None:
                continue
            storage.record_sync_latency(resource, 50)
            assert(self._get_http_latency(resource, storage)[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            storage.record_sync_latency(resource, 50000000)
            assert(self._get_http_latency(resource, storage)[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            for j in range(10):
                latency = random.randint(1001, 4987885)
                current_count = self._get_http_latency(resource, storage)[ModelTelemetry.get_latency_bucket_index(latency)]
                [storage.record_sync_latency(resource, latency) for i in range(2)]
                assert(self._get_http_latency(resource, storage)[ModelTelemetry.get_latency_bucket_index(latency)] == 2 + current_count)

    def _get_method_latency(self, resource, storage):
        if resource == ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT:
            return storage._method_latencies._treatment
        elif resource == ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS:
            return storage._method_latencies._treatments
        elif resource == ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG:
            return storage._method_latencies._treatment_with_config
        elif resource == ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG:
            return storage._method_latencies._treatments_with_config
        elif resource == ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET:
            return storage._method_latencies._treatments_by_flag_set
        elif resource == ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS:
            return storage._method_latencies._treatments_by_flag_sets
        elif resource == ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET:
            return storage._method_latencies._treatments_with_config_by_flag_set
        elif resource == ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS:
            return storage._method_latencies._treatments_with_config_by_flag_sets
        elif resource == ModelTelemetry.MethodExceptionsAndLatencies.TRACK:
            return storage._method_latencies._track
        else:
            return

    def _get_http_latency(self, resource, storage):
        if resource == ModelTelemetry.HTTPExceptionsAndLatencies.SPLIT:
            return storage._http_latencies._split
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.SEGMENT:
            return storage._http_latencies._segment
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION:
            return storage._http_latencies._impression
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION_COUNT:
            return storage._http_latencies._impression_count
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.EVENT:
            return storage._http_latencies._event
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.TELEMETRY:
            return storage._http_latencies._telemetry
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.TOKEN:
            return storage._http_latencies._token
        else:
            return

    def test_pop_counters(self):
        storage = InMemoryTelemetryStorage()

        [storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT) for i in range(2)]
        storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS)
        storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG)
        [storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG) for i in range(5)]
        [storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET) for i in range(3)]
        [storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS) for i in range(10)]
        [storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET)  for i in range(7)]
        [storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS) for i in range(6)]
        [storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TRACK) for i in range(3)]
        exceptions = storage.pop_exceptions()
        assert(storage._method_exceptions._treatment == 0)
        assert(storage._method_exceptions._treatments == 0)
        assert(storage._method_exceptions._treatment_with_config == 0)
        assert(storage._method_exceptions._treatments_with_config == 0)
        assert(storage._method_exceptions._treatments_by_flag_set == 0)
        assert(storage._method_exceptions._treatments_by_flag_sets == 0)
        assert(storage._method_exceptions._track == 0)
        assert(storage._method_exceptions._treatments_with_config_by_flag_set == 0)
        assert(storage._method_exceptions._treatments_with_config_by_flag_sets == 0)
        assert(exceptions == {'methodExceptions': {'treatment': 2, 'treatments': 1, 'treatment_with_config': 1, 'treatments_with_config': 5, 'treatments_by_flag_set': 3, 'treatments_by_flag_sets': 10, 'treatments_with_config_by_flag_set': 7, 'treatments_with_config_by_flag_sets': 6, 'track': 3}})

        storage.add_tag('tag1')
        storage.add_tag('tag2')
        tags = storage.pop_tags()
        assert(storage._tags == [])
        assert(tags == ['tag1', 'tag2'])

        [storage.record_sync_error(ModelTelemetry.HTTPExceptionsAndLatencies.SEGMENT, str(i)) for i in [500, 501, 502]]
        [storage.record_sync_error(ModelTelemetry.HTTPExceptionsAndLatencies.SPLIT, str(i)) for i in [400, 401, 402]]
        storage.record_sync_error(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION, '502')
        [storage.record_sync_error(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION_COUNT, str(i)) for i in [501, 502]]
        storage.record_sync_error(ModelTelemetry.HTTPExceptionsAndLatencies.EVENT, '501')
        storage.record_sync_error(ModelTelemetry.HTTPExceptionsAndLatencies.TELEMETRY, '505')
        [storage.record_sync_error(ModelTelemetry.HTTPExceptionsAndLatencies.TOKEN, '502') for i in range(5)]
        http_errors = storage.pop_http_errors()
        assert(http_errors == {'httpErrors': {'split': {'400': 1, '401': 1, '402': 1}, 'segment': {'500': 1, '501': 1, '502': 1},
                                        'impression': {'502': 1}, 'impressionCount': {'501': 1, '502': 1},
                                        'event': {'501': 1}, 'telemetry': {'505': 1}, 'token': {'502': 5}}})
        assert(storage._http_sync_errors._split == {})
        assert(storage._http_sync_errors._segment == {})
        assert(storage._http_sync_errors._impression == {})
        assert(storage._http_sync_errors._impression_count == {})
        assert(storage._http_sync_errors._event == {})
        assert(storage._http_sync_errors._telemetry == {})

        storage.record_auth_rejections()
        storage.record_auth_rejections()
        auth_rejections = storage.pop_auth_rejections()
        assert(storage._counters._auth_rejections == 0)
        assert(auth_rejections == 2)

        storage.record_token_refreshes()
        storage.record_token_refreshes()
        token_refreshes = storage.pop_token_refreshes()
        assert(storage._counters._token_refreshes == 0)
        assert(token_refreshes == 2)

        storage.record_streaming_event((ModelTelemetry.StreamingEventTypes.CONNECTION_ESTABLISHED, 'split', 1234))
        storage.record_streaming_event((ModelTelemetry.StreamingEventTypes.OCCUPANCY_PRI, 'split', 1234))
        streaming_events = storage.pop_streaming_events()
        assert(storage._streaming_events._streaming_events == [])
        assert(streaming_events == {'streamingEvents': [{'e': ModelTelemetry.StreamingEventTypes.CONNECTION_ESTABLISHED.value, 'd': 'split', 't': 1234},
                                    {'e': ModelTelemetry.StreamingEventTypes.OCCUPANCY_PRI.value, 'd': 'split', 't': 1234}]})

    def test_pop_latencies(self):
        storage = InMemoryTelemetryStorage()

        [storage.record_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT, i) for i in [5, 10, 10, 10]]
        [storage.record_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS, i) for i in [7, 10, 14, 13]]
        [storage.record_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG, i) for i in [200]]
        [storage.record_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG, i) for i in [50, 40]]
        [storage.record_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET, i) for i in [15, 20]]
        [storage.record_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS, i) for i in [14, 25]]
        [storage.record_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET, i) for i in [100]]
        [storage.record_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS, i) for i in [50, 20]]
        [storage.record_latency(ModelTelemetry.MethodExceptionsAndLatencies.TRACK, i) for i in [1, 10, 100]]
        latencies = storage.pop_latencies()

        assert(storage._method_latencies._treatment == [0] * 23)
        assert(storage._method_latencies._treatments == [0] * 23)
        assert(storage._method_latencies._treatment_with_config == [0] * 23)
        assert(storage._method_latencies._treatments_with_config == [0] * 23)
        assert(storage._method_latencies._treatments_by_flag_set == [0] * 23)
        assert(storage._method_latencies._treatments_by_flag_sets == [0] * 23)
        assert(storage._method_latencies._treatments_with_config_by_flag_set == [0] * 23)
        assert(storage._method_latencies._treatments_with_config_by_flag_sets == [0] * 23)
        assert(storage._method_latencies._track == [0] * 23)
        assert(latencies ==  {'methodLatencies': {
                    'treatment': [4] + [0] * 22,
                    'treatments': [4] + [0] * 22,
                    'treatment_with_config': [1] + [0] * 22,
                    'treatments_with_config': [2] + [0] * 22,
                    'treatments_by_flag_set': [2] + [0] * 22,
                    'treatments_by_flag_sets': [2] + [0] * 22,
                    'treatments_with_config_by_flag_set': [1] + [0] * 22,
                    'treatments_with_config_by_flag_sets': [2] + [0] * 22,
                    'track': [3] + [0] * 22}})

        [storage.record_sync_latency(ModelTelemetry.HTTPExceptionsAndLatencies.SPLIT, i) for i in [50, 10, 20, 40]]
        [storage.record_sync_latency(ModelTelemetry.HTTPExceptionsAndLatencies.SEGMENT, i) for i in [70, 100, 40, 30]]
        [storage.record_sync_latency(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION, i) for i in [10, 20]]
        [storage.record_sync_latency(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION_COUNT, i) for i in [5, 10]]
        [storage.record_sync_latency(ModelTelemetry.HTTPExceptionsAndLatencies.EVENT, i) for i in [50, 40]]
        [storage.record_sync_latency(ModelTelemetry.HTTPExceptionsAndLatencies.TELEMETRY, i) for i in [100, 50, 160]]
        [storage.record_sync_latency(ModelTelemetry.HTTPExceptionsAndLatencies.TOKEN, i) for i in [10, 15, 100]]
        sync_latency = storage.pop_http_latencies()

        assert(storage._http_latencies._split == [0] * 23)
        assert(storage._http_latencies._segment == [0] * 23)
        assert(storage._http_latencies._impression == [0] * 23)
        assert(storage._http_latencies._impression_count == [0] * 23)
        assert(storage._http_latencies._telemetry == [0] * 23)
        assert(storage._http_latencies._token == [0] * 23)
        assert(sync_latency == {'httpLatencies': {'split': [4] + [0] * 22, 'segment': [4] + [0] * 22,
                                'impression': [2] + [0] * 22, 'impressionCount': [2] + [0] * 22, 'event': [2] + [0] * 22,
                                'telemetry': [3] + [0] * 22, 'token': [3] + [0] * 22}})


class InMemoryTelemetryStorageAsyncTests(object):
    """InMemory telemetry async storage test cases."""

    @pytest.mark.asyncio
    async def test_resets(self):
        storage = await InMemoryTelemetryStorageAsync.create()

        assert(storage._counters._impressions_queued == 0)
        assert(storage._counters._impressions_deduped == 0)
        assert(storage._counters._impressions_dropped == 0)
        assert(storage._counters._events_dropped == 0)
        assert(storage._counters._events_queued == 0)
        assert(storage._counters._auth_rejections == 0)
        assert(storage._counters._token_refreshes == 0)

        assert(await storage._method_exceptions.pop_all() == {'methodExceptions': {'treatment': 0, 'treatments': 0, 'treatment_with_config': 0, 'treatments_with_config': 0, 'treatments_by_flag_set': 0, 'treatments_by_flag_sets': 0, 'treatments_with_config_by_flag_set': 0, 'treatments_with_config_by_flag_sets': 0, 'track': 0}})
        assert(await storage._last_synchronization.get_all() == {'lastSynchronizations': {'split': 0, 'segment': 0, 'impression': 0, 'impressionCount': 0, 'event': 0, 'telemetry': 0, 'token': 0}})
        assert(await storage._http_sync_errors.pop_all() == {'httpErrors': {'split': {}, 'segment': {}, 'impression': {}, 'impressionCount': {}, 'event': {}, 'telemetry': {}, 'token': {}}})
        assert(await storage._tel_config.get_stats() == {
                'bT':0,
                'nR':0,
                'tR': 0,
                'oM': None,
                'sT': None,
                'sE': None,
                'rR': {'sp': 0, 'se': 0, 'im': 0, 'ev': 0, 'te': 0},
                'uO': {'s': False, 'e': False, 'a': False, 'st': False, 't': False},
                'iQ': 0,
                'eQ': 0,
                'iM': None,
                'iL': False,
                'hp': None,
                'aF': 0,
                'rF': 0,
                'fsT': 0,
                'fsI': 0
            })
        assert(await storage._streaming_events.pop_streaming_events() == {'streamingEvents': []})
        assert(storage._tags == [])

        assert(await storage._method_latencies.pop_all() == {'methodLatencies': {'treatment': [0] * 23, 'treatments': [0] * 23, 'treatment_with_config': [0] * 23, 'treatments_with_config': [0] * 23, 'treatments_by_flag_set': [0] * 23, 'treatments_by_flag_sets': [0] * 23, 'treatments_with_config_by_flag_set': [0] * 23, 'treatments_with_config_by_flag_sets': [0] * 23, 'track': [0] * 23}})
        assert(await storage._http_latencies.pop_all() == {'httpLatencies': {'split': [0] * 23, 'segment': [0] * 23, 'impression': [0] * 23, 'impressionCount': [0] * 23, 'event': [0] * 23, 'telemetry': [0] * 23, 'token': [0] * 23}})

    @pytest.mark.asyncio
    async def test_record_config(self):
        storage = await InMemoryTelemetryStorageAsync.create()
        config = {'operationMode': 'standalone',
                  'streamingEnabled': True,
                  'impressionsQueueSize': 100,
                  'eventsQueueSize': 200,
                  'impressionsMode': 'DEBUG',''
                  'impressionListener': None,
                  'featuresRefreshRate': 30,
                  'segmentsRefreshRate': 30,
                  'impressionsRefreshRate': 60,
                  'eventsPushRate': 60,
                  'metricsRefreshRate': 10,
                  'storageType': None
                  }
        await storage.record_config(config, {}, 2, 1)
        await storage.record_active_and_redundant_factories(1, 0)
        assert(await storage._tel_config.get_stats() == {'oM': 0,
            'sT': storage._tel_config._get_storage_type(config['operationMode'], config['storageType']),
            'sE': config['streamingEnabled'],
            'rR': {'sp': 30, 'se': 30, 'im': 60, 'ev': 60, 'te': 10},
            'uO':  {'s': False, 'e': False, 'a': False, 'st': False, 't': False},
            'iQ': config['impressionsQueueSize'],
            'eQ': config['eventsQueueSize'],
            'iM': storage._tel_config._get_impressions_mode(config['impressionsMode']),
            'iL': True if config['impressionListener'] is not None else False,
            'hp': storage._tel_config._check_if_proxy_detected(),
            'bT': 0,
            'tR': 0,
            'nR': 0,
            'aF': 1,
            'rF': 0,
            'fsT': 2,
            'fsI': 1}
            )

    @pytest.mark.asyncio
    async def test_record_counters(self):
        storage = await InMemoryTelemetryStorageAsync.create()

        await storage.record_ready_time(10)
        assert(storage._tel_config._time_until_ready == 10)

        await storage.add_tag('tag')
        assert('tag' in storage._tags)
        [await storage.add_tag('tag') for i in range(1, 25)]
        assert(len(storage._tags) == 10)

        await storage.record_bur_time_out()
        await storage.record_bur_time_out()
        assert(await storage._tel_config.get_bur_time_outs() == 2)

        await storage.record_not_ready_usage()
        await storage.record_not_ready_usage()
        assert(await storage._tel_config.get_non_ready_usage() == 2)

        await storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT)
        assert(storage._method_exceptions._treatment == 1)

        await storage.record_impression_stats(ModelTelemetry.CounterConstants.IMPRESSIONS_QUEUED, 5)
        assert(await storage._counters.get_counter_stats(ModelTelemetry.CounterConstants.IMPRESSIONS_QUEUED) == 5)

        await storage.record_event_stats(ModelTelemetry.CounterConstants.EVENTS_DROPPED, 6)
        assert(await storage._counters.get_counter_stats(ModelTelemetry.CounterConstants.EVENTS_DROPPED) == 6)

        await storage.record_successful_sync(ModelTelemetry.HTTPExceptionsAndLatencies.SEGMENT, 10)
        assert(storage._last_synchronization._segment == 10)

        await storage.record_sync_error(ModelTelemetry.HTTPExceptionsAndLatencies.SEGMENT, '500')
        assert(storage._http_sync_errors._segment['500'] == 1)

        await storage.record_auth_rejections()
        await storage.record_auth_rejections()
        assert(await storage._counters.pop_auth_rejections() == 2)

        await storage.record_token_refreshes()
        await storage.record_token_refreshes()
        assert(await storage._counters.pop_token_refreshes() == 2)

        await storage.record_streaming_event((ModelTelemetry.StreamingEventTypes.CONNECTION_ESTABLISHED, 'split', 1234))
        assert(await storage._streaming_events.pop_streaming_events() == {'streamingEvents': [{'e': ModelTelemetry.StreamingEventTypes.CONNECTION_ESTABLISHED.value, 'd': 'split', 't': 1234}]})
        [await storage.record_streaming_event((ModelTelemetry.StreamingEventTypes.CONNECTION_ESTABLISHED, 'split', 1234)) for i in range(1, 25)]
        assert(len(storage._streaming_events._streaming_events) == 20)

        await storage.record_session_length(20)
        assert(await storage._counters.get_session_length() == 20)

    @pytest.mark.asyncio
    async def test_record_latencies(self):
        storage = await InMemoryTelemetryStorageAsync.create()

        for method in ModelTelemetry.MethodExceptionsAndLatencies:
            if self._get_method_latency(method, storage) == None:
                continue
            await storage.record_latency(method, 50)
            assert(self._get_method_latency(method, storage)[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            await storage.record_latency(method, 50000000)
            assert(self._get_method_latency(method, storage)[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            for j in range(10):
                latency = random.randint(1001, 4987885)
                current_count = self._get_method_latency(method, storage)[ModelTelemetry.get_latency_bucket_index(latency)]
                [await storage.record_latency(method, latency) for i in range(2)]
                assert(self._get_method_latency(method, storage)[ModelTelemetry.get_latency_bucket_index(latency)] == 2 + current_count)

        for resource in ModelTelemetry.HTTPExceptionsAndLatencies:
            if self._get_http_latency(resource, storage) == None:
                continue
            await storage.record_sync_latency(resource, 50)
            assert(self._get_http_latency(resource, storage)[ModelTelemetry.get_latency_bucket_index(50)] == 1)
            await storage.record_sync_latency(resource, 50000000)
            assert(self._get_http_latency(resource, storage)[ModelTelemetry.get_latency_bucket_index(50000000)] == 1)
            for j in range(10):
                latency = random.randint(1001, 4987885)
                current_count = self._get_http_latency(resource, storage)[ModelTelemetry.get_latency_bucket_index(latency)]
                [await storage.record_sync_latency(resource, latency) for i in range(2)]
                assert(self._get_http_latency(resource, storage)[ModelTelemetry.get_latency_bucket_index(latency)] == 2 + current_count)

    def _get_method_latency(self, resource, storage):
        if resource == ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT:
            return storage._method_latencies._treatment
        elif resource == ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS:
            return storage._method_latencies._treatments
        elif resource == ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG:
            return storage._method_latencies._treatment_with_config
        elif resource == ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG:
            return storage._method_latencies._treatments_with_config
        elif resource == ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET:
            return storage._method_latencies._treatments_by_flag_set
        elif resource == ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS:
            return storage._method_latencies._treatments_by_flag_sets
        elif resource == ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET:
            return storage._method_latencies._treatments_with_config_by_flag_set
        elif resource == ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS:
            return storage._method_latencies._treatments_with_config_by_flag_sets
        elif resource == ModelTelemetry.MethodExceptionsAndLatencies.TRACK:
            return storage._method_latencies._track
        else:
            return

    def _get_http_latency(self, resource, storage):
        if resource == ModelTelemetry.HTTPExceptionsAndLatencies.SPLIT:
            return storage._http_latencies._split
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.SEGMENT:
            return storage._http_latencies._segment
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION:
            return storage._http_latencies._impression
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION_COUNT:
            return storage._http_latencies._impression_count
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.EVENT:
            return storage._http_latencies._event
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.TELEMETRY:
            return storage._http_latencies._telemetry
        elif resource == ModelTelemetry.HTTPExceptionsAndLatencies.TOKEN:
            return storage._http_latencies._token
        else:
            return

    @pytest.mark.asyncio
    async def test_pop_counters(self):
        storage = await InMemoryTelemetryStorageAsync.create()

        [await storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT) for i in range(2)]
        await storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS)
        await storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG)
        [await storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG) for i in range(5)]
        [await storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET) for i in range(3)]
        [await storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS) for i in range(10)]
        [await storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET)  for i in range(7)]
        [await storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS) for i in range(6)]
        [await storage.record_exception(ModelTelemetry.MethodExceptionsAndLatencies.TRACK) for i in range(3)]
        exceptions = await storage.pop_exceptions()
        assert(storage._method_exceptions._treatment == 0)
        assert(storage._method_exceptions._treatments == 0)
        assert(storage._method_exceptions._treatment_with_config == 0)
        assert(storage._method_exceptions._treatments_with_config == 0)
        assert(storage._method_exceptions._treatments_by_flag_set == 0)
        assert(storage._method_exceptions._treatments_by_flag_sets == 0)
        assert(storage._method_exceptions._track == 0)
        assert(storage._method_exceptions._treatments_with_config_by_flag_set == 0)
        assert(storage._method_exceptions._treatments_with_config_by_flag_sets == 0)
        assert(exceptions == {'methodExceptions': {'treatment': 2, 'treatments': 1, 'treatment_with_config': 1, 'treatments_with_config': 5, 'treatments_by_flag_set': 3, 'treatments_by_flag_sets': 10, 'treatments_with_config_by_flag_set': 7, 'treatments_with_config_by_flag_sets': 6, 'track': 3}})

        await storage.add_tag('tag1')
        await storage.add_tag('tag2')
        tags = await storage.pop_tags()
        assert(storage._tags == [])
        assert(tags == ['tag1', 'tag2'])

        [await storage.record_sync_error(ModelTelemetry.HTTPExceptionsAndLatencies.SEGMENT, str(i)) for i in [500, 501, 502]]
        [await storage.record_sync_error(ModelTelemetry.HTTPExceptionsAndLatencies.SPLIT, str(i)) for i in [400, 401, 402]]
        await storage.record_sync_error(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION, '502')
        [await storage.record_sync_error(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION_COUNT, str(i)) for i in [501, 502]]
        await storage.record_sync_error(ModelTelemetry.HTTPExceptionsAndLatencies.EVENT, '501')
        await storage.record_sync_error(ModelTelemetry.HTTPExceptionsAndLatencies.TELEMETRY, '505')
        [await storage.record_sync_error(ModelTelemetry.HTTPExceptionsAndLatencies.TOKEN, '502') for i in range(5)]
        http_errors = await storage.pop_http_errors()
        assert(http_errors == {'httpErrors': {'split': {'400': 1, '401': 1, '402': 1}, 'segment': {'500': 1, '501': 1, '502': 1},
                                        'impression': {'502': 1}, 'impressionCount': {'501': 1, '502': 1},
                                        'event': {'501': 1}, 'telemetry': {'505': 1}, 'token': {'502': 5}}})
        assert(storage._http_sync_errors._split == {})
        assert(storage._http_sync_errors._segment == {})
        assert(storage._http_sync_errors._impression == {})
        assert(storage._http_sync_errors._impression_count == {})
        assert(storage._http_sync_errors._event == {})
        assert(storage._http_sync_errors._telemetry == {})

        await storage.record_auth_rejections()
        await storage.record_auth_rejections()
        auth_rejections = await storage.pop_auth_rejections()
        assert(storage._counters._auth_rejections == 0)
        assert(auth_rejections == 2)

        await storage.record_token_refreshes()
        await storage.record_token_refreshes()
        token_refreshes = await storage.pop_token_refreshes()
        assert(storage._counters._token_refreshes == 0)
        assert(token_refreshes == 2)

        await storage.record_streaming_event((ModelTelemetry.StreamingEventTypes.CONNECTION_ESTABLISHED, 'split', 1234))
        await storage.record_streaming_event((ModelTelemetry.StreamingEventTypes.OCCUPANCY_PRI, 'split', 1234))
        streaming_events = await storage.pop_streaming_events()
        assert(storage._streaming_events._streaming_events == [])
        assert(streaming_events == {'streamingEvents': [{'e': ModelTelemetry.StreamingEventTypes.CONNECTION_ESTABLISHED.value, 'd': 'split', 't': 1234},
                                    {'e': ModelTelemetry.StreamingEventTypes.OCCUPANCY_PRI.value, 'd': 'split', 't': 1234}]})

    @pytest.mark.asyncio
    async def test_pop_latencies(self):
        storage = await InMemoryTelemetryStorageAsync.create()

        [await storage.record_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT, i) for i in [5, 10, 10, 10]]
        [await storage.record_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS, i) for i in [7, 10, 14, 13]]
        [await storage.record_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENT_WITH_CONFIG, i) for i in [200]]
        [await storage.record_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG, i) for i in [50, 40]]
        [await storage.record_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SET, i) for i in [15, 20]]
        [await storage.record_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_BY_FLAG_SETS, i) for i in [14, 25]]
        [await storage.record_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SET, i) for i in [100]]
        [await storage.record_latency(ModelTelemetry.MethodExceptionsAndLatencies.TREATMENTS_WITH_CONFIG_BY_FLAG_SETS, i) for i in [50, 20]]
        [await storage.record_latency(ModelTelemetry.MethodExceptionsAndLatencies.TRACK, i) for i in [1, 10, 100]]
        latencies = await storage.pop_latencies()

        assert(storage._method_latencies._treatment == [0] * 23)
        assert(storage._method_latencies._treatments == [0] * 23)
        assert(storage._method_latencies._treatment_with_config == [0] * 23)
        assert(storage._method_latencies._treatments_with_config == [0] * 23)
        assert(storage._method_latencies._treatments_by_flag_set == [0] * 23)
        assert(storage._method_latencies._treatments_by_flag_sets == [0] * 23)
        assert(storage._method_latencies._treatments_with_config_by_flag_set == [0] * 23)
        assert(storage._method_latencies._treatments_with_config_by_flag_sets == [0] * 23)
        assert(storage._method_latencies._track == [0] * 23)
        assert(latencies ==  {'methodLatencies': {
                    'treatment': [4] + [0] * 22,
                    'treatments': [4] + [0] * 22,
                    'treatment_with_config': [1] + [0] * 22,
                    'treatments_with_config': [2] + [0] * 22,
                    'treatments_by_flag_set': [2] + [0] * 22,
                    'treatments_by_flag_sets': [2] + [0] * 22,
                    'treatments_with_config_by_flag_set': [1] + [0] * 22,
                    'treatments_with_config_by_flag_sets': [2] + [0] * 22,
                    'track': [3] + [0] * 22}})

        [await storage.record_sync_latency(ModelTelemetry.HTTPExceptionsAndLatencies.SPLIT, i) for i in [50, 10, 20, 40]]
        [await storage.record_sync_latency(ModelTelemetry.HTTPExceptionsAndLatencies.SEGMENT, i) for i in [70, 100, 40, 30]]
        [await storage.record_sync_latency(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION, i) for i in [10, 20]]
        [await storage.record_sync_latency(ModelTelemetry.HTTPExceptionsAndLatencies.IMPRESSION_COUNT, i) for i in [5, 10]]
        [await storage.record_sync_latency(ModelTelemetry.HTTPExceptionsAndLatencies.EVENT, i) for i in [50, 40]]
        [await storage.record_sync_latency(ModelTelemetry.HTTPExceptionsAndLatencies.TELEMETRY, i) for i in [100, 50, 160]]
        [await storage.record_sync_latency(ModelTelemetry.HTTPExceptionsAndLatencies.TOKEN, i) for i in [10, 15, 100]]
        sync_latency = await storage.pop_http_latencies()

        assert(storage._http_latencies._split == [0] * 23)
        assert(storage._http_latencies._segment == [0] * 23)
        assert(storage._http_latencies._impression == [0] * 23)
        assert(storage._http_latencies._impression_count == [0] * 23)
        assert(storage._http_latencies._telemetry == [0] * 23)
        assert(storage._http_latencies._token == [0] * 23)
        assert(sync_latency == {'httpLatencies': {'split': [4] + [0] * 22, 'segment': [4] + [0] * 22,
                                'impression': [2] + [0] * 22, 'impressionCount': [2] + [0] * 22, 'event': [2] + [0] * 22,
                                'telemetry': [3] + [0] * 22, 'token': [3] + [0] * 22}})
