"""Evaluator tests module."""
import logging

from splitio.models.splits import Split
from splitio.models.grammar.condition import Condition, ConditionType
from splitio.models.impressions import Label
from splitio.engine import evaluator, splitters
from splitio.storage import SplitStorage, SegmentStorage

class EvaluatorTests(object):
    """Test evaluator behavior."""

    def _build_evaluator_with_mocks(self, mocker):
        """Build an evaluator with mocked dependencies."""
        split_storage_mock = mocker.Mock(spec=SplitStorage)
        splitter_mock = mocker.Mock(spec=splitters.Splitter)
        segment_storage_mock = mocker.Mock(spec=SegmentStorage)
        logger_mock = mocker.Mock(spec=logging.Logger)
        e = evaluator.Evaluator(split_storage_mock, segment_storage_mock, splitter_mock)
        e._logger = logger_mock
        return e

    def test_evaluate_treatment_missing_split(self, mocker):
        """Test that a missing split logs and returns CONTROL."""
        e = self._build_evaluator_with_mocks(mocker)
        e._split_storage.get.return_value = None
        result = e.evaluate_feature('feature1', 'some_key', 'some_bucketing_key', {'attr1': 1})
        assert result['configurations'] == None
        assert result['treatment'] == evaluator.CONTROL
        assert result['impression']['change_number'] == -1
        assert result['impression']['label'] == Label.SPLIT_NOT_FOUND

    def test_evaluate_treatment_killed_split(self, mocker):
        """Test that a killed split returns the default treatment."""
        e = self._build_evaluator_with_mocks(mocker)
        mocked_split = mocker.Mock(spec=Split)
        mocked_split.default_treatment = 'off'
        mocked_split.killed = True
        mocked_split.change_number = 123
        mocked_split.get_configurations_for.return_value = '{"some_property": 123}'
        e._split_storage.get.return_value = mocked_split
        result = e.evaluate_feature('feature1', 'some_key', 'some_bucketing_key', {'attr1': 1})
        assert result['treatment'] == 'off'
        assert result['configurations'] == '{"some_property": 123}'
        assert result['impression']['change_number'] == 123
        assert result['impression']['label'] == Label.KILLED
        assert mocked_split.get_configurations_for.mock_calls == [mocker.call('off')]

    def test_evaluate_treatment_ok(self, mocker):
        """Test that a non-killed split returns the appropriate treatment."""
        e = self._build_evaluator_with_mocks(mocker)
        e._get_treatment_for_split = mocker.Mock()
        e._get_treatment_for_split.return_value = ('on', 'some_label')
        mocked_split = mocker.Mock(spec=Split)
        mocked_split.default_treatment = 'off'
        mocked_split.killed = False
        mocked_split.change_number = 123
        mocked_split.get_configurations_for.return_value = '{"some_property": 123}'
        e._split_storage.get.return_value = mocked_split
        result = e.evaluate_feature('feature1', 'some_key', 'some_bucketing_key', {'attr1': 1})
        assert result['treatment'] == 'on'
        assert result['configurations'] == '{"some_property": 123}'
        assert result['impression']['change_number'] == 123
        assert result['impression']['label'] == 'some_label'
        assert mocked_split.get_configurations_for.mock_calls == [mocker.call('on')]


    def test_evaluate_treatment_ok_no_config(self, mocker):
        """Test that a killed split returns the default treatment."""
        e = self._build_evaluator_with_mocks(mocker)
        e._get_treatment_for_split = mocker.Mock()
        e._get_treatment_for_split.return_value = ('on', 'some_label')
        mocked_split = mocker.Mock(spec=Split)
        mocked_split.default_treatment = 'off'
        mocked_split.killed = False
        mocked_split.change_number = 123
        mocked_split.get_configurations_for.return_value = None
        e._split_storage.get.return_value = mocked_split
        result = e.evaluate_feature('feature1', 'some_key', 'some_bucketing_key', {'attr1': 1})
        assert result['treatment'] == 'on'
        assert result['configurations'] == None
        assert result['impression']['change_number'] == 123
        assert result['impression']['label'] == 'some_label'
        assert mocked_split.get_configurations_for.mock_calls == [mocker.call('on')]

    def test_evaluate_treatments(self, mocker):
        """Test that a missing split logs and returns CONTROL."""
        e = self._build_evaluator_with_mocks(mocker)
        e._get_treatment_for_split = mocker.Mock()
        e._get_treatment_for_split.return_value = ('on', 'some_label')
        mocked_split = mocker.Mock(spec=Split)
        mocked_split.name = 'feature2'
        mocked_split.default_treatment = 'off'
        mocked_split.killed = False
        mocked_split.change_number = 123
        mocked_split.get_configurations_for.return_value = '{"some_property": 123}'
        e._split_storage.fetch_many.return_value = {
            'feature1': None,
            'feature2': mocked_split,
        }
        results = e.evaluate_features(['feature1', 'feature2'], 'some_key', 'some_bucketing_key', None)
        result = results['feature1']
        assert result['configurations'] == None
        assert result['treatment'] == evaluator.CONTROL
        assert result['impression']['change_number'] == -1
        assert result['impression']['label'] == Label.SPLIT_NOT_FOUND
        result = results['feature2']
        assert result['configurations'] == '{"some_property": 123}'
        assert result['treatment'] == 'on'
        assert result['impression']['change_number'] == 123
        assert result['impression']['label'] == 'some_label'

    def test_get_gtreatment_for_split_no_condition_matches(self, mocker):
        """Test no condition matches."""
        e = self._build_evaluator_with_mocks(mocker)
        e._splitter.get_treatment.return_value = 'on'
        conditions_mock = mocker.PropertyMock()
        conditions_mock.return_value = []
        mocked_split = mocker.Mock(spec=Split)
        mocked_split.killed = False
        type(mocked_split).conditions = conditions_mock
        treatment, label = e._get_treatment_for_split(mocked_split, 'some_key', 'some_bucketing', {'attr1': 1})
        assert treatment == None
        assert label == None

    def test_get_gtreatment_for_split_non_rollout(self, mocker):
        """Test condition matches."""
        e = self._build_evaluator_with_mocks(mocker)
        e._splitter.get_treatment.return_value = 'on'
        mocked_condition_1 = mocker.Mock(spec=Condition)
        mocked_condition_1.condition_type = ConditionType.WHITELIST
        mocked_condition_1.label = 'some_label'
        mocked_condition_1.matches.return_value = True
        conditions_mock = mocker.PropertyMock()
        conditions_mock.return_value = [mocked_condition_1]
        mocked_split = mocker.Mock(spec=Split)
        mocked_split.killed = False
        type(mocked_split).conditions = conditions_mock
        treatment, label = e._get_treatment_for_split(mocked_split, 'some_key', 'some_bucketing', {'attr1': 1})
        assert treatment == 'on'
        assert label == 'some_label'

    def test_get_treatment_for_split_rollout(self, mocker):
        """Test rollout condition returns default treatment."""
        e = self._build_evaluator_with_mocks(mocker)
        e._splitter.get_bucket.return_value = 60
        mocked_condition_1 = mocker.Mock(spec=Condition)
        mocked_condition_1.condition_type = ConditionType.ROLLOUT
        mocked_condition_1.label = 'some_label'
        mocked_condition_1.matches.return_value = True
        conditions_mock = mocker.PropertyMock()
        conditions_mock.return_value = [mocked_condition_1]
        mocked_split = mocker.Mock(spec=Split)
        mocked_split.traffic_allocation = 50
        mocked_split.default_treatment = 'almost-on'
        mocked_split.killed = False
        type(mocked_split).conditions = conditions_mock
        treatment, label = e._get_treatment_for_split(mocked_split, 'some_key', 'some_bucketing', {'attr1': 1})
        assert treatment == 'almost-on'
        assert label == Label.NOT_IN_SPLIT
