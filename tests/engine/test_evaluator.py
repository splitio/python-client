"""Evaluator tests module."""
import logging
import pytest

from splitio.models.splits import Split
from splitio.models.grammar.condition import Condition, ConditionType
from splitio.models.impressions import Label
from splitio.engine import evaluator, splitters

class EvaluatorTests(object):
    """Test evaluator behavior."""

    def _build_evaluator_with_mocks(self, mocker):
        """Build an evaluator with mocked dependencies."""
        splitter_mock = mocker.Mock(spec=splitters.Splitter)
        logger_mock = mocker.Mock(spec=logging.Logger)
        e = evaluator.Evaluator(splitter_mock)
        evaluator._LOGGER = logger_mock
        return e

    def test_evaluate_treatment_killed_split(self, mocker):
        """Test that a killed split returns the default treatment."""
        e = self._build_evaluator_with_mocks(mocker)
        mocked_split = mocker.Mock(spec=Split)
        mocked_split.default_treatment = 'off'
        mocked_split.killed = True
        mocked_split.change_number = 123
        mocked_split.get_configurations_for.return_value = '{"some_property": 123}'
        result = e.evaluate_feature(mocked_split, 'some_key', 'some_bucketing_key', mocker.Mock())
        assert result['treatment'] == 'off'
        assert result['configurations'] == '{"some_property": 123}'
        assert result['impression']['change_number'] == 123
        assert result['impression']['label'] == Label.KILLED
        assert mocked_split.get_configurations_for.mock_calls == [mocker.call('off')]

    def test_evaluate_treatment_ok(self, mocker):
        """Test that a non-killed split returns the appropriate treatment."""
        e = self._build_evaluator_with_mocks(mocker)
        e._get_treatment_for_feature_flag = mocker.Mock()
        e._get_treatment_for_feature_flag.return_value = ('on', 'some_label')
        mocked_split = mocker.Mock(spec=Split)
        mocked_split.default_treatment = 'off'
        mocked_split.killed = False
        mocked_split.change_number = 123
        mocked_split.get_configurations_for.return_value = '{"some_property": 123}'
        result = e.evaluate_feature(mocked_split, 'some_key', 'some_bucketing_key', mocker.Mock())
        assert result['treatment'] == 'on'
        assert result['configurations'] == '{"some_property": 123}'
        assert result['impression']['change_number'] == 123
        assert result['impression']['label'] == 'some_label'
        assert mocked_split.get_configurations_for.mock_calls == [mocker.call('on')]


    def test_evaluate_treatment_ok_no_config(self, mocker):
        """Test that a killed split returns the default treatment."""
        e = self._build_evaluator_with_mocks(mocker)
        e._get_treatment_for_feature_flag = mocker.Mock()
        e._get_treatment_for_feature_flag.return_value = ('on', 'some_label')
        mocked_split = mocker.Mock(spec=Split)
        mocked_split.default_treatment = 'off'
        mocked_split.killed = False
        mocked_split.change_number = 123
        mocked_split.get_configurations_for.return_value = None
        result = e.evaluate_feature(mocked_split, 'some_key', 'some_bucketing_key', mocker.Mock())
        assert result['treatment'] == 'on'
        assert result['configurations'] == None
        assert result['impression']['change_number'] == 123
        assert result['impression']['label'] == 'some_label'
        assert mocked_split.get_configurations_for.mock_calls == [mocker.call('on')]

    def test_evaluate_treatments(self, mocker):
        """Test that a missing split logs and returns CONTROL."""
        e = self._build_evaluator_with_mocks(mocker)
        e._get_treatment_for_feature_flag = mocker.Mock()
        e._get_treatment_for_feature_flag.return_value = ('on', 'some_label')
        mocked_split = mocker.Mock(spec=Split)
        mocked_split.name = 'feature2'
        mocked_split.default_treatment = 'off'
        mocked_split.killed = False
        mocked_split.change_number = 123
        mocked_split.get_configurations_for.return_value = '{"some_property": 123}'

        mocked_split2 = mocker.Mock(spec=Split)
        mocked_split2.name = 'feature4'
        mocked_split2.default_treatment = 'on'
        mocked_split2.killed = False
        mocked_split2.change_number = 123
        mocked_split2.get_configurations_for.return_value = None

#        pytest.set_trace()
        results = e.evaluate_features([mocked_split, mocked_split2], 'some_key', 'some_bucketing_key', {'feature2': {}, 'feature4': {}})
        result = results['feature4']
        assert result['configurations'] == None
        assert result['treatment'] == 'on'
        assert result['impression']['change_number'] == 123
        assert result['impression']['label'] == 'some_label'
        result = results['feature2']
        assert result['configurations'] == '{"some_property": 123}'
        assert result['treatment'] == 'on'
        assert result['impression']['change_number'] == 123
        assert result['impression']['label'] == 'some_label'

    def test_get_gtreatment_for_split_no_condition_matches(self, mocker):
        """Test no condition matches."""
        e = self._build_evaluator_with_mocks(mocker)
        e._splitter.get_treatment.return_value = 'on'
        mocked_split = mocker.Mock(spec=Split)
        mocked_split.killed = False
        treatment, label = e._get_treatment_for_feature_flag(mocked_split, 'some_key', 'some_bucketing', [])
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
        mocked_split = mocker.Mock(spec=Split)
        mocked_split.killed = False
        evaluation_contexts = [(True, mocked_condition_1)]
        treatment, label = e._get_treatment_for_feature_flag(mocked_split, 'some_key', 'some_bucketing', evaluation_contexts)
        assert treatment == 'on'
        assert label == 'some_label'