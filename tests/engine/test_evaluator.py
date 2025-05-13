"""Evaluator tests module."""
import json
import logging
import os
import pytest
import copy

from splitio.models.splits import Split, Status
from splitio.models import segments
from splitio.models.grammar.condition import Condition, ConditionType
from splitio.models.impressions import Label
from splitio.models.grammar import condition
from splitio.models import rule_based_segments
from splitio.engine import evaluator, splitters
from splitio.engine.evaluator import EvaluationContext
from splitio.storage.inmemmory import InMemorySplitStorage, InMemorySegmentStorage, InMemoryRuleBasedSegmentStorage, \
    InMemorySplitStorageAsync, InMemorySegmentStorageAsync, InMemoryRuleBasedSegmentStorageAsync
from splitio.engine.evaluator import EvaluationDataFactory, AsyncEvaluationDataFactory

rbs_raw = {
    "changeNumber": 123,
    "name": "sample_rule_based_segment",
    "status": "ACTIVE",
    "trafficTypeName": "user",
    "excluded":{
    "keys":["mauro@split.io","gaston@split.io"],
    "segments":[]
    },
    "conditions": [
    {
        "matcherGroup": {
        "combiner": "AND",
        "matchers": [
            {
            "keySelector": {
                "trafficType": "user",
                "attribute": "email"
            },
            "matcherType": "ENDS_WITH",
            "negate": False,
            "whitelistMatcherData": {
                "whitelist": [
                "@split.io"
                ]
            }
            }
        ]
        }
    }
    ]
}

split_conditions = [
    condition.from_raw({
        "conditionType": "ROLLOUT",
        "matcherGroup": {
        "combiner": "AND",
        "matchers": [
            {
            "keySelector": {
                "trafficType": "user"
            },
            "matcherType": "IN_RULE_BASED_SEGMENT",
            "negate": False,
            "userDefinedSegmentMatcherData": {
                "segmentName": "sample_rule_based_segment"
            }
            }
        ]
        },
        "partitions": [
        {
            "treatment": "on",
            "size": 100
        },
        {
            "treatment": "off",
            "size": 0
        }
        ],
        "label": "in rule based segment sample_rule_based_segment"
    }),
    condition.from_raw({
        "conditionType": "ROLLOUT",
        "matcherGroup": {
        "combiner": "AND",
        "matchers": [
            {
            "keySelector": {
                "trafficType": "user"
            },
            "matcherType": "ALL_KEYS",
            "negate": False
            }
        ]
        },
        "partitions": [
        {
            "treatment": "on",
            "size": 0
        },
        {
            "treatment": "off",
            "size": 100
        }
        ],
        "label": "default rule"
    })
]
        
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

        ctx = EvaluationContext(flags={'some': mocked_split}, segment_memberships=set(), rbs_segments={})
        result = e.eval_with_context('some_key', 'some_bucketing_key', 'some', {}, ctx)
        assert result['treatment'] == 'off'
        assert result['configurations'] == '{"some_property": 123}'
        assert result['impression']['change_number'] == 123
        assert result['impression']['label'] == Label.KILLED
        assert mocked_split.get_configurations_for.mock_calls == [mocker.call('off')]

    def test_evaluate_treatment_ok(self, mocker):
        """Test that a non-killed split returns the appropriate treatment."""
        e = self._build_evaluator_with_mocks(mocker)
        e._treatment_for_flag = mocker.Mock()
        e._treatment_for_flag.return_value = ('on', 'some_label')
        mocked_split = mocker.Mock(spec=Split)
        mocked_split.default_treatment = 'off'
        mocked_split.killed = False
        mocked_split.change_number = 123
        mocked_split.get_configurations_for.return_value = '{"some_property": 123}'
        ctx = EvaluationContext(flags={'some': mocked_split}, segment_memberships=set(), rbs_segments={})
        result = e.eval_with_context('some_key', 'some_bucketing_key', 'some', {}, ctx)
        assert result['treatment'] == 'on'
        assert result['configurations'] == '{"some_property": 123}'
        assert result['impression']['change_number'] == 123
        assert result['impression']['label'] == 'some_label'
        assert mocked_split.get_configurations_for.mock_calls == [mocker.call('on')]
        assert result['impressions_disabled'] == mocked_split.impressions_disabled

    def test_evaluate_treatment_ok_no_config(self, mocker):
        """Test that a killed split returns the default treatment."""
        e = self._build_evaluator_with_mocks(mocker)
        e._treatment_for_flag = mocker.Mock()
        e._treatment_for_flag.return_value = ('on', 'some_label')
        mocked_split = mocker.Mock(spec=Split)
        mocked_split.default_treatment = 'off'
        mocked_split.killed = False
        mocked_split.change_number = 123
        mocked_split.get_configurations_for.return_value = None
        ctx = EvaluationContext(flags={'some': mocked_split}, segment_memberships=set(), rbs_segments={})
        result = e.eval_with_context('some_key', 'some_bucketing_key', 'some', {}, ctx)
        assert result['treatment'] == 'on'
        assert result['configurations'] == None
        assert result['impression']['change_number'] == 123
        assert result['impression']['label'] == 'some_label'
        assert mocked_split.get_configurations_for.mock_calls == [mocker.call('on')]

    def test_evaluate_treatments(self, mocker):
        """Test that a missing split logs and returns CONTROL."""
        e = self._build_evaluator_with_mocks(mocker)
        e._treatment_for_flag = mocker.Mock()
        e._treatment_for_flag.return_value = ('on', 'some_label')
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

        ctx = EvaluationContext(flags={'feature2': mocked_split, 'feature4': mocked_split2}, segment_memberships=set(), rbs_segments={})
        results = e.eval_many_with_context('some_key', 'some_bucketing_key', ['feature2', 'feature4'], {}, ctx)
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
        mocked_split.default_treatment = 'off'
        mocked_split.change_number = '123'
        mocked_split.conditions = []
        mocked_split.get_configurations_for = None
        ctx = EvaluationContext(flags={'some': mocked_split}, segment_memberships=set(), rbs_segments={})
        assert e._treatment_for_flag(mocked_split, 'some_key', 'some_bucketing', {}, ctx) == (
            'off',
            Label.NO_CONDITION_MATCHED
        )

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
        mocked_split.conditions = [mocked_condition_1]
        treatment, label = e._treatment_for_flag(mocked_split, 'some_key', 'some_bucketing', {}, EvaluationContext(None, None, None))
        assert treatment == 'on'
        assert label == 'some_label'

    def test_evaluate_treatment_with_rule_based_segment(self, mocker):
        """Test that a non-killed split returns the appropriate treatment."""
        e = evaluator.Evaluator(splitters.Splitter())

        mocked_split = Split('some', 12345, False, 'off', 'user', Status.ACTIVE, 12, split_conditions, 1.2, 100, 1234, {}, None, False)
        
        ctx = EvaluationContext(flags={'some': mocked_split}, segment_memberships=set(), rbs_segments={'sample_rule_based_segment': rule_based_segments.from_raw(rbs_raw)})
        result = e.eval_with_context('bilal@split.io', 'bilal@split.io', 'some', {'email': 'bilal@split.io'}, ctx)
        assert result['treatment'] == 'on'
                
    def test_evaluate_treatment_with_rbs_in_condition(self):
        e = evaluator.Evaluator(splitters.Splitter())
        splits_storage = InMemorySplitStorage()
        rbs_storage = InMemoryRuleBasedSegmentStorage()
        segment_storage = InMemorySegmentStorage()
        evaluation_facctory = EvaluationDataFactory(splits_storage, segment_storage, rbs_storage)
        
        rbs_segments = os.path.join(os.path.dirname(__file__), 'files', 'rule_base_segments.json')
        with open(rbs_segments, 'r') as flo:
            data = json.loads(flo.read())
            
        mocked_split = Split('some', 12345, False, 'off', 'user', Status.ACTIVE, 12, split_conditions, 1.2, 100, 1234, {}, None, False)
        rbs = rule_based_segments.from_raw(data["rbs"]["d"][0])
        rbs2 = rule_based_segments.from_raw(data["rbs"]["d"][1])
        rbs_storage.update([rbs, rbs2], [], 12)
        splits_storage.update([mocked_split], [], 12)
        
        ctx = evaluation_facctory.context_for('bilal@split.io', ['some'])
        assert e.eval_with_context('bilal@split.io', 'bilal@split.io', 'some', {'email': 'bilal@split.io'}, ctx)['treatment'] == "on"

        ctx = evaluation_facctory.context_for('mauro@split.io', ['some'])
        assert e.eval_with_context('mauro@split.io', 'mauro@split.io', 'some', {'email': 'mauro@split.io'}, ctx)['treatment'] == "off"
                
    def test_using_segment_in_excluded(self):
        rbs_segments = os.path.join(os.path.dirname(__file__), 'files', 'rule_base_segments3.json')
        with open(rbs_segments, 'r') as flo:
            data = json.loads(flo.read())
        e = evaluator.Evaluator(splitters.Splitter())
        splits_storage = InMemorySplitStorage()
        rbs_storage = InMemoryRuleBasedSegmentStorage()
        segment_storage = InMemorySegmentStorage()
        evaluation_facctory = EvaluationDataFactory(splits_storage, segment_storage, rbs_storage)
                    
        mocked_split = Split('some', 12345, False, 'off', 'user', Status.ACTIVE, 12, split_conditions, 1.2, 100, 1234, {}, None, False)
        rbs = rule_based_segments.from_raw(data["rbs"]["d"][0])
        rbs_storage.update([rbs], [], 12)
        splits_storage.update([mocked_split], [], 12)
        segment = segments.from_raw({'name': 'segment1', 'added': ['pato@split.io'], 'removed': [], 'till': 123})
        segment_storage.put(segment)
        
        ctx = evaluation_facctory.context_for('bilal@split.io', ['some'])
        assert e.eval_with_context('bilal@split.io', 'bilal@split.io', 'some', {'email': 'bilal@split.io'}, ctx)['treatment'] == "on"
        ctx = evaluation_facctory.context_for('mauro@split.io', ['some'])
        assert e.eval_with_context('mauro@split.io', 'mauro@split.io', 'some', {'email': 'mauro@split.io'}, ctx)['treatment'] == "off"
        ctx = evaluation_facctory.context_for('pato@split.io', ['some'])
        assert e.eval_with_context('pato@split.io', 'pato@split.io', 'some', {'email': 'pato@split.io'}, ctx)['treatment'] == "off"
        
    def test_using_rbs_in_excluded(self):
        rbs_segments = os.path.join(os.path.dirname(__file__), 'files', 'rule_base_segments2.json')
        with open(rbs_segments, 'r') as flo:
            data = json.loads(flo.read())
        e = evaluator.Evaluator(splitters.Splitter())
        splits_storage = InMemorySplitStorage()
        rbs_storage = InMemoryRuleBasedSegmentStorage()
        segment_storage = InMemorySegmentStorage()
        evaluation_facctory = EvaluationDataFactory(splits_storage, segment_storage, rbs_storage)
                    
        mocked_split = Split('some', 12345, False, 'off', 'user', Status.ACTIVE, 12, split_conditions, 1.2, 100, 1234, {}, None, False)
        rbs = rule_based_segments.from_raw(data["rbs"]["d"][0])
        rbs2 = rule_based_segments.from_raw(data["rbs"]["d"][1])
        rbs_storage.update([rbs, rbs2], [], 12)
        splits_storage.update([mocked_split], [], 12)
        
        ctx = evaluation_facctory.context_for('bilal@split.io', ['some'])
        assert e.eval_with_context('bilal@split.io', 'bilal@split.io', 'some', {'email': 'bilal@split.io'}, ctx)['treatment'] == "off"
        ctx = evaluation_facctory.context_for('bilal', ['some'])
        assert e.eval_with_context('bilal', 'bilal', 'some', {'email': 'bilal'}, ctx)['treatment'] == "on"
        ctx = evaluation_facctory.context_for('bilal2', ['some'])
        assert e.eval_with_context('bilal2', 'bilal2', 'some', {'email': 'bilal2'}, ctx)['treatment'] == "off"
        
    @pytest.mark.asyncio
    async def test_evaluate_treatment_with_rbs_in_condition_async(self):
        e = evaluator.Evaluator(splitters.Splitter())
        splits_storage = InMemorySplitStorageAsync()
        rbs_storage = InMemoryRuleBasedSegmentStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        evaluation_facctory = AsyncEvaluationDataFactory(splits_storage, segment_storage, rbs_storage)
        
        rbs_segments = os.path.join(os.path.dirname(__file__), 'files', 'rule_base_segments.json')
        with open(rbs_segments, 'r') as flo:
            data = json.loads(flo.read())
            
        mocked_split = Split('some', 12345, False, 'off', 'user', Status.ACTIVE, 12, split_conditions, 1.2, 100, 1234, {}, None, False)
        rbs = rule_based_segments.from_raw(data["rbs"]["d"][0])
        rbs2 = rule_based_segments.from_raw(data["rbs"]["d"][1])
        await rbs_storage.update([rbs, rbs2], [], 12)
        await splits_storage.update([mocked_split], [], 12)
        
        ctx = await evaluation_facctory.context_for('bilal@split.io', ['some'])
        assert e.eval_with_context('bilal@split.io', 'bilal@split.io', 'some', {'email': 'bilal@split.io'}, ctx)['treatment'] == "on"
        ctx = await evaluation_facctory.context_for('mauro@split.io', ['some'])
        assert e.eval_with_context('mauro@split.io', 'mauro@split.io', 'some', {'email': 'mauro@split.io'}, ctx)['treatment'] == "off"
        
    @pytest.mark.asyncio
    async def test_using_segment_in_excluded_async(self):
        rbs_segments = os.path.join(os.path.dirname(__file__), 'files', 'rule_base_segments3.json')
        with open(rbs_segments, 'r') as flo:
            data = json.loads(flo.read())
        e = evaluator.Evaluator(splitters.Splitter())
        splits_storage = InMemorySplitStorageAsync()
        rbs_storage = InMemoryRuleBasedSegmentStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        evaluation_facctory = AsyncEvaluationDataFactory(splits_storage, segment_storage, rbs_storage)
                    
        mocked_split = Split('some', 12345, False, 'off', 'user', Status.ACTIVE, 12, split_conditions, 1.2, 100, 1234, {}, None, False)
        rbs = rule_based_segments.from_raw(data["rbs"]["d"][0])
        await rbs_storage.update([rbs], [], 12)
        await splits_storage.update([mocked_split], [], 12)
        segment = segments.from_raw({'name': 'segment1', 'added': ['pato@split.io'], 'removed': [], 'till': 123})
        await segment_storage.put(segment)
        
        ctx = await evaluation_facctory.context_for('bilal@split.io', ['some'])
        assert e.eval_with_context('bilal@split.io', 'bilal@split.io', 'some', {'email': 'bilal@split.io'}, ctx)['treatment'] == "on"
        ctx = await evaluation_facctory.context_for('mauro@split.io', ['some'])
        assert e.eval_with_context('mauro@split.io', 'mauro@split.io', 'some', {'email': 'mauro@split.io'}, ctx)['treatment'] == "off"
        ctx = await evaluation_facctory.context_for('pato@split.io', ['some'])
        assert e.eval_with_context('pato@split.io', 'pato@split.io', 'some', {'email': 'pato@split.io'}, ctx)['treatment'] == "off"
        
    @pytest.mark.asyncio
    async def test_using_rbs_in_excluded_async(self):
        rbs_segments = os.path.join(os.path.dirname(__file__), 'files', 'rule_base_segments2.json')
        with open(rbs_segments, 'r') as flo:
            data = json.loads(flo.read())
        e = evaluator.Evaluator(splitters.Splitter())
        splits_storage = InMemorySplitStorageAsync()
        rbs_storage = InMemoryRuleBasedSegmentStorageAsync()
        segment_storage = InMemorySegmentStorageAsync()
        evaluation_facctory = AsyncEvaluationDataFactory(splits_storage, segment_storage, rbs_storage)
                    
        mocked_split = Split('some', 12345, False, 'off', 'user', Status.ACTIVE, 12, split_conditions, 1.2, 100, 1234, {}, None, False)
        rbs = rule_based_segments.from_raw(data["rbs"]["d"][0])
        rbs2 = rule_based_segments.from_raw(data["rbs"]["d"][1])
        await rbs_storage.update([rbs, rbs2], [], 12)
        await splits_storage.update([mocked_split], [], 12)
        
        ctx = await evaluation_facctory.context_for('bilal@split.io', ['some'])
        assert e.eval_with_context('bilal@split.io', 'bilal@split.io', 'some', {'email': 'bilal@split.io'}, ctx)['treatment'] == "off"
        ctx = await evaluation_facctory.context_for('bilal', ['some'])
        assert e.eval_with_context('bilal', 'bilal', 'some', {'email': 'bilal'}, ctx)['treatment'] == "on"
        
class EvaluationDataFactoryTests(object):
    """Test evaluation factory class."""
    
    def test_get_context(self):
        """Test context."""
        mocked_split = Split('some', 12345, False, 'off', 'user', Status.ACTIVE, 12, split_conditions, 1.2, 100, 1234, {}, None, False)
        flag_storage = InMemorySplitStorage([])
        segment_storage = InMemorySegmentStorage()
        rbs_segment_storage = InMemoryRuleBasedSegmentStorage()
        flag_storage.update([mocked_split], [], -1)
        rbs = copy.deepcopy(rbs_raw)
        rbs['conditions'].append(
        {"matcherGroup": {
              "combiner": "AND",
              "matchers": [
                  {
                      "matcherType": "IN_SEGMENT",
                      "negate": False,
                      "userDefinedSegmentMatcherData": {
                          "segmentName": "employees"
                      },
                      "whitelistMatcherData": None
                  }
              ]
          },
        })
        rbs = rule_based_segments.from_raw(rbs)
        rbs_segment_storage.update([rbs], [], -1)
        
        eval_factory = EvaluationDataFactory(flag_storage, segment_storage, rbs_segment_storage)
        ec = eval_factory.context_for('bilal@split.io', ['some'])
        assert ec.rbs_segments == {'sample_rule_based_segment': rbs}
        assert ec.segment_memberships == {"employees": False}
        
        segment_storage.update("employees", {"mauro@split.io"}, {}, 1234)
        ec = eval_factory.context_for('mauro@split.io', ['some'])
        assert ec.rbs_segments == {'sample_rule_based_segment': rbs}
        assert ec.segment_memberships == {"employees": True}
        
class EvaluationDataFactoryAsyncTests(object):
    """Test evaluation factory class."""
    
    @pytest.mark.asyncio
    async def test_get_context(self):
        """Test context."""
        mocked_split = Split('some', 123, False, 'off', 'user', Status.ACTIVE, 12, split_conditions, 1.2, 100, 1234, {}, None, False)
        flag_storage = InMemorySplitStorageAsync([])
        segment_storage = InMemorySegmentStorageAsync()
        rbs_segment_storage = InMemoryRuleBasedSegmentStorageAsync()
        await flag_storage.update([mocked_split], [], -1)
        rbs = copy.deepcopy(rbs_raw)
        rbs['conditions'].append(
        {"matcherGroup": {
              "combiner": "AND",
              "matchers": [
                  {
                      "matcherType": "IN_SEGMENT",
                      "negate": False,
                      "userDefinedSegmentMatcherData": {
                          "segmentName": "employees"
                      },
                      "whitelistMatcherData": None
                  }
              ]
          },
        })
        rbs = rule_based_segments.from_raw(rbs)
        await rbs_segment_storage.update([rbs], [], -1)
        
        eval_factory = AsyncEvaluationDataFactory(flag_storage, segment_storage, rbs_segment_storage)
        ec = await eval_factory.context_for('bilal@split.io', ['some'])
        assert ec.rbs_segments == {'sample_rule_based_segment': rbs}
        assert ec.segment_memberships == {"employees": False}
        
        await segment_storage.update("employees", {"mauro@split.io"}, {}, 1234)
        ec = await eval_factory.context_for('mauro@split.io', ['some'])
        assert ec.rbs_segments == {'sample_rule_based_segment': rbs}
        assert ec.segment_memberships == {"employees": True}
