"""Condition model tests module."""

from splitio.models.grammar import condition
from splitio.models.grammar import partitions
from splitio.models.grammar import matchers

class ConditionTests(object):
    """Test the condition object model."""

    raw = {
        'partitions': [
            {'treatment': 'on', 'size': 50},
            {'treatment': 'off', 'size': 50}
        ],
        'contitionType': 'WHITELIST',
        'label': 'some_label',
        'matcherGroup': {
            'matchers': [
                {
                    'matcherType': 'ALL_KEYS',
                    'negate': False,
                }
            ],
            'combiner': 'AND'
        }
    }

    def test_parse(self):
        """Test parsing from raw dict."""
        parsed = condition.from_raw(self.raw)
        assert isinstance(parsed, condition.Condition)
        assert parsed.label == 'some_label'
        assert parsed.condition_type == condition.ConditionType.WHITELIST
        assert isinstance(parsed.matchers[0], matchers.AllKeysMatcher)
        assert isinstance(parsed.partitions[0], partitions.Partition)
        assert parsed.partitions[0].treatment == 'on'
        assert parsed.partitions[0].size == 50
        assert parsed.partitions[1].treatment == 'off'
        assert parsed.partitions[1].size == 50
        assert parsed._combiner == condition._MATCHER_COMBINERS['AND']

    def test_segment_names(self, mocker):
        """Test fetching segment_names."""
        matcher1 = mocker.Mock(spec=matchers.UserDefinedSegmentMatcher)
        matcher2 = mocker.Mock(spec=matchers.UserDefinedSegmentMatcher)
        matcher1._segment_name = 'segment1'
        matcher2._segment_name = 'segment2'
        cond = condition.Condition([matcher1, matcher2], condition._MATCHER_COMBINERS['AND'], [], 'some_label')
        assert cond.get_segment_names() == ['segment1', 'segment2']

    def test_to_json(self):
        """Test JSON serialization of a condition."""
        as_json = condition.from_raw(self.raw).to_json()
        assert as_json['partitions'] == [
            {'treatment': 'on', 'size': 50},
            {'treatment': 'off', 'size': 50}
        ]
        assert as_json['conditionType'] ==  'WHITELIST'
        assert as_json['label'] == 'some_label'
        assert as_json['matcherGroup']['matchers'][0]['matcherType'] == 'ALL_KEYS'
        assert as_json['matcherGroup']['matchers'][0]['negate'] == False
        assert as_json['matcherGroup']['combiner'] == 'AND'

    def test_matches(self, mocker):
        """Test that matches works properly."""
        matcher1_mock = mocker.Mock(spec=matchers.base.Matcher)
        matcher2_mock = mocker.Mock(spec=matchers.base.Matcher)
        matcher1_mock.evaluate.return_value = True
        matcher2_mock.evaluate.return_value = True
        cond = condition.Condition(
            [matcher1_mock, matcher2_mock],
            condition._MATCHER_COMBINERS['AND'],
            [partitions.Partition('on', 50), partitions.Partition('off', 50)],
            'some_label'
        )
        assert cond.matches('some_key', {'a': 1}, {'some_context_option': 0}) == True
        assert matcher1_mock.evaluate.mock_calls == [mocker.call('some_key', {'a': 1}, {'some_context_option': 0})]
        assert matcher2_mock.evaluate.mock_calls == [mocker.call('some_key', {'a': 1}, {'some_context_option': 0})]
