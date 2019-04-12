"""Split model tests module."""

from splitio.models import splits
from splitio.models.grammar.condition import Condition


class SplitTests(object):
    """Split model tests."""

    raw = {
        'changeNumber': 123,
        'trafficTypeName': 'user',
        'name': 'some_name',
        'trafficAllocation': 100,
        'trafficAllocationSeed': 123456,
        'seed': 321654,
        'status': 'ACTIVE',
        'killed': False,
        'defaultTreatment': 'off',
        'algo': 2,
        'conditions': [
            {
                'partitions': [
                    {'treatment': 'on', 'size': 50},
                    {'treatment': 'off', 'size': 50}
                ],
                'contitionType': 'WHITELIST',
                'label': 'some_label',
                'matcherGroup': {
                    'matchers': [
                        {
                            'matcherType': 'WHITELIST',
                            'whitelistMatcherData': {
                                'whitelist': ['k1', 'k2', 'k3']
                            },
                            'negate': False,
                        }
                    ],
                    'combiner': 'AND'
                }
            },
            {
                'partitions': [
                    {'treatment': 'on', 'size': 25},
                    {'treatment': 'off', 'size': 75}
                ],
                'contitionType': 'WHITELIST',
                'label': 'some_other_label',
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
        ],
        'configurations': {
            'on': '{"color": "blue", "size": 13}'
        },
    }

    def test_from_raw(self):
        """Test split model parsing."""
        parsed = splits.from_raw(self.raw)
        assert isinstance(parsed, splits.Split)
        assert parsed.change_number == 123
        assert parsed.traffic_type_name == 'user'
        assert parsed.name == 'some_name'
        assert parsed.traffic_allocation == 100
        assert parsed.traffic_allocation_seed == 123456
        assert parsed.seed == 321654
        assert parsed.status == splits.Status.ACTIVE
        assert parsed.killed is False
        assert parsed.default_treatment == 'off'
        assert parsed.algo == splits.HashAlgorithm.MURMUR
        assert len(parsed.conditions) == 2
        assert parsed.get_configurations_for('on') == '{"color": "blue", "size": 13}'
        assert parsed._configurations == {'on': '{"color": "blue", "size": 13}'}

    def test_get_segment_names(self, mocker):
        """Test fetching segment names."""
        cond1 = mocker.Mock(spec=Condition)
        cond2 = mocker.Mock(spec=Condition)
        cond1.get_segment_names.return_value = ['segment1', 'segment2']
        cond2.get_segment_names.return_value = ['segment3', 'segment4']
        split1 = splits.Split( 'some_split', 123, False, 'off', 'user', 'ACTIVE', 123, [cond1, cond2])
        assert split1.get_segment_names() == ['segment%d' % i for i in range(1, 5)]


    def test_to_json(self):
        """Test json serialization."""
        as_json = splits.from_raw(self.raw).to_json()
        assert isinstance(as_json, dict)
        assert as_json['changeNumber'] == 123
        assert as_json['trafficTypeName'] == 'user'
        assert as_json['name'] == 'some_name'
        assert as_json['trafficAllocation'] == 100
        assert as_json['trafficAllocationSeed'] == 123456
        assert as_json['seed'] == 321654
        assert as_json['status'] == 'ACTIVE'
        assert as_json['killed'] is False
        assert as_json['defaultTreatment'] == 'off'
        assert as_json['algo'] == 2
        assert len(as_json['conditions']) == 2

    def test_to_split_view(self):
        """Test SplitView creation."""
        as_split_view = splits.from_raw(self.raw).to_split_view()
        assert isinstance(as_split_view, splits.SplitView)
        assert as_split_view.name == self.raw['name']
        assert as_split_view.change_number == self.raw['changeNumber']
        assert as_split_view.killed == self.raw['killed']
        assert as_split_view.traffic_type == self.raw['trafficTypeName']
        assert set(as_split_view.treatments) == set(['on', 'off'])
