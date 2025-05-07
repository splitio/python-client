"""Split model tests module."""
import copy
import pytest
from splitio.models import rule_based_segments
from splitio.models import splits
from splitio.models.grammar.condition import Condition

class RuleBasedSegmentModelTests(object):
    """Rule based segment model tests."""

    raw = {
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

    def test_from_raw(self):
        """Test split model parsing."""
        parsed = rule_based_segments.from_raw(self.raw)
        assert isinstance(parsed, rule_based_segments.RuleBasedSegment)
        assert parsed.change_number == 123
        assert parsed.name == 'sample_rule_based_segment'
        assert parsed.status == splits.Status.ACTIVE
        assert len(parsed.conditions) == 1
        assert parsed.excluded.get_excluded_keys() == ["mauro@split.io","gaston@split.io"]
        assert parsed.excluded.get_excluded_segments() == []
        conditions = parsed.conditions[0].to_json()
        assert conditions['matcherGroup']['matchers'][0] == {
            'betweenMatcherData': None, 'booleanMatcherData': None, 'dependencyMatcherData': None,
            'stringMatcherData': None, 'unaryNumericMatcherData': None, 'userDefinedSegmentMatcherData': None,
            "keySelector": {
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
    
    def test_incorrect_matcher(self):
        """Test incorrect matcher in split model parsing."""
        rbs = copy.deepcopy(self.raw)
        rbs['conditions'][0]['matcherGroup']['matchers'][0]['matcherType'] = 'INVALID_MATCHER'
        rbs = rule_based_segments.from_raw(rbs)
        assert rbs.conditions[0].to_json() == splits._DEFAULT_CONDITIONS_TEMPLATE

        # using multiple conditions
        rbs = copy.deepcopy(self.raw)
        rbs['conditions'].append(rbs['conditions'][0])
        rbs['conditions'][0]['matcherGroup']['matchers'][0]['matcherType'] = 'INVALID_MATCHER'
        parsed = rule_based_segments.from_raw(rbs)
        assert parsed.conditions[0].to_json() == splits._DEFAULT_CONDITIONS_TEMPLATE
        
    def test_get_condition_segment_names(self):
        rbs = copy.deepcopy(self.raw)
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
        
        assert rbs.get_condition_segment_names() == {"employees"}
      