"""Rule based segment matcher classes."""
from splitio.models.grammar.matchers.base import Matcher

class RuleBasedSegmentMatcher(Matcher):
    
    def _build(self, raw_matcher):
        """
        Build an RuleBasedSegmentMatcher.

        :param raw_matcher: raw matcher as fetched from splitChanges response.
        :type raw_matcher: dict
        """
        self._rbs_segment_name = raw_matcher['userDefinedSegmentMatcherData']['segmentName']
    
    def _match(self, key, attributes=None, context=None):      
        """
        Evaluate user input against a matcher and return whether the match is successful.

        :param key: User key.
        :type key: str.
        :param attributes: Custom user attributes.
        :type attributes: dict.
        :param context: Evaluation context
        :type context: dict

        :returns: Wheter the match is successful.
        :rtype: bool
        """
        if self._rbs_segment_name == None:
            return False
        
        # Check if rbs segment has exclusions
        if context['ec'].segment_rbs_memberships.get(self._rbs_segment_name):
            return False
        
        for rbs_segment in context['ec'].excluded_rbs_segments:
            if self._match_conditions(rbs_segment.conditions, key, attributes, context):
                return True
        
        return self._match_conditions(context['ec'].segment_rbs_conditions.get(self._rbs_segment_name), key, attributes, context):
    
    def _add_matcher_specific_properties_to_json(self):
        """Return UserDefinedSegment specific properties."""
        return {
            'userDefinedSegmentMatcherData': {
                'segmentName': self._rbs_segment_name
            }
        }
        
    def _match_conditions(self, rbs_segment_conditions, key, attributes, context):
        for parsed_condition in rbs_segment_conditions:
            if parsed_condition.matches(key, attributes, context):
                return True
