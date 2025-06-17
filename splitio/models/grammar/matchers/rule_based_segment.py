"""Rule based segment matcher classes."""
from splitio.models.grammar.matchers.base import Matcher
from splitio.models.rule_based_segments import SegmentType

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
        
        rb_segment = context['ec'].rbs_segments.get(self._rbs_segment_name)
        
        if key in rb_segment.excluded.get_excluded_keys():
            return False 

        if self._match_dep_rb_segments(rb_segment.excluded.get_excluded_segments(), key, attributes, context):
            return False
                
        return self._match_conditions(rb_segment.conditions, key, attributes, context)
    
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
            
        return False
            
    def _match_dep_rb_segments(self, excluded_rb_segments, key, attributes, context):
        for excluded_rb_segment in excluded_rb_segments:
            if excluded_rb_segment.type == SegmentType.STANDARD:
                if context['ec'].segment_memberships[excluded_rb_segment.name]:
                    return True
            else:
                excluded_segment = context['ec'].rbs_segments.get(excluded_rb_segment.name)
                if key in excluded_segment.excluded.get_excluded_keys():
                    return False

                if self._match_dep_rb_segments(excluded_segment.excluded.get_excluded_segments(), key, attributes, context) \
                    or self._match_conditions(excluded_segment.conditions, key, attributes, context):
                    return True
                                    
        return False
