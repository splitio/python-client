"""Prerequisites matcher classes."""

class PrerequisitesMatcher(object):
    
    def __init__(self, prerequisites):
        """
        Build a PrerequisitesMatcher.

        :param prerequisites: prerequisites
        :type raw_matcher: List of Prerequisites
        """
        self._prerequisites = prerequisites
    
    def match(self, key, attributes=None, context=None):      
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
        if self._prerequisites == None:
            return True

        evaluator = context.get('evaluator')
        bucketing_key = context.get('bucketing_key')
        for prerequisite in self._prerequisites:
            result = evaluator.eval_with_context(key, bucketing_key, prerequisite.feature_flag_name, attributes, context['ec'])
            if result['treatment'] not in prerequisite.treatments:
                return False

        return True