"""Matchers entrypoint module."""
from splitio.models.grammar.matchers.keys import AllKeysMatcher, UserDefinedSegmentMatcher
from splitio.models.grammar.matchers.numeric import BetweenMatcher, EqualToMatcher, \
    GreaterThanOrEqualMatcher, LessThanOrEqualMatcher
from splitio.models.grammar.matchers.sets import ContainsAllOfSetMatcher, \
    ContainsAnyOfSetMatcher, EqualToSetMatcher, PartOfSetMatcher
from splitio.models.grammar.matchers.string import ContainsStringMatcher, \
    EndsWithMatcher, RegexMatcher, StartsWithMatcher, WhitelistMatcher
from splitio.models.grammar.matchers.misc import BooleanMatcher, DependencyMatcher


MATCHER_TYPE_ALL_KEYS = 'ALL_KEYS'
MATCHER_TYPE_IN_SEGMENT = 'IN_SEGMENT'
MATCHER_TYPE_WHITELIST = 'WHITELIST'
MATCHER_TYPE_EQUAL_TO = 'EQUAL_TO'
MATCHER_TYPE_GREATER_THAN_OR_EQUAL_TO = 'GREATER_THAN_OR_EQUAL_TO'
MATCHER_TYPE_LESS_THAN_OR_EQUAL_TO = 'LESS_THAN_OR_EQUAL_TO'
MATCHER_TYPE_BETWEEN = 'BETWEEN'
MATCHER_TYPE_EQUAL_TO_SET = 'EQUAL_TO_SET'
MATCHER_TYPE_PART_OF_SET = 'PART_OF_SET'
MATCHER_TYPE_CONTAINS_ALL_OF_SET = 'CONTAINS_ALL_OF_SET'
MATCHER_TYPE_CONTAINS_ANY_OF_SET = 'CONTAINS_ANY_OF_SET'
MATCHER_TYPE_STARTS_WITH = 'STARTS_WITH'
MATCHER_TYPE_ENDS_WITH = 'ENDS_WITH'
MATCHER_TYPE_CONTAINS_STRING = 'CONTAINS_STRING'
MATCHER_TYPE_IN_SPLIT_TREATMENT = 'IN_SPLIT_TREATMENT'
MATCHER_TYPE_EQUAL_TO_BOOLEAN = 'EQUAL_TO_BOOLEAN'
MATCHER_TYPE_MATCHES_STRING = 'MATCHES_STRING'


_MATCHER_BUILDERS = {
    MATCHER_TYPE_ALL_KEYS: AllKeysMatcher,
    MATCHER_TYPE_IN_SEGMENT: UserDefinedSegmentMatcher,
    MATCHER_TYPE_WHITELIST: WhitelistMatcher,
    MATCHER_TYPE_EQUAL_TO: EqualToMatcher,
    MATCHER_TYPE_GREATER_THAN_OR_EQUAL_TO: GreaterThanOrEqualMatcher,
    MATCHER_TYPE_LESS_THAN_OR_EQUAL_TO: LessThanOrEqualMatcher,
    MATCHER_TYPE_BETWEEN: BetweenMatcher,
    MATCHER_TYPE_EQUAL_TO_SET: EqualToSetMatcher,
    MATCHER_TYPE_PART_OF_SET: PartOfSetMatcher,
    MATCHER_TYPE_CONTAINS_ALL_OF_SET: ContainsAllOfSetMatcher,
    MATCHER_TYPE_CONTAINS_ANY_OF_SET: ContainsAnyOfSetMatcher,
    MATCHER_TYPE_STARTS_WITH: StartsWithMatcher,
    MATCHER_TYPE_ENDS_WITH: EndsWithMatcher,
    MATCHER_TYPE_CONTAINS_STRING: ContainsStringMatcher,
    MATCHER_TYPE_IN_SPLIT_TREATMENT: DependencyMatcher,
    MATCHER_TYPE_EQUAL_TO_BOOLEAN: BooleanMatcher,
    MATCHER_TYPE_MATCHES_STRING: RegexMatcher
}


def from_raw(raw_matcher):
    """
    Parse a condition from a JSON portion of splitChanges.

    :param raw_matcher: JSON object extracted from a condition's matcher array.
    :type raw_matcher: dict

    :return: A concrete Matcher object.
    :rtype: Matcher
    """
    matcher_type = raw_matcher['matcherType']
    try:
        builder = _MATCHER_BUILDERS[matcher_type]
    except KeyError:
        raise ValueError('Invalid matcher type %s' % matcher_type)
    return builder(raw_matcher)
