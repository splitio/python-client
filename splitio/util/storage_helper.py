"""Storage Helper."""
import logging
from splitio.models import splits
from splitio.models import rule_based_segments

_LOGGER = logging.getLogger(__name__)

def update_feature_flag_storage(feature_flag_storage, feature_flags, change_number, clear_storage=False):
    """
    Update feature flag storage from given list of feature flags while checking the flag set logic

    :param feature_flag_storage: Feature flag storage instance
    :type feature_flag_storage: splitio.storage.inmemory.InMemorySplitStorage
    :param feature_flag: Feature flag instance to validate.
    :type feature_flag: splitio.models.splits.Split
    :param: last change number
    :type: int

    :return: segments list from feature flags list
    :rtype: list(str)
    """
    segment_list = set()
    to_add = []
    to_delete = []
    if clear_storage:
        feature_flag_storage.clear()
        
    for feature_flag in feature_flags:
        if feature_flag_storage.flag_set_filter.intersect(feature_flag.sets) and feature_flag.status == splits.Status.ACTIVE:
            to_add.append(feature_flag)
            segment_list.update(set(feature_flag.get_segment_names()))
        else:
            if feature_flag_storage.get(feature_flag.name) is not None:
                to_delete.append(feature_flag.name)

    feature_flag_storage.update(to_add, to_delete, change_number)
    return segment_list

def update_rule_based_segment_storage(rule_based_segment_storage, rule_based_segments, change_number, clear_storage=False):
    """
    Update rule based segment storage from given list of rule based segments

    :param rule_based_segment_storage: rule based segment storage instance
    :type rule_based_segment_storage: splitio.storage.RuleBasedSegmentStorage
    :param rule_based_segments: rule based segment instance to validate.
    :type rule_based_segments: splitio.models.rule_based_segments.RuleBasedSegment
    :param: last change number
    :type: int

    :return: segments list from excluded segments list
    :rtype: list(str)
    """
    if clear_storage:
        rule_based_segment_storage.clear()

    segment_list = set()
    to_add = []
    to_delete = []
    for rule_based_segment in rule_based_segments:
        if rule_based_segment.status == splits.Status.ACTIVE:
            to_add.append(rule_based_segment)
            segment_list.update(set(rule_based_segment.excluded.get_excluded_standard_segments()))
            segment_list.update(rule_based_segment.get_condition_segment_names())
        else:
            if rule_based_segment_storage.get(rule_based_segment.name) is not None:
                to_delete.append(rule_based_segment.name)

    rule_based_segment_storage.update(to_add, to_delete, change_number)
    return segment_list
    
def get_standard_segment_names_in_rbs_storage(rule_based_segment_storage):
    """
    Retrieve a list of all standard segments names.

    :return: Set of segment names.
    :rtype: Set(str)
    """
    segment_list = set()
    for rb_segment in rule_based_segment_storage.get_segment_names():
        rb_segment_obj = rule_based_segment_storage.get(rb_segment)
        segment_list.update(set(rb_segment_obj.excluded.get_excluded_standard_segments()))
        segment_list.update(rb_segment_obj.get_condition_segment_names())
        
    return segment_list
    
async def update_feature_flag_storage_async(feature_flag_storage, feature_flags, change_number, clear_storage=False):
    """
    Update feature flag storage from given list of feature flags while checking the flag set logic

    :param feature_flag_storage: Feature flag storage instance
    :type feature_flag_storage: splitio.storage.inmemory.InMemorySplitStorage
    :param feature_flag: Feature flag instance to validate.
    :type feature_flag: splitio.models.splits.Split
    :param: last change number
    :type: int

    :return: segments list from feature flags list
    :rtype: list(str)
    """
    if clear_storage:
        await feature_flag_storage.clear()
    
    segment_list = set()
    to_add = []
    to_delete = []
    for feature_flag in feature_flags:
        if feature_flag_storage.flag_set_filter.intersect(feature_flag.sets) and feature_flag.status == splits.Status.ACTIVE:
            to_add.append(feature_flag)
            segment_list.update(set(feature_flag.get_segment_names()))
        else:
            if await feature_flag_storage.get(feature_flag.name) is not None:
                to_delete.append(feature_flag.name)

    await feature_flag_storage.update(to_add, to_delete, change_number)
    return segment_list

async def update_rule_based_segment_storage_async(rule_based_segment_storage, rule_based_segments, change_number, clear_storage=False):
    """
    Update rule based segment storage from given list of rule based segments

    :param rule_based_segment_storage: rule based segment storage instance
    :type rule_based_segment_storage: splitio.storage.RuleBasedSegmentStorage
    :param rule_based_segments: rule based segment instance to validate.
    :type rule_based_segments: splitio.models.rule_based_segments.RuleBasedSegment
    :param: last change number
    :type: int

    :return: segments list from excluded segments list
    :rtype: list(str)
    """
    if clear_storage:
        await rule_based_segment_storage.clear()
    
    segment_list = set()
    to_add = []
    to_delete = []
    for rule_based_segment in rule_based_segments:
        if rule_based_segment.status == splits.Status.ACTIVE:
            to_add.append(rule_based_segment)
            segment_list.update(set(rule_based_segment.excluded.get_excluded_standard_segments()))
            segment_list.update(rule_based_segment.get_condition_segment_names())
        else:
            if await rule_based_segment_storage.get(rule_based_segment.name) is not None:
                to_delete.append(rule_based_segment.name)

    await rule_based_segment_storage.update(to_add, to_delete, change_number)
    return segment_list

async def get_standard_segment_names_in_rbs_storage_async(rule_based_segment_storage):
    """
    Retrieve a list of all standard segments names.

    :return: Set of segment names.
    :rtype: Set(str)
    """
    segment_list = set()
    segment_names = await rule_based_segment_storage.get_segment_names()
    for rb_segment in segment_names:
        rb_segment_obj = await rule_based_segment_storage.get(rb_segment)
        segment_list.update(set(rb_segment_obj.excluded.get_excluded_standard_segments()))
        segment_list.update(rb_segment_obj.get_condition_segment_names())
        
    return segment_list

def get_valid_flag_sets(flag_sets, flag_set_filter):
    """
    Check each flag set in given array, return it if exist in a given config flag set array, if config array is empty return all

    :param flag_sets: Flag sets array
    :type flag_sets: list(str)
    :param config_flag_sets: Config flag sets array
    :type config_flag_sets: list(str)

    :return: array of flag sets
    :rtype: list(str)
    """
    sets_to_fetch = []
    for flag_set in flag_sets:
        if not flag_set_filter.set_exist(flag_set) and flag_set_filter.should_filter:
            _LOGGER.warning("Flag set %s is not part of the configured flag set list, ignoring the request." % (flag_set))
            continue
        sets_to_fetch.append(flag_set)

    return sets_to_fetch

def combine_valid_flag_sets(result_sets):
    """
    Check each flag set in given array of sets, combine all flag sets in one unique set

    :param result_sets: Flag sets set
    :type flag_sets: list(set)

    :return: flag sets set
    :rtype: set
    """
    to_return = set()
    for result_set in result_sets:
        if isinstance(result_set, set) and len(result_set) > 0:
            to_return.update(result_set)
    return to_return