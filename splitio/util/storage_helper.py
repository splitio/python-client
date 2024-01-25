"""Storage Helper."""
import logging

from splitio.models import splits

_LOGGER = logging.getLogger(__name__)

def update_feature_flag_storage(feature_flag_storage, feature_flags, change_number):
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
    for feature_flag in feature_flags:
        if feature_flag_storage.flag_set_filter.intersect(feature_flag.sets) and feature_flag.status == splits.Status.ACTIVE:
            to_add.append(feature_flag)
            segment_list.update(set(feature_flag.get_segment_names()))
        else:
            if feature_flag_storage.get(feature_flag.name) is not None:
                to_delete.append(feature_flag.name)

    feature_flag_storage.update(to_add, to_delete, change_number)
    return segment_list

async def update_feature_flag_storage_async(feature_flag_storage, feature_flags, change_number):
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
    for feature_flag in feature_flags:
        if feature_flag_storage.flag_set_filter.intersect(feature_flag.sets) and feature_flag.status == splits.Status.ACTIVE:
            to_add.append(feature_flag)
            segment_list.update(set(feature_flag.get_segment_names()))
        else:
            if await feature_flag_storage.get(feature_flag.name) is not None:
                to_delete.append(feature_flag.name)

    await feature_flag_storage.update(to_add, to_delete, change_number)
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