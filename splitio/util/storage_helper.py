"""Storage Helper."""

from splitio.models import splits

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
        if (feature_flag_storage.config_flag_sets_used == 0 and feature_flag.status == splits.Status.ACTIVE) or \
        (feature_flag.status == splits.Status.ACTIVE and _check_flag_sets(feature_flag_storage, feature_flag)):
            to_add.append(feature_flag)
            segment_list.update(set(feature_flag.get_segment_names()))
        else:
            if feature_flag_storage.get(feature_flag.name) is not None:
                to_delete.append(feature_flag.name)

    feature_flag_storage.update(to_add, to_delete, change_number)
    return segment_list

def _check_flag_sets(feature_flag_storage, feature_flag):
    """
    Check all flag sets in a feature flag, return True if any of sets exist in storage

    :param feature_flag_storage: Feature flag storage instance
    :type feature_flag_storage: splitio.storage.inmemory.InMemorySplitStorage
    :param feature_flag: Feature flag instance to validate.
    :type feature_flag: splitio.models.splits.Split

    :return: True if any of its flag_set exist. False otherwise.
    :rtype: bool
    """
    for flag_set in feature_flag.sets:
        if feature_flag_storage.is_flag_set_exist(flag_set):
            return True
    return False