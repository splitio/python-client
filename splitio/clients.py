"""A module for Split.io SDK API clients"""
from __future__ import absolute_import, division, print_function, \
    unicode_literals

import logging
import time
from splitio.treatments import CONTROL
from splitio.splitters import Splitter
from splitio.impressions import Impression, Label
from splitio.metrics import SDK_GET_TREATMENT
from splitio.splits import ConditionType
from splitio.events import Event

class Key(object):
    def __init__(self, matching_key, bucketing_key):
        """Bucketing Key implementation"""
        self.matching_key = matching_key
        self.bucketing_key = bucketing_key


class Client(object):
    def __init__(self, broker, labels_enabled=True):
        """Basic interface of a Client. Specific implementations need to override the
        get_split_fetcher method (and optionally the get_splitter method).
        """
        self._logger = logging.getLogger(self.__class__.__name__)
        self._splitter = Splitter()
        self._broker = broker
        self._labels_enabled = labels_enabled
        self._destroyed = False

    @staticmethod
    def _get_keys(key):
        """
        """
        if isinstance(key, Key):
            matching_key = key.matching_key
            bucketing_key = key.bucketing_key
        else:
            matching_key = str(key)
            bucketing_key = None
        return matching_key, bucketing_key

    def destroy(self):
        """
        Disable the split-client and free all allocated resources.
        Only applicable when using in-memory operation mode.
        """
        self._destroyed = True
        self._broker.destroy()

    def get_treatment(self, key, feature, attributes=None):
        """
        Get the treatment for a feature and key, with an optional dictionary of attributes. This
        method never raises an exception. If there's a problem, the appropriate log message will
        be generated and the method will return the CONTROL treatment.
        :param key: The key for which to get the treatment
        :type key: str
        :param feature: The name of the feature for which to get the treatment
        :type feature: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: The treatment for the key and feature
        :rtype: str
        """
        if self._destroyed:
            return CONTROL

        if key is None or feature is None:
            return CONTROL

        start = int(round(time.time() * 1000))

        matching_key, bucketing_key = self._get_keys(key)

        try:
            label = ''
            _treatment = CONTROL
            _change_number = -1

            # Fetching Split definition
            split = self._broker.fetch_feature(feature)

            if split is None:
                self._logger.warning('Unknown or invalid feature: %s', feature)
                label = Label.SPLIT_NOT_FOUND
                _treatment = CONTROL
            else:
                _change_number = split.change_number
                if split.killed:
                    label = Label.KILLED
                    _treatment = split.default_treatment
                else:
                    treatment, label = self._get_treatment_for_split(
                        split,
                        matching_key,
                        bucketing_key,
                        attributes
                    )
                    if treatment is None:
                        label = Label.NO_CONDITION_MATCHED
                        _treatment = split.default_treatment
                    else:
                        _treatment = treatment

            impression = self._build_impression(matching_key, feature, _treatment, label,
                                                _change_number, bucketing_key, start)
            self._record_stats(impression, start, SDK_GET_TREATMENT)
            return _treatment
        except:
            self._logger.exception('Exception caught getting treatment for feature')

            try:
                impression = self._build_impression(matching_key, feature, CONTROL, Label.EXCEPTION,
                                                    self._broker.get_change_number(), bucketing_key, start)
                self._record_stats(impression, start, SDK_GET_TREATMENT)
            except:
                self._logger.exception('Exception reporting impression into get_treatment exception block')

            return CONTROL

    def _build_impression(self, matching_key, feature_name, treatment, label, change_number, bucketing_key, time):

        if not self._labels_enabled:
            label = None

        return Impression(
            matching_key=matching_key, feature_name=feature_name,
            treatment=treatment, label=label, change_number=change_number,
            bucketing_key=bucketing_key, time=time
        )

    def _record_stats(self, impression, start, operation):
        try:
            end = int(round(time.time() * 1000))
            self._broker.log_impression(impression)
            self._broker.log_operation_time(operation, end - start)
        except:
            self._logger.exception('Exception caught recording impressions and metrics')

    def _get_treatment_for_split(self, split, matching_key, bucketing_key, attributes=None):
        """
        Internal method to get the treatment for a given Split and optional attributes. This
        method might raise exceptions and should never be used directly.
        :param split: The split for which to get the treatment
        :type split: Split
        :param key: The key for which to get the treatment
        :type key: str
        :param attributes: An optional dictionary of attributes
        :type attributes: dict
        :return: The treatment for the key and split
        :rtype: str
        """
        if bucketing_key is None:
            bucketing_key = matching_key

        matcher_client = MatcherClient(self._broker, self._splitter, self._logger)

        roll_out = False
        for condition in split.conditions:
            if (not roll_out and
                    condition.condition_type == ConditionType.ROLLOUT):
                if split.traffic_allocation < 100:
                    bucket = self._splitter.get_bucket(
                        bucketing_key,
                        split.traffic_allocation_seed,
                        split.algo
                    )
                    if bucket >= split.traffic_allocation:
                        return split.default_treatment, Label.NOT_IN_SPLIT
                roll_out = True

            condition_matches = condition.matcher.match(
                Key(matching_key, bucketing_key),
                attributes=attributes,
                client=matcher_client
            )

            if condition_matches:
                return self._splitter.get_treatment(
                    bucketing_key,
                    split.seed,
                    condition.partitions,
                    split.algo
                ), condition.label

        # No condition matches
        return None, None

    def track(self, key, traffic_type, event_type, value=None):
        """
        Track an event
        """
        e = Event(
            key=key,
            trafficTypeName=traffic_type,
            eventTypeId=event_type,
            value=value,
            timestamp=int(time.time()*1000)
        )
        return self._broker.get_events_log().log_event(e)

class MatcherClient(Client):
    """
    """
    def __init__(self, broker, splitter, logger):
        self._broker = broker
        self._splitter = splitter
        self._logger = logger

    def get_treatment(self, key, feature, attributes=None):
        """
        """
        if key is None or feature is None: return CONTROL

        matching_key, bucketing_key = self._get_keys(key)
        try:
            # Fetching Split definition
            split = self._broker.fetch_feature(feature)

            if split is None:
                self._logger.warning(
                    'Unknown or invalid dependent feature: %s',
                    feature
                )
                return CONTROL

            if split.killed: return split.default_treatment

            treatment, _ = self._get_treatment_for_split(
                split,
                matching_key,
                bucketing_key,
                attributes
            )

            if treatment is None: return split.default_treatment
            return treatment
        except:
            self._logger.exception('Exception caught retrieving dependent feature. Returning CONTROL')
            return CONTROL
