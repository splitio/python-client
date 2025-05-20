"""RuleBasedSegment module."""

from enum import Enum
import logging

from splitio.models import MatcherNotFoundException
from splitio.models.splits import _DEFAULT_CONDITIONS_TEMPLATE
from splitio.models.grammar import condition
from splitio.models.splits import Status

_LOGGER = logging.getLogger(__name__)

class SegmentType(Enum):
    """Segment type."""
    
    STANDARD = "standard"
    RULE_BASED = "rule-based"

class RuleBasedSegment(object):
    """RuleBasedSegment object class."""

    def __init__(self, name, traffic_type_name, change_number, status, conditions, excluded):
        """
        Class constructor.

        :param name: Segment name.
        :type name: str
        :param traffic_type_name: traffic type name.
        :type traffic_type_name: str
        :param change_number: change number.
        :type change_number: str
        :param status: status.
        :type status: str
        :param conditions: List of conditions belonging to the segment.
        :type conditions: List
        :param excluded: excluded objects.
        :type excluded: Excluded
        """
        self._name = name
        self._traffic_type_name = traffic_type_name
        self._change_number = change_number
        self._conditions = conditions
        self._excluded = excluded
        try:
            self._status = Status(status)
        except ValueError:
            self._status = Status.ARCHIVED
        
    @property
    def name(self):
        """Return segment name."""
        return self._name
    
    @property
    def traffic_type_name(self):
        """Return traffic type name."""
        return self._traffic_type_name
    
    @property
    def change_number(self):
        """Return change number."""
        return self._change_number
    
    @property
    def status(self):
        """Return status."""
        return self._status
    
    @property
    def conditions(self):
        """Return conditions."""
        return self._conditions
    
    @property
    def excluded(self):
        """Return excluded."""
        return self._excluded

    def to_json(self):
        """Return a JSON representation of this rule based segment."""
        return {
            'changeNumber': self.change_number,
            'trafficTypeName': self.traffic_type_name,
            'name': self.name,
            'status': self.status.value,
            'conditions': [c.to_json() for c in self.conditions],
            'excluded': self.excluded.to_json()
        }
        
    def get_condition_segment_names(self):
        segments = set()
        for condition in self._conditions:
            for matcher in condition.matchers:
                if matcher._matcher_type == 'IN_SEGMENT':
                    segments.add(matcher.to_json()['userDefinedSegmentMatcherData']['segmentName'])
        return segments
        
def from_raw(raw_rule_based_segment):
    """
    Parse a Rule based segment from a JSON portion of splitChanges.

    :param raw_rule_based_segment: JSON object extracted from a splitChange's response
    :type raw_rule_based_segment: dict

    :return: A parsed RuleBasedSegment object capable of performing evaluations.
    :rtype: RuleBasedSegment
    """
    try:
        conditions = [condition.from_raw(c) for c in raw_rule_based_segment['conditions']]
    except MatcherNotFoundException as e:
        _LOGGER.error(str(e))
        _LOGGER.debug("Using default conditions template for feature flag: %s", raw_rule_based_segment['name'])
        conditions = [condition.from_raw(_DEFAULT_CONDITIONS_TEMPLATE)]
    
    if raw_rule_based_segment.get('excluded') == None:
        raw_rule_based_segment['excluded'] = {'keys': [], 'segments': []}
        
    if raw_rule_based_segment['excluded'].get('keys') == None:
        raw_rule_based_segment['excluded']['keys'] = []

    if raw_rule_based_segment['excluded'].get('segments') == None:
        raw_rule_based_segment['excluded']['segments'] = []
        
    return RuleBasedSegment(
        raw_rule_based_segment['name'],
        raw_rule_based_segment['trafficTypeName'],        
        raw_rule_based_segment['changeNumber'],
        raw_rule_based_segment['status'],
        conditions,
        Excluded(raw_rule_based_segment['excluded']['keys'], raw_rule_based_segment['excluded']['segments'])
    )

class Excluded(object):
    
    def __init__(self, keys, segments):
        """
        Class constructor.

        :param keys: List of excluded keys in a rule based segment.
        :type keys: List
        :param segments: List of excluded segments in a rule based segment.
        :type segments: List
        """
        self._keys = keys
        self._segments = [ExcludedSegment(segment['name'], segment['type']) for segment in segments]

    def get_excluded_keys(self):
        """Return excluded keys."""        
        return self._keys

    def get_excluded_segments(self):
        """Return excluded segments"""
        return self._segments

    def get_excluded_standard_segments(self):
        """Return excluded segments"""
        to_return = []
        for segment in self._segments:
            if segment.type == SegmentType.STANDARD:
                to_return.append(segment.name)
        return to_return

    def to_json(self):
        """Return a JSON representation of this object."""
        return {
            'keys': self._keys,
            'segments': self._segments
        }

class ExcludedSegment(object):
    
    def __init__(self, name, type):
        """
        Class constructor.

        :param name: rule based segment name
        :type name: str
        :param type: segment type 
        :type type: str
        """
        self._name = name
        try:
            self._type = SegmentType(type)
        except ValueError:
            self._type = SegmentType.STANDARD

    @property
    def name(self):
        """Return name."""
        return self._name

    @property
    def type(self):
        """Return type."""
        return self._type
