"""Matchers tests module."""
#pylint: disable=protected-access,line-too-long,unsubscriptable-object

import abc
import calendar
import json
import os.path
import re

from datetime import datetime

from splitio.models.grammar import matchers
from splitio.models.grammar.matchers.semver import Semver
from splitio.storage import SegmentStorage
from splitio.engine.evaluator import Evaluator


class MatcherTestsBase(object):
    """Abstract class to make sure we test all relevant methods."""

    __metaclass__ = abc.ABCMeta

    @abc.abstractmethod
    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        pass

    @abc.abstractmethod
    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        pass

    @abc.abstractmethod
    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        pass


class AllKeysMatcherTests(MatcherTestsBase):
    """Test AllKeys matcher methods."""

    raw = {
        'matcherType': "ALL_KEYS",
        'negate': False
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed = matchers.from_raw(self.raw)
        assert isinstance(parsed, matchers.AllKeysMatcher)

    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        matcher = matchers.AllKeysMatcher(self.raw)
        assert matcher.evaluate(None) is False
        assert matcher.evaluate('asd') is True
        assert matcher.evaluate('asd', {'a': 1}, {}) is True

    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json = matchers.AllKeysMatcher(self.raw).to_json()
        assert as_json['matcherType'] == 'ALL_KEYS'


class BetweenMatcherTests(MatcherTestsBase):
    """Test in between matcher behaviour."""

    raw_number = {
        'matcherType': 'BETWEEN',
        'negate': False,
        'betweenMatcherData': {
            'start': 1,
            'end': 3,
            'dataType': 'NUMBER'
        }
    }

    raw_date = {
        'matcherType': 'BETWEEN',
        'negate': False,
        'betweenMatcherData': {
            'start': int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 45)).timetuple())) * 1000,
            'end': int(calendar.timegm((datetime(2019, 12, 23, 9, 30, 45)).timetuple())) * 1000,
            'dataType': 'DATETIME'
        }
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed_number = matchers.from_raw(self.raw_number)
        assert isinstance(parsed_number, matchers.BetweenMatcher)
        assert parsed_number._data_type == 'NUMBER'
        assert parsed_number._negate is False
        assert parsed_number._original_lower == 1
        assert parsed_number._original_upper == 3
        assert parsed_number._lower == 1
        assert parsed_number._upper == 3

        parsed_date = matchers.from_raw(self.raw_date)
        assert isinstance(parsed_number, matchers.BetweenMatcher)
        assert parsed_date._data_type == 'DATETIME'
        assert parsed_date._original_lower == int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 45)).timetuple())) * 1000
        assert parsed_date._original_upper == int(calendar.timegm((datetime(2019, 12, 23, 9, 30, 45)).timetuple())) * 1000
        assert parsed_date._lower == int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 0)).timetuple()))
        assert parsed_date._upper == int(calendar.timegm((datetime(2019, 12, 23, 9, 30, 0)).timetuple()))

    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        parsed_number = matchers.BetweenMatcher(self.raw_number)
        assert parsed_number.evaluate(0) is False
        assert parsed_number.evaluate(1) is True
        assert parsed_number.evaluate(2) is True
        assert parsed_number.evaluate(3) is True
        assert parsed_number.evaluate(4) is False
        assert parsed_number.evaluate('a') is False
        assert parsed_number.evaluate([]) is False
        assert parsed_number.evaluate({}) is False
        assert parsed_number.evaluate(True) is False
        assert parsed_number.evaluate(object()) is False


        parsed_date = matchers.BetweenMatcher(self.raw_date)
        assert parsed_date.evaluate(int(calendar.timegm((datetime(2019, 12, 20, 9, 30)).timetuple()))) is False
        assert parsed_date.evaluate(int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 45)).timetuple()))) is True
        assert parsed_date.evaluate(int(calendar.timegm((datetime(2019, 12, 22, 9, 30)).timetuple()))) is True
        assert parsed_date.evaluate(int(calendar.timegm((datetime(2019, 12, 23, 9, 30, 45)).timetuple()))) is True
        assert parsed_date.evaluate(int(calendar.timegm((datetime(2019, 12, 24, 9, 30)).timetuple()))) is False
        assert parsed_date.evaluate('a') is False
        assert parsed_date.evaluate([]) is False
        assert parsed_date.evaluate({}) is False
        assert parsed_date.evaluate(True) is False
        assert parsed_date.evaluate(object()) is False

    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json_number = matchers.BetweenMatcher(self.raw_number).to_json()
        assert as_json_number['betweenMatcherData']['start'] == 1
        assert as_json_number['betweenMatcherData']['end'] == 3
        assert as_json_number['betweenMatcherData']['dataType'] == 'NUMBER'

        as_json_date = matchers.BetweenMatcher(self.raw_date).to_json()
        assert as_json_date['betweenMatcherData']['start'] == int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 45)).timetuple()) * 1000)
        assert as_json_date['betweenMatcherData']['end'] == int(calendar.timegm((datetime(2019, 12, 23, 9, 30, 45)).timetuple()) * 1000)
        assert as_json_date['betweenMatcherData']['dataType'] == 'DATETIME'


class EqualToMatcherTests(MatcherTestsBase):
    """Test equal to matcher."""

    raw_number = {
        'matcherType': 'EQUAL_TO',
        'negate': False,
        'unaryNumericMatcherData': {
            'value': 5,
            'dataType': 'NUMBER'
        }
    }

    raw_date = {
        'matcherType': 'EQUAL_TO',
        'negate': False,
        'unaryNumericMatcherData': {
            'value': int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 45)).timetuple())) * 1000,
            'dataType': 'DATETIME'
        }
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed_number = matchers.from_raw(self.raw_number)
        assert isinstance(parsed_number, matchers.EqualToMatcher)
        assert parsed_number._data_type == 'NUMBER'
        assert parsed_number._original_value == 5
        assert parsed_number._value == 5

        parsed_date = matchers.from_raw(self.raw_date)
        assert isinstance(parsed_date, matchers.EqualToMatcher)
        assert parsed_date._data_type == 'DATETIME'
        assert parsed_date._original_value == int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 45, 0)).timetuple())) * 1000
        assert parsed_date._value == int(calendar.timegm((datetime(2019, 12, 21, 0, 0, 0)).timetuple()))

    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        matcher_number = matchers.EqualToMatcher(self.raw_number)
        assert matcher_number.evaluate(4) is False
        assert matcher_number.evaluate(5) is True
        assert matcher_number.evaluate(6) is False
        assert matcher_number.evaluate('a') is False
        assert matcher_number.evaluate([]) is False
        assert matcher_number.evaluate({}) is False
        assert matcher_number.evaluate(True) is False
        assert matcher_number.evaluate(object()) is False

        matcher_date = matchers.EqualToMatcher(self.raw_date)
        assert matcher_date.evaluate(int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 45)).timetuple()))) is True
        assert matcher_date.evaluate(int(calendar.timegm((datetime(2019, 12, 21, 0, 0, 0)).timetuple()))) is True
        assert matcher_date.evaluate(int(calendar.timegm((datetime(2019, 12, 20, 0, 0, 0)).timetuple()))) is False
        assert matcher_date.evaluate(int(calendar.timegm((datetime(2019, 12, 22, 0, 0, 0)).timetuple()))) is False
        assert matcher_date.evaluate('a') is False
        assert matcher_date.evaluate([]) is False
        assert matcher_date.evaluate({}) is False
        assert matcher_date.evaluate(True) is False
        assert matcher_date.evaluate(object()) is False


    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json_number = matchers.from_raw(self.raw_number).to_json()
        assert as_json_number['unaryNumericMatcherData']['dataType'] == 'NUMBER'
        assert as_json_number['unaryNumericMatcherData']['value'] == 5
        assert as_json_number['matcherType'] == 'EQUAL_TO'
        assert as_json_number['negate'] is False

        as_json_number = matchers.from_raw(self.raw_date).to_json()
        assert as_json_number['unaryNumericMatcherData']['dataType'] == 'DATETIME'
        assert as_json_number['unaryNumericMatcherData']['value'] == int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 45)).timetuple())) * 1000
        assert as_json_number['matcherType'] == 'EQUAL_TO'
        assert as_json_number['negate'] is False


class GreaterOrEqualMatcherTests(MatcherTestsBase):
    """Test greater or equal matcher."""

    raw_number = {
        'matcherType': 'GREATER_THAN_OR_EQUAL_TO',
        'negate': False,
        'unaryNumericMatcherData': {
            'value': 5,
            'dataType': 'NUMBER'
        }
    }

    raw_date = {
        'matcherType': 'GREATER_THAN_OR_EQUAL_TO',
        'negate': False,
        'unaryNumericMatcherData': {
            'value': int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 45)).timetuple())) * 1000,
            'dataType': 'DATETIME'
        }
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed_number = matchers.from_raw(self.raw_number)
        assert isinstance(parsed_number, matchers.GreaterThanOrEqualMatcher)
        assert parsed_number._data_type == 'NUMBER'
        assert parsed_number._original_value == 5
        assert parsed_number._value == 5
        assert parsed_number.evaluate('a') is False
        assert parsed_number.evaluate([]) is False
        assert parsed_number.evaluate({}) is False
        assert parsed_number.evaluate(True) is False
        assert parsed_number.evaluate(object()) is False

        parsed_date = matchers.from_raw(self.raw_date)
        assert isinstance(parsed_date, matchers.GreaterThanOrEqualMatcher)
        assert parsed_date._data_type == 'DATETIME'
        assert parsed_date._original_value == int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 45, 0)).timetuple())) * 1000
        assert parsed_date._value == int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 0)).timetuple()))
        assert parsed_date.evaluate('a') is False
        assert parsed_date.evaluate([]) is False
        assert parsed_date.evaluate({}) is False
        assert parsed_date.evaluate(True) is False
        assert parsed_date.evaluate(object()) is False


    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        matcher_number = matchers.GreaterThanOrEqualMatcher(self.raw_number)
        assert matcher_number.evaluate(4) is False
        assert matcher_number.evaluate(5) is True
        assert matcher_number.evaluate(6) is True
        assert matcher_number.evaluate('a') is False
        assert matcher_number.evaluate([]) is False
        assert matcher_number.evaluate({}) is False
        assert matcher_number.evaluate(True) is False
        assert matcher_number.evaluate(object()) is False

        matcher_date = matchers.GreaterThanOrEqualMatcher(self.raw_date)
        assert matcher_date.evaluate(int(calendar.timegm((datetime(2019, 12, 20, 0, 0, 0)).timetuple()))) is False
        assert matcher_date.evaluate(int(calendar.timegm((datetime(2019, 12, 21, 0, 0, 0)).timetuple()))) is False
        assert matcher_date.evaluate(int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 0)).timetuple()))) is True
        assert matcher_date.evaluate(int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 45)).timetuple()))) is True
        assert matcher_date.evaluate(int(calendar.timegm((datetime(2019, 12, 22, 0, 0, 0)).timetuple()))) is True
        assert matcher_date.evaluate('a') is False
        assert matcher_date.evaluate([]) is False
        assert matcher_date.evaluate({}) is False
        assert matcher_date.evaluate(True) is False
        assert matcher_date.evaluate(object()) is False

    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json_number = matchers.from_raw(self.raw_number).to_json()
        assert as_json_number['unaryNumericMatcherData']['dataType'] == 'NUMBER'
        assert as_json_number['unaryNumericMatcherData']['value'] == 5
        assert as_json_number['matcherType'] == 'GREATER_THAN_OR_EQUAL_TO'
        assert as_json_number['negate'] is False

        as_json_number = matchers.from_raw(self.raw_date).to_json()
        assert as_json_number['unaryNumericMatcherData']['dataType'] == 'DATETIME'
        assert as_json_number['unaryNumericMatcherData']['value'] == int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 45)).timetuple())) * 1000
        assert as_json_number['matcherType'] == 'GREATER_THAN_OR_EQUAL_TO'
        assert as_json_number['negate'] is False


class LessOrEqualMatcherTests(MatcherTestsBase):
    """Test less than or equal matcher."""

    raw_number = {
        'matcherType': 'LESS_THAN_OR_EQUAL_TO',
        'negate': False,
        'unaryNumericMatcherData': {
            'value': 5,
            'dataType': 'NUMBER'
        }
    }

    raw_date = {
        'matcherType': 'LESS_THAN_OR_EQUAL_TO',
        'negate': False,
        'unaryNumericMatcherData': {
            'value': int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 45)).timetuple())) * 1000,
            'dataType': 'DATETIME'
        }
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed_number = matchers.from_raw(self.raw_number)
        assert isinstance(parsed_number, matchers.LessThanOrEqualMatcher)
        assert parsed_number._data_type == 'NUMBER'
        assert parsed_number._original_value == 5
        assert parsed_number._value == 5

        parsed_date = matchers.from_raw(self.raw_date)
        assert isinstance(parsed_date, matchers.LessThanOrEqualMatcher)
        assert parsed_date._data_type == 'DATETIME'
        assert parsed_date._original_value == int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 45, 0)).timetuple())) * 1000
        assert parsed_date._value == int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 0)).timetuple()))

    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        matcher_number = matchers.LessThanOrEqualMatcher(self.raw_number)
        assert matcher_number.evaluate(4) is True
        assert matcher_number.evaluate(5) is True
        assert matcher_number.evaluate(6) is False
        assert matcher_number.evaluate('a') is False
        assert matcher_number.evaluate([]) is False
        assert matcher_number.evaluate({}) is False
        assert matcher_number.evaluate(True) is False
        assert matcher_number.evaluate(object()) is False


        matcher_date = matchers.LessThanOrEqualMatcher(self.raw_date)
        assert matcher_date.evaluate(int(calendar.timegm((datetime(2019, 12, 20, 0, 0, 0)).timetuple()))) is True
        assert matcher_date.evaluate(int(calendar.timegm((datetime(2019, 12, 21, 0, 0, 0)).timetuple()))) is True
        assert matcher_date.evaluate(int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 0)).timetuple()))) is True
        assert matcher_date.evaluate(int(calendar.timegm((datetime(2019, 12, 21, 9, 31, 45)).timetuple()))) is False
        assert matcher_date.evaluate(int(calendar.timegm((datetime(2019, 12, 22, 0, 0, 0)).timetuple()))) is False
        assert matcher_date.evaluate('a') is False
        assert matcher_date.evaluate([]) is False
        assert matcher_date.evaluate({}) is False
        assert matcher_date.evaluate(True) is False
        assert matcher_date.evaluate(object()) is False


    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json_number = matchers.from_raw(self.raw_number).to_json()
        assert as_json_number['unaryNumericMatcherData']['dataType'] == 'NUMBER'
        assert as_json_number['unaryNumericMatcherData']['value'] == 5
        assert as_json_number['matcherType'] == 'LESS_THAN_OR_EQUAL_TO'
        assert as_json_number['negate'] is False

        as_json_number = matchers.from_raw(self.raw_date).to_json()
        assert as_json_number['unaryNumericMatcherData']['dataType'] == 'DATETIME'
        assert as_json_number['unaryNumericMatcherData']['value'] == int(calendar.timegm((datetime(2019, 12, 21, 9, 30, 45)).timetuple())) * 1000
        assert as_json_number['matcherType'] == 'LESS_THAN_OR_EQUAL_TO'
        assert as_json_number['negate'] is False


class UserDefinedSegmentMatcherTests(MatcherTestsBase):
    """Test user defined segment matcher."""

    raw = {
        'matcherType': 'IN_SEGMENT',
        'negate': False,
        'userDefinedSegmentMatcherData': {
            'segmentName': 'some_segment'
        }
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed = matchers.from_raw(self.raw)
        assert isinstance(parsed, matchers.UserDefinedSegmentMatcher)
        assert parsed._segment_name == 'some_segment'

    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        matcher = matchers.UserDefinedSegmentMatcher(self.raw)
        segment_storage = mocker.Mock(spec=SegmentStorage)

        # Test that if the key if the storage wrapper finds the key in the segment, it matches.
        segment_storage.segment_contains.return_value = True
        assert matcher.evaluate('some_key', {}, {'segment_storage': segment_storage}) is True

        # Test that if the key if the storage wrapper doesn't find the key in the segment, it fails.
        segment_storage.segment_contains.return_value = False
        assert matcher.evaluate('some_key', {}, {'segment_storage': segment_storage}) is False

        assert segment_storage.segment_contains.mock_calls == [
            mocker.call('some_segment', 'some_key'),
            mocker.call('some_segment', 'some_key')
        ]

        assert matcher.evaluate([], {}, {'segment_storage': segment_storage}) is False
        assert matcher.evaluate({}, {}, {'segment_storage': segment_storage}) is False
        assert matcher.evaluate(123, {}, {'segment_storage': segment_storage}) is False
        assert matcher.evaluate(True, {}, {'segment_storage': segment_storage}) is False
        assert matcher.evaluate(False, {}, {'segment_storage': segment_storage}) is False

    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json = matchers.UserDefinedSegmentMatcher(self.raw).to_json()
        assert as_json['userDefinedSegmentMatcherData']['segmentName'] == 'some_segment'
        assert as_json['matcherType'] == 'IN_SEGMENT'
        assert as_json['negate'] is False


class WhitelistMatcherTests(MatcherTestsBase):
    """Test whitelist matcher."""

    raw = {
        'matcherType': 'WHITELIST',
        'negate': False,
        'whitelistMatcherData': {
            'whitelist': ['key1', 'key2', 'key3'],
        }
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed = matchers.from_raw(self.raw)
        assert isinstance(parsed, matchers.WhitelistMatcher)
        assert parsed._whitelist == frozenset(['key1', 'key2', 'key3'])

    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        matcher = matchers.WhitelistMatcher(self.raw)
        assert matcher.evaluate('key1') is True
        assert matcher.evaluate('key2') is True
        assert matcher.evaluate('key3') is True
        assert matcher.evaluate('key4') is False
        assert matcher.evaluate(None) is False

        assert matcher.evaluate([]) is False
        assert matcher.evaluate({}) is False
        assert matcher.evaluate(123) is False
        assert matcher.evaluate(True) is False
        assert matcher.evaluate(False) is False

    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json = matchers.WhitelistMatcher(self.raw).to_json()
        assert 'key1' in as_json['whitelistMatcherData']['whitelist']
        assert 'key2' in as_json['whitelistMatcherData']['whitelist']
        assert 'key3' in as_json['whitelistMatcherData']['whitelist']
        assert as_json['matcherType'] == 'WHITELIST'
        assert as_json['negate'] is False


class StartsWithMatcherTests(MatcherTestsBase):
    """Test StartsWith matcher."""

    raw = {
        'matcherType': 'STARTS_WITH',
        'negate': False,
        'whitelistMatcherData': {
            'whitelist': ['key1', 'key2', 'key3'],
        }
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed = matchers.from_raw(self.raw)
        assert isinstance(parsed, matchers.StartsWithMatcher)
        assert parsed._whitelist == frozenset(['key1', 'key2', 'key3'])

    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        matcher = matchers.StartsWithMatcher(self.raw)
        assert matcher.evaluate('key1AA') is True
        assert matcher.evaluate('key2BB') is True
        assert matcher.evaluate('key3CC') is True
        assert matcher.evaluate('key4DD') is False
        assert matcher.evaluate('Akey1A') is False
        assert matcher.evaluate(None) is False
        assert matcher.evaluate([]) is False
        assert matcher.evaluate({}) is False
        assert matcher.evaluate(123) is False
        assert matcher.evaluate(True) is False
        assert matcher.evaluate(False) is False

    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json = matchers.StartsWithMatcher(self.raw).to_json()
        assert 'key1' in as_json['whitelistMatcherData']['whitelist']
        assert 'key2' in as_json['whitelistMatcherData']['whitelist']
        assert 'key3' in as_json['whitelistMatcherData']['whitelist']
        assert as_json['matcherType'] == 'STARTS_WITH'
        assert as_json['negate'] is False


class EndsWithMatcherTests(MatcherTestsBase):
    """Test EndsWith matcher."""

    raw = {
        'matcherType': 'ENDS_WITH',
        'negate': False,
        'whitelistMatcherData': {
            'whitelist': ['key1', 'key2', 'key3'],
        }
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed = matchers.from_raw(self.raw)
        assert isinstance(parsed, matchers.EndsWithMatcher)
        assert parsed._whitelist == frozenset(['key1', 'key2', 'key3'])

    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        matcher = matchers.EndsWithMatcher(self.raw)
        assert matcher.evaluate('AAkey1') is True
        assert matcher.evaluate('BBkey2') is True
        assert matcher.evaluate('CCkey3') is True
        assert matcher.evaluate('DDkey4') is False
        assert matcher.evaluate('Akey1A') is False
        assert matcher.evaluate(None) is False
        assert matcher.evaluate([]) is False
        assert matcher.evaluate({}) is False
        assert matcher.evaluate(123) is False
        assert matcher.evaluate(True) is False
        assert matcher.evaluate(False) is False

    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json = matchers.EndsWithMatcher(self.raw).to_json()
        assert 'key1' in as_json['whitelistMatcherData']['whitelist']
        assert 'key2' in as_json['whitelistMatcherData']['whitelist']
        assert 'key3' in as_json['whitelistMatcherData']['whitelist']
        assert as_json['matcherType'] == 'ENDS_WITH'
        assert as_json['negate'] is False


class ContainsStringMatcherTests(MatcherTestsBase):
    """Test string matcher."""

    raw = {
        'matcherType': 'CONTAINS_STRING',
        'negate': False,
        'whitelistMatcherData': {
            'whitelist': ['key1', 'key2', 'key3'],
        }
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed = matchers.from_raw(self.raw)
        assert isinstance(parsed, matchers.ContainsStringMatcher)
        assert parsed._whitelist == frozenset(['key1', 'key2', 'key3'])

    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        matcher = matchers.ContainsStringMatcher(self.raw)
        assert matcher.evaluate('AAkey1') is True
        assert matcher.evaluate('BBkey2') is True
        assert matcher.evaluate('CCkey3') is True
        assert matcher.evaluate('Akey1A') is True
        assert matcher.evaluate('DDkey4') is False
        assert matcher.evaluate('asdsad') is False
        assert matcher.evaluate(None) is False
        assert matcher.evaluate([]) is False
        assert matcher.evaluate({}) is False
        assert matcher.evaluate(123) is False
        assert matcher.evaluate(True) is False
        assert matcher.evaluate(False) is False

    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json = matchers.ContainsStringMatcher(self.raw).to_json()
        assert 'key1' in as_json['whitelistMatcherData']['whitelist']
        assert 'key2' in as_json['whitelistMatcherData']['whitelist']
        assert 'key3' in as_json['whitelistMatcherData']['whitelist']
        assert as_json['matcherType'] == 'CONTAINS_STRING'
        assert as_json['negate'] is False


class AllOfSetMatcherTests(MatcherTestsBase):
    """Test all of set matcher."""

    raw = {
        'matcherType': 'CONTAINS_ALL_OF_SET',
        'negate': False,
        'whitelistMatcherData': {
            'whitelist': ['key1', 'key2', 'key3'],
        }
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed = matchers.from_raw(self.raw)
        assert isinstance(parsed, matchers.ContainsAllOfSetMatcher)
        assert parsed._whitelist == frozenset(['key1', 'key2', 'key3'])

    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        matcher = matchers.ContainsAllOfSetMatcher(self.raw)
        assert matcher.evaluate(['key1', 'key2', 'key3']) is True
        assert matcher.evaluate(['key1', 'key2', 'key3', 'key4']) is True
        assert matcher.evaluate(['key4', 'key3', 'key1', 'key5', 'key2']) is True
        assert matcher.evaluate(['key1', 'key2']) is False
        assert matcher.evaluate([]) is False
        assert matcher.evaluate('asdsad') is False
        assert matcher.evaluate(3) is False
        assert matcher.evaluate(None) is False
        assert matcher.evaluate({}) is False
        assert matcher.evaluate(object()) is False
        assert matcher.evaluate(True) is False

    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json = matchers.ContainsAllOfSetMatcher(self.raw).to_json()
        assert 'key1' in as_json['whitelistMatcherData']['whitelist']
        assert 'key2' in as_json['whitelistMatcherData']['whitelist']
        assert 'key3' in as_json['whitelistMatcherData']['whitelist']
        assert as_json['matcherType'] == 'CONTAINS_ALL_OF_SET'
        assert as_json['negate'] is False


class AnyOfSetMatcherTests(MatcherTestsBase):
    """Test any of set matcher."""

    raw = {
        'matcherType': 'CONTAINS_ANY_OF_SET',
        'negate': False,
        'whitelistMatcherData': {
            'whitelist': ['key1', 'key2', 'key3'],
        }
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed = matchers.from_raw(self.raw)
        assert isinstance(parsed, matchers.ContainsAnyOfSetMatcher)
        assert parsed._whitelist == frozenset(['key1', 'key2', 'key3'])

    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        matcher = matchers.ContainsAnyOfSetMatcher(self.raw)
        assert matcher.evaluate(['key1', 'key2', 'key3']) is True
        assert matcher.evaluate(['key1', 'key2', 'key3', 'key4']) is True
        assert matcher.evaluate(['key4', 'key3', 'key1', 'key5', 'key2']) is True
        assert matcher.evaluate(['key1', 'key2']) is True
        assert matcher.evaluate([]) is False
        assert matcher.evaluate('asdsad') is False
        assert matcher.evaluate(3) is False
        assert matcher.evaluate(None) is False
        assert matcher.evaluate({}) is False
        assert matcher.evaluate(object()) is False
        assert matcher.evaluate(True) is False

    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json = matchers.ContainsAnyOfSetMatcher(self.raw).to_json()
        assert 'key1' in as_json['whitelistMatcherData']['whitelist']
        assert 'key2' in as_json['whitelistMatcherData']['whitelist']
        assert 'key3' in as_json['whitelistMatcherData']['whitelist']
        assert as_json['matcherType'] == 'CONTAINS_ANY_OF_SET'
        assert as_json['negate'] is False


class EqualToSetMatcherTests(MatcherTestsBase):
    """Test equal to set matcher."""

    raw = {
        'matcherType': 'EQUAL_TO_SET',
        'negate': False,
        'whitelistMatcherData': {
            'whitelist': ['key1', 'key2', 'key3'],
        }
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed = matchers.from_raw(self.raw)
        assert isinstance(parsed, matchers.EqualToSetMatcher)
        assert parsed._whitelist == frozenset(['key1', 'key2', 'key3'])

    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        matcher = matchers.EqualToSetMatcher(self.raw)
        assert matcher.evaluate(['key1', 'key2', 'key3']) is True
        assert matcher.evaluate(['key3', 'key2', 'key1']) is True
        assert matcher.evaluate(['key1', 'key2', 'key3', 'key4']) is False
        assert matcher.evaluate(['key4', 'key3', 'key1', 'key5', 'key2']) is False
        assert matcher.evaluate(['key1', 'key2']) is False
        assert matcher.evaluate([]) is False
        assert matcher.evaluate('asdsad') is False
        assert matcher.evaluate(3) is False
        assert matcher.evaluate(None) is False

    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json = matchers.EqualToSetMatcher(self.raw).to_json()
        assert 'key1' in as_json['whitelistMatcherData']['whitelist']
        assert 'key2' in as_json['whitelistMatcherData']['whitelist']
        assert 'key3' in as_json['whitelistMatcherData']['whitelist']
        assert as_json['matcherType'] == 'EQUAL_TO_SET'
        assert as_json['negate'] is False


class PartOfSetMatcherTests(MatcherTestsBase):
    """Test part of set matcher."""

    raw = {
        'matcherType': 'PART_OF_SET',
        'negate': False,
        'whitelistMatcherData': {
            'whitelist': ['key1', 'key2', 'key3'],
        }
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed = matchers.from_raw(self.raw)
        assert isinstance(parsed, matchers.PartOfSetMatcher)
        assert parsed._whitelist == frozenset(['key1', 'key2', 'key3'])

    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        matcher = matchers.PartOfSetMatcher(self.raw)
        assert matcher.evaluate(['key1', 'key2', 'key3']) is True
        assert matcher.evaluate(['key3', 'key2', 'key1']) is True
        assert matcher.evaluate(['key1']) is True
        assert matcher.evaluate(['key1', 'key2']) is True
        assert matcher.evaluate(['key4', 'key3', 'key1', 'key5', 'key2']) is False
        assert matcher.evaluate([]) is False
        assert matcher.evaluate('asdsad') is False
        assert matcher.evaluate(3) is False
        assert matcher.evaluate(None) is False
        assert matcher.evaluate({}) is False
        assert matcher.evaluate(object()) is False
        assert matcher.evaluate(True) is False

    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json = matchers.PartOfSetMatcher(self.raw).to_json()
        assert 'key1' in as_json['whitelistMatcherData']['whitelist']
        assert 'key2' in as_json['whitelistMatcherData']['whitelist']
        assert 'key3' in as_json['whitelistMatcherData']['whitelist']
        assert as_json['matcherType'] == 'PART_OF_SET'
        assert as_json['negate'] is False


class DependencyMatcherTests(MatcherTestsBase):
    """tests for dependency matcher."""

    raw = {
        'matcherType': 'IN_SPLIT_TREATMENT',
        'negate': False,
        'dependencyMatcherData': {
            'split': 'some_split',
            'treatments': ['on', 'almost_on']
        }
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed = matchers.from_raw(self.raw)
        assert isinstance(parsed, matchers.DependencyMatcher)
        assert parsed._split_name == 'some_split'
        assert parsed._treatments == ['on', 'almost_on']

    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        parsed = matchers.DependencyMatcher(self.raw)
        evaluator = mocker.Mock(spec=Evaluator)

        evaluator.evaluate_feature.return_value = {'treatment': 'on'}
        assert parsed.evaluate('test1', {}, {'bucketing_key': 'buck', 'evaluator': evaluator}) is True

        evaluator.evaluate_feature.return_value = {'treatment': 'off'}
        assert parsed.evaluate('test1', {}, {'bucketing_key': 'buck', 'evaluator': evaluator}) is False

        assert evaluator.evaluate_feature.mock_calls == [
            mocker.call('some_split', 'test1', 'buck', {}),
            mocker.call('some_split', 'test1', 'buck', {})
        ]

        assert parsed.evaluate([], {}, {'bucketing_key': 'buck', 'evaluator': evaluator}) is False
        assert parsed.evaluate({}, {}, {'bucketing_key': 'buck', 'evaluator': evaluator}) is False
        assert parsed.evaluate(123, {}, {'bucketing_key': 'buck', 'evaluator': evaluator}) is False
        assert parsed.evaluate(object(), {}, {'bucketing_key': 'buck', 'evaluator': evaluator}) is False

    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json = matchers.DependencyMatcher(self.raw).to_json()
        assert as_json['matcherType'] == 'IN_SPLIT_TREATMENT'
        assert as_json['dependencyMatcherData']['split'] == 'some_split'
        assert as_json['dependencyMatcherData']['treatments'] == ['on', 'almost_on']


class BooleanMatcherTests(MatcherTestsBase):
    """Boolean matcher test cases."""

    raw = {
        'negate': False,
        'matcherType': 'EQUAL_TO_BOOLEAN',
        'booleanMatcherData': True
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed = matchers.from_raw(self.raw)
        assert isinstance(parsed, matchers.BooleanMatcher)
        assert parsed._data

    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        parsed = matchers.BooleanMatcher(self.raw)
        assert parsed.evaluate(True) is True
        assert parsed.evaluate('true') is True
        assert parsed.evaluate('True') is True
        assert parsed.evaluate('tRUe') is True
        assert parsed.evaluate('dasd') is False
        assert parsed.evaluate(123) is False
        assert parsed.evaluate(None) is False

    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json = matchers.BooleanMatcher(self.raw).to_json()
        assert as_json['matcherType'] == 'EQUAL_TO_BOOLEAN'
        assert as_json['booleanMatcherData']


class RegexMatcherTests(MatcherTestsBase):
    """Regex matcher test cases."""

    raw = {
        'negate': False,
        'matcherType': 'MATCHES_STRING',
        'stringMatcherData': "^[a-z][A-Z][0-9]$"
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed = matchers.from_raw(self.raw)
        assert isinstance(parsed, matchers.RegexMatcher)
        assert parsed._data == "^[a-z][A-Z][0-9]$"
        assert parsed._regex == re.compile("^[a-z][A-Z][0-9]$")

    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        filename = os.path.join(os.path.dirname(__file__), 'files', 'regex.txt')
        with open(filename, 'r') as flo:
            test_cases = flo.read().split('\n')
            for test_case in test_cases:
                if not test_case:
                    continue

                regex, string, should_match = test_case.split('#')
                raw = {
                    'negate': False,
                    'matcherType': 'MATCHES_STRING',
                    'stringMatcherData': regex
                }

                parsed = matchers.RegexMatcher(raw)
                assert parsed.evaluate(string) == json.loads(should_match)

    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json = matchers.RegexMatcher(self.raw).to_json()
        assert as_json['matcherType'] == 'MATCHES_STRING'
        assert as_json['stringMatcherData'] == "^[a-z][A-Z][0-9]$"

class EqualToSemverMatcherTests(MatcherTestsBase):
    """Semver equalto matcher test cases."""

    raw = {
        'negate': False,
        'matcherType': 'EQUAL_TO_SEMVER',
        'stringMatcherData': "2.1.8"
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed = matchers.from_raw(self.raw)
        assert isinstance(parsed, matchers.EqualToSemverMatcher)
        assert parsed._data == "2.1.8"
        assert isinstance(parsed._semver, Semver)
        assert parsed._semver._major == 2
        assert parsed._semver._minor == 1
        assert parsed._semver._patch == 8
        assert parsed._semver._pre_release == []

    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        parsed = matchers.from_raw(self.raw)
        assert parsed._match("2.1.8+rc")
        assert parsed._match("2.1.8")
        assert not parsed._match("2.1.5")
        assert not parsed._match("2.1.5-rc1")

    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json = matchers.EqualToSemverMatcher(self.raw).to_json()
        assert as_json['matcherType'] == 'EQUAL_TO_SEMVER'
        assert as_json['stringMatcherData'] == "2.1.8"

    def test_to_str(self):
        """Test that the object serializes to str properly."""
        as_str = matchers.EqualToSemverMatcher(self.raw)
        assert str(as_str) == "equal semver 2.1.8"

class GreaterThanOrEqualToSemverMatcherTests(MatcherTestsBase):
    """Semver greater or equalto matcher test cases."""

    raw = {
        'negate': False,
        'matcherType': 'GREATER_THAN_OR_EQUAL_TO_SEMVER',
        'stringMatcherData': "2.1.8"
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed = matchers.from_raw(self.raw)
        assert isinstance(parsed, matchers.GreaterThanOrEqualToSemverMatcher)
        assert parsed._data == "2.1.8"
        assert isinstance(parsed._semver, Semver)
        assert parsed._semver._major == 2
        assert parsed._semver._minor == 1
        assert parsed._semver._patch == 8
        assert parsed._semver._pre_release == []

    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        parsed = matchers.from_raw(self.raw)
        assert parsed._match("2.1.8+rc")
        assert parsed._match("2.1.8")
        assert parsed._match("2.1.11")
        assert not parsed._match("2.1.5")
        assert not parsed._match("2.1.5-rc1")

    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json = matchers.GreaterThanOrEqualToSemverMatcher(self.raw).to_json()
        assert as_json['matcherType'] == 'GREATER_THAN_OR_EQUAL_TO_SEMVER'
        assert as_json['stringMatcherData'] == "2.1.8"

    def test_to_str(self):
        """Test that the object serializes to str properly."""
        as_str = matchers.GreaterThanOrEqualToSemverMatcher(self.raw)
        assert str(as_str) == "greater than or equal to semver 2.1.8"

class LessThanOrEqualToSemverMatcherTests(MatcherTestsBase):
    """Semver less or equalto matcher test cases."""

    raw = {
        'negate': False,
        'matcherType': 'LESS_THAN_OR_EQUAL_TO_SEMVER',
        'stringMatcherData': "2.1.8"
    }

    def test_from_raw(self, mocker):
        """Test parsing from raw json/dict."""
        parsed = matchers.from_raw(self.raw)
        assert isinstance(parsed, matchers.LessThanOrEqualToSemverMatcher)
        assert parsed._data == "2.1.8"
        assert isinstance(parsed._semver, Semver)
        assert parsed._semver._major == 2
        assert parsed._semver._minor == 1
        assert parsed._semver._patch == 8
        assert parsed._semver._pre_release == []

    def test_matcher_behaviour(self, mocker):
        """Test if the matcher works properly."""
        parsed = matchers.from_raw(self.raw)
        assert parsed._match("2.1.8+rc")
        assert parsed._match("2.1.8")
        assert not parsed._match("2.1.11")
        assert parsed._match("2.1.5")
        assert parsed._match("2.1.5-rc1")

    def test_to_json(self):
        """Test that the object serializes to JSON properly."""
        as_json = matchers.LessThanOrEqualToSemverMatcher(self.raw).to_json()
        assert as_json['matcherType'] == 'LESS_THAN_OR_EQUAL_TO_SEMVER'
        assert as_json['stringMatcherData'] == "2.1.8"

    def test_to_str(self):
        """Test that the object serializes to str properly."""
        as_str = matchers.LessThanOrEqualToSemverMatcher(self.raw)
        assert str(as_str) == "less than or equal to semver 2.1.8"
