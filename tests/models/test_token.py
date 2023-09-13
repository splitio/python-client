"""Split model tests module."""

from splitio.models import token
from splitio.models.grammar.condition import Condition


class TokenTests(object):
    """Token model tests."""
    raw_false = {'pushEnabled': False}

    def test_from_raw_false(self):
        """Test token model parsing."""
        parsed = token.from_raw(self.raw_false)
        assert parsed.push_enabled == False
        assert parsed.iat == None
        assert parsed.channels == None
        assert parsed.exp == None
        assert parsed.token == None

    raw_empty = {
        'pushEnabled': True,
        'token': '',
    }

    def test_from_raw_empty(self):
        """Test token model parsing."""
        parsed = token.from_raw(self.raw_empty)
        assert parsed.push_enabled == False
        assert parsed.iat == None
        assert parsed.channels == None
        assert parsed.exp == None
        assert parsed.token == None

    raw_ok = {
        'pushEnabled': True,
        'token': 'eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE2MDIwODgxMjcsImlhdCI6MTYwMjA4NDUyN30.5_MjWonhs6yoFhw44hNJm3H7_YMjXpSW105DwjjppqE',
    }

    def test_from_raw(self):
        """Test token model parsing."""
        parsed = token.from_raw(self.raw_ok)
        assert isinstance(parsed, token.Token)
        assert parsed.push_enabled == True
        assert parsed.iat == 1602084527
        assert parsed.exp == 1602088127
        assert parsed.channels['NzM2MDI5Mzc0_MTgyNTg1MTgwNg==_segments'] == ['subscribe']
        assert parsed.channels['NzM2MDI5Mzc0_MTgyNTg1MTgwNg==_splits'] == ['subscribe']
        assert parsed.channels['control_pri'] == ['subscribe', 'channel-metadata:publishers']
        assert parsed.channels['control_sec'] == ['subscribe', 'channel-metadata:publishers']
