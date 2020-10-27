"""Backoff unit tests."""
from splitio.util.backoff import Backoff


class BackOffTests(object):  # pylint:disable=too-few-public-methods
    """Backoff test cases."""

    def test_basic_functionality(self):  # pylint:disable=no-self-use
        """Test basic working."""
        backoff = Backoff()
        assert backoff.get() == 1
        assert backoff.get() == 2
        assert backoff.get() == 4
        assert backoff.get() == 8
        assert backoff.get() == 16
        assert backoff.get() == 32
        assert backoff.get() == 64
        assert backoff.get() == 128
        assert backoff.get() == 256
        assert backoff.get() == 512
        assert backoff.get() == 1024

        # assert that it's limited to 30 minutes
        assert backoff.get() == 1800
        assert backoff.get() == 1800
        assert backoff.get() == 1800
        assert backoff.get() == 1800

        # assert that resetting begins on 1
        backoff.reset()
        assert backoff.get() == 1
        assert backoff.get() == 2
        assert backoff.get() == 4
        assert backoff.get() == 8
        assert backoff.get() == 16
        assert backoff.get() == 32
        assert backoff.get() == 64
        assert backoff.get() == 128
        assert backoff.get() == 256
        assert backoff.get() == 512
        assert backoff.get() == 1024
        assert backoff.get() == 1800
        assert backoff.get() == 1800
        assert backoff.get() == 1800
        assert backoff.get() == 1800
