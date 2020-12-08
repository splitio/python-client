"""SDK main manager test module."""

from splitio.client.factory import SplitFactory
from splitio.client.manager import SplitManager, _LOGGER as _logger


class ManagerTests(object):  # pylint: disable=too-few-public-methods
    """Split manager test cases."""

    def test_evaluations_before_running_post_fork(self, mocker):
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        factory = mocker.Mock(spec=SplitFactory)
        factory._waiting_fork.return_value = True
        type(factory).destroyed = destroyed_property

        expected_msg = [
            mocker.call('Client is not ready - no calls possible')
        ]

        manager = SplitManager(factory)
        _logger = mocker.Mock()
        mocker.patch('splitio.client.manager._LOGGER', new=_logger)

        assert manager.split_names() == []
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert manager.split('some_feature') is None
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert manager.splits() == []
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()
