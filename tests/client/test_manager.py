"""SDK main manager test module."""

from splitio.client.factory import SplitFactory
from splitio.client.manager import SplitManager, _LOGGER as _logger
from splitio.storage import SplitStorage, EventStorage, ImpressionStorage, SegmentStorage
from splitio.storage.inmemmory import InMemoryTelemetryStorage
from splitio.engine.impressions.impressions import Manager as ImpressionManager
from splitio.engine.telemetry import TelemetryStorageProducer, TelemetryStorageConsumer
from splitio.recorder.recorder import StandardRecorder


class ManagerTests(object):  # pylint: disable=too-few-public-methods
    """Split manager test cases."""

    def test_evaluations_before_running_post_fork(self, mocker):
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_consumer = TelemetryStorageConsumer(telemetry_storage)
        recorder = StandardRecorder(impmanager, mocker.Mock(), mocker.Mock(), telemetry_producer.get_telemetry_evaluation_producer())
        factory = SplitFactory(mocker.Mock(),
            {'splits': mocker.Mock(),
            'segments': mocker.Mock(),
            'impressions': mocker.Mock(),
            'events': mocker.Mock()},
            mocker.Mock(),
            recorder,
            impmanager,
            mocker.Mock(),
            telemetry_producer,
            telemetry_consumer.get_telemetry_init_consumer(),
            mocker.Mock(),
            True
        )

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
