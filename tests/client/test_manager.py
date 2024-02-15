"""SDK main manager test module."""
import pytest

from splitio.client.factory import SplitFactory
from splitio.client.manager import SplitManager, _LOGGER as _logger
from splitio.storage import SplitStorage, EventStorage, ImpressionStorage, SegmentStorage
from splitio.storage.inmemmory import InMemoryTelemetryStorage, InMemorySplitStorage
from splitio.models import splits
from splitio.engine.impressions.impressions import Manager as ImpressionManager
from splitio.engine.telemetry import TelemetryStorageProducer, TelemetryStorageConsumer
from splitio.recorder.recorder import StandardRecorder
from tests.models.test_splits import SplitTests

class ManagerTests(object):  # pylint: disable=too-few-public-methods
    """Split manager test cases."""

    def test_evaluations_before_running_post_fork(self, mocker):
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
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
            telemetry_producer.get_telemetry_init_producer(),
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

    def test_manager_calls(self, mocker):
        split_storage = InMemorySplitStorage()
        split = splits.from_raw(SplitTests.raw)
        split_storage.update([split], [], 123)
        factory = SplitFactory(mocker.Mock(),
            {'splits': split_storage,
            'segments': mocker.Mock(),
            'impressions': mocker.Mock(),
            'events': mocker.Mock()},
            mocker.Mock(),
            mocker.Mock(),
            mocker.Mock(),
            mocker.Mock(),
            mocker.Mock(),
            mocker.Mock(),
            mocker.Mock(),
            False
        )
        manager = SplitManager(factory)
        splits_view = manager.splits()
        self._verify_split(splits_view[0])
        assert manager.split_names() == ['some_name']
        split_view = manager.split('some_name')
        self._verify_split(split_view)
        split2 = SplitTests.raw.copy()
        split2['sets'] = None
        split2['name'] = 'no_sets_split'
        split_storage.update([splits.from_raw(split2)], [], 123)

        split_view = manager.split('no_sets_split')
        assert split_view.sets == []

    def _verify_split(self, split):
        assert split.name == 'some_name'
        assert split.traffic_type == 'user'
        assert split.killed == False
        assert sorted(split.treatments) == ['off', 'on']
        assert split.change_number == 123
        assert split.configs == {'on': '{"color": "blue", "size": 13}'}
        assert sorted(split.sets) == ['set1', 'set2']
