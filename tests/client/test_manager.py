"""SDK main manager test module."""
import pytest

from splitio.client.factory import SplitFactory
from splitio.client.manager import SplitManager, SplitManagerAsync
from splitio.models import splits
from splitio.storage.inmemmory import InMemoryTelemetryStorage, InMemoryTelemetryStorageAsync, InMemorySplitStorage, InMemorySplitStorageAsync
from splitio.engine.impressions.impressions import Manager as ImpressionManager
from splitio.engine.telemetry import TelemetryStorageProducer, TelemetryStorageProducerAsync, TelemetryStorageConsumer, TelemetryStorageConsumerAsync
from splitio.recorder.recorder import StandardRecorder, StandardRecorderAsync
from tests.integration import splits_json


class SplitManagerTests(object):  # pylint: disable=too-few-public-methods
    """Split manager test cases."""

    def test_manager_calls(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        storage = InMemorySplitStorage()

        factory = mocker.Mock(spec=SplitFactory)
        factory._storages = {'split': storage}
        factory._telemetry_init_producer = telemetry_producer._telemetry_init_producer
        factory.destroyed = False
        factory._waiting_fork.return_value = False
        factory.ready = True

        manager = SplitManager(factory)
        assert manager._LOGGER.name == 'splitio.client.manager'
        split1 =  splits.from_raw(splits_json["splitChange1_1"]["splits"][0])
        split2 =  splits.from_raw(splits_json["splitChange1_3"]["splits"][0])
        storage.update([split1, split2], [], -1)
        manager._storage = storage

        assert manager.split_names() == ['SPLIT_2', 'SPLIT_1']
        assert manager.split('SPLIT_3') is None
        assert manager.split('SPLIT_2') == split1.to_split_view()
        assert manager.splits() == [split.to_split_view() for split in storage.get_all_splits()]

    def test_evaluations_before_running_post_fork(self, mocker):
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        recorder = StandardRecorder(impmanager, mocker.Mock(), mocker.Mock(), telemetry_producer.get_telemetry_evaluation_producer(),
                                    telemetry_producer.get_telemetry_runtime_producer())
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
        manager._LOGGER = _logger

        assert manager.split_names() == []
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert manager.split('some_feature') is None
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert manager.splits() == []
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()


class SplitManagerAsyncTests(object):  # pylint: disable=too-few-public-methods
    """Split manager test cases."""

    @pytest.mark.asyncio
    async def test_manager_calls(self, mocker):
        telemetry_storage = InMemoryTelemetryStorageAsync()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        storage = InMemorySplitStorageAsync()

        factory = mocker.Mock(spec=SplitFactory)
        factory._storages = {'split': storage}
        factory._telemetry_init_producer = telemetry_producer._telemetry_init_producer
        factory.destroyed = False
        factory._waiting_fork.return_value = False
        factory.ready = True

        manager = SplitManagerAsync(factory)
        assert manager._LOGGER.name == 'asyncio'
        split1 =  splits.from_raw(splits_json["splitChange1_1"]["splits"][0])
        split2 =  splits.from_raw(splits_json["splitChange1_3"]["splits"][0])
        await storage.update([split1, split2], [], -1)
        manager._storage = storage

        assert await manager.split_names() == ['SPLIT_2', 'SPLIT_1']
        assert await manager.split('SPLIT_3') is None
        assert await manager.split('SPLIT_2') == split1.to_split_view()
        assert await manager.splits() == [split.to_split_view() for split in await storage.get_all_splits()]

    @pytest.mark.asyncio
    async def test_evaluations_before_running_post_fork(self, mocker):
        destroyed_property = mocker.PropertyMock()
        destroyed_property.return_value = False

        impmanager = mocker.Mock(spec=ImpressionManager)
        telemetry_storage = InMemoryTelemetryStorageAsync()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        recorder = StandardRecorderAsync(impmanager, mocker.Mock(), mocker.Mock(), telemetry_producer.get_telemetry_evaluation_producer(),
                                         telemetry_producer.get_telemetry_runtime_producer())
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

        manager = SplitManagerAsync(factory)
        _logger = mocker.Mock()
        manager._LOGGER = _logger

        assert await manager.split_names() == []
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert await manager.split('some_feature') is None
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert await manager.splits() == []
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()
