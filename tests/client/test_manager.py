"""SDK main manager test module."""
import pytest
import queue
import asyncio

from splitio.client.factory import SplitFactory
from splitio.client.manager import SplitManager, SplitManagerAsync, _LOGGER as _logger
from splitio.models import splits
from splitio.storage.inmemory import InMemorySplitStorage, InMemorySplitStorageAsync
from harness_commons.storage.inmemmory import InMemoryTelemetryStorage, InMemoryTelemetryStorageAsync
from harness_commons.engine.impressions.impressions import Manager as ImpressionManager
from harness_commons.engine.telemetry import TelemetryStorageProducer, TelemetryStorageProducerAsync, TelemetryStorageConsumer, TelemetryStorageConsumerAsync
from harness_commons.recorder.recorder import StandardRecorder, StandardRecorderAsync
from tests.integration import splits_json

class SplitManagerTests(object):  # pylint: disable=too-few-public-methods
    """Split manager test cases."""

    def test_manager_calls(self, mocker):
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        events_queue = queue.Queue()
        storage = InMemorySplitStorage(events_queue)

        factory = mocker.Mock(spec=SplitFactory)
        factory._storages = {'split': storage}
        factory._telemetry_init_producer = telemetry_producer._telemetry_init_producer
        factory.destroyed = False
        factory._waiting_fork.return_value = False
        factory.ready = True

        manager = SplitManager(factory)
        split1 =  splits.from_raw(splits_json["splitChange1_1"]['ff']['d'][0])
        split2 =  splits.from_raw(splits_json["splitChange1_3"]['ff']['d'][0])
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
            mocker.Mock(),
            mocker.Mock(),
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


class SplitManagerAsyncTests(object):  # pylint: disable=too-few-public-methods
    """Split manager test cases."""

    @pytest.mark.asyncio
    async def test_manager_calls(self, mocker):
        internal_events_queue = asyncio.Queue()
        telemetry_storage = InMemoryTelemetryStorageAsync()
        telemetry_producer = TelemetryStorageProducerAsync(telemetry_storage)
        storage = InMemorySplitStorageAsync(internal_events_queue)

        factory = mocker.Mock(spec=SplitFactory)
        factory._storages = {'split': storage}
        factory._telemetry_init_producer = telemetry_producer._telemetry_init_producer
        factory.destroyed = False
        factory._waiting_fork.return_value = False
        factory.ready = True

        manager = SplitManagerAsync(factory)
        split1 =  splits.from_raw(splits_json["splitChange1_1"]['ff']['d'][0])
        split2 =  splits.from_raw(splits_json["splitChange1_3"]['ff']['d'][0])
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
            mocker.Mock(),
            mocker.Mock(),
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
        mocker.patch('splitio.client.manager._LOGGER', new=_logger)

        assert await manager.split_names() == []
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert await manager.split('some_feature') is None
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()

        assert await manager.splits() == []
        assert _logger.error.mock_calls == expected_msg
        _logger.reset_mock()
