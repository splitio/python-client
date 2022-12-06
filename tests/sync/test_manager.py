"""Manager tests."""

import threading
import unittest.mock as mock
import time

from splitio.api.auth import AuthAPI
from splitio.api import auth, client, APIException
from splitio.client.util import get_metadata
from splitio.client.config import DEFAULT_CONFIG
from splitio.tasks.split_sync import SplitSynchronizationTask
from splitio.tasks.segment_sync import SegmentSynchronizationTask
from splitio.tasks.impressions_sync import ImpressionsSyncTask, ImpressionsCountSyncTask
from splitio.tasks.events_sync import EventsSyncTask
from splitio.engine.telemetry import TelemetryStorageProducer
from splitio.storage.inmemmory import InMemoryTelemetryStorage
from splitio.models.telemetry import SSESyncMode, StreamingEventTypes
from splitio.push.manager import Status

from splitio.sync.split import SplitSynchronizer
from splitio.sync.segment import SegmentSynchronizer
from splitio.sync.impression import ImpressionSynchronizer, ImpressionsCountSynchronizer
from splitio.sync.event import EventSynchronizer
from splitio.sync.synchronizer import Synchronizer, SplitTasks, SplitSynchronizers, RedisSynchronizer
from splitio.sync.manager import Manager, RedisManager

from splitio.storage import SplitStorage

from splitio.api import APIException

from splitio.client.util import SdkMetadata


class SyncManagerTests(object):
    """Synchronizer Manager tests."""

    def test_error(self, mocker):
        split_task = mocker.Mock(spec=SplitSynchronizationTask)
        split_tasks = SplitTasks(split_task, mocker.Mock(), mocker.Mock(), mocker.Mock(),
                                 mocker.Mock(), mocker.Mock())

        storage = mocker.Mock(spec=SplitStorage)
        api = mocker.Mock()

        def run(x):
            raise APIException("something broke")

        api.fetch_splits.side_effect = run
        storage.get_change_number.return_value = -1

        split_sync = SplitSynchronizer(api, storage)
        synchronizers = SplitSynchronizers(split_sync, mocker.Mock(), mocker.Mock(),
                                           mocker.Mock(), mocker.Mock(), mocker.Mock())

        synchronizer = Synchronizer(synchronizers, split_tasks)
        manager = Manager(threading.Event(), synchronizer,  mocker.Mock(), False, SdkMetadata('1.0', 'some', '1.2.3.4'), mocker.Mock())

        manager._SYNC_ALL_ATTEMPTS = 1
        manager.start(2) # should not throw!

    def test_start_streaming_false(self, mocker):
        splits_ready_event = threading.Event()
        synchronizer = mocker.Mock(spec=Synchronizer)
        manager = Manager(splits_ready_event, synchronizer, mocker.Mock(), False, SdkMetadata('1.0', 'some', '1.2.3.4'), mocker.Mock())
        try:
            manager.start()
        except:
            pass
        splits_ready_event.wait(2)
        assert splits_ready_event.is_set()
        assert len(synchronizer.sync_all.mock_calls) == 1
        assert len(synchronizer.start_periodic_fetching.mock_calls) == 1
        assert len(synchronizer.start_periodic_data_recording.mock_calls) == 1

    def test_telemetry(self, mocker):
        httpclient = mocker.Mock(spec=client.HttpClient)
        token = "eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk56TTJNREk1TXpjMF9NVGd5TlRnMU1UZ3dOZz09X3NlZ21lbnRzXCI6W1wic3Vic2NyaWJlXCJdLFwiTnpNMk1ESTVNemMwX01UZ3lOVGcxTVRnd05nPT1fc3BsaXRzXCI6W1wic3Vic2NyaWJlXCJdLFwiY29udHJvbF9wcmlcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXSxcImNvbnRyb2xfc2VjXCI6W1wic3Vic2NyaWJlXCIsXCJjaGFubmVsLW1ldGFkYXRhOnB1Ymxpc2hlcnNcIl19IiwieC1hYmx5LWNsaWVudElkIjoiY2xpZW50SWQiLCJleHAiOjE2MDIwODgxMjcsImlhdCI6MTYwMjA4NDUyN30.5_MjWonhs6yoFhw44hNJm3H7_YMjXpSW105DwjjppqE"
        payload = '{{"pushEnabled": true, "token": "{token}"}}'.format(token=token)
        cfg = DEFAULT_CONFIG.copy()
        cfg.update({'IPAddressesEnabled': True, 'machineName': 'some_machine_name', 'machineIp': '123.123.123.123'})
        sdk_metadata = get_metadata(cfg)
        httpclient.get.return_value = client.HttpResponse(200, payload)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        auth_api = auth.AuthAPI(httpclient, 'some_api_key', sdk_metadata, telemetry_runtime_producer)
        splits_ready_event = threading.Event()
        synchronizer = mocker.Mock(spec=Synchronizer)
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        manager = Manager(splits_ready_event, synchronizer, auth_api, True, sdk_metadata, telemetry_runtime_producer)
        manager.start()
        time.sleep(1)
        manager._push_status_handler_active = True
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._type == StreamingEventTypes.SYNC_MODE_UPDATE.value)
        assert(telemetry_storage._streaming_events._streaming_events[len(telemetry_storage._streaming_events._streaming_events)-1]._data == SSESyncMode.POLLING.value)

class RedisSyncManagerTests(object):
    """Synchronizer Redis Manager tests."""

    synchronizers = SplitSynchronizers(None, None, None, None, None, None, None, None)
    tasks = SplitTasks(None, None, None, None, None, None, None, None)
    synchronizer = RedisSynchronizer(synchronizers, tasks)
    manager = RedisManager(synchronizer)

    @mock.patch('splitio.sync.synchronizer.RedisSynchronizer.start_periodic_data_recording')
    def test_recreate_and_start(self, mocker):

        assert(isinstance(self.manager._synchronizer, RedisSynchronizer))

        self.manager.recreate()
        assert(not mocker.called)

        self.manager.start()
        assert(mocker.called)

    @mock.patch('splitio.sync.synchronizer.RedisSynchronizer.shutdown')
    def test_recreate_and_stop(self, mocker):

        self.manager.recreate()
        assert(not mocker.called)

        self.manager.stop(True)
        assert(mocker.called)
