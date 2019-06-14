"""UWSGI Task wrappers test module."""
#pylint: disable=no-self-use,protected-access
from splitio.storage import SplitStorage
from splitio.tasks.split_sync import SplitSynchronizationTask
from splitio.tasks.impressions_sync import ImpressionsSyncTask
from splitio.tasks.events_sync import EventsSyncTask
from splitio.tasks.telemetry_sync import TelemetrySynchronizationTask
from splitio.tasks.util.workerpool import WorkerPool
from splitio.storage.uwsgi import UWSGISplitStorage
from splitio.tasks.uwsgi_wrappers import uwsgi_update_splits, uwsgi_update_segments, \
    uwsgi_report_events, uwsgi_report_impressions, uwsgi_report_telemetry


class NonCatchableException(BaseException):
    """Exception to be used to stop sync task's infinite loop."""

    pass


class TaskWrappersTests(object):
    """Task wrappers task test cases."""

    def test_update_splits(self, mocker):
        """Test split sync task wrapper."""
        data = {'executions': 0}
        def _update_splits_side_effect(*_, **__):
            data['executions'] += 1
            if data['executions'] > 1:
                raise NonCatchableException('asd')

        stmock = mocker.Mock(spec=SplitSynchronizationTask)
        stmock._update_splits.side_effect = _update_splits_side_effect
        stmock_class = mocker.Mock(spec=SplitSynchronizationTask)
        stmock_class.return_value = stmock
        mocker.patch('splitio.tasks.uwsgi_wrappers.SplitSynchronizationTask', new=stmock_class)

        try:
            uwsgi_update_splits({'apikey' : 'asd', 'featuresRefreshRate': 1})
        except NonCatchableException:
            # Make sure that the task was called before being forced to stop.
            assert data['executions'] > 1
            assert len(stmock._update_splits.mock_calls) > 1

    def test_update_segments(self, mocker):
        """Test split sync task wrapper."""
        data = {'executions': 0}
        def _submit_work(*_, **__):
            data['executions'] += 1
            # we mock 2 segments, so we expect this to be called at least twice before ending.
            if data['executions'] > 2:
                raise NonCatchableException('asd')

        wpmock = mocker.Mock(spec=WorkerPool)
        wpmock.submit_work.side_effect = _submit_work
        wpmock_class = mocker.Mock(spec=WorkerPool)
        wpmock_class.return_value = wpmock
        mocker.patch('splitio.tasks.uwsgi_wrappers.workerpool.WorkerPool', new=wpmock_class)

        mocked_update_segment = mocker.patch.object(SplitStorage, 'get_segment_names')
        mocked_update_segment.return_value = ['segment1', 'segment2']
        mocked_split_storage_instance = UWSGISplitStorage(True)
        split_storage_mock = mocker.Mock(spec=UWSGISplitStorage)
        split_storage_mock.return_value = mocked_split_storage_instance

        mocker.patch('splitio.tasks.uwsgi_wrappers.UWSGISplitStorage', new=split_storage_mock)

        try:
            uwsgi_update_segments({'apikey' : 'asd', 'segmentsRefreshRate': 1})
        except NonCatchableException:
            # Make sure that the task was called before being forced to stop.
            assert data['executions'] > 2
            assert len(wpmock.submit_work.mock_calls) > 2

    def test_post_impressions(self, mocker):
        """Test split sync task wrapper."""
        data = {'executions': 0}
        def _report_impressions_side_effect(*_, **__):
            data['executions'] += 1
            if data['executions'] > 1:
                raise NonCatchableException('asd')

        stmock = mocker.Mock(spec=ImpressionsSyncTask)
        stmock._send_impressions.side_effect = _report_impressions_side_effect
        stmock_class = mocker.Mock(spec=ImpressionsSyncTask)
        stmock_class.return_value = stmock
        mocker.patch('splitio.tasks.uwsgi_wrappers.ImpressionsSyncTask', new=stmock_class)
        try:
            uwsgi_report_impressions({'apikey' : 'asd', 'impressionsRefreshRate': 1})
        except NonCatchableException:
            # Make sure that the task was called before being forced to stop.
            assert data['executions'] > 1
        # TODO: Test impressions flushing.

    def test_post_events(self, mocker):
        """Test split sync task wrapper."""
        data = {'executions': 0}
        def _send_events_side_effect(*_, **__):
            data['executions'] += 1
            if data['executions'] > 1:
                raise NonCatchableException('asd')

        stmock = mocker.Mock(spec=EventsSyncTask)
        stmock._send_events.side_effect = _send_events_side_effect
        stmock_class = mocker.Mock(spec=EventsSyncTask)
        stmock_class.return_value = stmock
        mocker.patch('splitio.tasks.uwsgi_wrappers.EventsSyncTask', new=stmock_class)
        try:
            uwsgi_report_events({'apikey' : 'asd', 'eventsRefreshRate': 1})
        except NonCatchableException:
            # Make sure that the task was called before being forced to stop.
            assert data['executions'] > 1
        # TODO: Test impressions flushing.

    def test_post_telemetry(self, mocker):
        """Test split sync task wrapper."""
        data = {'executions': 0}
        def _flush_telemetry_side_effect(*_, **__):
            data['executions'] += 1
            if data['executions'] > 1:
                raise NonCatchableException('asd')

        stmock = mocker.Mock(spec=TelemetrySynchronizationTask)
        stmock._flush_telemetry.side_effect = _flush_telemetry_side_effect
        stmock_class = mocker.Mock(spec=TelemetrySynchronizationTask)
        stmock_class.return_value = stmock
        mocker.patch('splitio.tasks.uwsgi_wrappers.TelemetrySynchronizationTask', new=stmock_class)
        try:
            uwsgi_report_telemetry({'apikey' : 'asd', 'metricsRefreshRate': 1})
        except NonCatchableException:
            # Make sure that the task was called before being forced to stop.
            assert data['executions'] > 1
