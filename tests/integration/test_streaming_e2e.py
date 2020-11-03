"""Streaming integration tests."""
# pylint:disable=no-self-use,invalid-name,too-many-arguments,too-few-public-methods,line-too-long
# pylint:disable=too-many-statements
import time
import json
from queue import Queue
from threading import Event
from splitio.client.factory import get_factory
from tests.helpers.mockserver import SSEMockServer, SplitMockServer

try:  # try to import python3 names. fallback to python2
    from urllib.parse import parse_qs
except ImportError:
    from urlparse import parse_qs


class StreamingIntegrationTests(object):
    """Test streaming operation and failover."""

    def test_happiness(self):  # pylint: disable=too-many-locals
        """Test initialization & splits/segment updates."""
        split_changes = {
            -1: {
                'since': -1,
                'till': 1,
                'splits': [make_simple_split('split1', 1, True, False, 'on', 'user', True)]
            },
            1: {
                'since': 1,
                'till': 1,
                'splits': []
            }
        }

        auth_server_response = {
            'pushEnabled': True,
            'token': ('eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.'
                      'eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk1UWXlNVGN4T1RRNE13PT1fTWpBNE16Y3pO'
                      'RFUxTWc9PV9zZWdtZW50c1wiOltcInN1YnNjcmliZVwiXSxcIk1UWXlNVGN4T1RRNE13P'
                      'T1fTWpBNE16Y3pORFUxTWc9PV9zcGxpdHNcIjpbXCJzdWJzY3JpYmVcIl0sXCJjb250cm'
                      '9sX3ByaVwiOltcInN1YnNjcmliZVwiLFwiY2hhbm5lbC1tZXRhZGF0YTpwdWJsaXNoZXJ'
                      'zXCJdLFwiY29udHJvbF9zZWNcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRh'
                      'dGE6cHVibGlzaGVyc1wiXX0iLCJ4LWFibHktY2xpZW50SWQiOiJjbGllbnRJZCIsImV4c'
                      'CI6MTYwNDEwMDU5MSwiaWF0IjoxNjA0MDk2OTkxfQ.aP9BfR534K6J9h8gfDWg_CQgpz5E'
                      'vJh17WlOlAKhcD0')
        }

        segment_changes = {}
        split_backend_requests = Queue()
        split_backend = SplitMockServer(split_changes, segment_changes, split_backend_requests,
                                        auth_server_response)
        sse_requests = Queue()
        sse_server = SSEMockServer(sse_requests)

        split_backend.start()
        sse_server.start()
        sse_server.publish(make_initial_event())
        sse_server.publish(make_occupancy('control_pri', 2))
        sse_server.publish(make_occupancy('control_sec', 2))

        kwargs = {
            'sdk_api_base_url': 'http://localhost:%d/api' % split_backend.port(),
            'events_api_base_url': 'http://localhost:%d/api' % split_backend.port(),
            'auth_api_base_url': 'http://localhost:%d/api' % split_backend.port(),
            'streaming_api_base_url': 'http://localhost:%d' % sse_server.port(),
            'config': {'connectTimeout': 10000}
        }

        factory = get_factory('some_apikey', **kwargs)
        factory.block_until_ready(1)
        assert factory.ready
        assert factory.client().get_treatment('maldo', 'split1') == 'on'

        time.sleep(1)
        split_changes[1] = {
            'since': 1,
            'till': 2,
            'splits': [make_simple_split('split1', 2, True, False, 'off', 'user', False)]
        }
        split_changes[2] = {'since': 2, 'till': 2, 'splits': []}
        sse_server.publish(make_split_change_event(2))
        time.sleep(1)
        assert factory.client().get_treatment('maldo', 'split1') == 'off'

        split_changes[2] = {
            'since': 2,
            'till': 3,
            'splits': [make_split_with_segment('split2', 2, True, False,
                                               'off', 'user', 'off', 'segment1')]
        }
        split_changes[3] = {'since': 3, 'till': 3, 'splits': []}
        segment_changes[('segment1', -1)] = {
            'name': 'segment1',
            'added': ['maldo'],
            'removed': [],
            'since': -1,
            'till': 1
        }
        segment_changes[('segment1', 1)] = {'name': 'segment1', 'added': [],
                                            'removed': [], 'since': 1, 'till': 1}

        sse_server.publish(make_split_change_event(3))
        time.sleep(1)
        sse_server.publish(make_segment_change_event('segment1', 1))
        time.sleep(1)

        assert factory.client().get_treatment('pindon', 'split2') == 'off'
        assert factory.client().get_treatment('maldo', 'split2') == 'on'

        # Validate the SSE request
        sse_request = sse_requests.get()
        assert sse_request.method == 'GET'
        path, qs = sse_request.path.split('?', 1)
        assert path == '/event-stream'
        qs = parse_qs(qs)
        assert qs['accessToken'][0] == (
            'eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05'
            'US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk1UW'
            'XlNVGN4T1RRNE13PT1fTWpBNE16Y3pORFUxTWc9PV9zZWdtZW50c1wiOltcInN1YnNjc'
            'mliZVwiXSxcIk1UWXlNVGN4T1RRNE13PT1fTWpBNE16Y3pORFUxTWc9PV9zcGxpdHNcI'
            'jpbXCJzdWJzY3JpYmVcIl0sXCJjb250cm9sX3ByaVwiOltcInN1YnNjcmliZVwiLFwiY'
            '2hhbm5lbC1tZXRhZGF0YTpwdWJsaXNoZXJzXCJdLFwiY29udHJvbF9zZWNcIjpbXCJzd'
            'WJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXX0iLCJ4LWFib'
            'HktY2xpZW50SWQiOiJjbGllbnRJZCIsImV4cCI6MTYwNDEwMDU5MSwiaWF0IjoxNjA0M'
            'Dk2OTkxfQ.aP9BfR534K6J9h8gfDWg_CQgpz5EvJh17WlOlAKhcD0'
        )

        assert set(qs['channels'][0].split(',')) == set(['MTYyMTcxOTQ4Mw==_MjA4MzczNDU1Mg==_splits',
                                                         'MTYyMTcxOTQ4Mw==_MjA4MzczNDU1Mg==_segments',
                                                         '[?occupancy=metrics.publishers]control_pri',
                                                         '[?occupancy=metrics.publishers]control_sec'])
        assert qs['v'][0] == '1.1'

        # Initial apikey validation
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/segmentChanges/__SOME_INVALID_SEGMENT__?since=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Initial splits fetch
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Auth
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/auth'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after streaming connected
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after first notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=2'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after second notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=2'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=3'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Segment change notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/segmentChanges/segment1?since=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until segment1 since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/segmentChanges/segment1?since=1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Cleanup
        destroy_event = Event()
        factory.destroy(destroy_event)
        destroy_event.wait()
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        sse_server.stop()
        split_backend.stop()

    def test_occupancy_flicker(self):  # pylint: disable=too-many-locals
        """Test initialization & splits/segment updates."""
        import logging
        logging.getLogger('splitio').setLevel(logging.INFO)
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s'))
        logging.getLogger('splitio').addHandler(handler)

        split_changes = {
            -1: {
                'since': -1,
                'till': 1,
                'splits': [make_simple_split('split1', 1, True, False, 'off', 'user', True)]
            },
            1: {'since': 1, 'till': 1, 'splits': []}
        }

        auth_server_response = {
            'pushEnabled': True,
            'token': ('eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.'
                      'eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk1UWXlNVGN4T1RRNE13PT1fTWpBNE16Y3pO'
                      'RFUxTWc9PV9zZWdtZW50c1wiOltcInN1YnNjcmliZVwiXSxcIk1UWXlNVGN4T1RRNE13P'
                      'T1fTWpBNE16Y3pORFUxTWc9PV9zcGxpdHNcIjpbXCJzdWJzY3JpYmVcIl0sXCJjb250cm'
                      '9sX3ByaVwiOltcInN1YnNjcmliZVwiLFwiY2hhbm5lbC1tZXRhZGF0YTpwdWJsaXNoZXJ'
                      'zXCJdLFwiY29udHJvbF9zZWNcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRh'
                      'dGE6cHVibGlzaGVyc1wiXX0iLCJ4LWFibHktY2xpZW50SWQiOiJjbGllbnRJZCIsImV4c'
                      'CI6MTYwNDEwMDU5MSwiaWF0IjoxNjA0MDk2OTkxfQ.aP9BfR534K6J9h8gfDWg_CQgpz5E'
                      'vJh17WlOlAKhcD0')
        }

        segment_changes = {}
        split_backend_requests = Queue()
        split_backend = SplitMockServer(split_changes, segment_changes, split_backend_requests,
                                        auth_server_response)
        sse_requests = Queue()
        sse_server = SSEMockServer(sse_requests)

        split_backend.start()
        sse_server.start()
        sse_server.publish(make_initial_event())
        sse_server.publish(make_occupancy('control_pri', 2))
        sse_server.publish(make_occupancy('control_sec', 2))

        kwargs = {
            'sdk_api_base_url': 'http://localhost:%d/api' % split_backend.port(),
            'events_api_base_url': 'http://localhost:%d/api' % split_backend.port(),
            'auth_api_base_url': 'http://localhost:%d/api' % split_backend.port(),
            'streaming_api_base_url': 'http://localhost:%d' % sse_server.port(),
            'config': {'connectTimeout': 10000, 'featuresRefreshRate': 10}
        }

        factory = get_factory('some_apikey', **kwargs)
        factory.block_until_ready(1)
        assert factory.ready
        time.sleep(2)

        # Get a hook of the task so we can query its status
        task = factory._sync_manager._synchronizer._split_tasks.split_task._task  # pylint:disable=protected-access
        assert not task.running()

        assert factory.client().get_treatment('maldo', 'split1') == 'on'

        # Make a change in the BE but don't send the event.
        # After dropping occupancy, the sdk should switch to polling
        # and perform a syncAll that gets this change
        split_changes[1] = {
            'since': 1,
            'till': 2,
            'splits': [make_simple_split('split1', 2, True, False, 'off', 'user', False)]
        }
        split_changes[2] = {'since': 2, 'till': 2, 'splits': []}

        sse_server.publish(make_occupancy('control_pri', 0))
        sse_server.publish(make_occupancy('control_sec', 0))
        time.sleep(2)
        assert factory.client().get_treatment('maldo', 'split1') == 'off'
        assert task.running()

        # We make another chagne in the BE and don't send the event.
        # We restore occupancy, and it should be fetched by the
        # sync all after streaming is restored.
        split_changes[2] = {
            'since': 2,
            'till': 3,
            'splits': [make_simple_split('split1', 3, True, False, 'off', 'user', True)]
        }
        split_changes[3] = {'since': 3, 'till': 3, 'splits': []}

        sse_server.publish(make_occupancy('control_pri', 1))
        time.sleep(2)
        assert factory.client().get_treatment('maldo', 'split1') == 'on'
        assert not task.running()

        # Now we make another change and send an event so it's propagated
        split_changes[3] = {
            'since': 3,
            'till': 4,
            'splits': [make_simple_split('split1', 4, True, False, 'off', 'user', False)]
        }
        split_changes[4] = {'since': 4, 'till': 4, 'splits': []}
        sse_server.publish(make_split_change_event(4))
        time.sleep(2)
        assert factory.client().get_treatment('maldo', 'split1') == 'off'

        # Validate the SSE request
        sse_request = sse_requests.get()
        assert sse_request.method == 'GET'
        path, qs = sse_request.path.split('?', 1)
        assert path == '/event-stream'
        qs = parse_qs(qs)
        assert qs['accessToken'][0] == (
            'eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05'
            'US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk1UW'
            'XlNVGN4T1RRNE13PT1fTWpBNE16Y3pORFUxTWc9PV9zZWdtZW50c1wiOltcInN1YnNjc'
            'mliZVwiXSxcIk1UWXlNVGN4T1RRNE13PT1fTWpBNE16Y3pORFUxTWc9PV9zcGxpdHNcI'
            'jpbXCJzdWJzY3JpYmVcIl0sXCJjb250cm9sX3ByaVwiOltcInN1YnNjcmliZVwiLFwiY'
            '2hhbm5lbC1tZXRhZGF0YTpwdWJsaXNoZXJzXCJdLFwiY29udHJvbF9zZWNcIjpbXCJzd'
            'WJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXX0iLCJ4LWFib'
            'HktY2xpZW50SWQiOiJjbGllbnRJZCIsImV4cCI6MTYwNDEwMDU5MSwiaWF0IjoxNjA0M'
            'Dk2OTkxfQ.aP9BfR534K6J9h8gfDWg_CQgpz5EvJh17WlOlAKhcD0'
        )

        assert set(qs['channels'][0].split(',')) == set(['MTYyMTcxOTQ4Mw==_MjA4MzczNDU1Mg==_splits',
                                                         'MTYyMTcxOTQ4Mw==_MjA4MzczNDU1Mg==_segments',
                                                         '[?occupancy=metrics.publishers]control_pri',
                                                         '[?occupancy=metrics.publishers]control_sec'])
        assert qs['v'][0] == '1.1'

        # Initial apikey validation
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/segmentChanges/__SOME_INVALID_SEGMENT__?since=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Initial splits fetch
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Auth
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/auth'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after streaming connected
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after first notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=2'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after second notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=2'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=3'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=3'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=4'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Cleanup
        destroy_event = Event()
        factory.destroy(destroy_event)
        destroy_event.wait()
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        sse_server.stop()
        split_backend.stop()

    def test_start_without_occupancy(self):  # pylint: disable=too-many-locals
        """Test initialization & splits/segment updates."""
        # import logging
        # logging.getLogger('splitio').setLevel(logging.DEBUG)
        # handler = logging.StreamHandler()
        # handler.setFormatter(logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s'))
        # logging.getLogger('splitio').addHandler(handler)

        split_changes = {
            -1: {
                'since': -1,
                'till': 1,
                'splits': [make_simple_split('split1', 1, True, False, 'off', 'user', True)]
            },
            1: {'since': 1, 'till': 1, 'splits': []}
        }

        auth_server_response = {
            'pushEnabled': True,
            'token': ('eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.'
                      'eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk1UWXlNVGN4T1RRNE13PT1fTWpBNE16Y3pO'
                      'RFUxTWc9PV9zZWdtZW50c1wiOltcInN1YnNjcmliZVwiXSxcIk1UWXlNVGN4T1RRNE13P'
                      'T1fTWpBNE16Y3pORFUxTWc9PV9zcGxpdHNcIjpbXCJzdWJzY3JpYmVcIl0sXCJjb250cm'
                      '9sX3ByaVwiOltcInN1YnNjcmliZVwiLFwiY2hhbm5lbC1tZXRhZGF0YTpwdWJsaXNoZXJ'
                      'zXCJdLFwiY29udHJvbF9zZWNcIjpbXCJzdWJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRh'
                      'dGE6cHVibGlzaGVyc1wiXX0iLCJ4LWFibHktY2xpZW50SWQiOiJjbGllbnRJZCIsImV4c'
                      'CI6MTYwNDEwMDU5MSwiaWF0IjoxNjA0MDk2OTkxfQ.aP9BfR534K6J9h8gfDWg_CQgpz5E'
                      'vJh17WlOlAKhcD0')
        }

        segment_changes = {}
        split_backend_requests = Queue()
        split_backend = SplitMockServer(split_changes, segment_changes, split_backend_requests,
                                        auth_server_response)
        sse_requests = Queue()
        sse_server = SSEMockServer(sse_requests)

        split_backend.start()
        sse_server.start()
        sse_server.publish(make_initial_event())
        sse_server.publish(make_occupancy('control_pri', 0))
        sse_server.publish(make_occupancy('control_sec', 0))

        kwargs = {
            'sdk_api_base_url': 'http://localhost:%d/api' % split_backend.port(),
            'events_api_base_url': 'http://localhost:%d/api' % split_backend.port(),
            'auth_api_base_url': 'http://localhost:%d/api' % split_backend.port(),
            'streaming_api_base_url': 'http://localhost:%d' % sse_server.port(),
            'config': {'connectTimeout': 10000, 'featuresRefreshRate': 10}
        }

        factory = get_factory('some_apikey', **kwargs)
        factory.block_until_ready(1)
        assert factory.ready
        time.sleep(2)

        # Get a hook of the task so we can query its status
        task = factory._sync_manager._synchronizer._split_tasks.split_task._task  # pylint:disable=protected-access
        assert task.running()
        assert factory.client().get_treatment('maldo', 'split1') == 'on'

        # Make a change in the BE but don't send the event.
        # After restoring occupancy, the sdk should switch to polling
        # and perform a syncAll that gets this change
        split_changes[1] = {
            'since': 1,
            'till': 2,
            'splits': [make_simple_split('split1', 2, True, False, 'off', 'user', False)]
        }
        split_changes[2] = {'since': 2, 'till': 2, 'splits': []}

        sse_server.publish(make_occupancy('control_sec', 1))
        time.sleep(2)
        assert factory.client().get_treatment('maldo', 'split1') == 'off'
        assert not task.running()

        # Validate the SSE request
        sse_request = sse_requests.get()
        assert sse_request.method == 'GET'
        path, qs = sse_request.path.split('?', 1)
        assert path == '/event-stream'
        qs = parse_qs(qs)
        assert qs['accessToken'][0] == (
            'eyJhbGciOiJIUzI1NiIsImtpZCI6IjVZOU05'
            'US45QnJtR0EiLCJ0eXAiOiJKV1QifQ.eyJ4LWFibHktY2FwYWJpbGl0eSI6IntcIk1UW'
            'XlNVGN4T1RRNE13PT1fTWpBNE16Y3pORFUxTWc9PV9zZWdtZW50c1wiOltcInN1YnNjc'
            'mliZVwiXSxcIk1UWXlNVGN4T1RRNE13PT1fTWpBNE16Y3pORFUxTWc9PV9zcGxpdHNcI'
            'jpbXCJzdWJzY3JpYmVcIl0sXCJjb250cm9sX3ByaVwiOltcInN1YnNjcmliZVwiLFwiY'
            '2hhbm5lbC1tZXRhZGF0YTpwdWJsaXNoZXJzXCJdLFwiY29udHJvbF9zZWNcIjpbXCJzd'
            'WJzY3JpYmVcIixcImNoYW5uZWwtbWV0YWRhdGE6cHVibGlzaGVyc1wiXX0iLCJ4LWFib'
            'HktY2xpZW50SWQiOiJjbGllbnRJZCIsImV4cCI6MTYwNDEwMDU5MSwiaWF0IjoxNjA0M'
            'Dk2OTkxfQ.aP9BfR534K6J9h8gfDWg_CQgpz5EvJh17WlOlAKhcD0'
        )

        assert set(qs['channels'][0].split(',')) == set(['MTYyMTcxOTQ4Mw==_MjA4MzczNDU1Mg==_splits',
                                                         'MTYyMTcxOTQ4Mw==_MjA4MzczNDU1Mg==_segments',
                                                         '[?occupancy=metrics.publishers]control_pri',
                                                         '[?occupancy=metrics.publishers]control_sec'])
        assert qs['v'][0] == '1.1'

        # Initial apikey validation
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/segmentChanges/__SOME_INVALID_SEGMENT__?since=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Initial splits fetch
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Auth
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/auth'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after streaming connected
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after push down
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after push restored
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Second iteration of previous syncAll
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?since=2'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Cleanup
        destroy_event = Event()
        factory.destroy(destroy_event)
        destroy_event.wait()
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        sse_server.stop()
        split_backend.stop()

def make_split_change_event(change_number):
    """Make a split change event."""
    return {
        'event': 'message',
        'data': json.dumps({
            'id':'TVUsxaabHs:0:0',
            'clientId':'pri:MzM0ODI1MTkxMw==',
            'timestamp': change_number-1,
            'encoding':'json',
            'channel':'MTYyMTcxOTQ4Mw==_MjA4MzczNDU1Mg==_splits',
            'data': json.dumps({
                'type': 'SPLIT_UPDATE',
                'changeNumber': change_number
            })
        })
    }

def make_initial_event():
    """Make a split change event."""
    return {'id':'TVUsxaabHs:0:0'}

def make_occupancy(channel, publishers):
    """Make an occupancy event."""
    return {
        'event': 'message',
        'data': json.dumps({
            'id':'aP6EuhrcUm:0:0',
            'timestamp':1604325712734,
            'encoding': 'json',
            'channel': "[?occupancy=metrics.publishers]%s" % channel,
            'data': json.dumps({'metrics': {'publishers': publishers}}),
            'name':'[meta]occupancy'
        })
    }

def make_segment_change_event(name, change_number):
    """Make a split change event."""
    return {
        'event': 'message',
        'data': json.dumps({
            'id':'TVUsxaabHs:0:0',
            'clientId':'pri:MzM0ODI1MTkxMw==',
            'timestamp': change_number-1,
            'encoding':'json',
            'channel':'MTYyMTcxOTQ4Mw==_MjA4MzczNDU1Mg==_splits',
            'data': json.dumps({
                'type': 'SEGMENT_UPDATE',
                'segmentName': name,
                'changeNumber': change_number
            })
        })
    }

def make_simple_split(name, cn, active, killed, default_treatment, tt, on):
    """Make a simple split."""
    return {
        'trafficTypeName': tt,
        'name': name,
        'seed': 1699838640,
        'status': 'ACTIVE' if active else 'ARCHIVED',
        'changeNumber': cn,
        'killed': killed,
        'defaultTreatment': default_treatment,
        'conditions': [
            {
                'matcherGroup': {
                    'combiner': 'AND',
                    'matchers': [
                        {
                            'matcherType': 'ALL_KEYS',
                            'negate': False,
                            'userDefinedSegmentMatcherData': None,
                            'whitelistMatcherData': None
                        }
                    ]
                },
                'partitions': [
                    {'treatment': 'on' if on else 'off', 'size': 100},
                    {'treatment': 'off' if on else 'on', 'size': 0}
                ]
            }
        ]
    }

def make_split_with_segment(name, cn, active, killed, default_treatment,
                            tt, on, segment):
    """Make a split with a segment."""
    return {
        'trafficTypeName': tt,
        'name': name,
        'seed': cn,
        'status': 'ACTIVE' if active else 'ARCHIVED',
        'changeNumber': cn,
        'killed': killed,
        'defaultTreatment': default_treatment,
        'configurations': {
            'on': '{\'size\':15,\'test\':20}'
        },
        'conditions': [
            {
                'matcherGroup': {
                    'combiner': 'AND',
                    'matchers': [
                        {
                            'matcherType': 'IN_SEGMENT',
                            'negate': False,
                            'userDefinedSegmentMatcherData': {'segmentName': segment},
                            'whitelistMatcherData': None
                        }
                    ]
                },
                'partitions': [{
                    'treatment': 'on' if on else 'off',
                    'size': 100
                }]
            }
        ]
    }
