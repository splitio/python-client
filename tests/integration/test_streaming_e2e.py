"""Streaming integration tests."""
# pylint:disable=no-self-use,invalid-name,too-many-arguments,too-few-public-methods,line-too-long
# pylint:disable=too-many-statements,too-many-locals,too-many-lines
import threading
import time
import json
import base64
import pytest

from queue import Queue
from splitio.optional.loaders import asyncio
from splitio.client.factory import get_factory, get_factory_async
from tests.helpers.mockserver import SSEMockServer, SplitMockServer
from urllib.parse import parse_qs
from splitio.models.telemetry import StreamingEventTypes, SSESyncMode


class StreamingIntegrationTests(object):
    """Test streaming operation and failover."""

    def test_happiness(self):
        """Test initialization & splits/segment updates."""
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

        split_changes = {
            -1: {'ff': {
                's': -1,
                't': 1,
                'd': [make_simple_split('split1', 1, True, False, 'on', 'user', True)]}, 
                'rbs': {'s': -1, 't': -1, 'd': []}
            },
            1: {'ff': {
                's': 1,
                't': 1,
                'd': []}, 
                'rbs': {'s': -1, 't': -1, 'd': []}
            }
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
        assert(factory._telemetry_evaluation_producer._telemetry_storage._streaming_events._streaming_events[len(factory._telemetry_evaluation_producer._telemetry_storage._streaming_events._streaming_events)-1]._type == StreamingEventTypes.SYNC_MODE_UPDATE.value)
        assert(factory._telemetry_evaluation_producer._telemetry_storage._streaming_events._streaming_events[len(factory._telemetry_evaluation_producer._telemetry_storage._streaming_events._streaming_events)-1]._data == SSESyncMode.STREAMING.value)
        split_changes[1] = {
            'ff': {
            's': 1,
            't': 2,
            'd': [make_simple_split('split1', 2, True, False, 'off', 'user', False)]}, 
            'rbs': {'s': -1, 't': -1, 'd': []}
        }
        split_changes[2] = {'ff': {'s': 2, 't': 2, 'd': []}, 'rbs': {'s': -1, 't': -1, 'd': []}}
        sse_server.publish(make_split_change_event(2))
        time.sleep(1)
        assert factory.client().get_treatment('maldo', 'split1') == 'off'

        split_changes[2] = {
            'ff': {
            's': 2,
            't': 3,
            'd': [make_split_with_segment('split2', 2, True, False,
                                               'off', 'user', 'off', 'segment1')]}, 
            'rbs': {'s': -1, 't': -1, 'd': []}
        }
        split_changes[3] = {'ff': {'s': 3, 't': 3, 'd': []}, 'rbs': {'s': -1, 't': -1, 'd': []}}
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

        sse_server.publish(make_split_fast_change_event(4))
        time.sleep(1)
        assert factory.client().get_treatment('maldo', 'split5') == 'on'

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

        # Initial splits fetch
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=-1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Auth
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/v2/auth?s=1.3'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after streaming connected
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after first notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after second notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=3&rbSince=-1'
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
        destroy_event = threading.Event()
        factory.destroy(destroy_event)
        destroy_event.wait()
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        sse_server.stop()
        split_backend.stop()

    def test_occupancy_flicker(self):
        """Test that changes in occupancy switch between polling & streaming properly."""
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

        split_changes = {
            -1: {'ff': {
                's': -1,
                't': 1,
                'd': [make_simple_split('split1', 1, True, False, 'off', 'user', True)]}, 
                'rbs': {'s': -1, 't': -1, 'd': []}
            },
            1: {'ff': {'s': 1, 't': 1, 'd': []}, 
                'rbs': {'s': -1, 't': -1, 'd': []}}
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
            'ff': {'s': 1,
            't': 2,
            'd': [make_simple_split('split1', 2, True, False, 'off', 'user', False)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[2] = {'ff': {'s': 2, 't': 2, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}

        sse_server.publish(make_occupancy('control_pri', 0))
        sse_server.publish(make_occupancy('control_sec', 0))
        time.sleep(2)
        assert factory.client().get_treatment('maldo', 'split1') == 'off'
        assert task.running()

        # We make another chagne in the BE and don't send the event.
        # We restore occupancy, and it should be fetched by the
        # sync all after streaming is restored.
        split_changes[2] = {
            'ff': {'s': 2,
            't': 3,
            'd': [make_simple_split('split1', 3, True, False, 'off', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[3] = {'ff': {'s': 3, 't': 3, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}

        sse_server.publish(make_occupancy('control_pri', 1))
        time.sleep(2)
        assert factory.client().get_treatment('maldo', 'split1') == 'on'
        assert not task.running()

        # Now we make another change and send an event so it's propagated
        split_changes[3] = {
            'ff': {'s': 3,
            't': 4,
            'd': [make_simple_split('split1', 4, True, False, 'off', 'user', False)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[4] = {'ff': {'s': 4, 't': 4, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}        
        sse_server.publish(make_split_change_event(4))
        time.sleep(2)
        assert factory.client().get_treatment('maldo', 'split1') == 'off'

        # Kill the split
        split_changes[4] = {
            'ff': {'s': 4,
            't': 5,
            'd': [make_simple_split('split1', 5, True, True, 'frula', 'user', False)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[5] = {'ff': {'s': 5, 't': 5, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}
        sse_server.publish(make_split_kill_event('split1', 'frula', 5))
        time.sleep(2)
        assert factory.client().get_treatment('maldo', 'split1') == 'frula'

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

        # Initial splits fetch
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=-1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Auth
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/v2/auth?s=1.3'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after streaming connected
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after first notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after second notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=3&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=3&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=4&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Split kill
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=4&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=5&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Cleanup
        destroy_event = threading.Event()
        factory.destroy(destroy_event)
        destroy_event.wait()
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        sse_server.stop()
        split_backend.stop()

    def test_start_without_occupancy(self):
        """Test an SDK starting with occupancy on 0 and switching to streamin afterwards."""
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

        split_changes = {
            -1: {'ff': {
                's': -1,
                't': 1,
                'd': [make_simple_split('split1', 1, True, False, 'off', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
            },
            1: {'ff': {'s': 1, 't': 1, 'd': []},
            'rbs': {'t': -1, 's': -1, 'd': []}}
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
            'ff': {'s': 1,
            't': 2,
            'd': [make_simple_split('split1', 2, True, False, 'off', 'user', False)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[2] = {'ff': {'s': 2, 't': 2, 'd': []},
            'rbs': {'t': -1, 's': -1, 'd': []}}

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

        # Initial splits fetch
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=-1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Auth
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/v2/auth?s=1.3'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after streaming connected
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after push down
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after push restored
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Second iteration of previous syncAll
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Cleanup
        destroy_event = threading.Event()
        factory.destroy(destroy_event)
        destroy_event.wait()
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        sse_server.stop()
        split_backend.stop()
        
    def test_streaming_status_changes(self):
        """Test changes between streaming enabled, paused and disabled."""
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

        split_changes = {
            -1: {'ff': {
                's': -1,
                't': 1,
                'd': [make_simple_split('split1', 1, True, False, 'off', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
            },
            1: {'ff': {'s': 1, 't': 1, 'd': []},
            'rbs': {'t': -1, 's': -1, 'd': []}}
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
            'ff': {'s': 1,
            't': 2,
            'd': [make_simple_split('split1', 2, True, False, 'off', 'user', False)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[2] = {'ff': {'s': 2, 't': 2, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}

        sse_server.publish(make_control_event('STREAMING_PAUSED', 1))
        time.sleep(2)
        assert factory.client().get_treatment('maldo', 'split1') == 'off'
        assert task.running()

        # We make another chagne in the BE and don't send the event.
        # We restore occupancy, and it should be fetched by the
        # sync all after streaming is restored.
        split_changes[2] = {
            'ff': {'s': 2,
            't': 3,
            'd': [make_simple_split('split1', 3, True, False, 'off', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[3] = {'ff': {'s': 3, 't': 3, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}

        sse_server.publish(make_control_event('STREAMING_ENABLED', 2))
        time.sleep(2)
        assert factory.client().get_treatment('maldo', 'split1') == 'on'
        assert not task.running()

        # Now we make another change and send an event so it's propagated
        split_changes[3] = {
            'ff': {'s': 3,
            't': 4,
            'd': [make_simple_split('split1', 4, True, False, 'off', 'user', False)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[4] = {'ff': {'s': 4, 't': 4, 'd': []},
            'rbs': {'t': -1, 's': -1, 'd': []}}
        sse_server.publish(make_split_change_event(4))
        time.sleep(2)
        assert factory.client().get_treatment('maldo', 'split1') == 'off'
        assert not task.running()

        split_changes[4] = {
            'ff': {'s': 4,
            't': 5,
            'd': [make_simple_split('split1', 5, True, False, 'off', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[5] = {'ff': {'s': 5, 't': 5, 'd': []},
            'rbs': {'t': -1, 's': -1, 'd': []}}
        sse_server.publish(make_control_event('STREAMING_DISABLED', 2))
        time.sleep(2)
        assert factory.client().get_treatment('maldo', 'split1') == 'on'
        assert task.running()
        assert 'PushStatusHandler' not in [t.name for t in threading.enumerate()]

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

        # Initial splits fetch
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=-1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Auth
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/v2/auth?s=1.3'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after streaming connected
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll on push down
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after push is up
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=3&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=3&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=4&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after streaming disabled
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=4&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=5&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Cleanup
        destroy_event = threading.Event()
        factory.destroy(destroy_event)
        destroy_event.wait()
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        sse_server.stop()
        split_backend.stop()

    def test_server_closes_connection(self):
        """Test that if the server closes the connection, the whole flow is retried with BO."""
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

        split_changes = {
            -1: {'ff': {
                's': -1,
                't': 1,
                'd': [make_simple_split('split1', 1, True, False, 'on', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
            },
            1: {'ff': {
                's': 1,
                't': 1,
                'd': []},
            'rbs': {'t': -1, 's': -1, 'd': []}
            }
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
            'config': {'connectTimeout': 10000, 'featuresRefreshRate': 100,
                       'segmentsRefreshRate': 100, 'metricsRefreshRate': 100,
                       'impressionsRefreshRate': 100, 'eventsPushRate': 100}
        }

        factory = get_factory('some_apikey', **kwargs)
        factory.block_until_ready(1)
        assert factory.ready
        assert factory.client().get_treatment('maldo', 'split1') == 'on'
        task = factory._sync_manager._synchronizer._split_tasks.split_task._task  # pylint:disable=protected-access
        assert not task.running()

        time.sleep(1)
        split_changes[1] = {'ff': {
            's': 1,
            't': 2,
            'd': [make_simple_split('split1', 2, True, False, 'off', 'user', False)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[2] = {'ff': {'s': 2, 't': 2, 'd': []},
            'rbs': {'t': -1, 's': -1, 'd': []}}
        sse_server.publish(make_split_change_event(2))
        time.sleep(1)
        assert factory.client().get_treatment('maldo', 'split1') == 'off'

        sse_server.publish(SSEMockServer.GRACEFUL_REQUEST_END)
        time.sleep(1)
        assert factory.client().get_treatment('maldo', 'split1') == 'off'
        assert task.running()

        time.sleep(2)  # wait for the backoff to expire so streaming gets re-attached

        # re-send initial event AND occupancy
        sse_server.publish(make_initial_event())
        sse_server.publish(make_occupancy('control_pri', 2))
        sse_server.publish(make_occupancy('control_sec', 2))
        time.sleep(2)

        assert not task.running()
        split_changes[2] = {'ff': {
            's': 2,
            't': 3,
            'd': [make_simple_split('split1', 3, True, False, 'off', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[3] = {'ff': {'s': 3, 't': 3, 'd': [],
            'rbs': {'t': -1, 's': -1, 'd': []}}}
        sse_server.publish(make_split_change_event(3))
        time.sleep(1)
        assert factory.client().get_treatment('maldo', 'split1') == 'on'
        assert not task.running()

        # Validate the SSE requests
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

        # Initial splits fetch
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=-1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Auth
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/v2/auth?s=1.3'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after streaming connected
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after first notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll on retryable error handling
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Auth after connection breaks
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/v2/auth?s=1.3'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after streaming connected again
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after new notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=3&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Cleanup
        destroy_event = threading.Event()
        factory.destroy(destroy_event)
        destroy_event.wait()
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        sse_server.stop()
        split_backend.stop()

    def test_ably_errors_handling(self):
        """Test incoming ably errors and validate its handling."""
        import logging
        logger = logging.getLogger('splitio')
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
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

        split_changes = {
            -1: {'ff': {
                's': -1,
                't': 1,
                'd': [make_simple_split('split1', 1, True, False, 'off', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
            },
            1: {'ff': {'s': 1, 't': 1, 'd': []},
            'rbs': {'t': -1, 's': -1, 'd': []}}
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
        # We'll send an ignorable error and check it has nothing happened
        split_changes[1] = {'ff': {
            's': 1,
            't': 2,
            'd': [make_simple_split('split1', 2, True, False, 'off', 'user', False)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[2] = {'ff': {'s': 2, 't': 2, 'd': []},
            'rbs': {'t': -1, 's': -1, 'd': []}}

        sse_server.publish(make_ably_error_event(60000, 600))
        time.sleep(1)
        assert factory.client().get_treatment('maldo', 'split1') == 'on'
        assert not task.running()

        sse_server.publish(make_ably_error_event(40145, 401))
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        time.sleep(3)
        assert task.running()
        assert factory.client().get_treatment('maldo', 'split1') == 'off'

        # Re-publish initial events so that the retry succeeds
        sse_server.publish(make_initial_event())
        sse_server.publish(make_occupancy('control_pri', 2))
        sse_server.publish(make_occupancy('control_sec', 2))
        time.sleep(3)
        assert not task.running()

        # Assert streaming is working properly
        split_changes[2] = {'ff': {
            's': 2,
            't': 3,
            'd': [make_simple_split('split1', 3, True, False, 'off', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[3] = {'ff': {'s': 3, 't': 3, 'd': []},
            'rbs': {'t': -1, 's': -1, 'd': []}}
        sse_server.publish(make_split_change_event(3))
        time.sleep(2)
        assert factory.client().get_treatment('maldo', 'split1') == 'on'
        assert not task.running()

        # Send a non-retryable ably error
        sse_server.publish(make_ably_error_event(40200, 402))
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        time.sleep(3)

        # Assert sync-task is running and the streaming status handler thread is over
        assert task.running()
        assert 'PushStatusHandler' not in [t.name for t in threading.enumerate()]

        # Validate the SSE requests
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

        # Initial splits fetch
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=-1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Auth
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/v2/auth?s=1.3'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after streaming connected
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll retriable error
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Auth again
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/v2/auth?s=1.3'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after push is up
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=3&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after non recoverable ably error
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=3&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Cleanup
        destroy_event = threading.Event()
        factory.destroy(destroy_event)
        destroy_event.wait()
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        sse_server.stop()
        split_backend.stop()

    def test_change_number(mocker):
        # test if changeNumber is missing
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

        split_changes = {
            -1: {'ff': {
                's': -1,
                't': 1,
                'd': [make_simple_split('split1', 1, True, False, 'off', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
            },
            1: {'ff': {'s': 1, 't': 1, 'd': []},
            'rbs': {'t': -1, 's': -1, 'd': []}}
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

        split_changes = make_split_fast_change_event(5).copy()
        data = json.loads(split_changes['data'])
        inner_data = json.loads(data['data'])
        inner_data['changeNumber'] = None
        data['data'] = json.dumps(inner_data)
        split_changes['data'] = json.dumps(data)
        sse_server.publish(split_changes)
        time.sleep(1)
        assert factory._storages['splits'].get_change_number() == 1

        # Cleanup
        destroy_event = threading.Event()
        factory.destroy(destroy_event)
        destroy_event.wait()
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        sse_server.stop()
        split_backend.stop()


class StreamingIntegrationAsyncTests(object):
    """Test streaming operation and failover."""

    @pytest.mark.asyncio
    async def test_happiness(self):
        """Test initialization & splits/segment updates."""
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

        split_changes = {
            -1: {'ff': {
                's': -1,
                't': 1,
                'd': [make_simple_split('split1', 1, True, False, 'on', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
            },
            1: {'ff': {
                's': 1,
                't': 1,
                'd': []},
            'rbs': {'t': -1, 's': -1, 'd': []}
            }
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

        factory = await get_factory_async('some_apikey', **kwargs)
        await factory.block_until_ready(1)
        assert factory.ready
        assert await factory.client().get_treatment('maldo', 'split1') == 'on'

        await asyncio.sleep(1)
        assert(factory._telemetry_evaluation_producer._telemetry_storage._streaming_events._streaming_events[len(factory._telemetry_evaluation_producer._telemetry_storage._streaming_events._streaming_events)-1]._type == StreamingEventTypes.SYNC_MODE_UPDATE.value)
        assert(factory._telemetry_evaluation_producer._telemetry_storage._streaming_events._streaming_events[len(factory._telemetry_evaluation_producer._telemetry_storage._streaming_events._streaming_events)-1]._data == SSESyncMode.STREAMING.value)
        split_changes[1] = {'ff': {
            's': 1,
            't': 2,
            'd': [make_simple_split('split1', 2, True, False, 'off', 'user', False)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[2] = {'ff': {'s': 2, 't': 2, 'd': []},
            'rbs': {'t': -1, 's': -1, 'd': []}}
        sse_server.publish(make_split_change_event(2))
        await asyncio.sleep(1)
        assert await factory.client().get_treatment('maldo', 'split1') == 'off'

        split_changes[2] = {'ff': {
            's': 2,
            't': 3,
            'd': [make_split_with_segment('split2', 2, True, False,
                                               'off', 'user', 'off', 'segment1')]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[3] = {'ff': {'s': 3, 't': 3, 'd': []},
            'rbs': {'t': -1, 's': -1, 'd': []}}
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
        await asyncio.sleep(1)
        sse_server.publish(make_segment_change_event('segment1', 1))
        await asyncio.sleep(1)

        assert await factory.client().get_treatment('pindon', 'split2') == 'off'
        assert await factory.client().get_treatment('maldo', 'split2') == 'on'

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

        # Initial splits fetch
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=-1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Auth
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/v2/auth?s=1.3'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after streaming connected
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after first notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after second notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=3&rbSince=-1'
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
        await factory.destroy()
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        sse_server.stop()
        split_backend.stop()

    @pytest.mark.asyncio
    async def test_occupancy_flicker(self):
        """Test that changes in occupancy switch between polling & streaming properly."""
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

        split_changes = {
            -1: {'ff': {
                's': -1,
                't': 1,
                'd': [make_simple_split('split1', 1, True, False, 'off', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
            },
            1: {'ff': {'s': 1, 't': 1, 'd': []},
            'rbs': {'t': -1, 's': -1, 'd': []}}
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

        factory = await get_factory_async('some_apikey', **kwargs)
        await factory.block_until_ready(1)
        assert factory.ready
        await asyncio.sleep(2)

        # Get a hook of the task so we can query its status
        task = factory._sync_manager._synchronizer._split_tasks.split_task._task  # pylint:disable=protected-access
        assert not task.running()

        assert await factory.client().get_treatment('maldo', 'split1') == 'on'

        # Make a change in the BE but don't send the event.
        # After dropping occupancy, the sdk should switch to polling
        # and perform a syncAll that gets this change
        split_changes[1] = {'ff': {
            's': 1,
            't': 2,
            'd': [make_simple_split('split1', 2, True, False, 'off', 'user', False)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[2] = {'ff': {'s': 2, 't': 2, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}
        sse_server.publish(make_occupancy('control_pri', 0))
        sse_server.publish(make_occupancy('control_sec', 0))
        await asyncio.sleep(2)
        assert await factory.client().get_treatment('maldo', 'split1') == 'off'
        assert task.running()

        # We make another chagne in the BE and don't send the event.
        # We restore occupancy, and it should be fetched by the
        # sync all after streaming is restored.
        split_changes[2] = {'ff': {
            's': 2,
            't': 3,
            'd': [make_simple_split('split1', 3, True, False, 'off', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[3] = {'ff': {'s': 3, 't': 3, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}
        sse_server.publish(make_occupancy('control_pri', 1))
        await asyncio.sleep(2)
        assert await factory.client().get_treatment('maldo', 'split1') == 'on'
        assert not task.running()

        # Now we make another change and send an event so it's propagated
        split_changes[3] = {'ff': {
            's': 3,
            't': 4,
            'd': [make_simple_split('split1', 4, True, False, 'off', 'user', False)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[4] = {'ff': {'s': 4, 't': 4, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}
        sse_server.publish(make_split_change_event(4))
        await asyncio.sleep(2)
        assert await factory.client().get_treatment('maldo', 'split1') == 'off'

        # Kill the split
        split_changes[4] = {'ff': {
            's': 4,
            't': 5,
            'd': [make_simple_split('split1', 5, True, True, 'frula', 'user', False)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[5] = {'ff': {'s': 5, 't': 5, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}
        sse_server.publish(make_split_kill_event('split1', 'frula', 5))
        await asyncio.sleep(2)
        assert await factory.client().get_treatment('maldo', 'split1') == 'frula'

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

        # Initial splits fetch
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=-1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Auth
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/v2/auth?s=1.3'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after streaming connected
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after first notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after second notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=3&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=3&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=4&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Split kill
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=4&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=5&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Cleanup
        await factory.destroy()
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        sse_server.stop()
        split_backend.stop()

    @pytest.mark.asyncio
    async def test_start_without_occupancy(self):
        """Test an SDK starting with occupancy on 0 and switching to streamin afterwards."""
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

        split_changes = {
            -1: {'ff': {
                's': -1,
                't': 1,
                'd': [make_simple_split('split1', 1, True, False, 'off', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
            },
            1: {'ff': {'s': 1, 't': 1, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}
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

        factory = await get_factory_async('some_apikey', **kwargs)
        try:
            await factory.block_until_ready(1)
        except Exception:
            pass
        assert factory.ready
        await asyncio.sleep(2)

        # Get a hook of the task so we can query its status
        task = factory._sync_manager._synchronizer._split_tasks.split_task._task  # pylint:disable=protected-access
        assert task.running()
        assert await factory.client().get_treatment('maldo', 'split1') == 'on'

        # Make a change in the BE but don't send the event.
        # After restoring occupancy, the sdk should switch to polling
        # and perform a syncAll that gets this change
        split_changes[1] = {'ff': {
            's': 1,
            't': 2,
            'd': [make_simple_split('split1', 2, True, False, 'off', 'user', False)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[2] = {'ff': {'s': 2, 't': 2, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}

        sse_server.publish(make_occupancy('control_sec', 1))
        await asyncio.sleep(2)
        assert await factory.client().get_treatment('maldo', 'split1') == 'off'
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

        # Initial splits fetch
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=-1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Auth
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/v2/auth?s=1.3'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after streaming connected
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after push down
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after push restored
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Second iteration of previous syncAll
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Cleanup
        await factory.destroy()
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        sse_server.stop()
        split_backend.stop()

    @pytest.mark.asyncio
    async def test_streaming_status_changes(self):
        """Test changes between streaming enabled, paused and disabled."""
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

        split_changes = {
            -1: {'ff': {
                's': -1,
                't': 1,
                'd': [make_simple_split('split1', 1, True, False, 'off', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
            },
            1: {'ff': {'s': 1, 't': 1, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}
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

        factory = await get_factory_async('some_apikey', **kwargs)
        try:
            await factory.block_until_ready(1)
        except Exception:
            pass
        assert factory.ready
        await asyncio.sleep(2)

        # Get a hook of the task so we can query its status
        task = factory._sync_manager._synchronizer._split_tasks.split_task._task  # pylint:disable=protected-access
        assert not task.running()

        assert await factory.client().get_treatment('maldo', 'split1') == 'on'

        # Make a change in the BE but don't send the event.
        # After dropping occupancy, the sdk should switch to polling
        # and perform a syncAll that gets this change
        split_changes[1] = {'ff': {
            's': 1,
            't': 2,
            'd': [make_simple_split('split1', 2, True, False, 'off', 'user', False)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[2] = {'ff': {'s': 2, 't': 2, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}

        sse_server.publish(make_control_event('STREAMING_PAUSED', 1))
        await asyncio.sleep(4)

        assert await factory.client().get_treatment('maldo', 'split1') == 'off'
        assert task.running()

        # We make another chagne in the BE and don't send the event.
        # We restore occupancy, and it should be fetched by the
        # sync all after streaming is restored.
        split_changes[2] = {'ff': {
            's': 2,
            't': 3,
            'd': [make_simple_split('split1', 3, True, False, 'off', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[3] = {'ff': {'s': 3, 't': 3, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}

        sse_server.publish(make_control_event('STREAMING_ENABLED', 2))
        await asyncio.sleep(2)

        assert await factory.client().get_treatment('maldo', 'split1') == 'on'
        assert not task.running()

        # Now we make another change and send an event so it's propagated
        split_changes[3] = {'ff': {
            's': 3,
            't': 4,
            'd': [make_simple_split('split1', 4, True, False, 'off', 'user', False)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[4] = {'ff': {'s': 4, 't': 4, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}
        sse_server.publish(make_split_change_event(4))
        await asyncio.sleep(2)

        assert await factory.client().get_treatment('maldo', 'split1') == 'off'
        assert not task.running()

        split_changes[4] = {'ff': {
            's': 4,
            't': 5,
            'd': [make_simple_split('split1', 5, True, False, 'off', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[5] = {'ff': {'s': 5, 't': 5, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}
        sse_server.publish(make_control_event('STREAMING_DISABLED', 2))
        await asyncio.sleep(2)

        assert await factory.client().get_treatment('maldo', 'split1') == 'on'
        assert task.running()

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

        # Initial splits fetch
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=-1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Auth
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/v2/auth?s=1.3'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after streaming connected
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll on push down
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after push is up
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=3&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=3&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=4&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after streaming disabled
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=4&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=5&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Cleanup
        await factory.destroy()
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        sse_server.stop()
        split_backend.stop()

    @pytest.mark.asyncio
    async def test_server_closes_connection(self):
        """Test that if the server closes the connection, the whole flow is retried with BO."""
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

        split_changes = {
            -1: {'ff': {
                's': -1,
                't': 1,
                'd': [make_simple_split('split1', 1, True, False, 'on', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
            },
            1: {'ff': {'s': 1, 't': 1, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}
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
            'config': {'connectTimeout': 10000, 'featuresRefreshRate': 100,
                       'segmentsRefreshRate': 100, 'metricsRefreshRate': 100,
                       'impressionsRefreshRate': 100, 'eventsPushRate': 100}
        }
        factory = await get_factory_async('some_apikey', **kwargs)
        await factory.block_until_ready(1)
        assert factory.ready
        assert await factory.client().get_treatment('maldo', 'split1') == 'on'
        task = factory._sync_manager._synchronizer._split_tasks.split_task._task  # pylint:disable=protected-access
        assert not task.running()

        await asyncio.sleep(1)
        split_changes[1] = {'ff': {
            's': 1,
            't': 2,
            'd': [make_simple_split('split1', 2, True, False, 'off', 'user', False)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[2] = {'ff': {'s': 2, 't': 2, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}
        sse_server.publish(make_split_change_event(2))
        await asyncio.sleep(1)
        assert await factory.client().get_treatment('maldo', 'split1') == 'off'

        sse_server.publish(SSEMockServer.GRACEFUL_REQUEST_END)
        await asyncio.sleep(1)
        assert await factory.client().get_treatment('maldo', 'split1') == 'off'
        assert task.running()

#          # wait for the backoff to expire so streaming gets re-attached
        await asyncio.sleep(2)

        # re-send initial event AND occupancy
        sse_server.publish(make_initial_event())
        sse_server.publish(make_occupancy('control_pri', 2))
        sse_server.publish(make_occupancy('control_sec', 2))
        await asyncio.sleep(2)

        assert not task.running()
        split_changes[2] = {'ff': {
            's': 2,
            't': 3,
            'd': [make_simple_split('split1', 3, True, False, 'off', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[3] = {'ff': {'s': 3, 't': 3, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}
        sse_server.publish(make_split_change_event(3))
        await asyncio.sleep(1)

        assert await factory.client().get_treatment('maldo', 'split1') == 'on'
        assert not task.running()

        # Validate the SSE requests
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

        # Initial splits fetch
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=-1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Auth
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/v2/auth?s=1.3'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after streaming connected
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after first notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll on retryable error handling
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Auth after connection breaks
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/v2/auth?s=1.3'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after streaming connected again
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after new notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=3&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Cleanup
        await factory.destroy()
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        sse_server.stop()
        split_backend.stop()

    @pytest.mark.asyncio
    async def test_ably_errors_handling(self):
        """Test incoming ably errors and validate its handling."""
        import logging
        logger = logging.getLogger('splitio')
        handler = logging.StreamHandler()
        formatter = logging.Formatter('%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
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

        split_changes = {
            -1: {'ff': {
                's': -1,
                't': 1,
                'd': [make_simple_split('split1', 1, True, False, 'off', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
            },
            1: {'ff': {'s': 1, 't': 1, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}
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

        factory = await get_factory_async('some_apikey', **kwargs)
        try:
            await factory.block_until_ready(5)
        except Exception:
            pass
        assert factory.ready
        await asyncio.sleep(2)
        # Get a hook of the task so we can query its status
        task = factory._sync_manager._synchronizer._split_tasks.split_task._task  # pylint:disable=protected-access
        assert not task.running()

        assert await factory.client().get_treatment('maldo', 'split1') == 'on'

        # Make a change in the BE but don't send the event.
        # We'll send an ignorable error and check it has nothing happened
        split_changes[1] = {'ff': {
            's': 1,
            't': 2,
            'd': [make_simple_split('split1', 2, True, False, 'off', 'user', False)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[2] = {'ff': {'s': 2, 't': 2, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}

        sse_server.publish(make_ably_error_event(60000, 600))
        await asyncio.sleep(1)

        assert await factory.client().get_treatment('maldo', 'split1') == 'on'
        assert not task.running()

        sse_server.publish(make_ably_error_event(40145, 401))
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        await asyncio.sleep(3)

        assert task.running()
        assert await factory.client().get_treatment('maldo', 'split1') == 'off'

        # Re-publish initial events so that the retry succeeds
        sse_server.publish(make_initial_event())
        sse_server.publish(make_occupancy('control_pri', 2))
        sse_server.publish(make_occupancy('control_sec', 2))
        await asyncio.sleep(3)
        assert not task.running()

        # Assert streaming is working properly
        split_changes[2] = {'ff': {
            's': 2,
            't': 3,
            'd': [make_simple_split('split1', 3, True, False, 'off', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
        }
        split_changes[3] = {'ff': {'s': 3, 't': 3, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}
        sse_server.publish(make_split_change_event(3))
        await asyncio.sleep(2)
        assert await factory.client().get_treatment('maldo', 'split1') == 'on'
        assert not task.running()

        # Send a non-retryable ably error
        sse_server.publish(make_ably_error_event(40200, 402))
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        await asyncio.sleep(3)

        # Assert sync-task is running and the streaming status handler thread is over
        assert task.running()

        # Validate the SSE requests
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

        # Initial splits fetch
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=-1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Auth
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/v2/auth?s=1.3'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after streaming connected
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll retriable error
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=1&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Auth again
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/v2/auth?s=1.3'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after push is up
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Fetch after notification
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=2&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Iteration until since == till
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=3&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # SyncAll after non recoverable ably error
        req = split_backend_requests.get()
        assert req.method == 'GET'
        assert req.path == '/api/splitChanges?s=1.3&since=3&rbSince=-1'
        assert req.headers['authorization'] == 'Bearer some_apikey'

        # Cleanup
        await factory.destroy()
        sse_server.publish(sse_server.GRACEFUL_REQUEST_END)
        sse_server.stop()
        split_backend.stop()

    @pytest.mark.asyncio
    async def test_change_number(mocker):
        # test if changeNumber is missing
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

        split_changes = {
            -1: {'ff': {
                's': -1,
                't': 1,
                'd': [make_simple_split('split1', 1, True, False, 'off', 'user', True)]},
            'rbs': {'t': -1, 's': -1, 'd': []}
            },
            1: {'ff': {'s': 1, 't': 1, 'd': []}, 'rbs': {'t': -1, 's': -1, 'd': []}}
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
            'config': {'connectTimeout': 10000, 'featuresRefreshRate': 100}
        }
        factory2 = await get_factory_async('some_apikey', **kwargs)
        await factory2.block_until_ready(1)
        assert factory2.ready
        await asyncio.sleep(2)

        split_changes = make_split_fast_change_event(5).copy()
        data = json.loads(split_changes['data'])
        inner_data = json.loads(data['data'])
        inner_data['changeNumber'] = None
        data['data'] = json.dumps(inner_data)
        split_changes['data'] = json.dumps(data)
        sse_server.publish(split_changes)
        await asyncio.sleep(1)
        assert await factory2._storages['splits'].get_change_number() == 1

        # Cleanup
        await factory2.destroy()
        sse_server.publish(sse_server.VIOLENT_REQUEST_END)
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

def make_split_fast_change_event(change_number):
    """Make a split change event."""
    json1 = make_simple_split('split5', 1, True, False, 'off', 'user', True)
    str1 = json.dumps(json1)
    byt1 = bytes(str1, encoding='utf-8')
    compressed = base64.b64encode(byt1)
    final = compressed.decode('utf-8')

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
                'changeNumber': change_number,
                'pcn': 3,
                'c': 0,
                'd': final
            })
        })
    }

def make_split_kill_event(name, default_treatment, change_number):
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
                'type': 'SPLIT_KILL',
                'splitName': name,
                'defaultTreatment': default_treatment,
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
            'channel':'MTYyMTcxOTQ4Mw==_MjA4MzczNDU1Mg==_segments',
            'data': json.dumps({
                'type': 'SEGMENT_UPDATE',
                'segmentName': name,
                'changeNumber': change_number
            })
        })
    }

def make_control_event(control_type, timestamp):
    """Make a control event."""
    return {
        'event': 'message',
        'data': json.dumps({
            'id':'TVUsxaabHs:0:0',
            'clientId':'pri:MzM0ODI1MTkxMw==',
            'timestamp': timestamp,
            'encoding':'json',
            'channel':'[?occupancy=metrics.publishers]control_pri',
            'data': json.dumps({
                'type': 'CONTROL',
                'controlType': control_type,
            })
        })
    }

def make_ably_error_event(code, status):
    """Make a control event."""
    return {
        'event': 'error',
        'data': json.dumps({
            'message':'Invalid accessToken in request: sarasa',
            'code': code,
            'statusCode': status,
            'href':"https://help.ably.io/error/%d" % code
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
            },
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
                    {'treatment': 'on' if on else 'off', 'size': 0},
                    {'treatment': 'off' if on else 'on', 'size': 100}
                ]
            }
        ]
    }
