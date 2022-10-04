"""In-Memory storage test module."""
# pylint: disable=no-self-use
from splitio.models.splits import Split
from splitio.models.segments import Segment
from splitio.models.impressions import Impression
from splitio.models.events import Event, EventWrapper

from splitio.storage.inmemmory import InMemorySplitStorage, InMemorySegmentStorage, \
    InMemoryImpressionStorage, InMemoryEventStorage, InMemoryTelemetryStorage

import pytest

class InMemorySplitStorageTests(object):
    """In memory split storage test cases."""

    def test_storing_retrieving_splits(self, mocker):
        """Test storing and retrieving splits works."""
        storage = InMemorySplitStorage()

        split = mocker.Mock(spec=Split)
        name_property = mocker.PropertyMock()
        name_property.return_value = 'some_split'
        type(split).name = name_property

        storage.put(split)
        assert storage.get('some_split') == split
        assert storage.get_split_names() == ['some_split']
        assert storage.get_all_splits() == [split]
        assert storage.get('nonexistant_split') is None

        storage.remove('some_split')
        assert storage.get('some_split') is None

    def test_get_splits(self, mocker):
        """Test retrieving a list of passed splits."""
        split1 = mocker.Mock()
        name1_prop = mocker.PropertyMock()
        name1_prop.return_value = 'split1'
        type(split1).name = name1_prop
        split2 = mocker.Mock()
        name2_prop = mocker.PropertyMock()
        name2_prop.return_value = 'split2'
        type(split2).name = name2_prop

        storage = InMemorySplitStorage()
        storage.put(split1)
        storage.put(split2)

        splits = storage.fetch_many(['split1', 'split2', 'split3'])
        assert len(splits) == 3
        assert splits['split1'].name == 'split1'
        assert splits['split2'].name == 'split2'
        assert 'split3' in splits

    def test_store_get_changenumber(self):
        """Test that storing and retrieving change numbers works."""
        storage = InMemorySplitStorage()
        assert storage.get_change_number() == -1
        storage.set_change_number(5)
        assert storage.get_change_number() == 5

    def test_get_split_names(self, mocker):
        """Test retrieving a list of all split names."""
        split1 = mocker.Mock()
        name1_prop = mocker.PropertyMock()
        name1_prop.return_value = 'split1'
        type(split1).name = name1_prop
        split2 = mocker.Mock()
        name2_prop = mocker.PropertyMock()
        name2_prop.return_value = 'split2'
        type(split2).name = name2_prop

        storage = InMemorySplitStorage()
        storage.put(split1)
        storage.put(split2)

        assert set(storage.get_split_names()) == set(['split1', 'split2'])

    def test_get_all_splits(self, mocker):
        """Test retrieving a list of all split names."""
        split1 = mocker.Mock()
        name1_prop = mocker.PropertyMock()
        name1_prop.return_value = 'split1'
        type(split1).name = name1_prop
        split2 = mocker.Mock()
        name2_prop = mocker.PropertyMock()
        name2_prop.return_value = 'split2'
        type(split2).name = name2_prop

        storage = InMemorySplitStorage()
        storage.put(split1)
        storage.put(split2)

        all_splits = storage.get_all_splits()
        assert next(s for s in all_splits if s.name == 'split1')
        assert next(s for s in all_splits if s.name == 'split2')

    def test_is_valid_traffic_type(self, mocker):
        """Test that traffic type validation works properly."""
        split1 = mocker.Mock()
        name1_prop = mocker.PropertyMock()
        name1_prop.return_value = 'split1'
        type(split1).name = name1_prop
        split2 = mocker.Mock()
        name2_prop = mocker.PropertyMock()
        name2_prop.return_value = 'split2'
        type(split2).name = name2_prop
        split3 = mocker.Mock()
        tt_user = mocker.PropertyMock()
        tt_user.return_value = 'user'
        tt_account = mocker.PropertyMock()
        tt_account.return_value = 'account'
        name3_prop = mocker.PropertyMock()
        name3_prop.return_value = 'split3'
        type(split3).name = name3_prop
        type(split1).traffic_type_name = tt_user
        type(split2).traffic_type_name = tt_account
        type(split3).traffic_type_name = tt_user

        storage = InMemorySplitStorage()

        storage.put(split1)
        assert storage.is_valid_traffic_type('user') is True
        assert storage.is_valid_traffic_type('account') is False

        storage.put(split2)
        assert storage.is_valid_traffic_type('user') is True
        assert storage.is_valid_traffic_type('account') is True

        storage.put(split3)
        assert storage.is_valid_traffic_type('user') is True
        assert storage.is_valid_traffic_type('account') is True

        storage.remove('split1')
        assert storage.is_valid_traffic_type('user') is True
        assert storage.is_valid_traffic_type('account') is True

        storage.remove('split2')
        assert storage.is_valid_traffic_type('user') is True
        assert storage.is_valid_traffic_type('account') is False

        storage.remove('split3')
        assert storage.is_valid_traffic_type('user') is False
        assert storage.is_valid_traffic_type('account') is False

    def test_traffic_type_inc_dec_logic(self, mocker):
        """Test that adding/removing split, handles traffic types correctly."""
        storage = InMemorySplitStorage()

        split1 = mocker.Mock()
        name1_prop = mocker.PropertyMock()
        name1_prop.return_value = 'split1'
        type(split1).name = name1_prop

        split2 = mocker.Mock()
        name2_prop = mocker.PropertyMock()
        name2_prop.return_value = 'split1'
        type(split2).name = name2_prop

        tt_user = mocker.PropertyMock()
        tt_user.return_value = 'user'

        tt_account = mocker.PropertyMock()
        tt_account.return_value = 'account'

        type(split1).traffic_type_name = tt_user
        type(split2).traffic_type_name = tt_account

        storage.put(split1)
        assert storage.is_valid_traffic_type('user') is True
        assert storage.is_valid_traffic_type('account') is False

        storage.put(split2)
        assert storage.is_valid_traffic_type('user') is False
        assert storage.is_valid_traffic_type('account') is True

    def test_kill_locally(self):
        """Test kill local."""
        storage = InMemorySplitStorage()

        split = Split('some_split', 123456789, False, 'some', 'traffic_type',
                      'ACTIVE', 1)
        storage.put(split)
        storage.set_change_number(1)

        storage.kill_locally('test', 'default_treatment', 2)
        assert storage.get('test') is None

        storage.kill_locally('some_split', 'default_treatment', 0)
        assert storage.get('some_split').change_number == 1
        assert storage.get('some_split').killed is False
        assert storage.get('some_split').default_treatment == 'some'

        storage.kill_locally('some_split', 'default_treatment', 3)
        assert storage.get('some_split').change_number == 3


class InMemorySegmentStorageTests(object):
    """In memory segment storage tests."""

    def test_segment_storage_retrieval(self, mocker):
        """Test storing and retrieving segments."""
        storage = InMemorySegmentStorage()
        segment = mocker.Mock(spec=Segment)
        name_property = mocker.PropertyMock()
        name_property.return_value = 'some_segment'
        type(segment).name = name_property

        storage.put(segment)
        assert storage.get('some_segment') == segment
        assert storage.get('nonexistant-segment') is None

    def test_change_number(self, mocker):
        """Test storing and retrieving segment changeNumber."""
        storage = InMemorySegmentStorage()
        storage.set_change_number('some_segment', 123)
        # Change number is not updated if segment doesn't exist
        assert storage.get_change_number('some_segment') is None
        assert storage.get_change_number('nonexistant-segment') is None

        # Change number is updated if segment does exist.
        storage = InMemorySegmentStorage()
        segment = mocker.Mock(spec=Segment)
        name_property = mocker.PropertyMock()
        name_property.return_value = 'some_segment'
        type(segment).name = name_property
        storage.put(segment)
        storage.set_change_number('some_segment', 123)
        assert storage.get_change_number('some_segment') == 123

    def test_segment_contains(self, mocker):
        """Test using storage to determine whether a key belongs to a segment."""
        storage = InMemorySegmentStorage()
        segment = mocker.Mock(spec=Segment)
        name_property = mocker.PropertyMock()
        name_property.return_value = 'some_segment'
        type(segment).name = name_property
        storage.put(segment)

        storage.segment_contains('some_segment', 'abc')
        assert segment.contains.mock_calls[0] == mocker.call('abc')

    def test_segment_update(self):
        """Test updating a segment."""
        storage = InMemorySegmentStorage()
        segment = Segment('some_segment', ['key1', 'key2', 'key3'], 123)
        storage.put(segment)
        assert storage.get('some_segment') == segment

        storage.update('some_segment', ['key4', 'key5'], ['key2', 'key3'], 456)
        assert storage.segment_contains('some_segment', 'key1')
        assert storage.segment_contains('some_segment', 'key4')
        assert storage.segment_contains('some_segment', 'key5')
        assert not storage.segment_contains('some_segment', 'key2')
        assert not storage.segment_contains('some_segment', 'key3')
        assert storage.get_change_number('some_segment') == 456


class InMemoryImpressionsStorageTests(object):
    """InMemory impressions storage test cases."""

    def test_push_pop_impressions(self):
        """Test pushing and retrieving impressions."""
        storage = InMemoryImpressionStorage(100)
        storage.put([Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])
        storage.put([Impression('key2', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])
        storage.put([Impression('key3', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])

        # Assert impressions are retrieved in the same order they are inserted.
        assert storage.pop_many(1) == [
            Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert storage.pop_many(1) == [
            Impression('key2', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert storage.pop_many(1) == [
            Impression('key3', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]

        # Assert inserting multiple impressions at once works and maintains order.
        impressions = [
            Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654),
            Impression('key2', 'feature1', 'on', 'l1', 123456, 'b1', 321654),
            Impression('key3', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert storage.put(impressions)

        # Assert impressions are retrieved in the same order they are inserted.
        assert storage.pop_many(1) == [
            Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert storage.pop_many(1) == [
            Impression('key2', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]
        assert storage.pop_many(1) == [
            Impression('key3', 'feature1', 'on', 'l1', 123456, 'b1', 321654)
        ]

    def test_queue_full_hook(self, mocker):
        """Test queue_full_hook is executed when the queue is full."""
        storage = InMemoryImpressionStorage(100)
        queue_full_hook = mocker.Mock()
        storage.set_queue_full_hook(queue_full_hook)
        impressions = [
            Impression('key%d' % i, 'feature1', 'on', 'l1', 123456, 'b1', 321654)
            for i in range(0, 101)
        ]
        storage.put(impressions)
        assert queue_full_hook.mock_calls == mocker.call()

    def test_clear(self):
        """Test clear method."""
        storage = InMemoryImpressionStorage(100)
        storage.put([Impression('key1', 'feature1', 'on', 'l1', 123456, 'b1', 321654)])

        assert storage._impressions.qsize() == 1
        storage.clear()
        assert storage._impressions.qsize() == 0


class InMemoryEventsStorageTests(object):
    """InMemory events storage test cases."""

    def test_push_pop_events(self):
        """Test pushing and retrieving events."""
        storage = InMemoryEventStorage(100)
        storage.put([EventWrapper(
            event=Event('key1', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])
        storage.put([EventWrapper(
            event=Event('key2', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])
        storage.put([EventWrapper(
            event=Event('key3', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])

        # Assert impressions are retrieved in the same order they are inserted.
        assert storage.pop_many(1) == [Event('key1', 'user', 'purchase', 3.5, 123456, None)]
        assert storage.pop_many(1) == [Event('key2', 'user', 'purchase', 3.5, 123456, None)]
        assert storage.pop_many(1) == [Event('key3', 'user', 'purchase', 3.5, 123456, None)]

        # Assert inserting multiple impressions at once works and maintains order.
        events = [
            EventWrapper(
                event=Event('key1', 'user', 'purchase', 3.5, 123456, None),
                size=1024,
            ),
            EventWrapper(
                event=Event('key2', 'user', 'purchase', 3.5, 123456, None),
                size=1024,
            ),
            EventWrapper(
                event=Event('key3', 'user', 'purchase', 3.5, 123456, None),
                size=1024,
            ),
        ]
        assert storage.put(events)

        # Assert events are retrieved in the same order they are inserted.
        assert storage.pop_many(1) == [Event('key1', 'user', 'purchase', 3.5, 123456, None)]
        assert storage.pop_many(1) == [Event('key2', 'user', 'purchase', 3.5, 123456, None)]
        assert storage.pop_many(1) == [Event('key3', 'user', 'purchase', 3.5, 123456, None)]

    def test_queue_full_hook(self, mocker):
        """Test queue_full_hook is executed when the queue is full."""
        storage = InMemoryEventStorage(100)
        queue_full_hook = mocker.Mock()
        storage.set_queue_full_hook(queue_full_hook)
        events = [EventWrapper(event=Event('key%d' % i, 'user', 'purchase', 12.5, 321654, None), size=1024) for i in range(0, 101)]
        storage.put(events)
        assert queue_full_hook.mock_calls == [mocker.call()]

    def test_queue_full_hook_properties(self, mocker):
        """Test queue_full_hook is executed when the queue is full regarding properties."""
        storage = InMemoryEventStorage(200)
        queue_full_hook = mocker.Mock()
        storage.set_queue_full_hook(queue_full_hook)
        events = [EventWrapper(event=Event('key%d' % i, 'user', 'purchase', 12.5, 1, None),  size=32768) for i in range(160)]
        storage.put(events)
        assert queue_full_hook.mock_calls == [mocker.call()]

    def test_clear(self):
        """Test clear method."""
        storage = InMemoryEventStorage(100)
        storage.put([EventWrapper(
            event=Event('key1', 'user', 'purchase', 3.5, 123456, None),
            size=1024,
        )])

        assert storage._events.qsize() == 1
        storage.clear()
        assert storage._events.qsize() == 0

class InMemoryTelemetryStorageTests(object):
    """InMemory telemetry storage test cases."""

    def test_resets(self):
        storage = InMemoryTelemetryStorage()

        assert(storage._counters == {'impressionsQueued': 0, 'impressionsDeduped': 0, 'impressionsDropped': 0, 'eventsQueued': 0, 'eventsDropped': 0,
                        'authRejections': 0, 'tokenRefreshes': 0})
        assert(storage._exceptions == {'methodExceptions': {'treatment': 0, 'treatments': 0, 'treatmentWithConfig': 0, 'treatmentsWithConfig': 0, 'track': 0}})
        assert(storage._records == {'lastSynchronizations': {'split': 0, 'segment': 0, 'impression': 0, 'impressionCount': 0, 'event': 0, 'telemetry': 0, 'token': 0},
                         'sessionLength': 0})
        assert(storage._http_errors == {'split': {}, 'segment': {}, 'impression': {}, 'impressionCount': {}, 'event': {}, 'telemetry': {}, 'token': {}})
        assert(storage._config == {'blockUntilReadyTimeout':0, 'notReady':0, 'timeUntilReady': 0})
        assert(storage._streaming_events == [])
        assert(storage._tags == [])

        assert(storage._latencies == {'methodLatencies': {'treatment': [], 'treatments': [], 'treatmentWithConfig': [], 'treatmentsWithConfig': [], 'track': []},
                           'httpLatencies': {'split': [], 'segment': [], 'impression': [], 'impressionCount': [], 'event': [], 'telemetry': [], 'token': []}})

    def test_record_config(self):
        storage = InMemoryTelemetryStorage()
        config = {'operationMode': 'inmemory',
                  'streamingEnabled': True,
                  'impressionsQueueSize': 100,
                  'eventsQueueSize': 200,
                  'impressionsMode': 'DEBUG',''
                  'impressionListener': None,
                  'featuresRefreshRate': 30,
                  'segmentsRefreshRate': 30,
                  'impressionsRefreshRate': 60,
                  'eventsPushRate': 60,
                  'metrcsRefreshRate': 10,
                  'activeFactoryCount': 1,
                  'redundantFactoryCount': 0
                  }
        storage.record_config(config)
        assert(storage.get_config_stats() == {'operationMode': 2,
            'storageType': storage._get_storage_type(config['operationMode']),
            'streamingEnabled': config['streamingEnabled'],
            'refreshRate': storage._get_refresh_rates(config),
            'urlOverride': storage._get_url_overrides(config),
            'impressionsQueueSize': config['impressionsQueueSize'],
            'eventsQueueSize': config['eventsQueueSize'],
            'impressionsMode': storage._get_impressions_mode(config['impressionsMode']),
            'impressionListener': True if config['impressionListener'] is not None else False,
            'httpProxy': storage._check_if_proxy_detected(),
            'activeFactoryCount': 1,
            'blockUntilReadyTimeout': 0,
            'timeUntilReady': 0,
            'notReady': 0,
            'redundantFactoryCount': 0}
            )

    def test_record_counters(self):
        storage = InMemoryTelemetryStorage()

        storage.record_ready_time(10)
        assert(storage._config['timeUntilReady'] == 10)

        storage.add_tag('tag')
        assert('tag' in storage._tags)
        [storage.add_tag('tag') for i in range(1, 25)]
        assert(len(storage._tags) == 10)

        storage.record_bur_time_out()
        storage.record_bur_time_out()
        assert(storage._config['blockUntilReadyTimeout'] == 2)
        assert(storage.get_bur_time_outs() == 2)

        storage.record_not_ready_usage()
        storage.record_not_ready_usage()
        assert(storage._config['notReady'] ==  2)
        assert(storage.get_non_ready_usage() == 2)

        storage.record_exception('treatment')
        assert(storage._exceptions['methodExceptions']['treatment'] == 1)

        storage.record_impression_stats('impressionsQueued', 5)
        assert(storage._counters['impressionsQueued'] == 5)

        storage.record_event_stats('eventsDropped', 6)
        assert(storage._counters['eventsDropped'] == 6)

        storage.record_suceessful_sync('segment', 10)
        assert(storage._records['lastSynchronizations']['segment'] == 10)

        storage.record_sync_error('segment', '500')
        assert(storage._http_errors['segment']['500'] == 1)

        storage.record_auth_rejections()
        storage.record_auth_rejections()
        assert(storage._counters['authRejections'] == 2)

        storage.record_token_refreshes()
        storage.record_token_refreshes()
        assert(storage._counters['tokenRefreshes'] == 2)

        storage.record_streaming_event({'type': 'update', 'data': 'split', 'time': 1234})
        assert(storage._streaming_events[0] == {'type': 'update', 'data': 'split', 'time': 1234})
        [storage.record_streaming_event({'type': 'update', 'data': 'split', 'time': 1234}) for i in range(1, 25)]
        assert(len(storage._streaming_events) == 20)

        storage.record_session_length(20)
        assert(storage._records['sessionLength'] == 20)

    def test_record_latencies(self):
        storage = InMemoryTelemetryStorage()

        storage.record_latency('treatment', 10)
        assert(storage._latencies['methodLatencies']['treatment'][0] == 10)
        [storage.record_latency('treatment', 10) for i in range(1, 25)]
        assert(len(storage._latencies['methodLatencies']['treatment']) == 23)

        storage.record_sync_latency('split', 20)
        assert(storage._latencies['httpLatencies']['split'][0] == 20)
        [storage.record_sync_latency('split', 20) for i in range(1, 25)]
        assert(len(storage._latencies['httpLatencies']['split']) == 23)

    def test_pop_counters(self):
        storage = InMemoryTelemetryStorage()

        [storage.record_exception('treatment') for i in range(2)]
        storage.record_exception('treatments')
        storage.record_exception('treatmentWithConfig')
        [storage.record_exception('treatmentsWithConfig') for i in range(5)]
        [storage.record_exception('track') for i in range(3)]
        exceptions = storage.pop_exceptions()
        assert(storage._exceptions == {'methodExceptions': {'treatment': 0, 'treatments': 0, 'treatmentWithConfig': 0, 'treatmentsWithConfig': 0, 'track': 0}})
        assert(exceptions == {'treatment': 2, 'treatments': 1, 'treatmentWithConfig': 1, 'treatmentsWithConfig': 5, 'track': 3})

        storage.add_tag('tag1')
        storage.add_tag('tag2')
        tags = storage.pop_tags()
        assert(storage._tags == [])
        assert(tags == ['tag1', 'tag2'])

        [storage.record_sync_error('segment', str(i)) for i in [500, 501, 502]]
        [storage.record_sync_error('split', str(i)) for i in [400, 401, 402]]
        storage.record_sync_error('impression', '502')
        [storage.record_sync_error('impressionCount', str(i)) for i in [501, 502]]
        storage.record_sync_error('event', '501')
        storage.record_sync_error('telemetry', '505')
        [storage.record_sync_error('token', '502') for i in range(5)]
        http_errors = storage.pop_http_errors()
        assert(http_errors == {'split': {'400': 1, '401': 1, '402': 1}, 'segment': {'500': 1, '501': 1, '502': 1},
                                        'impression': {'502': 1}, 'impressionCount': {'501': 1, '502': 1},
                                        'event': {'501': 1}, 'telemetry': {'505': 1}, 'token': {'502': 5}})
        assert(storage._http_errors == {'split': {}, 'segment': {}, 'impression': {},
                               'impressionCount': {}, 'event': {}, 'telemetry': {}, 'token': {}})

        storage.record_auth_rejections()
        storage.record_auth_rejections()
        auth_rejections = storage.pop_auth_rejections()
        assert(storage._counters['authRejections'] == 0)
        assert(auth_rejections == 2)

        storage.record_token_refreshes()
        storage.record_token_refreshes()
        token_refreshes = storage.pop_token_refreshes()
        assert(storage._counters['tokenRefreshes'] == 0)
        assert(token_refreshes == 2)

        storage.record_streaming_event({'type': 'update', 'data': 'split', 'time': 1234})
        storage.record_streaming_event({'type': 'delete', 'data': 'split', 'time': 1234})
        streaming_events = storage.pop_streaming_events()
        assert(storage._streaming_events == [])
        assert(streaming_events == [{'type': 'update', 'data': 'split', 'time': 1234},
                                    {'type': 'delete', 'data': 'split', 'time': 1234}])

    def test_pop_latencies(self):
        storage = InMemoryTelemetryStorage()

        [storage.record_latency('treatment', i) for i in [5, 1, 0, 0]]
        [storage.record_latency('treatments', i) for i in [7, 10, 4, 3]]
        [storage.record_latency('treatmentWithConfig', i) for i in [2]]
        [storage.record_latency('treatmentsWithConfig', i) for i in [5, 4]]
        [storage.record_latency('track', i) for i in [1, 0, 1]]
        latencies = storage.pop_latencies()
        assert(storage._latencies['methodLatencies'] ==  {'treatment': [], 'treatments': [],
                                'treatmentWithConfig': [], 'treatmentsWithConfig': [], 'track': []})
        assert(latencies ==  {'treatment': [5, 1, 0, 0], 'treatments': [7, 10, 4, 3],
                              'treatmentWithConfig': [2], 'treatmentsWithConfig': [5, 4], 'track': [1, 0, 1]})

        [storage.record_sync_latency('split', i) for i in [50, 10, 20, 40]]
        [storage.record_sync_latency('segment', i) for i in [70, 100, 40, 30]]
        [storage.record_sync_latency('impression', i) for i in [10, 20]]
        [storage.record_sync_latency('impressionCount', i) for i in [5, 10]]
        [storage.record_sync_latency('event', i) for i in [50, 40]]
        [storage.record_sync_latency('telemetry', i) for i in [100, 50, 160]]
        [storage.record_sync_latency('token', i) for i in [10, 15, 100]]
        sync_latency = storage.pop_http_latencies()
        assert(storage._latencies['httpLatencies'] == {'split': [], 'segment': [], 'impression': [], 'impressionCount': [], 'event': [], 'telemetry': [], 'token': []})
        assert(sync_latency == {'split': [50, 10, 20, 40], 'segment': [70, 100, 40, 30],
                                'impression': [10, 20], 'impressionCount': [5, 10], 'event': [50, 40],
                                'telemetry': [100, 50, 160], 'token': [10, 15, 100]})
