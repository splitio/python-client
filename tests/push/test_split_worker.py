"""Split Worker tests."""
import time
import queue
import pytest

from splitio.api import APIException
from splitio.push.splitworker import SplitWorker
from splitio.push.parser import SplitChangeUpdate
from splitio.engine.telemetry import TelemetryStorageProducer
from splitio.storage.inmemmory import InMemoryTelemetryStorage, InMemorySplitStorage, InMemorySegmentStorage
change_number_received = None


def handler_sync(change_number):
    global change_number_received
    change_number_received = change_number
    return


class SplitWorkerTests(object):

    def test_on_error(self, mocker):
        q = queue.Queue()
        def handler_sync(change_number):
            raise APIException('some')

        split_worker = SplitWorker(handler_sync, mocker.Mock(), q, mocker.Mock(), mocker.Mock(), mocker.Mock())
        split_worker.start()
        assert split_worker.is_running()

        q.put(SplitChangeUpdate('some', 'SPLIT_UPDATE', 123456789, None, None, None))
        with pytest.raises(Exception):
            split_worker._handler()

        assert split_worker.is_running()
        assert split_worker._worker.is_alive()
        split_worker.stop()
        time.sleep(1)
        assert not split_worker.is_running()
        assert not split_worker._worker.is_alive()

    def test_handler(self, mocker):
        q = queue.Queue()
        split_worker = SplitWorker(handler_sync, mocker.Mock(), q, mocker.Mock(), mocker.Mock(), mocker.Mock())

        global change_number_received
        assert not split_worker.is_running()
        split_worker.start()
        assert split_worker.is_running()

        # should call the handler
        q.put(SplitChangeUpdate('some', 'SPLIT_UPDATE', 123456789, None, None, None))
        time.sleep(0.1)
        assert change_number_received == 123456789

        def get_change_number():
            return 2345
        split_worker._feature_flag_storage.get_change_number = get_change_number

        self.new_change_number = 0
        def update(to_add, to_delete, change_number):
            self.new_change_number = change_number
        split_worker._feature_flag_storage.update = update
        split_worker._feature_flag_storage.config_flag_sets_used = 0

        # should call the handler
        q.put(SplitChangeUpdate('some', 'SPLIT_UPDATE', 123456790, 12345,  "{}", 1))
        time.sleep(0.1)
        assert change_number_received == 123456790

        # should call the handler
        change_number_received = 0
        q.put(SplitChangeUpdate('some', 'SPLIT_UPDATE', 123456790, 12345,  "{}", 3))
        time.sleep(0.1)
        assert change_number_received == 123456790

        # should Not call the handler
        change_number_received = 0
        q.put(SplitChangeUpdate('some', 'SPLIT_UPDATE', 123456, 2345, "eJzEUtFq20AQ/JUwz2c4WZZr3ZupTQh1FKjcQinGrKU95cjpZE6nh9To34ssJ3FNX0sfd3Zm53b2TgietDbF9vXIGdUMha5lDwFTQiGOmTQlchLRPJlEEZeTVJZ6oimWZTpP5WyWQMCNyoOxZPft0ZoA8TZ5aW1TUDCNg4qk/AueM5dQkyiez6IonS6mAu0IzWWSxovFLBZoA4WuhcLy8/bh+xoCL8bagaXJtixQsqbOhq1nCjW7AIVGawgUz+Qqzrr6wB4qmi9m00/JIk7TZCpAtmqgpgJF47SpOn9+UQt16s9YaS71z9NHOYQFha9Pm83Tty0EagrFM/t733RHqIFZH4wb7LDMVh+Ecc4Lv+ZsuQiNH8hXF3hLv39XXNCHbJ+v7x/X2eDmuKLA74sPihVr47jMuRpWfxy1Kwo0GLQjmv1xpBFD3+96gSP5cLVouM7QQaA1vxhK9uKmd853bEZS9jsBSwe2UDDu7mJxd2Mo/muQy81m/2X9I7+N8R/FcPmUd76zjH7X/w4AAP//90glTw==", 2))
        time.sleep(0.1)
        assert change_number_received == 0

        split_worker.stop()
        assert not split_worker.is_running()

    def test_compression(self, mocker):
        q = queue.Queue()
        telemetry_storage = InMemoryTelemetryStorage()
        telemetry_producer = TelemetryStorageProducer(telemetry_storage)
        telemetry_runtime_producer = telemetry_producer.get_telemetry_runtime_producer()
        split_worker = SplitWorker(handler_sync, mocker.Mock(), q, mocker.Mock(), mocker.Mock(), telemetry_runtime_producer)
        global change_number_received
        split_worker.start()
        def get_change_number():
            return 2345
        split_worker._feature_flag_storage.get_change_number = get_change_number

        self._feature_flag_added = None
        self._feature_flag_deleted = None
        def update(feature_flag_add, feature_flag_delete, change_number):
            self._feature_flag_added = feature_flag_add
            self._feature_flag_deleted = feature_flag_delete
        split_worker._feature_flag_storage.update = update
        split_worker._feature_flag_storage.config_flag_sets_used = 0

        # compression 0
        self._feature_flag_added = None
        q.put(SplitChangeUpdate('some', 'SPLIT_UPDATE', 123456790, 2345,  'eyJ0cmFmZmljVHlwZU5hbWUiOiJ1c2VyIiwiaWQiOiIzM2VhZmE1MC0xYTY1LTExZWQtOTBkZi1mYTMwZDk2OTA0NDUiLCJuYW1lIjoiYmlsYWxfc3BsaXQiLCJ0cmFmZmljQWxsb2NhdGlvbiI6MTAwLCJ0cmFmZmljQWxsb2NhdGlvblNlZWQiOi0xMzY0MTE5MjgyLCJzZWVkIjotNjA1OTM4ODQzLCJzdGF0dXMiOiJBQ1RJVkUiLCJraWxsZWQiOmZhbHNlLCJkZWZhdWx0VHJlYXRtZW50Ijoib2ZmIiwiY2hhbmdlTnVtYmVyIjoxNjg0MzQwOTA4NDc1LCJhbGdvIjoyLCJjb25maWd1cmF0aW9ucyI6e30sImNvbmRpdGlvbnMiOlt7ImNvbmRpdGlvblR5cGUiOiJST0xMT1VUIiwibWF0Y2hlckdyb3VwIjp7ImNvbWJpbmVyIjoiQU5EIiwibWF0Y2hlcnMiOlt7ImtleVNlbGVjdG9yIjp7InRyYWZmaWNUeXBlIjoidXNlciJ9LCJtYXRjaGVyVHlwZSI6IklOX1NFR01FTlQiLCJuZWdhdGUiOmZhbHNlLCJ1c2VyRGVmaW5lZFNlZ21lbnRNYXRjaGVyRGF0YSI6eyJzZWdtZW50TmFtZSI6ImJpbGFsX3NlZ21lbnQifX1dfSwicGFydGl0aW9ucyI6W3sidHJlYXRtZW50Ijoib24iLCJzaXplIjowfSx7InRyZWF0bWVudCI6Im9mZiIsInNpemUiOjEwMH1dLCJsYWJlbCI6ImluIHNlZ21lbnQgYmlsYWxfc2VnbWVudCJ9LHsiY29uZGl0aW9uVHlwZSI6IlJPTExPVVQiLCJtYXRjaGVyR3JvdXAiOnsiY29tYmluZXIiOiJBTkQiLCJtYXRjaGVycyI6W3sia2V5U2VsZWN0b3IiOnsidHJhZmZpY1R5cGUiOiJ1c2VyIn0sIm1hdGNoZXJUeXBlIjoiQUxMX0tFWVMiLCJuZWdhdGUiOmZhbHNlfV19LCJwYXJ0aXRpb25zIjpbeyJ0cmVhdG1lbnQiOiJvbiIsInNpemUiOjB9LHsidHJlYXRtZW50Ijoib2ZmIiwic2l6ZSI6MTAwfV0sImxhYmVsIjoiZGVmYXVsdCBydWxlIn1dfQ==', 0))
        time.sleep(0.1)
#        pytest.set_trace()
        assert self._feature_flag_added[0].name == 'bilal_split'
        assert telemetry_storage._counters._update_from_sse['sp'] == 1

        # compression 2
        self._feature_flag_added = None
        q.put(SplitChangeUpdate('some', 'SPLIT_UPDATE', 123456790, 2345,  'eJzEUtFq20AQ/JUwz2c4WZZr3ZupTQh1FKjcQinGrKU95cjpZE6nh9To34ssJ3FNX0sfd3Zm53b2TgietDbF9vXIGdUMha5lDwFTQiGOmTQlchLRPJlEEZeTVJZ6oimWZTpP5WyWQMCNyoOxZPft0ZoA8TZ5aW1TUDCNg4qk/AueM5dQkyiez6IonS6mAu0IzWWSxovFLBZoA4WuhcLy8/bh+xoCL8bagaXJtixQsqbOhq1nCjW7AIVGawgUz+Qqzrr6wB4qmi9m00/JIk7TZCpAtmqgpgJF47SpOn9+UQt16s9YaS71z9NHOYQFha9Pm83Tty0EagrFM/t733RHqIFZH4wb7LDMVh+Ecc4Lv+ZsuQiNH8hXF3hLv39XXNCHbJ+v7x/X2eDmuKLA74sPihVr47jMuRpWfxy1Kwo0GLQjmv1xpBFD3+96gSP5cLVouM7QQaA1vxhK9uKmd853bEZS9jsBSwe2UDDu7mJxd2Mo/muQy81m/2X9I7+N8R/FcPmUd76zjH7X/w4AAP//90glTw==', 2))
        time.sleep(0.1)
        assert self._feature_flag_added[0].name == 'bilal_split'
        assert telemetry_storage._counters._update_from_sse['sp'] == 2

        # compression 1
        self._feature_flag_added = None
        q.put(SplitChangeUpdate('some', 'SPLIT_UPDATE', 123456790, 2345,  'H4sIAAkVZWQC/8WST0+DQBDFv0qzZ0ig/BF6a2xjGismUk2MaZopzOKmy9Isy0EbvrtDwbY2Xo233Tdv5se85cCMBs5FtvrYYwIlsglratTMYiKns+chcAgc24UwsF0Xczt2cm5z8Jw8DmPH9wPyqr5zKyTITb2XwpA4TJ5KWWVgRKXYxHWcX/QUkVi264W+68bjaGyxupdCJ4i9KPI9UgyYpibI9Ha1eJnT/J2QsnNxkDVaLEcOjTQrjWBKVIasFefky95BFZg05Zb2mrhh5I9vgsiL44BAIIuKTeiQVYqLotHHLyLOoT1quRjub4fztQuLxj89LpePzytClGCyd9R3umr21ErOcitUh2PTZHY29HN2+JGixMxUujNfvMB3+u2pY1AXySad3z3Mk46msACDp8W7jhly4uUpFt3qD33vDAx0gLpXkx+P1GusbdcE24M2F4uaywwVEWvxSa1Oa13Vjvn2RXradm0xCVuUVBJqNCBGV0DrX4OcLpeb+/lreh3jH8Uw/JQj3UhkxPgCCurdEnADAAA=', 1))
        time.sleep(0.1)
        assert self._feature_flag_added[0].name == 'bilal_split'
        assert telemetry_storage._counters._update_from_sse['sp'] == 3

        # should call delete split
        self._feature_flag_added = None
        self._feature_flag_deleted = None
        q.put(SplitChangeUpdate('some', 'SPLIT_UPDATE', 123456790, 2345,  'eyJ0cmFmZmljVHlwZU5hbWUiOiAidXNlciIsICJpZCI6ICIzM2VhZmE1MC0xYTY1LTExZWQtOTBkZi1mYTMwZDk2OTA0NDUiLCAibmFtZSI6ICJiaWxhbF9zcGxpdCIsICJ0cmFmZmljQWxsb2NhdGlvbiI6IDEwMCwgInRyYWZmaWNBbGxvY2F0aW9uU2VlZCI6IC0xMzY0MTE5MjgyLCAic2VlZCI6IC02MDU5Mzg4NDMsICJzdGF0dXMiOiAiQVJDSElWRUQiLCAia2lsbGVkIjogZmFsc2UsICJkZWZhdWx0VHJlYXRtZW50IjogIm9mZiIsICJjaGFuZ2VOdW1iZXIiOiAxNjg0Mjc1ODM5OTUyLCAiYWxnbyI6IDIsICJjb25maWd1cmF0aW9ucyI6IHt9LCAiY29uZGl0aW9ucyI6IFt7ImNvbmRpdGlvblR5cGUiOiAiUk9MTE9VVCIsICJtYXRjaGVyR3JvdXAiOiB7ImNvbWJpbmVyIjogIkFORCIsICJtYXRjaGVycyI6IFt7ImtleVNlbGVjdG9yIjogeyJ0cmFmZmljVHlwZSI6ICJ1c2VyIn0sICJtYXRjaGVyVHlwZSI6ICJJTl9TRUdNRU5UIiwgIm5lZ2F0ZSI6IGZhbHNlLCAidXNlckRlZmluZWRTZWdtZW50TWF0Y2hlckRhdGEiOiB7InNlZ21lbnROYW1lIjogImJpbGFsX3NlZ21lbnQifX1dfSwgInBhcnRpdGlvbnMiOiBbeyJ0cmVhdG1lbnQiOiAib24iLCAic2l6ZSI6IDB9LCB7InRyZWF0bWVudCI6ICJvZmYiLCAic2l6ZSI6IDEwMH1dLCAibGFiZWwiOiAiaW4gc2VnbWVudCBiaWxhbF9zZWdtZW50In0sIHsiY29uZGl0aW9uVHlwZSI6ICJST0xMT1VUIiwgIm1hdGNoZXJHcm91cCI6IHsiY29tYmluZXIiOiAiQU5EIiwgIm1hdGNoZXJzIjogW3sia2V5U2VsZWN0b3IiOiB7InRyYWZmaWNUeXBlIjogInVzZXIifSwgIm1hdGNoZXJUeXBlIjogIkFMTF9LRVlTIiwgIm5lZ2F0ZSI6IGZhbHNlfV19LCAicGFydGl0aW9ucyI6IFt7InRyZWF0bWVudCI6ICJvbiIsICJzaXplIjogMH0sIHsidHJlYXRtZW50IjogIm9mZiIsICJzaXplIjogMTAwfV0sICJsYWJlbCI6ICJkZWZhdWx0IHJ1bGUifV19', 0))
        time.sleep(0.1)
        assert self._feature_flag_deleted[0] == 'bilal_split'
        self._feature_flag_added = None

    def test_edge_cases(self, mocker):
        q = queue.Queue()
        split_worker = SplitWorker(handler_sync, mocker.Mock(), q, mocker.Mock(), mocker.Mock(), mocker.Mock())
        global change_number_received
        split_worker.start()

        def get_change_number():
            return 2345

        def put(feature_flag):
            self._feature_flag = feature_flag

        split_worker._feature_flag_storage.get_change_number = get_change_number
        split_worker._feature_flag_storage.put = put

        # should Not call the handler
        self._feature_flag = None
        change_number_received = 0
        q.put(SplitChangeUpdate('some', 'SPLIT_UPDATE', 123456, 2345, "/2X9I7+N8R/FcPmUd76zjH7X/w4AAP//90glTw==", 2))
        time.sleep(0.1)
        assert self._feature_flag == None

        # should Not call the handler
        self._feature_flag = None
        change_number_received = 0
        q.put(SplitChangeUpdate('some', 'SPLIT_UPDATE', 123456, 2345, "/2X9I7+N8R/FcPmUd76zjH7X/w4AAP//90glTw==", 4))
        time.sleep(0.1)
        assert self._feature_flag == None

        # should Not call the handler
        self._feature_flag = None
        change_number_received = 0
        q.put(SplitChangeUpdate('some', 'SPLIT_UPDATE', 123456, None,  'eJzEUtFq20AQ/JUwz2c4WZZr3ZupTQh1FKjcQinGrKU95cjpZE6nh9To34ssJ3FNX0sfd3Zm53b2TgietDbF9vXIGdUMha5lDwFTQiGOmTQlchLRPJlEEZeTVJZ6oimWZTpP5WyWQMCNyoOxZPft0ZoA8TZ5aW1TUDCNg4qk/AueM5dQkyiez6IonS6mAu0IzWWSxovFLBZoA4WuhcLy8/bh+xoCL8bagaXJtixQsqbOhq1nCjW7AIVGawgUz+Qqzrr6wB4qmi9m00/JIk7TZCpAtmqgpgJF47SpOn9+UQt16s9YaS71z9NHOYQFha9Pm83Tty0EagrFM/t733RHqIFZH4wb7LDMVh+Ecc4Lv+ZsuQiNH8hXF3hLv39XXNCHbJ+v7x/X2eDmuKLA74sPihVr47jMuRpWfxy1Kwo0GLQjmv1xpBFD3+96gSP5cLVouM7QQaA1vxhK9uKmd853bEZS9jsBSwe2UDDu7mJxd2Mo/muQy81m/2X9I7+N8R/FcPmUd76zjH7X/w4AAP//90glTw==', 2))
        time.sleep(0.1)
        assert self._feature_flag == None

        # should Not call the handler
        self._feature_flag = None
        change_number_received = 0
        q.put(SplitChangeUpdate('some', 'SPLIT_UPDATE', 123456, 2345, None, 1))
        time.sleep(0.1)
        assert self._feature_flag == None

    def test_fetch_segment(self, mocker):
        q = queue.Queue()
        split_storage = InMemorySplitStorage()
        segment_storage = InMemorySegmentStorage()

        self.segment_name = None
        def segment_handler_sync(segment_name, change_number):
            self.segment_name = segment_name
            return
        split_worker = SplitWorker(handler_sync, segment_handler_sync, q, split_storage, segment_storage, mocker.Mock())
        split_worker.start()

        def get_change_number():
            return 2345
        split_worker._feature_flag_storage.get_change_number = get_change_number

        def check_instant_ff_update(event):
            return True
        split_worker._check_instant_ff_update = check_instant_ff_update

        q.put(SplitChangeUpdate('some', 'SPLIT_UPDATE', 1675095324253, 2345, 'eyJjaGFuZ2VOdW1iZXIiOiAxNjc1MDk1MzI0MjUzLCAidHJhZmZpY1R5cGVOYW1lIjogInVzZXIiLCAibmFtZSI6ICJiaWxhbF9zcGxpdCIsICJ0cmFmZmljQWxsb2NhdGlvbiI6IDEwMCwgInRyYWZmaWNBbGxvY2F0aW9uU2VlZCI6IC0xMzY0MTE5MjgyLCAic2VlZCI6IC02MDU5Mzg4NDMsICJzdGF0dXMiOiAiQUNUSVZFIiwgImtpbGxlZCI6IGZhbHNlLCAiZGVmYXVsdFRyZWF0bWVudCI6ICJvZmYiLCAiYWxnbyI6IDIsICJjb25kaXRpb25zIjogW3siY29uZGl0aW9uVHlwZSI6ICJST0xMT1VUIiwgIm1hdGNoZXJHcm91cCI6IHsiY29tYmluZXIiOiAiQU5EIiwgIm1hdGNoZXJzIjogW3sia2V5U2VsZWN0b3IiOiB7InRyYWZmaWNUeXBlIjogInVzZXIiLCAiYXR0cmlidXRlIjogbnVsbH0sICJtYXRjaGVyVHlwZSI6ICJJTl9TRUdNRU5UIiwgIm5lZ2F0ZSI6IGZhbHNlLCAidXNlckRlZmluZWRTZWdtZW50TWF0Y2hlckRhdGEiOiB7InNlZ21lbnROYW1lIjogImJpbGFsX3NlZ21lbnQifSwgIndoaXRlbGlzdE1hdGNoZXJEYXRhIjogbnVsbCwgInVuYXJ5TnVtZXJpY01hdGNoZXJEYXRhIjogbnVsbCwgImJldHdlZW5NYXRjaGVyRGF0YSI6IG51bGwsICJkZXBlbmRlbmN5TWF0Y2hlckRhdGEiOiBudWxsLCAiYm9vbGVhbk1hdGNoZXJEYXRhIjogbnVsbCwgInN0cmluZ01hdGNoZXJEYXRhIjogbnVsbH1dfSwgInBhcnRpdGlvbnMiOiBbeyJ0cmVhdG1lbnQiOiAib24iLCAic2l6ZSI6IDB9LCB7InRyZWF0bWVudCI6ICJvZmYiLCAic2l6ZSI6IDEwMH1dLCAibGFiZWwiOiAiaW4gc2VnbWVudCBiaWxhbF9zZWdtZW50In0sIHsiY29uZGl0aW9uVHlwZSI6ICJST0xMT1VUIiwgIm1hdGNoZXJHcm91cCI6IHsiY29tYmluZXIiOiAiQU5EIiwgIm1hdGNoZXJzIjogW3sia2V5U2VsZWN0b3IiOiB7InRyYWZmaWNUeXBlIjogInVzZXIiLCAiYXR0cmlidXRlIjogbnVsbH0sICJtYXRjaGVyVHlwZSI6ICJBTExfS0VZUyIsICJuZWdhdGUiOiBmYWxzZSwgInVzZXJEZWZpbmVkU2VnbWVudE1hdGNoZXJEYXRhIjogbnVsbCwgIndoaXRlbGlzdE1hdGNoZXJEYXRhIjogbnVsbCwgInVuYXJ5TnVtZXJpY01hdGNoZXJEYXRhIjogbnVsbCwgImJldHdlZW5NYXRjaGVyRGF0YSI6IG51bGwsICJkZXBlbmRlbmN5TWF0Y2hlckRhdGEiOiBudWxsLCAiYm9vbGVhbk1hdGNoZXJEYXRhIjogbnVsbCwgInN0cmluZ01hdGNoZXJEYXRhIjogbnVsbH1dfSwgInBhcnRpdGlvbnMiOiBbeyJ0cmVhdG1lbnQiOiAib24iLCAic2l6ZSI6IDUwfSwgeyJ0cmVhdG1lbnQiOiAib2ZmIiwgInNpemUiOiA1MH1dLCAibGFiZWwiOiAiZGVmYXVsdCBydWxlIn1dLCAiY29uZmlndXJhdGlvbnMiOiB7fX0=', 0))
        time.sleep(0.1)
        assert self.segment_name == "bilal_segment"