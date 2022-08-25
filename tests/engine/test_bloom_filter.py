"""BloomFilter unit tests."""

from random import random
import uuid
from splitio.engine.filters import BloomFilter

class BloomFilterTests(object):
    """StandardRecorderTests test cases."""

    def test_bloom_filter_methods(self, mocker):
        bloom_filter = BloomFilter()
        key1 = str(uuid.uuid4())
        key2 = str(uuid.uuid4())
        bloom_filter.add(key1)

        assert(bloom_filter.contains(key1))
        assert(not bloom_filter.contains(key2))

        bloom_filter.clear()
        assert(not bloom_filter.contains(key1))

        bloom_filter.add(key1)
        bloom_filter.add(key2)
        assert(bloom_filter.contains(key1))
        assert(bloom_filter.contains(key2))

    def test_bloom_filter_error_percentage(self, mocker):
        arr_storage = []
        total_sample = 20000
        error_rate = 0.01
        bloom_filter = BloomFilter(total_sample, error_rate)

        for x in range(1, total_sample):
            myuuid = str(uuid.uuid4())
            bloom_filter.add(myuuid)
            arr_storage.append(myuuid)

        false_positive_count = 0
        for x in range(1, total_sample):
            y = int(random()*total_sample*5)
            if y > total_sample - 2:
                myuuid = str(uuid.uuid4())
                if myuuid in arr_storage:
                    # False Negative
                    assert(bloom_filter.contains(myuuid))
                else:
                    if bloom_filter.contains(myuuid):
                        # False Positive
                        false_positive_count = false_positive_count + 1
            else:
                myuuid = arr_storage[y]
                assert(bloom_filter.contains(myuuid))
                # False Negative

        assert(false_positive_count/total_sample <= error_rate)