_UNIQUE_KEYS_MAX_BULK_SIZE = 5000

class UniqueKeysSynchronizer(object):
    """Unique Keys Synchronizer class."""

    def __init__(self, impressions_sender_adapter, uniqe_keys_tracker):
        """
        Initialize Unique keys synchronizer instance

        :param uniqe_keys_tracker: instance of uniqe keys tracker
        :type uniqe_keys_tracker: splitio.engine.uniqur_key_tracker.UniqueKeysTracker
        """
        self._uniqe_keys_tracker = uniqe_keys_tracker
        self._max_bulk_size = _UNIQUE_KEYS_MAX_BULK_SIZE
        self._impressions_sender_adapter = impressions_sender_adapter

    def send_all(self):
        """
        Flush the unique keys dictionary to split back end.
        Limit each post to the max_bulk_size value.

        """
        cache, cache_size = self._uniqe_keys_tracker.get_cache_info_and_pop_all()
        if cache_size <= self._max_bulk_size:
            self._impressions_sender_adapter.record_unique_keys(cache)
        else:
            for bulk in self._split_cache_to_bulks(cache):
                self._impressions_sender_adapter.record_unique_keys(bulk)

    def _split_cache_to_bulks(self, cache):
        """
        Split the current unique keys dictionary into seperate dictionaries,
        each with the size of max_bulk_size. Overflow the last feature set() to new unique keys dictionary.

        :return: array of unique keys dictionaries
        :rtype: [Dict{'feature1': set(), 'feature2': set(), .. }]
        """
        bulks = []
        bulk = {}
        total_size = 0
        for feature in cache:
            total_size = total_size + len(cache[feature])
            if total_size > self._max_bulk_size:
                keys_list = list(cache[feature])
                chunk_list = self._chunks(keys_list)
                if bulk != {}:
                    bulks.append(bulk)
                for bulk_keys in chunk_list:
                    bulk[feature] = set(bulk_keys)
                    bulks.append(bulk)
                    bulk = {}
            else:
                bulk[feature] = self.cache[feature]
        if total_size != 0 and bulk != {}:
            bulks.append(bulk)

        return bulks

    def _chunks(self, keys_list):
        """
        Split array into chunks
        """
        for i in range(0, len(keys_list), self._max_bulk_size):
            yield keys_list[i:i + self._max_bulk_size]

class ClearFilterSynchronizer(object):
    """Clear filter class."""

    def __init__(self, unique_keys_tracker):
        """
        Initialize Unique keys synchronizer instance

        :param uniqe_keys_tracker: instance of uniqe keys tracker
        :type uniqe_keys_tracker: splitio.engine.uniqur_key_tracker.UniqueKeysTracker
        """
        self._unique_keys_tracker = unique_keys_tracker

    def clear_all(self):
        """
        Clear the bloom filter cache

        """
        self._unique_keys_tracker.clear_filter()
