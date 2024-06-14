_UNIQUE_KEYS_MAX_BULK_SIZE = 5000

class UniqueKeysSynchronizerBase(object):
    """Unique Keys Synchronizer base class."""

    def __init__(self):
        """
        Initialize Unique keys synchronizer instance

        :param uniqe_keys_tracker: instance of uniqe keys tracker
        :type uniqe_keys_tracker: splitio.engine.uniqur_key_tracker.UniqueKeysTracker
        """
        self._max_bulk_size = _UNIQUE_KEYS_MAX_BULK_SIZE

    def _split_cache_to_bulks(self, cache):
        """
        Split the current unique keys dictionary into seperate dictionaries,
        each with the size of max_bulk_size. Overflow the last feature_flag set() to new unique keys dictionary.

        :return: array of unique keys dictionaries
        :rtype: [Dict{'feature_flag1': set(), 'feature_flag2': set(), .. }]
        """
        bulks = []
        bulk = {}
        total_size = 0
        for feature_flag in cache:
            total_size += len(cache[feature_flag])
            if total_size > self._max_bulk_size:
                keys_list = list(cache[feature_flag])
                chunk_list = self._chunks(keys_list)
                if bulk != {}:
                    bulks.append(bulk)
                for bulk_keys in chunk_list:
                    bulk[feature_flag] = set(bulk_keys)
                    bulks.append(bulk)
                    bulk = {}
            else:
                bulk[feature_flag] = cache[feature_flag]
        if total_size != 0 and bulk != {}:
            bulks.append(bulk)

        return bulks

    def _chunks(self, keys_list):
        """
        Split array into chunks
        """
        for i in range(0, len(keys_list), self._max_bulk_size):
            yield keys_list[i:i + self._max_bulk_size]


class UniqueKeysSynchronizer(UniqueKeysSynchronizerBase):
    """Unique Keys Synchronizer class."""

    def __init__(self, impressions_sender_adapter, uniqe_keys_tracker):
        """
        Initialize Unique keys synchronizer instance

        :param uniqe_keys_tracker: instance of uniqe keys tracker
        :type uniqe_keys_tracker: splitio.engine.uniqur_key_tracker.UniqueKeysTracker
        """
        UniqueKeysSynchronizerBase.__init__(self)
        self._uniqe_keys_tracker = uniqe_keys_tracker
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


class UniqueKeysSynchronizerAsync(UniqueKeysSynchronizerBase):
    """Unique Keys Synchronizer async class."""

    def __init__(self, impressions_sender_adapter, uniqe_keys_tracker):
        """
        Initialize Unique keys synchronizer instance

        :param uniqe_keys_tracker: instance of uniqe keys tracker
        :type uniqe_keys_tracker: splitio.engine.uniqur_key_tracker.UniqueKeysTracker
        """
        UniqueKeysSynchronizerBase.__init__(self)
        self._uniqe_keys_tracker = uniqe_keys_tracker
        self._impressions_sender_adapter = impressions_sender_adapter

    async def send_all(self):
        """
        Flush the unique keys dictionary to split back end.
        Limit each post to the max_bulk_size value.

        """
        cache, cache_size = await self._uniqe_keys_tracker.get_cache_info_and_pop_all()
        if cache_size <= self._max_bulk_size:
            await self._impressions_sender_adapter.record_unique_keys(cache)
        else:
            for bulk in self._split_cache_to_bulks(cache):
                await self._impressions_sender_adapter.record_unique_keys(bulk)


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

class ClearFilterSynchronizerAsync(object):
    """Clear filter async class."""

    def __init__(self, unique_keys_tracker):
        """
        Initialize Unique keys synchronizer instance

        :param uniqe_keys_tracker: instance of uniqe keys tracker
        :type uniqe_keys_tracker: splitio.engine.uniqur_key_tracker.UniqueKeysTracker
        """
        self._unique_keys_tracker = unique_keys_tracker

    async def clear_all(self):
        """
        Clear the bloom filter cache

        """
        await self._unique_keys_tracker.clear_filter()
