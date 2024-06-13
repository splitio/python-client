from splitio.engine.impressions.impressions import ImpressionsMode
from splitio.engine.impressions.strategies import StrategyNoneMode, StrategyDebugMode, StrategyOptimizedMode
from splitio.engine.impressions.adapters import InMemorySenderAdapter, RedisSenderAdapter, PluggableSenderAdapter, RedisSenderAdapterAsync, \
    InMemorySenderAdapterAsync, PluggableSenderAdapterAsync
from splitio.tasks.unique_keys_sync import UniqueKeysSyncTask, ClearFilterSyncTask, UniqueKeysSyncTaskAsync, ClearFilterSyncTaskAsync
from splitio.sync.unique_keys import UniqueKeysSynchronizer, ClearFilterSynchronizer, UniqueKeysSynchronizerAsync, ClearFilterSynchronizerAsync
from splitio.sync.impression import ImpressionsCountSynchronizer, ImpressionsCountSynchronizerAsync
from splitio.tasks.impressions_sync import ImpressionsCountSyncTask, ImpressionsCountSyncTaskAsync

def set_classes(storage_mode, impressions_mode, api_adapter, imp_counter, unique_keys_tracker, prefix=None):
    """
    Createe and return instances based on storage, impressions and threading mode

    :param storage_mode: storage mode (MEMORY, REDIS or PLUGGABLE)
    :type storage_mode: str
    :param impressions_mode: impressions mode used
    :type impressions_mode: splitio.engine.impressions.impressions.ImpressionsMode
    :param api_adapter: api adapter instance(s)
    :type impressions_mode: dict or splitio.storage.adapters.redis.RedisAdapter/splitio.storage.adapters.redis.RedisAdapterAsync
    :param imp_counter: Impressions Counter instance
    :type imp_counter: splitio.engine.impressions.Counter/splitio.engine.impressions.Counter
    :param unique_keys_tracker: Unique Keys Tracker instance
    :type unique_keys_tracker: splitio.engine.unique_keys_tracker.UniqueKeysTracker/splitio.engine.unique_keys_tracker.UniqueKeysTrackerAsync
    :param prefix: Prefix used for redis or pluggable adapters
    :type prefix: str

    :return: tuple of classes instances.
    :rtype: (splitio.sync.unique_keys.UniqueKeysSynchronizer,
            splitio.sync.unique_keys.ClearFilterSynchronizer,
            splitio.tasks.unique_keys_sync.UniqueKeysTask,
            splitio.tasks.unique_keys_sync.ClearFilterTask,
            splitio.sync.impressions_sync.ImpressionsCountSynchronizer,
            splitio.tasks.impressions_sync.ImpressionsCountSyncTask,
            splitio.engine.impressions.strategies.StrategyNoneMode/splitio.engine.impressions.strategies.StrategyDebugMode/splitio.engine.impressions.strategies.StrategyOptimizedMode)
    """
    unique_keys_synchronizer = None
    clear_filter_sync = None
    unique_keys_task = None
    clear_filter_task = None
    impressions_count_sync = None
    impressions_count_task = None
    sender_adapter = None
    if storage_mode == 'PLUGGABLE':
        sender_adapter = PluggableSenderAdapter(api_adapter, prefix)
        api_telemetry_adapter = sender_adapter
        api_impressions_adapter = sender_adapter
    elif storage_mode == 'REDIS':
        sender_adapter = RedisSenderAdapter(api_adapter)
        api_telemetry_adapter = sender_adapter
        api_impressions_adapter = sender_adapter
    else:
        api_telemetry_adapter = api_adapter['telemetry']
        api_impressions_adapter = api_adapter['impressions']
        sender_adapter = InMemorySenderAdapter(api_telemetry_adapter)

    if impressions_mode == ImpressionsMode.NONE:
        imp_strategy = StrategyNoneMode()
        unique_keys_synchronizer = UniqueKeysSynchronizer(sender_adapter, unique_keys_tracker)
        unique_keys_task = UniqueKeysSyncTask(unique_keys_synchronizer.send_all)
        clear_filter_sync = ClearFilterSynchronizer(unique_keys_tracker)
        impressions_count_sync = ImpressionsCountSynchronizer(api_impressions_adapter, imp_counter)
        impressions_count_task = ImpressionsCountSyncTask(impressions_count_sync.synchronize_counters)
        clear_filter_task = ClearFilterSyncTask(clear_filter_sync.clear_all)
        unique_keys_tracker.set_queue_full_hook(unique_keys_task.flush)
    elif impressions_mode == ImpressionsMode.DEBUG:
        imp_strategy = StrategyDebugMode()
    else:
        imp_strategy = StrategyOptimizedMode()
        impressions_count_sync = ImpressionsCountSynchronizer(api_impressions_adapter, imp_counter)
        impressions_count_task = ImpressionsCountSyncTask(impressions_count_sync.synchronize_counters)

    return unique_keys_synchronizer, clear_filter_sync, unique_keys_task, clear_filter_task, \
            impressions_count_sync, impressions_count_task, imp_strategy

def set_classes_async(storage_mode, impressions_mode, api_adapter, imp_counter, unique_keys_tracker, prefix=None):
    """
    Createe and return instances based on storage, impressions and async mode

    :param storage_mode: storage mode (MEMORY, REDIS or PLUGGABLE)
    :type storage_mode: str
    :param impressions_mode: impressions mode used
    :type impressions_mode: splitio.engine.impressions.impressions.ImpressionsMode
    :param api_adapter: api adapter instance(s)
    :type impressions_mode: dict or splitio.storage.adapters.redis.RedisAdapter/splitio.storage.adapters.redis.RedisAdapterAsync
    :param imp_counter: Impressions Counter instance
    :type imp_counter: splitio.engine.impressions.Counter/splitio.engine.impressions.Counter
    :param unique_keys_tracker: Unique Keys Tracker instance
    :type unique_keys_tracker: splitio.engine.unique_keys_tracker.UniqueKeysTracker/splitio.engine.unique_keys_tracker.UniqueKeysTrackerAsync
    :param prefix: Prefix used for redis or pluggable adapters
    :type prefix: str

    :return: tuple of classes instances.
    :rtype: (splitio.sync.unique_keys.UniqueKeysSynchronizerAsync,
            splitio.sync.unique_keys.ClearFilterSynchronizerAsync,
            splitio.tasks.unique_keys_sync.UniqueKeysTaskAsync,
            splitio.tasks.unique_keys_sync.ClearFilterTaskAsync,
            splitio.sync.impressions_sync.ImpressionsCountSynchronizerAsync,
            splitio.tasks.impressions_sync.ImpressionsCountSyncTaskAsync,
            splitio.engine.impressions.strategies.StrategyNoneMode/splitio.engine.impressions.strategies.StrategyDebugMode/splitio.engine.impressions.strategies.StrategyOptimizedMode)
    """
    unique_keys_synchronizer = None
    clear_filter_sync = None
    unique_keys_task = None
    clear_filter_task = None
    impressions_count_sync = None
    impressions_count_task = None
    sender_adapter = None
    if storage_mode == 'PLUGGABLE':
        sender_adapter = PluggableSenderAdapterAsync(api_adapter, prefix)
        api_telemetry_adapter = sender_adapter
        api_impressions_adapter = sender_adapter
    elif storage_mode == 'REDIS':
        sender_adapter = RedisSenderAdapterAsync(api_adapter)
        api_telemetry_adapter = sender_adapter
        api_impressions_adapter = sender_adapter
    else:
        api_telemetry_adapter = api_adapter['telemetry']
        api_impressions_adapter = api_adapter['impressions']
        sender_adapter = InMemorySenderAdapterAsync(api_telemetry_adapter)

    if impressions_mode == ImpressionsMode.NONE:
        imp_strategy = StrategyNoneMode()
        unique_keys_synchronizer = UniqueKeysSynchronizerAsync(sender_adapter, unique_keys_tracker)
        unique_keys_task = UniqueKeysSyncTaskAsync(unique_keys_synchronizer.send_all)
        clear_filter_sync = ClearFilterSynchronizerAsync(unique_keys_tracker)
        impressions_count_sync = ImpressionsCountSynchronizerAsync(api_impressions_adapter, imp_counter)
        impressions_count_task = ImpressionsCountSyncTaskAsync(impressions_count_sync.synchronize_counters)
        clear_filter_task = ClearFilterSyncTaskAsync(clear_filter_sync.clear_all)
        unique_keys_tracker.set_queue_full_hook(unique_keys_task.flush)
    elif impressions_mode == ImpressionsMode.DEBUG:
        imp_strategy = StrategyDebugMode()
    else:
        imp_strategy = StrategyOptimizedMode()
        impressions_count_sync = ImpressionsCountSynchronizerAsync(api_impressions_adapter, imp_counter)
        impressions_count_task = ImpressionsCountSyncTaskAsync(impressions_count_sync.synchronize_counters)

    return unique_keys_synchronizer, clear_filter_sync, unique_keys_task, clear_filter_task, \
            impressions_count_sync, impressions_count_task, imp_strategy
