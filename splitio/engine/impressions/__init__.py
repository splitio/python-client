from splitio.engine.impressions.impressions import ImpressionsMode
from splitio.engine.impressions.strategies import StrategyNoneMode, StrategyDebugMode, StrategyOptimizedMode
from splitio.engine.impressions.adapters import InMemorySenderAdapter, RedisSenderAdapter, PluggableSenderAdapter, RedisSenderAdapterAsync, \
    InMemorySenderAdapterAsync, PluggableSenderAdapterAsync
from splitio.tasks.unique_keys_sync import UniqueKeysSyncTask, ClearFilterSyncTask, UniqueKeysSyncTaskAsync, ClearFilterSyncTaskAsync
from splitio.sync.unique_keys import UniqueKeysSynchronizer, ClearFilterSynchronizer, UniqueKeysSynchronizerAsync, ClearFilterSynchronizerAsync
from splitio.sync.impression import ImpressionsCountSynchronizer, ImpressionsCountSynchronizerAsync
from splitio.tasks.impressions_sync import ImpressionsCountSyncTask, ImpressionsCountSyncTaskAsync

def set_classes(storage_mode, impressions_mode, api_adapter, imp_counter, unique_keys_tracker, prefix=None, parallel_tasks_mode='threading'):
    unique_keys_synchronizer = None
    clear_filter_sync = None
    unique_keys_task = None
    clear_filter_task = None
    impressions_count_sync = None
    impressions_count_task = None
    sender_adapter = None
    if storage_mode == 'PLUGGABLE':
        if parallel_tasks_mode == 'asyncio':
            sender_adapter = PluggableSenderAdapterAsync(api_adapter, prefix)
        else:
            sender_adapter = PluggableSenderAdapter(api_adapter, prefix)
        api_telemetry_adapter = sender_adapter
        api_impressions_adapter = sender_adapter
    elif storage_mode == 'REDIS':
        if parallel_tasks_mode == 'asyncio':
            sender_adapter = RedisSenderAdapterAsync(api_adapter)
        else:
            sender_adapter = RedisSenderAdapter(api_adapter)
        api_telemetry_adapter = sender_adapter
        api_impressions_adapter = sender_adapter
    else:
        api_telemetry_adapter = api_adapter['telemetry']
        api_impressions_adapter = api_adapter['impressions']
        if parallel_tasks_mode == 'asyncio':
            sender_adapter = InMemorySenderAdapterAsync(api_telemetry_adapter)
        else:
            sender_adapter = InMemorySenderAdapter(api_telemetry_adapter)

    if impressions_mode == ImpressionsMode.NONE:
        imp_strategy = StrategyNoneMode()
        if parallel_tasks_mode == 'asyncio':
            unique_keys_synchronizer = UniqueKeysSynchronizerAsync(sender_adapter, unique_keys_tracker)
            unique_keys_task = UniqueKeysSyncTaskAsync(unique_keys_synchronizer.send_all)
            clear_filter_sync = ClearFilterSynchronizerAsync(unique_keys_tracker)
            impressions_count_sync = ImpressionsCountSynchronizerAsync(api_impressions_adapter, imp_counter)
            impressions_count_task = ImpressionsCountSyncTaskAsync(impressions_count_sync.synchronize_counters)
            clear_filter_task = ClearFilterSyncTaskAsync(clear_filter_sync.clear_all)
        else:
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
        if parallel_tasks_mode == 'asyncio':
            impressions_count_sync = ImpressionsCountSynchronizerAsync(api_impressions_adapter, imp_counter)
            impressions_count_task = ImpressionsCountSyncTaskAsync(impressions_count_sync.synchronize_counters)
        else:
            impressions_count_sync = ImpressionsCountSynchronizer(api_impressions_adapter, imp_counter)
            impressions_count_task = ImpressionsCountSyncTask(impressions_count_sync.synchronize_counters)

    return unique_keys_synchronizer, clear_filter_sync, unique_keys_task, clear_filter_task, \
            impressions_count_sync, impressions_count_task, imp_strategy
