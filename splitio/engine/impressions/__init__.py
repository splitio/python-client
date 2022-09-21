from splitio.engine.impressions.impressions import ImpressionsMode
from splitio.engine.impressions.manager import Counter as ImpressionsCounter
from splitio.engine.impressions.strategies import StrategyNoneMode, StrategyDebugMode, StrategyOptimizedMode
from splitio.engine.impressions.adapters import InMemorySenderAdapter, RedisSenderAdapter
from splitio.tasks.unique_keys_sync import UniqueKeysSyncTask, ClearFilterSyncTask
from splitio.sync.unique_keys import UniqueKeysSynchronizer, ClearFilterSynchronizer
from splitio.sync.impression import ImpressionsCountSynchronizer
from splitio.tasks.impressions_sync import ImpressionsCountSyncTask


def set_classes(storage_mode, impressions_mode, api_adapter):
    unique_keys_synchronizer = None
    clear_filter_sync = None
    unique_keys_task = None
    clear_filter_task = None
    impressions_count_sync = None
    impressions_count_task = None
    if storage_mode == 'REDIS':
        redis_sender_adapter = RedisSenderAdapter(api_adapter)
        api_telemetry_adapter = redis_sender_adapter
        api_impressions_adapter = redis_sender_adapter
    else:
        api_telemetry_adapter = api_adapter['telemetry']
        api_impressions_adapter = api_adapter['impressions']

    if impressions_mode == ImpressionsMode.NONE:
        imp_counter = ImpressionsCounter()
        imp_strategy = StrategyNoneMode(imp_counter)
        clear_filter_sync = ClearFilterSynchronizer(imp_strategy.get_unique_keys_tracker())
        unique_keys_synchronizer = UniqueKeysSynchronizer(InMemorySenderAdapter(api_telemetry_adapter), imp_strategy.get_unique_keys_tracker())
        unique_keys_task = UniqueKeysSyncTask(unique_keys_synchronizer.send_all)
        clear_filter_task = ClearFilterSyncTask(clear_filter_sync.clear_all)
        imp_strategy.get_unique_keys_tracker().set_queue_full_hook(unique_keys_task.flush)
        impressions_count_sync = ImpressionsCountSynchronizer(api_impressions_adapter, imp_counter)
        impressions_count_task = ImpressionsCountSyncTask(impressions_count_sync.synchronize_counters)
    elif impressions_mode == ImpressionsMode.DEBUG:
        imp_strategy = StrategyDebugMode()
    else:
        imp_counter = ImpressionsCounter()
        imp_strategy = StrategyOptimizedMode(imp_counter)
        impressions_count_sync = ImpressionsCountSynchronizer(api_impressions_adapter, imp_counter)
        impressions_count_task = ImpressionsCountSyncTask(impressions_count_sync.synchronize_counters)

    return unique_keys_synchronizer, clear_filter_sync, unique_keys_task, clear_filter_task, \
            impressions_count_sync, impressions_count_task, imp_strategy
