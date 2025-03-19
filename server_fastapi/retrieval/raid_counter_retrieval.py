from datetime import datetime, timedelta
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from server_fastapi.utils import filtering_keys

redis_manager = RedisManager()

async def retrieve_totals_weekly(area: str, start: datetime, end: datetime, mode: str = "sum") -> dict:
    """
    Retrieve weekly raid totals.

    Key format: "counter:raid_total:{area}:{YYYYMMDD}"
    Uses the "retrieval_pool" for Redis operations.

    In "sum" mode, it aggregates all fields and groups them by the metric (the last component).
    In "grouped" mode, it combines data from all keys into one dictionary keyed by the full field,
    then sorts the result by the numeric value of the first component.
    """
    client = await redis_manager.check_redis_connection("retrieval_pool")
    if not client:
        logger.error("❌ Retrieval pool connection not available")
        return {"mode": mode, "data": {}}

    time_format = "%Y%m%d"

    if area.lower() == "global":
        pattern = "counter:raid_total:*"
    else:
        pattern = f"counter:raid_total:{area}:*"
    keys = await client.keys(pattern)
    keys = filtering_keys.filter_keys_by_time(keys, time_format, start, end)
    if not keys:
        return {"mode": mode, "data": {}}

    raw_aggregated = await filtering_keys.aggregate_keys(keys, mode)
    if mode == ["sum"]:
        logger.info("▶️ Transforming weekly raid_totals sum")
        final_data = filtering_keys.transform_raid_totals_sum(raw_aggregated)
    elif mode == ["grouped"]:
        final_data = filtering_keys.transform_aggregated_totals(raw_aggregated, mode)
    else:
        final_data = raw_aggregated
    return {"mode": mode, "data": final_data}

async def retrieve_totals_hourly(area: str, start: datetime, end: datetime, mode: str = "sum") -> dict:
    """
    Retrieve hourly raid totals.

    Key format: "counter:raid_hourly:{area}:{YYYYMMDDHH}"
    Uses the "retrieval_pool" for Redis operations.

    In "sum" mode, it aggregates all fields and groups them by the metric.
    In "grouped" mode, it groups the data by the actual hour extracted from the key.
    In "surged" mode (if desired), you could implement similar logic to group by the actual hour across days.
    Here, for demonstration, we'll support "sum" and "grouped".
    """
    client = await redis_manager.check_redis_connection("retrieval_pool")
    if not client:
        logger.error("❌ Retrieval pool connection not available")
        return {"mode": mode, "data": {}}

    time_format = "%Y%m%d%H"
    if area.lower() == "global":
        pattern = "counter:raid_hourly:*"
    else:
        pattern = f"counter:raid_hourly:{area}:*"
    keys = await client.keys(pattern)
    keys = filtering_keys.filter_keys_by_time(keys, time_format, start, end)
    if not keys:
        return {"mode": mode, "data": {}}

    raw_aggregated = await filtering_keys.aggregate_keys(keys, mode)
    if mode == ["sum"]:
        logger.info("▶️ Transforming hourly raid_totals sum")
        final_data = filtering_keys.transform_raid_totals_sum(raw_aggregated)
    elif mode == ["grouped"]:
        final_data = filtering_keys.transform_aggregated_totals(raw_aggregated, mode)
    elif mode == "surged":
        # If you want a surged mode for raids as well, you can implement a similar helper.
        final_data = filtering_keys.transform_surged_totals_hourly_by_hour(raw_aggregated)
    else:
        final_data = raw_aggregated
    return {"mode": mode, "data": final_data}
