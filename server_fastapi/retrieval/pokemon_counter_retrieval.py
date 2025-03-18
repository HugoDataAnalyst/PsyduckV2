from datetime import datetime, timedelta
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from server_fastapi.utils import filtering_keys

redis_manager = RedisManager()

# --- Retrieval functions for totals ---

async def retrieve_totals_hourly(area: str, start: datetime, end: datetime, mode: str = "sum") -> dict:
    time_format = "%Y%m%d%H"
    client = await redis_manager.check_redis_connection("retrieval_pool")
    if not client:
        logger.error("❌ Retrieval pool connection not available")
        return {"mode": mode, "data": {}}
    if area.lower() == "global":
        pattern = "counter:pokemon_hourly:*"
    else:
        pattern = f"counter:pokemon_hourly:{area}:*"
    keys = await client.keys(pattern)
    keys = filtering_keys.filter_keys_by_time(keys, time_format, start, end)
    if not keys:
        return {"mode": mode, "data": {}}
    raw_aggregated = await filtering_keys.aggregate_keys(keys, mode)
    if mode == "sum":
        final_data = filtering_keys.transform_aggregated_totals(raw_aggregated, mode)
    elif mode == "grouped":
        final_data = filtering_keys.transform_grouped_totals_hourly(raw_aggregated)
    return {"mode": mode, "data": final_data}

async def retrieve_totals_weekly(area: str, start: datetime, end: datetime, mode: str = "sum") -> dict:
    """
    Retrieve weekly totals for Pokémon counters.
    Key format: "counter:pokemon_total:{area}:{YYYYMMDD}"
    Uses the "retrieval_pool" for Redis operations.

    In SUM mode, the function aggregates all hash fields from all matching keys and then
    groups them by metric (the third component, e.g. "total", "iv100", etc.).

    In GROUPED mode, the function combines data from all keys into a single dictionary
    keyed by the full field (e.g., "1:163:total") summing counts across keys, then sorts
    the final result by pokemon_id (the first component).
    """
    client = await redis_manager.check_redis_connection("retrieval_pool")
    if not client:
        logger.error("❌ Retrieval pool connection not available")
        return {"mode": mode, "data": {}}

    time_format = "%Y%m%d"
    if area.lower() == "global":
        pattern = "counter:pokemon_total:*"
    else:
        pattern = f"counter:pokemon_total:{area}:*"
    keys = await client.keys(pattern)
    keys = filtering_keys.filter_keys_by_time(keys, time_format, start, end)
    if not keys:
        return {"mode": mode, "data": {}}

    raw_aggregated = await filtering_keys.aggregate_keys(keys, mode)
    final_data = filtering_keys.transform_aggregated_totals(raw_aggregated, mode)
    return {"mode": mode, "data": final_data}

# --- Retrieval functions for TTH ---

async def retrieve_tth_hourly(area: str, start: datetime, end: datetime, mode: str = "sum") -> dict:
    """
    Retrieve hourly TTH counters.
    Key format: "counter:tth_pokemon_hourly:{area}:{YYYYMMDDHH}"

    In "sum" mode, values are summed across matching keys.
    In "grouped" mode, only hours that have data are combined into a single dictionary,
    then re-labeled sequentially as "hour 1", "hour 2", etc., with inner dictionaries ordered by TTH bucket.
    """
    time_format = "%Y%m%d%H"
    client = await redis_manager.check_redis_connection("retrieval_pool")
    if not client:
        logger.error("❌ Retrieval pool connection not available")
        return {"mode": mode, "data": {}}

    if area.lower() == "global":
        pattern = "counter:tth_pokemon_hourly:*"
    else:
        pattern = f"counter:tth_pokemon_hourly:{area}:*"

    keys = await client.keys(pattern)
    keys = filtering_keys.filter_keys_by_time(keys, time_format, start, end)
    if not keys:
        return {"mode": mode, "data": {}}

    raw_aggregated = await filtering_keys.aggregate_keys(keys, mode)
    if mode == "sum":
        final_data = filtering_keys.transform_aggregated_tth(raw_aggregated, mode)
    elif mode == "grouped":
        final_data = filtering_keys.transform_grouped_tth_hourly_by_hour(raw_aggregated)
    else:
        final_data = raw_aggregated
    return {"mode": mode, "data": final_data}


async def retrieve_tth_weekly(area: str, start: datetime, end: datetime, mode: str = "sum") -> dict:
    """
    Retrieve weekly TTH counters.
    Key format: "counter:tth_pokemon:{area}:{YYYYMMDD}"
    For mode "grouped", data is grouped by day and the average per field is computed.
    Always returns a complete timeline (each day in the requested range).
    """
    time_format = "%Y%m%d"
    client = await redis_manager.check_redis_connection("retrieval_pool")
    if not client:
        logger.error("❌ Retrieval pool connection not available")
        return {"mode": mode, "data": {}}

    if area.lower() == "global":
        pattern = "counter:tth_pokemon:*"
    else:
        pattern = f"counter:tth_pokemon:{area}:*"

    keys = await client.keys(pattern)
    keys = filtering_keys.filter_keys_by_time(keys, time_format, start, end)
    if not keys:
        return {"mode": mode, "data": {}}

    raw_aggregated = await filtering_keys.aggregate_keys(keys, mode)
    final_data = filtering_keys.transform_aggregated_tth(raw_aggregated, mode, start, end)
    return {"mode": mode, "data": final_data}

# --- Retrieval function for Weather (monthly) ---

async def retrieve_weather_monthly(area: str, start: datetime, end: datetime, mode: str = "sum") -> dict:
    """
    Retrieve monthly weather counters.
    Key format: "counter:pokemon_weather_iv:{area}:{YYYYMM}:{weather_boost}"

    In "sum" mode, for each weather boost flag (0 or 1) the function sums all IV bucket counts.
    In "grouped" mode, it groups keys by month and weather boost and sums the fields.

    Returns a dictionary with aggregated data.
    """
    time_format = "%Y%m"
    client = await redis_manager.check_redis_connection("retrieval_pool")
    if not client:
        logger.error("❌ Retrieval pool connection not available")
        return {"mode": mode, "data": {}}

    if area.lower() == "global":
        pattern = "counter:pokemon_weather_iv:*"
    else:
        pattern = f"counter:pokemon_weather_iv:{area}:*"

    keys = await client.keys(pattern)
    # For weather keys, use component_index=-2 to extract the YYYYMM part.
    keys = filtering_keys.filter_keys_by_time(keys, time_format, start, end, component_index=-2)
    if not keys:
        return {"mode": mode, "data": {}}

    if mode == "sum":
        aggregated = {}
        for key in keys:
            # Expected key format: "counter:pokemon_weather_iv:{area}:{YYYYMM}:{weather_boost}"
            parts = key.split(":")
            if len(parts) < 5:
                continue
            weather_boost = parts[-1]
            data = await client.hgetall(key)
            data = {k: int(v) for k, v in data.items()}
            if weather_boost not in aggregated:
                aggregated[weather_boost] = {}
            for field, value in data.items():
                aggregated[weather_boost][field] = aggregated[weather_boost].get(field, 0) + value
        return {"mode": "sum", "data": aggregated}

    elif mode == "grouped":
        grouped = {}
        for key in keys:
            parts = key.split(":")
            if len(parts) < 5:
                continue
            month = parts[-2]  # the YYYYMM part
            weather_boost = parts[-1]
            composite_key = f"{month}:{weather_boost}"
            data = await client.hgetall(key)
            data = {k: int(v) for k, v in data.items()}
            if composite_key not in grouped:
                grouped[composite_key] = {}
            for field, value in data.items():
                grouped[composite_key][field] = grouped[composite_key].get(field, 0) + value
        return {"mode": "grouped", "data": grouped}
