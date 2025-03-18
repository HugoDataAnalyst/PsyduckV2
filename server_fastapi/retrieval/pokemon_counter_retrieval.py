from datetime import datetime, timedelta
from my_redis.connect_redis import RedisManager
from utils.logger import logger
from server_fastapi.utils.filtering_keys import aggregate_keys, filter_keys_by_time

redis_manager = RedisManager()

# --- Retrieval functions for totals ---

async def retrieve_totals_hourly(area: str, start: datetime, end: datetime, mode: str = "sum") -> dict:
    """
    Retrieve hourly totals for Pokémon counters.
    If area is "global", aggregate data across all areas.
    Key format: "counter:pokemon_hourly:{area}:{YYYYMMDDHH}"
    Uses the "retrieval_pool" for Redis operations.
    """
    client = await redis_manager.check_redis_connection("retrieval_pool")
    if not client:
        logger.error("❌ Retrieval pool connection not available")
        return {"mode": mode, "data": {}}

    time_format = "%Y%m%d%H"
    if area.lower() == "global":
        pattern = "counter:pokemon_hourly:*"
    else:
        pattern = f"counter:pokemon_hourly:{area}:*"
    # Use retrieval_pool  for keys
    keys = await client.keys(pattern)
    keys = filter_keys_by_time(keys, time_format, start, end)
    if not keys:
        return {"mode": mode, "data": {}}
    aggregated = await aggregate_keys(keys, mode)
    return {"mode": mode, "data": aggregated}

async def retrieve_totals_weekly(area: str, start: datetime, end: datetime, mode: str = "sum") -> dict:
    """
    Retrieve weekly totals for Pokémon counters.
    Key format: "counter:pokemon_total:{area}:{YYYYMMDD}"
    Uses the "retrieval_pool" for Redis operations.
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
    keys = filter_keys_by_time(keys, time_format, start, end)
    if not keys:
        return {"mode": mode, "data": {}}
    aggregated = await aggregate_keys(keys, mode)
    return {"mode": mode, "data": aggregated}

# --- Retrieval functions for TTH ---

async def retrieve_tth_hourly(area: str, start: datetime, end: datetime, mode: str = "sum") -> dict:
    """
    Retrieve hourly TTH counters.
    Key format: "counter:tth_pokemon_hourly:{area}:{YYYYMMDDHH}"
    For mode "grouped", data is grouped by hour and the average per field is computed.
    Always returns a complete timeline (each hour in the requested range).
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
    keys = filter_keys_by_time(keys, time_format, start, end)
    if not keys:
        # Even if no keys, return the timeline with no data.
        timeline = {}
        current = start.replace(minute=0, second=0, microsecond=0)
        while current <= end:
            timeline[current.strftime(time_format)] = {}
            current += timedelta(hours=1)
        return {"mode": mode, "data": timeline}

    if mode == "sum":
        aggregated = await aggregate_keys(keys, mode)
        return {"mode": "sum", "data": aggregated}
    elif mode == "grouped":
        # Group by hour, compute average per field, and ensure every hour in the time window is present.
        hourly_data = {}
        for key in keys:
            hour_str = key.split(":")[-1]
            data = await client.hgetall(key)
            data = {k: int(v) for k, v in data.items()}
            if hour_str not in hourly_data:
                hourly_data[hour_str] = {"count": 0, "fields": {}}
            hourly_data[hour_str]["count"] += 1
            for field, value in data.items():
                hourly_data[hour_str]["fields"][field] = hourly_data[hour_str]["fields"].get(field, 0) + value

        # Build a complete timeline for each hour in the range.
        timeline = {}
        current = start.replace(minute=0, second=0, microsecond=0)
        while current <= end:
            hour_str = current.strftime(time_format)
            timeline[hour_str] = hourly_data.get(hour_str, {"count": 0, "fields": {}})
            current += timedelta(hours=1)

        # Compute averages: if no data (count==0), return empty dict.
        averaged = {}
        for hour_str, info in timeline.items():
            cnt = info["count"] if info["count"] > 0 else 1  # avoid division by zero
            averaged[hour_str] = {field: (val / cnt) for field, val in info["fields"].items()} if info["fields"] else {}
        return {"mode": "grouped", "data": averaged}

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
    keys = filter_keys_by_time(keys, time_format, start, end)
    if not keys:
        timeline = {}
        current = start.date()
        while current <= end.date():
            timeline[current.strftime(time_format)] = {}
            current += timedelta(days=1)
        return {"mode": mode, "data": timeline}

    if mode == "sum":
        aggregated = await aggregate_keys(keys, mode)
        return {"mode": "sum", "data": aggregated}
    elif mode == "grouped":
        daily_data = {}
        for key in keys:
            day_str = key.split(":")[-1]
            data = await client.hgetall(key)
            data = {k: int(v) for k, v in data.items()}
            if day_str not in daily_data:
                daily_data[day_str] = {"count": 0, "fields": {}}
            daily_data[day_str]["count"] += 1
            for field, value in data.items():
                daily_data[day_str]["fields"][field] = daily_data[day_str]["fields"].get(field, 0) + value

        # Build a complete timeline for each day in the range.
        timeline = {}
        current = start.date()
        while current <= end.date():
            day_str = current.strftime(time_format)
            timeline[day_str] = daily_data.get(day_str, {"count": 0, "fields": {}})
            current += timedelta(days=1)

        averaged = {}
        for day_str, info in timeline.items():
            cnt = info["count"] if info["count"] > 0 else 1
            averaged[day_str] = {field: (val / cnt) for field, val in info["fields"].items()} if info["fields"] else {}
        return {"mode": "grouped", "data": averaged}

# --- Retrieval function for Weather (monthly) ---

async def retrieve_weather_monthly(area: str, start: datetime, end: datetime, mode: str = "sum") -> dict:
    """
    Retrieve monthly weather counters.
    Key format: "counter:pokemon_weather_iv:{area}:{YYYYMM}:{weather_boost}"

    In "sum" mode, for each weather boost flag (0 or 1) the function sums all IV bucket counts.
    In "grouped" mode, it groups keys by month and weather boost and computes the average for each IV bucket.

    Returns a dictionary with:
      {
        "mode": mode,
        "data": { ... }  # aggregated data
      }
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
    keys = filter_keys_by_time(keys, time_format, start, end)
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
            # Convert values to int
            data = {k: int(v) for k, v in data.items()}
            if weather_boost not in aggregated:
                aggregated[weather_boost] = {}
            for field, value in data.items():
                aggregated[weather_boost][field] = aggregated[weather_boost].get(field, 0) + value
        return {"mode": "sum", "data": aggregated}

    elif mode == "grouped":
        # Group by composite key: "YYYYMM:weather_boost" and sum the fields.
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
