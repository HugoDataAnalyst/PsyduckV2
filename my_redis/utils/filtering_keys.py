from datetime import datetime, timedelta, timezone
from typing import Optional
from my_redis.connect_redis import RedisManager
from utils.logger import logger
import re
import pytz
from server_fastapi import global_state

redis_manager = RedisManager()

async def aggregate_keys(keys: list, mode: str) -> dict:
    """
    Aggregates hash data from a list of keys.
    For "sum" mode, sums all field values (assuming integer values).
    For "grouped" mode, returns a dictionary mapping each key to its hash data.
    """
    client = await redis_manager.check_redis_connection()
    if not client:
        logger.error("❌ Retrieval pool connection not available")
        return {"mode": mode, "data": {}}

    aggregated = {}
    for key in keys:
        data = await client.hgetall(key)
        logger.debug(f"🔑 Aggregating key: {key} with data: {data}")
        if mode == "sum":
            for field, value in data.items():
                try:
                    value = int(value)
                except Exception:
                    value = 0
                aggregated[field] = aggregated.get(field, 0) + value
        elif mode in ["grouped", "surged"]:
            aggregated[key] = {k: int(v) for k, v in data.items()}
    logger.debug(f"✅ Mode:{mode} Aggregation complete. Aggregated data: {aggregated}")
    return aggregated

def filter_keys_by_time(keys: list, time_format: str, start: datetime, end: datetime, component_index: int = -1) -> list:
    """
    Filters keys by extracting the time component from the specified component index (default is the last component)
    after splitting the key by a colon, parsing it according to time_format, and returning only those keys
    whose datetime falls between start and end.
    """
    filtered = []
    for key in keys:
        parts = key.split(":")
        if len(parts) < abs(component_index):
            continue
        try:
            timestr = parts[component_index]
            dt = datetime.strptime(timestr, time_format)
            logger.debug(f"Key: {key} parsed time: {dt}")
            if start <= dt < end:
                logger.debug(f"Key: {key} is in range.")
                filtered.append(key)
            else:
                logger.debug(f"Key: {key} with time {dt} is out of range.")
        except Exception as e:
            logger.warning(f"❌ Could not parse time from key {key}: {e}")
    logger.info(f"✅ Filtered down to {len(filtered)} keys")
    return filtered

def parse_time_input(time_str: str, area_offset: int = 0) -> datetime:
    """
    Parses time input with precise handling for Redis queries:
    - ISO timestamps: treated as exact local_area_utc (what's stored in Redis)
    - Relative times: calculated in area's local time then converted to local_area_utc
    - "now": current time in area's local time converted to local_area_utc

    Args:
        time_str: Time string to parse
        area_offset: Timezone offset in hours for the area

    Returns:
        datetime object matching Redis' local_area_utc storage
    """
    time_str = time_str.strip().lower()

    # 1. Handle ISO format - treat as exact local_area_utc (already adjusted)
    try:
        dt = datetime.fromisoformat(time_str)
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except ValueError:
        pass

    # Get current UTC time
    now_utc = datetime.utcnow()

    # 2. Handle "now" - current local time in area converted to local_area_utc
    if time_str == "now":
        # Equivalent to: (now_utc + area_offset) - 0
        return now_utc + timedelta(hours=area_offset)

    # 3. Handle relative times
    pattern = re.compile(r"(\d+)\s*(hour|hours|second|seconds|minute|minutes|day|days|week|weeks|month|months|year|years)")
    match = pattern.fullmatch(time_str)
    if match:
        value = int(match.group(1))
        unit = match.group(2).rstrip('s')  # Remove plural

        # Calculate reference time (current local time in area)
        local_now = now_utc + timedelta(hours=area_offset)

        # Calculate the past time in local timezone
        if unit == "hour":
            past_local = local_now - timedelta(hours=value)
        elif unit == "second":
            past_local = local_now - timedelta(seconds=value)
        elif unit == "minute":
            past_local = local_now - timedelta(minutes=value)
        elif unit == "day":
            past_local = local_now - timedelta(days=value)
        elif unit == "week":
            past_local = local_now - timedelta(weeks=value)
        elif unit == "month":
            past_local = local_now - timedelta(months=value)
        elif unit == "year":
            past_local = local_now - timedelta(years=value)

        # Convert to local_area_utc (what Redis stores)
        return past_local

    raise ValueError(f"Unrecognized time format: {time_str}")

def get_area_offset(area: str, geofences: list) -> int:
    """
    Get timezone offset for a given area.
    Defaults to 0 (UTC) if area not found or is global.
    """
    if area.lower() in ["global", "all"]:
        return 0

    for geofence in geofences:
        if geofence["name"].lower() == area.lower():
            return geofence.get("offset", 0)

    return 0
