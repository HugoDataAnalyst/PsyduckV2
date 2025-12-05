import asyncio
from datetime import datetime, timedelta, timezone
from dateutil.relativedelta import relativedelta
from typing import Optional
from my_redis.connect_redis import RedisManager
from utils.logger import logger
import re
import pytz
from server_fastapi import global_state

redis_manager = RedisManager()

# Configuration for concurrent fetching
CONCURRENT_BATCH_SIZE = 500  # Number of concurrent hgetall calls per batch
USE_PIPELINE = True  # Use Redis pipeline for even faster fetching


async def aggregate_keys(keys: list, mode: str) -> dict:
    """
    Aggregates hash data from a list of keys CONCURRENTLY.
    For "sum" mode, sums all field values (assuming integer values).
    For "grouped" mode, returns a dictionary mapping each key to its hash data.

    Uses asyncio.gather for concurrent fetching instead of sequential awaits.
    """
    client = await redis_manager.check_redis_connection()
    if not client:
        logger.error("❌ Retrieval pool connection not available")
        return {"mode": mode, "data": {}}

    if not keys:
        return {}

    # Fetch all keys concurrently using pipeline or gather
    if USE_PIPELINE:
        all_data = await _fetch_keys_pipeline(client, keys)
    else:
        all_data = await _fetch_keys_concurrent(client, keys)

    # Process the fetched data
    aggregated = {}

    if mode == "sum":
        for key, data in all_data.items():
            for field, value in data.items():
                try:
                    value = int(value)
                except Exception:
                    value = 0
                aggregated[field] = aggregated.get(field, 0) + value
    elif mode in ["grouped", "surged"]:
        for key, data in all_data.items():
            aggregated[key] = {k: int(v) for k, v in data.items()}

    logger.debug(f"✅ Mode:{mode} Aggregation complete. Keys processed: {len(keys)}")
    return aggregated


async def _fetch_keys_pipeline(client, keys: list) -> dict:
    """
    Fetch multiple keys using Redis pipeline for maximum efficiency.
    Pipeline batches all commands and sends them in one network round-trip.
    """
    if not keys:
        return {}

    result = {}

    # Process in batches to avoid memory issues with huge key lists
    for i in range(0, len(keys), CONCURRENT_BATCH_SIZE):
        batch_keys = keys[i:i + CONCURRENT_BATCH_SIZE]

        # Create pipeline
        pipe = client.pipeline(transaction=False)  # No transaction for better performance

        for key in batch_keys:
            pipe.hgetall(key)

        # Execute pipeline - single network round trip for all commands
        responses = await pipe.execute()

        # Map responses back to keys
        for key, data in zip(batch_keys, responses):
            if data:  # Skip empty hashes
                result[key] = data

    logger.debug(f"✅ Pipeline fetch complete. Fetched {len(result)} keys with data")
    return result


async def _fetch_keys_concurrent(client, keys: list) -> dict:
    """
    Fetch multiple keys concurrently using asyncio.gather.
    Falls back option if pipeline isn't available.
    """
    if not keys:
        return {}

    result = {}

    async def fetch_single(key):
        """Fetch a single key and return (key, data) tuple"""
        try:
            data = await client.hgetall(key)
            return (key, data)
        except Exception as e:
            logger.warning(f"⚠️ Error fetching key {key}: {e}")
            return (key, {})

    # Process in batches to avoid overwhelming Redis
    for i in range(0, len(keys), CONCURRENT_BATCH_SIZE):
        batch_keys = keys[i:i + CONCURRENT_BATCH_SIZE]

        # Fetch batch concurrently
        tasks = [fetch_single(key) for key in batch_keys]
        batch_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Process results
        for item in batch_results:
            if isinstance(item, Exception):
                logger.warning(f"⚠️ Batch fetch exception: {item}")
                continue
            key, data = item
            if data:  # Skip empty hashes
                result[key] = data

    logger.debug(f"✅ Concurrent fetch complete. Fetched {len(result)} keys with data")
    return result


async def scan_keys(client, pattern: str, count: int = 1000) -> list:
    """
    Use SCAN instead of KEYS for non-blocking key retrieval.
    SCAN is cursor-based and doesn't block Redis.

    Args:
        client: Redis client
        pattern: Key pattern to match
        count: Hint for how many keys to return per iteration

    Returns:
        List of matching keys
    """
    keys = []
    cursor = 0

    while True:
        cursor, batch = await client.scan(cursor=cursor, match=pattern, count=count)
        keys.extend(batch)
        if cursor == 0:
            break

    logger.debug(f"✅ SCAN complete. Found {len(keys)} keys matching pattern: {pattern}")
    return keys

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
    logger.debug(f"✅ Filtered down to {len(filtered)} keys")
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
    pattern = re.compile(r"(\d+)\s*(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months|year|years)")
    match = pattern.fullmatch(time_str)
    if match:
        value = int(match.group(1))
        unit = match.group(2).rstrip('s')  # Remove plural

        # Calculate reference time (current local time in area)
        local_now = now_utc + timedelta(hours=area_offset)

        # Calculate the past time in local timezone
        if unit == "second":
            past_local = local_now - timedelta(seconds=value)
        elif unit == "minute":
            past_local = local_now - timedelta(minutes=value)
        elif unit == "hour":
            past_local = local_now - timedelta(hours=value)
        elif unit == "day":
            past_local = local_now - timedelta(days=value)
        elif unit == "week":
            past_local = local_now - timedelta(weeks=value)
        elif unit == "month":
            past_local = local_now - relativedelta(months=value)
        elif unit == "year":
            past_local = local_now - relativedelta(years=value)

        # Convert to local_area_utc
        return past_local

    raise ValueError(f"Unrecognized time format: {time_str}")

def get_area_offset(area: str, geofences: list) -> int:
    """
    Get timezone offset for a given area.
    Defaults to 0 (UTC) if area not found or is global.
    """
    if area.lower() in ["global", "all"]:
        return {geofence["name"]: geofence.get("offset", 0) for geofence in geofences}

    for geofence in geofences:
        if geofence["name"].lower() == area.lower():
            return geofence.get("offset", 0)

    return 0

def get_area_offsets_for_list(areas: list[str], geofences: list) -> dict[str, int]:
    """
    Map a list of area names to offsets using geofences.
    Case-insensitive match; returns dict[name] = offset.
    """
    index = {g["name"].lower(): g for g in geofences}
    out = {}
    for raw in areas:
        name = raw.strip()
        if not name:
            continue
        g = index.get(name.lower())
        if g:
            out[g["name"]] = g.get("offset", 0)
    return out
